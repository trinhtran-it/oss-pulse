"""oss_pulse_daily: one UTC day of GH Archive, end to end.

ingest_raw > spark_flatten > load_bq_partition > dbt_source_freshness > dbt_build > record_audit

Each task is safe to re-run for the same day:

- **ingest_raw**: 24 hourly files to gs://<bucket>/raw/gharchive/dt=<ds>/.
  Skips hours already uploaded with the same size.
- **spark_flatten**: Dataproc Serverless, raw JSON to processed/events/dt=<ds>/ (Parquet).
  Overwrites that dt= folder.
- **load_bq_partition**: processed Parquet to raw.events$<ds_nodash>.
  WRITE_TRUNCATE replaces exactly that one partition.
- **dbt_source_freshness**: dbt source freshness, only for recent runs (see the task). Read-only.
- **dbt_build**: dbt build --target prod with run_date=<ds> (rebuilds ds + the lookback day).
  insert_overwrite for facts, merge for dims.
- **record_audit**: appends one row to audit.load_log, success or failed.

Every date comes from the run's logical date ({{ ds }}, {{ ds_nodash }}), never from "now", so
catchup, backfills and cleared runs process exactly the day they are for.

Configuration comes from the environment (repo-root .env, passed by docker-compose.yaml).
Credentials: Application Default Credentials mounted read-only, never in this file.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, CronDataIntervalTimetable, TriggerRule, task
from airflow.sdk.exceptions import AirflowException, AirflowFailException
from oss_pulse_lib.alerts import notify_failure

log = logging.getLogger(__name__)

# --- Configuration (fail at parse time if a required value is missing) -----------------------
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
REGION = os.environ["GCP_REGION"]
BUCKET = os.environ["GCS_BUCKET"]
RAW_DATASET = os.environ.get("BQ_DATASET_RAW", "oss_pulse_raw")
AUDIT_DATASET = os.environ.get("BQ_DATASET_AUDIT", "oss_pulse_audit")

DATAPROC_SA = os.environ.get(
    "DATAPROC_SA", f"oss-pulse-pipeline@{PROJECT_ID}.iam.gserviceaccount.com"
)
DATAPROC_SUBNET = os.environ.get("DATAPROC_SUBNET", "oss-pulse-subnet")
DATAPROC_RUNTIME = os.environ.get("DATAPROC_RUNTIME", "2.2")  # Spark 3.5
# Uploaded by `make spark-upload`: the DAG runs a released copy, never your working tree.
SPARK_MAIN = f"gs://{BUCKET}/code/spark/flatten_events.py"

PROCESSED_PREFIX = "processed/events"

# Days to schedule (YYYY-MM-DD). catchup=True creates one run per day from START_DATE, so a new
# deployment backfills everything from there. Set both to the same day to reproduce one day only.
# END_DATE unset = run forever (normal operation).
START_DATE = os.environ.get("OSS_PULSE_START_DATE") or "2026-10-01"
END_DATE = os.environ.get("OSS_PULSE_END_DATE") or None

# dbt: target/ and logs/ go under Airflow's logs folder, one directory per day and task,
# so runs never share state and every run_results.json is kept for debugging.
DBT_ARTIFACTS = "/opt/airflow/logs/dbt/{{ ds }}/{{ ti.task_id }}"
DBT_FLAGS = (
    '--project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROJECT_DIR" --target prod '
    f'--target-path "{DBT_ARTIFACTS}/target" --log-path "{DBT_ARTIFACTS}/logs"'
)

PIPELINE_TASKS = [
    "ingest_raw",
    "spark_flatten",
    "load_bq_partition",
    "dbt_source_freshness",
    "dbt_build",
]


class PipelineFailed(AirflowFailException):
    """Raised by record_audit after logging a failed run. The failing task already alerted."""

    already_alerted = True


default_args = {
    "owner": "oss-pulse",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    # Airflow 3.4: a float multiplier (delay * 2 ** (try - 1)): 5, 10, 20 min. `True` would be
    # 1.0 = no backoff at all.
    "retry_exponential_backoff": 2.0,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_failure,  # after the LAST retry fails, once per task
}

with DAG(
    dag_id="oss_pulse_daily",
    description="GH Archive -> GCS -> Spark -> BigQuery -> dbt, one UTC day per run",
    # "@daily" as a DATA INTERVAL: the run for ds=2026-10-01 covers [10-01, 10-02) and starts
    # after 10-02 00:00 UTC, once the day is complete. Airflow 3's plain schedule="@daily" is a
    # cron TRIGGER (create_cron_data_intervals=False): it would start the 10-01 run at 10-01
    # 00:00, before any of that day's GH Archive hours exist.
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=pendulum.parse(START_DATE, tz="UTC"),
    end_date=pendulum.parse(END_DATE, tz="UTC") if END_DATE else None,
    catchup=True,
    max_active_runs=2,
    default_args=default_args,
    doc_md=__doc__,
    tags=["oss-pulse"],
) as dag:
    # Exit codes (ingestion/gharchive.py): 1 = some hours failed, 3 = hours not published yet.
    # Both fail the task, so Airflow retries with backoff.
    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command="cd /opt/airflow && python -m ingestion.gharchive --date {{ ds }}",
        execution_timeout=timedelta(minutes=60),
    )

    # Same shape as `make spark-dataproc`. batch_id_prefix -> "flatten-<ds_nodash>-<8 hex>":
    # a new batch id per attempt, so a retry never collides with the failed batch's id.
    spark_flatten = DataprocCreateBatchOperator(
        task_id="spark_flatten",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id_prefix="flatten-{{ ds_nodash }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": SPARK_MAIN,
                "args": [
                    "--date",
                    "{{ ds }}",
                    "--input",
                    f"gs://{BUCKET}/raw/gharchive/dt={{{{ ds }}}}/",
                    "--output",
                    f"gs://{BUCKET}/{PROCESSED_PREFIX}",
                ],
            },
            "runtime_config": {
                "version": DATAPROC_RUNTIME,
                # Smallest Serverless shape: driver + 2 executors, no autoscaling up.
                "properties": {
                    "spark.executor.instances": "2",
                    "spark.dynamicAllocation.maxExecutors": "2",
                },
            },
            "environment_config": {
                "execution_config": {
                    "service_account": DATAPROC_SA,
                    "subnetwork_uri": DATAPROC_SUBNET,
                    "ttl": {"seconds": 3600},
                },
            },
            "labels": {"project": "oss-pulse", "step": "flatten", "dt": "{{ ds_nodash }}"},
        },
        deferrable=True,  # waits in the triggerer, not in a worker slot
        polling_interval_seconds=30,
        # One batch at a time across all runs. A batch takes 12 vCPUs (driver + 2 executors x 4)
        # and the project's CPUS_ALL_REGIONS quota is 16: a second concurrent batch is rejected
        # (PERMISSION_DENIED ... quota exceeded). Deferred tasks count toward this limit.
        max_active_tis_per_dag=1,
        # Even one at a time, Dataproc frees the previous batch's vCPUs a little AFTER it reports
        # SUCCEEDED, so the next batch can still be rejected ("Requested 12.0, available 4.0").
        # Short fixed retries absorb that lag; a failed submit costs nothing.
        retries=6,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=0,
        execution_timeout=timedelta(minutes=45),
    )

    @task(execution_timeout=timedelta(minutes=15))
    def load_bq_partition(ds: str | None = None, ds_nodash: str | None = None) -> int:
        """Replace raw.events$<ds_nodash> with processed/events/dt=<ds>/. Same as load_events.sh."""
        from google.cloud import bigquery

        folder = f"{PROCESSED_PREFIX}/dt={ds}"
        # Spark writes _SUCCESS only after a complete write: never load a half-written day.
        if not GCSHook().exists(bucket_name=BUCKET, object_name=f"{folder}/_SUCCESS"):
            raise AirflowException(f"gs://{BUCKET}/{folder}/_SUCCESS not found; not loading.")

        client = BigQueryHook(location=REGION).get_client(project_id=PROJECT_ID)
        destination = f"{PROJECT_ID}.{RAW_DATASET}.events${ds_nodash}"
        job = client.load_table_from_uri(
            f"gs://{BUCKET}/{folder}/*.parquet",
            destination,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                # One partition is replaced atomically; other days are untouched. BigQuery
                # rejects the job if any row belongs to another date.
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            ),
            job_id_prefix=f"oss_pulse_load_{ds_nodash}_",
            location=REGION,
        )
        job.result(timeout=600)
        rows = int(job.output_rows or 0)
        if rows == 0:
            raise AirflowException(f"{destination}: load wrote 0 rows")
        log.info("Loaded %s rows into %s (job %s)", rows, destination, job.job_id)
        return rows

    # dbt freshness compares max(created_at) with the CURRENT time, so it only means something
    # for recent days. For catchup/backfill runs (data interval ended > 2 days ago) every check
    # would be "stale" by definition, so the task logs that and passes. This is the one place the
    # wall clock is used, on purpose: freshness is a question about "now".
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            "set -euo pipefail\n"
            "age=$(( $(date +%s) - {{ data_interval_end.timestamp() | int }} ))\n"
            'if [ "$age" -gt 172800 ]; then\n'
            '  echo "Historical run ({{ ds }}): freshness vs. now does not apply, skipping."\n'
            "  exit 0\n"
            "fi\n"
            f'"$DBT_BIN" source freshness {DBT_FLAGS}\n'
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # One dbt build at a time across all runs, so two builds never fight over the same tables
    # (temp tables, MERGE on the dims). Date ORDER is not needed: facts/aggregates replace
    # independent partitions, and the dims keep min/max timestamps and the name with the newer
    # last_seen_at. Deliberately NOT depends_on_past: with max_active_runs=2, re-running an old
    # failed day deadlocks (its run waits for a free slot; the newer runs' dbt waits for it).
    # The whole project is built: seeds and dim_date are tiny, and the relationships tests need
    # every parent to exist.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"\"$DBT_BIN\" build {DBT_FLAGS} --vars '{{run_date: {{{{ ds }}}}}}'",
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(minutes=45),
    )

    @task(
        trigger_rule=TriggerRule.ALL_DONE,  # also runs when an upstream task failed
        retries=1,
        execution_timeout=timedelta(minutes=5),
    )
    def record_audit(**context) -> str:
        """Append (run_date, rows_loaded, status, failed_tasks, dag_run_id, finished_at)."""
        from google.cloud import bigquery

        ti = context["ti"]
        run_id = context["run_id"]
        states = ti.get_task_states(dag_id=ti.dag_id, task_ids=PIPELINE_TASKS, run_ids=[run_id])
        run_states = states.get(run_id, {})
        not_ok = [t for t in PIPELINE_TASKS if run_states.get(t) != "success"]
        status = "failed" if not_ok else "success"
        rows_loaded = ti.xcom_pull(task_ids="load_bq_partition")  # None if the load failed

        client = BigQueryHook(location=REGION).get_client(project_id=PROJECT_ID)
        client.query(
            f"""
            INSERT INTO `{PROJECT_ID}.{AUDIT_DATASET}.load_log`
              (run_date, rows_loaded, status, failed_tasks, dag_run_id, finished_at)
            VALUES (@run_date, @rows_loaded, @status, @failed_tasks, @dag_run_id,
                    CURRENT_TIMESTAMP())
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("run_date", "DATE", context["ds"]),
                    bigquery.ScalarQueryParameter("rows_loaded", "INT64", rows_loaded),
                    bigquery.ScalarQueryParameter("status", "STRING", status),
                    bigquery.ScalarQueryParameter(
                        "failed_tasks", "STRING", ",".join(not_ok) or None
                    ),
                    bigquery.ScalarQueryParameter("dag_run_id", "STRING", run_id),
                ]
            ),
            location=REGION,
        ).result(timeout=120)
        log.info("audit: run_date=%s status=%s rows_loaded=%s", context["ds"], status, rows_loaded)

        # This task is the only leaf, so its state decides the run's state. Fail it (no retry,
        # no second alert) when the pipeline failed, or the run would show as successful.
        if not_ok:
            raise PipelineFailed(f"Pipeline failed for {context['ds']}: {', '.join(not_ok)}")
        return status

    loaded = load_bq_partition()
    audit = record_audit()

    ingest_raw >> spark_flatten >> loaded >> dbt_source_freshness >> dbt_build >> audit
