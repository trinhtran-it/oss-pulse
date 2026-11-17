"""Daily pipeline: GH Archive -> GCS -> Spark flatten -> BigQuery -> dbt."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "oss-pulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="oss_pulse_daily",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["oss-pulse"],
) as dag:
    ingest = BashOperator(
        task_id="ingest_gharchive",
        # exit 1 = failed hours, 3 = hours not published yet: both fail the task so Airflow retries.
        bash_command="cd /opt/airflow && python -m ingestion.gharchive --date {{ ds }}",
    )

    flatten = BashOperator(
        task_id="spark_flatten_events",
        bash_command="echo 'TODO: submit spark/flatten_events.py for {{ ds }}'",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/dbt/oss_pulse && dbt build --vars '{run_date: {{ ds }}}'",
    )

    ingest >> flatten >> dbt_build
