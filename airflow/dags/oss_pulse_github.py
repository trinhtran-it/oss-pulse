"""
### oss_pulse_github: enrich the top repositories with GitHub metadata

Daily, independent of `oss_pulse_daily`:

1. **fetch_github_metadata**: `python -m ingestion.github_repos` calls the GitHub REST API for the
   top 1,000 repositories by stars (latest 30 loaded days) that weren't fetched in the last 7 days,
   and appends the answers to `raw.github_repo_metadata`. Stops cleanly at the rate limit; the
   next run continues where it stopped.
2. **dbt_build_github**: `stg_github_repos` -> `dim_repo_github` -> `rpt_top_repos_7d`,
   `rpt_stars_by_language`, with their tests.

Why a separate DAG: the API returns the repository *as it is now*, so this is not a backfillable,
per-day job like the event pipeline (`catchup=False`). And a GitHub outage or an expired token
must never fail the daily event load. dbt tasks of both DAGs share the `dbt` pool (1 slot), so
two builds never run at the same time.

Token: Airflow Variable `github_token_secret` (env `AIRFLOW_VAR_GITHUB_TOKEN_SECRET` in `.env`),
a fine-grained token with public repositories read-only. Masked in logs ("secret" in the name).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from oss_pulse_lib.alerts import notify_failure

DBT_ARTIFACTS = "/opt/airflow/logs/dbt/github/{{ ds }}/{{ ti.task_id }}"
DBT_FLAGS = (
    '--project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROJECT_DIR" --target prod '
    f'--target-path "{DBT_ARTIFACTS}/target" --log-path "{DBT_ARTIFACTS}/logs"'
)

default_args = {
    "owner": "oss-pulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": 2.0,
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="oss_pulse_github",
    description="GitHub REST API -> raw.github_repo_metadata -> dim_repo_github",
    # 03:00 UTC: after the event pipeline's run for the previous day has usually finished.
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,  # the API only knows "now": nothing to backfill
    max_active_runs=1,
    default_args=default_args,
    doc_md=__doc__,
    tags=["oss-pulse", "enrichment"],
) as dag:
    fetch_github_metadata = BashOperator(
        task_id="fetch_github_metadata",
        bash_command="cd /opt/airflow && python -m ingestion.github_repos",
        # Templated from the Variable (env or secrets backend); empty = unauthenticated, 50 calls.
        env={"GITHUB_TOKEN": "{{ var.value.get('github_token_secret', '') }}"},
        append_env=True,
        execution_timeout=timedelta(minutes=45),
    )

    dbt_build_github = BashOperator(
        task_id="dbt_build_github",
        bash_command=f'"$DBT_BIN" build {DBT_FLAGS} --select stg_github_repos+',
        pool="dbt",  # shared with oss_pulse_daily.dbt_build: one dbt build at a time
        execution_timeout=timedelta(minutes=20),
    )

    fetch_github_metadata >> dbt_build_github
