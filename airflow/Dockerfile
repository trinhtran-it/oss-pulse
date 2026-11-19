# Airflow image for oss-pulse: the official image + what our tasks need.
# Built by docker-compose.yaml (context = repo root, so it can read the requirements files):
#   make airflow-build
#
# Code (dags/, ingestion/, dbt/) is NOT baked in: compose mounts it, so edits need no rebuild.
# Rebuild only when a requirements file or AIRFLOW_VERSION changes.
ARG AIRFLOW_VERSION=3.4.0
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Ingestion runs in Airflow's own interpreter (BashOperator -> python -m ingestion.gharchive).
# Pinning apache-airflow stops pip from up/downgrading Airflow to satisfy our requirements.
# The Google provider (Dataproc, BigQuery, GCS hooks) is already in the official image.
COPY ingestion/requirements.txt /tmp/ingestion-requirements.txt
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/ingestion-requirements.txt

# dbt gets an isolated venv: its pins (protobuf, agate, ...) must not fight Airflow's.
# Tasks call it by full path ($DBT_BIN), so there is never doubt about which dbt runs.
COPY dbt/requirements.txt /tmp/dbt-requirements.txt
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install -r /tmp/dbt-requirements.txt \
    && /home/airflow/dbt-venv/bin/dbt --version
