-- audit.load_log: one row per finished Airflow run of oss_pulse_daily (written by record_audit).
--
-- Append-only: clearing or re-running a day adds a new row instead of updating the old one, so the
-- table is the history of every attempt. Latest status per day:
--   SELECT * FROM load_log WHERE run_date >= '2026-10-01'
--   QUALIFY ROW_NUMBER() OVER (PARTITION BY run_date ORDER BY finished_at DESC) = 1
--
-- Run with a default dataset (see `make bq-audit-table`), so the table name is unqualified here.

CREATE TABLE IF NOT EXISTS load_log (
  run_date      DATE      NOT NULL OPTIONS (description = 'Logical date of the run ({{ ds }}): the UTC day processed'),
  rows_loaded   INT64              OPTIONS (description = 'Rows written to raw.events for run_date. NULL if the load did not succeed'),
  status        STRING    NOT NULL OPTIONS (description = 'success or failed'),
  failed_tasks  STRING             OPTIONS (description = 'Comma-separated task_ids that failed or did not run. NULL on success'),
  dag_run_id    STRING    NOT NULL OPTIONS (description = 'Airflow run_id, e.g. scheduled__2026-10-01T00:00:00+00:00'),
  finished_at   TIMESTAMP NOT NULL OPTIONS (description = 'When record_audit wrote the row (BigQuery server time)')
)
PARTITION BY run_date
OPTIONS (
  description = 'Pipeline audit log: one row per oss_pulse_daily run attempt.',
  labels = [('project', 'oss-pulse'), ('layer', 'audit')]
);
