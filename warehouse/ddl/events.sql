-- raw.events: one row per public GitHub event (GH Archive), loaded daily by
-- warehouse/load_events.sh from gs://<bucket>/processed/events/dt=YYYY-MM-DD/*.parquet.
--
-- Run with a default dataset (see `make bq-table`), so the table name is unqualified here.
-- Columns mirror spark/flatten_events.py OUTPUT_COLUMNS and are matched by name on load.
-- Columns are NULLABLE on purpose: Spark writes OPTIONAL Parquet fields, and a REQUIRED column
-- would reject the load. not_null/unique checks live in dbt tests instead.

CREATE TABLE IF NOT EXISTS events (
  event_id           STRING    OPTIONS (description = 'GitHub event id; a string in the source'),
  event_type         STRING    OPTIONS (description = 'e.g. WatchEvent, PullRequestEvent'),
  created_at         TIMESTAMP OPTIONS (description = 'Event time, UTC. Partition column'),
  event_date         DATE      OPTIONS (description = 'DATE(created_at), UTC'),
  event_hour         INT64     OPTIONS (description = 'Hour of created_at, UTC (0-23)'),
  actor_id           INT64,
  actor_login        STRING    OPTIONS (description = 'Ends with [bot] for GitHub Apps'),
  repo_id            INT64     OPTIONS (description = 'Stable across renames. Rarely NULL (repo = {})'),
  repo_name          STRING    OPTIONS (description = 'owner/name at event time'),
  repo_owner         STRING    OPTIONS (description = 'repo_name before the /'),
  org_id             INT64     OPTIONS (description = 'NULL for user-owned repos'),
  org_login          STRING,
  is_public          BOOL,
  payload_action     STRING    OPTIONS (description = 'Meaning depends on event_type'),
  pr_number          INT64,
  pr_merged          BOOL      OPTIONS (description = 'Before ~Oct 2025 only; later merges use payload_action = merged'),
  issue_number       INT64,
  push_size          INT64     OPTIONS (description = 'Commits in the push. Before ~Oct 2025 only'),
  push_distinct_size INT64     OPTIONS (description = 'Before ~Oct 2025 only'),
  ref                STRING,
  ref_type           STRING,
  release_tag        STRING,
  payload_json       STRING    OPTIONS (description = 'Raw payload JSON; extract new fields with JSON_VALUE'),
  source_file        STRING    OPTIONS (description = 'Raw .json.gz the row came from (lineage)')
)
PARTITION BY DATE(created_at)
CLUSTER BY event_type, repo_name
OPTIONS (
  -- Cost guardrail: every query must filter on created_at, so nobody scans all history by accident.
  require_partition_filter = TRUE,
  description = 'GH Archive events, one row per event. Daily partitions replaced idempotently.',
  labels = [('project', 'oss-pulse'), ('layer', 'raw')]
);
