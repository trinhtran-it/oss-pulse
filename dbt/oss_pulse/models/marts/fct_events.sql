-- Grain: one row per GitHub event. Foreign keys: repo_id -> dim_repo, actor_id -> dim_actor,
-- event_date -> dim_date. Incremental insert_overwrite: each run replaces only the event_date
-- partitions it rebuilds (see macros/event_window.sql), so re-runs are idempotent.

{#- copy_partitions: replace partitions with BigQuery copy jobs (no bytes billed) instead of a MERGE.
    partitions: static list from run_date (macros/event_window.sql): exactly those days are replaced. -#}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        'field': 'event_date', 'data_type': 'date', 'granularity': 'day',
        'copy_partitions': true,
    },
    cluster_by=['event_type', 'repo_id'],
    partitions=partitions_to_replace(),
    on_schema_change='fail',
) }}

select
    event_id,
    event_date,
    created_at,
    event_hour,
    event_type,
    event_category,

    repo_id,
    actor_id,
    org_id,
    is_bot,

    payload_action,
    pr_number,
    issue_number,

    is_star,
    is_fork,
    is_push,
    is_pr_opened,
    is_pr_merged,
    is_issue_opened

from {{ ref('int_events_enriched') }}
where {{ event_window('created_at') }}
