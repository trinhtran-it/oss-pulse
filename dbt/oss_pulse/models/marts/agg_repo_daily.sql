-- Grain: one row per repo_id x event_date with at least one non-bot event.
-- Bots are excluded so counts reflect human activity. Incremental insert_overwrite on event_date.

{#- copy_partitions: replace partitions with BigQuery copy jobs (no bytes billed) instead of a MERGE.
    partitions: static list from run_date (macros/event_window.sql): exactly those days are replaced. -#}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        'field': 'event_date', 'data_type': 'date', 'granularity': 'day',
        'copy_partitions': true,
    },
    cluster_by=['repo_id'],
    partitions=partitions_to_replace(),
    on_schema_change='fail',
) }}

select
    event_date,
    repo_id,
    countif(is_star) as stars,
    countif(is_fork) as forks,
    countif(is_push) as pushes,
    countif(is_pr_opened) as prs_opened,
    countif(is_pr_merged) as prs_merged,
    countif(is_issue_opened) as issues_opened,
    count(distinct actor_id) as unique_actors,
    count(*) as events
from {{ ref('fct_events') }}
where
    {{ event_window('event_date', 'date') }}
    and not is_bot
    and repo_id is not null
group by event_date, repo_id
