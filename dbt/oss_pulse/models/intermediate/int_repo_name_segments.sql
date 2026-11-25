-- Grain: one row per repo_id x event_date x continuous run of events under one name.
-- Input for dim_repo_history (SCD2). Compacts each day's events to a few rows per repo, so the
-- history can be rebuilt from ~10M rows a month instead of re-reading every event.
--
-- "Continuous run" (gaps and islands): a repo renamed A -> B -> A in one day gives 3 rows, not 2;
-- grouping by name would merge the two A periods into one that overlaps B.
-- Ties (two events in the same second) are ordered by event_id, as in dim_repo, so both agree.

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

with events as (

    select
        event_id,
        event_date,
        created_at,
        repo_id,
        repo_name
    from {{ ref('int_events_enriched') }}
    where
        {{ event_window('created_at') }}
        and repo_id is not null

),

flagged as (

    select
        *,
        -- 1 where a new run starts: first event of the day, or the name differs from the previous event
        if(
            repo_name = lag(repo_name) over (
                partition by repo_id, event_date order by created_at asc, event_id asc
            ),
            0,
            1
        ) as is_new_run
    from events

),

numbered as (

    select
        *,
        sum(is_new_run) over (
            partition by repo_id, event_date
            order by created_at asc, event_id asc
            rows between unbounded preceding and current row
        ) as run_number
    from flagged

)

select
    event_date,
    repo_id,
    run_number,
    repo_name,
    min(created_at) as first_seen_at,
    max(created_at) as last_seen_at,
    count(*) as events
from numbered
group by event_date, repo_id, run_number, repo_name
