-- Grain: one row per event_date x event_hour x event_type x is_bot.
-- Serves the dashboard's category, time-series and heatmap tiles. About 18k rows a month, so a
-- dashboard refresh reads kilobytes instead of scanning fct_events (30M rows, 2.4 GB).
-- Incremental insert_overwrite on event_date, same window as the other marts.

{#- copy_partitions: replace partitions with BigQuery copy jobs (no bytes billed) instead of a MERGE.
    partitions: static list from run_date (macros/event_window.sql): exactly those days are replaced. -#}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        'field': 'event_date', 'data_type': 'date', 'granularity': 'day',
        'copy_partitions': true,
    },
    cluster_by=['event_category', 'event_type'],
    partitions=partitions_to_replace(),
    on_schema_change='fail',
) }}

select
    event_date,
    event_hour,
    -- ISO weekday with a label that sorts correctly as text: '1 Mon' ... '7 Sun'
    format_date('%u %a', event_date) as weekday,
    event_type,
    event_category,
    is_bot,
    count(*) as events
from {{ ref('fct_events') }}
where {{ event_window('event_date', 'date') }}
group by event_date, event_hour, weekday, event_type, event_category, is_bot
