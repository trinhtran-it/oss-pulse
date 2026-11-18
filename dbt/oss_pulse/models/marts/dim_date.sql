-- One row per calendar day from history_start_date up to date_spine_end (exclusive).

{{ config(materialized='table') }}

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="date '" ~ var('history_start_date') ~ "'",
        end_date="date '" ~ var('date_spine_end') ~ "'"
    ) }}

)

select
    cast(date_day as date) as date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    format_date('%B', date_day) as month_name,
    date_trunc(cast(date_day as date), month) as month_start,
    extract(isoweek from date_day) as iso_week,
    date_trunc(cast(date_day as date), isoweek) as iso_week_start,
    extract(dayofweek from date_day) as day_of_week,
    format_date('%A', date_day) as day_name,
    extract(dayofweek from date_day) in (1, 7) as is_weekend
from spine
