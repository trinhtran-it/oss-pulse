{#
  WHERE-clause predicate selecting the days an incremental model should (re)build.

    Incremental run with run_date set:            [run_date - lookback_days, run_date]
    First build / --full-refresh with run_date:   [history_start_date, run_date]
    No run_date:                                  everything since history_start_date

  CI uses the middle case to build a small window, e.g.
    --vars '{history_start_date: 2026-10-30, run_date: 2026-10-31}'

  Bounds are constant expressions, so BigQuery prunes partitions (and raw.events'
  require_partition_filter is satisfied). Never derive bounds from a subquery such as
  (select max(event_date) from {{ this }}): pruning would be lost.

  column_type: 'timestamp' (e.g. created_at) or 'date' (e.g. event_date).
#}
{% macro event_window(column, column_type='timestamp') -%}
    {%- set run_date = var('run_date', none) -%}
    {%- if is_incremental() and run_date -%}
        {%- set start_date = "date_sub(date '" ~ run_date ~ "', interval " ~ var('lookback_days') ~ " day)" -%}
        {%- set end_date = "date '" ~ run_date ~ "'" -%}
    {%- else -%}
        {%- set start_date = "date '" ~ var('history_start_date') ~ "'" -%}
        {%- set end_date = ("date '" ~ run_date ~ "'") if run_date else none -%}
    {%- endif -%}

    {%- if column_type == 'timestamp' -%}
        {{ column }} >= timestamp({{ start_date }})
        {%- if end_date %} and {{ column }} < timestamp(date_add({{ end_date }}, interval 1 day)){% endif %}
    {%- elif column_type == 'date' -%}
        {{ column }} >= {{ start_date }}
        {%- if end_date %} and {{ column }} <= {{ end_date }}{% endif %}
    {%- else -%}
        {{ exceptions.raise_compiler_error("event_window: column_type must be 'timestamp' or 'date'") }}
    {%- endif -%}
{%- endmacro %}


{#
  Static partition list for insert_overwrite when run_date is set:
  [run_date - lookback_days, ..., run_date] as BigQuery date literals.
  With an explicit list dbt replaces exactly those days, even ones that came out empty.
  The big saving comes from copy_partitions (see fct_events), not from this list. Measured on a
  2-day fct_events run (INFORMATION_SCHEMA.JOBS): dynamic insert_overwrite = 333 MB temp table +
  655 MB MERGE = 987 MB; static list alone = same 987 MB; with copy_partitions = 333 MB.
  Returns none (dynamic mode) when run_date is not set; ignored on full builds.
#}
{% macro partitions_to_replace() -%}
    {%- set run_date = var('run_date', none) -%}
    {%- if not run_date -%}
        {{ return(none) }}
    {%- else -%}
        {%- set end = modules.datetime.date.fromisoformat(run_date | string) -%}
        {%- set ns = namespace(days=[]) -%}
        {%- for offset in range(var('lookback_days') | int, -1, -1) -%}
            {%- set day = end - modules.datetime.timedelta(days=offset) -%}
            {%- set ns.days = ns.days + ["date '" ~ day.isoformat() ~ "'"] -%}
        {%- endfor -%}
        {{ return(ns.days) }}
    {%- endif -%}
{%- endmacro %}
