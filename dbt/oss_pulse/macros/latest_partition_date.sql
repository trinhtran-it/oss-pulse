{#
  The latest non-empty day partition of a day-partitioned table, as 'YYYY-MM-DD' (or none).

  Read from INFORMATION_SCHEMA.PARTITIONS (metadata, ~10 MB billed) at run time, so a model can
  use it as a CONSTANT filter: BigQuery then prunes to the window's partitions. A subquery like
  (select max(event_date) from t) would scan the column and disable pruning.
  Reports use it to end their window at the latest LOADED day, not at today: a paused pipeline or
  a historical backfill still shows a full window. Returns none at parse time and when empty.
#}
{% macro latest_partition_date(relation) -%}
    {%- if not execute -%}
        {{ return(none) }}
    {%- else -%}
        {%- set result = run_query(
            "select cast(max(parse_date('%Y%m%d', partition_id)) as string) "
            ~ "from `" ~ relation.database ~ "." ~ relation.schema ~ "`.INFORMATION_SCHEMA.PARTITIONS "
            ~ "where table_name = '" ~ relation.identifier ~ "' "
            ~ "and regexp_contains(partition_id, r'^[0-9]{8}$') and total_rows > 0"
        ) -%}
        {{ return(result.columns[0].values()[0]) }}
    {%- endif -%}
{%- endmacro %}
