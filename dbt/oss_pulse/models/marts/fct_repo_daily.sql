{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'event_date', 'data_type': 'date'},
        cluster_by=['repo_id']
    )
}}

select *
from {{ ref('int_repo_daily_activity') }}
{% if is_incremental() %}
where event_date = date('{{ var("run_date") }}')
{% endif %}
