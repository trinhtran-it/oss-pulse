-- One row per repository (repo_id). Incremental merge: each run reads only the new days and
-- combines them with the existing row, so history is never re-scanned.

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='repo_id',
    cluster_by=['repo_id'],
    on_schema_change='fail',
) }}

with window_events as (

    select repo_id, repo_name, repo_owner, created_at
    from {{ ref('int_events_enriched') }}
    where {{ event_window('created_at') }}
        and repo_id is not null  -- rare upstream quirk: "repo": {}

),

repos_in_window as (

    select
        repo_id,
        -- the name at the most recent event: repos can be renamed or transferred
        array_agg(struct(repo_name, repo_owner) order by created_at desc limit 1)[offset(0)] as latest,
        min(created_at) as first_seen_at,
        max(created_at) as last_seen_at
    from window_events
    group by repo_id

),

combined as (

{% if is_incremental() %}
    select
        w.repo_id,
        if(existing.last_seen_at > w.last_seen_at, existing.repo_name, w.latest.repo_name) as repo_name,
        if(existing.last_seen_at > w.last_seen_at, existing.repo_owner, w.latest.repo_owner) as repo_owner,
        least(w.first_seen_at, coalesce(existing.first_seen_at, w.first_seen_at)) as first_seen_at,
        greatest(w.last_seen_at, coalesce(existing.last_seen_at, w.last_seen_at)) as last_seen_at
    from repos_in_window as w
    left join {{ this }} as existing
        on existing.repo_id = w.repo_id
{% else %}
    select
        repo_id,
        latest.repo_name as repo_name,
        latest.repo_owner as repo_owner,
        first_seen_at,
        last_seen_at
    from repos_in_window
{% endif %}

)

select
    repo_id,
    repo_name,
    repo_owner,
    concat('https://github.com/', repo_name) as repo_url,
    first_seen_at,
    last_seen_at
from combined
