-- One row per GitHub account (actor_id). Incremental merge, like dim_repo.
-- is_bot is recomputed from the latest login for every row this run touches. After editing
-- seeds/known_bots.csv, run with --full-refresh to re-flag actors not seen recently.

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='actor_id',
    cluster_by=['actor_id'],
    on_schema_change='fail',
) }}

with window_events as (

    select actor_id, actor_login, created_at
    from {{ ref('int_events_enriched') }}
    where {{ event_window('created_at') }}
        and actor_id is not null

),

actors_in_window as (

    select
        actor_id,
        array_agg(actor_login order by created_at desc limit 1)[offset(0)] as latest_login,
        min(created_at) as first_seen_at,
        max(created_at) as last_seen_at
    from window_events
    group by actor_id

),

combined as (

{% if is_incremental() %}
    select
        w.actor_id,
        if(existing.last_seen_at > w.last_seen_at, existing.actor_login, w.latest_login) as actor_login,
        least(w.first_seen_at, coalesce(existing.first_seen_at, w.first_seen_at)) as first_seen_at,
        greatest(w.last_seen_at, coalesce(existing.last_seen_at, w.last_seen_at)) as last_seen_at
    from actors_in_window as w
    left join {{ this }} as existing
        on existing.actor_id = w.actor_id
{% else %}
    select actor_id, latest_login as actor_login, first_seen_at, last_seen_at
    from actors_in_window
{% endif %}

)

select
    actor_id,
    actor_login,
    {{ is_bot('actor_login') }} as is_bot,
    first_seen_at,
    last_seen_at
from combined
