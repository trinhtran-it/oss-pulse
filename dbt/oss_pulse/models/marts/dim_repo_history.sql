-- SCD Type 2: one row per repository per period under one name (owner/name).
-- A new row starts when a repo is renamed or transferred, including a rename back (A -> B -> A
-- gives 3 rows). The latest row (is_current) matches dim_repo.
--
-- Built from events, not with `dbt snapshot`: a snapshot only records what it sees when it runs,
-- so a backfill would collapse a month of renames into one row and the history could never be
-- rebuilt. Here every period comes from event timestamps, so any backfill or full refresh
-- reproduces exactly the same history.
--
-- valid_from is the first event seen under that name. GitHub doesn't publish the rename itself,
-- so the true rename happened somewhere between the previous period's last_seen_at and valid_from.
--
-- Point-in-time lookup ("what was this repo called when the event happened?"):
--   join dim_repo_history h on h.repo_id = e.repo_id
--     and e.created_at >= h.valid_from and (e.created_at < h.valid_to or h.valid_to is null)

{{ config(
    materialized='table',
    cluster_by=['repo_id'],
) }}

with segments as (

    select
        repo_id,
        event_date,
        run_number,
        repo_name,
        first_seen_at,
        last_seen_at,
        events
    from {{ ref('int_repo_name_segments') }}

),

flagged as (

    -- (event_date, run_number) is the exact event order, ties included. Runs split at midnight
    -- continue across days: a new period starts only when the name changes.
    select
        *,
        coalesce(
            repo_name != lag(repo_name) over (
                partition by repo_id order by event_date asc, run_number asc
            ),
            true
        ) as is_new_period
    from segments

),

numbered as (

    select
        *,
        countif(is_new_period) over (
            partition by repo_id
            order by event_date asc, run_number asc
            rows between unbounded preceding and current row
        ) as repo_version
    from flagged

),

periods as (

    select
        repo_id,
        repo_version,
        any_value(repo_name) as repo_name,
        min(first_seen_at) as valid_from,
        max(last_seen_at) as last_seen_at,
        sum(events) as events
    from numbered
    group by repo_id, repo_version

),

with_neighbors as (

    select
        *,
        split(repo_name, '/')[safe_offset(0)] as repo_owner,
        split(repo_name, '/')[safe_offset(1)] as short_name,
        lag(repo_name) over (partition by repo_id order by repo_version asc) as previous_repo_name,
        lead(valid_from) over (partition by repo_id order by repo_version asc) as valid_to
    from periods

)

select
    {{ dbt_utils.generate_surrogate_key(['repo_id', 'repo_version']) }} as repo_version_key,
    repo_id,
    repo_version,
    repo_name,
    repo_owner,
    previous_repo_name,
    case
        when previous_repo_name is null then 'first_seen'
        when
            split(previous_repo_name, '/')[safe_offset(0)] != repo_owner
            and split(previous_repo_name, '/')[safe_offset(1)] != short_name
            then 'transferred_and_renamed'
        when split(previous_repo_name, '/')[safe_offset(0)] != repo_owner then 'transferred'
        else 'renamed'
    end as change_type,
    valid_from,
    valid_to,
    valid_to is null as is_current,
    last_seen_at,
    events
from with_neighbors
