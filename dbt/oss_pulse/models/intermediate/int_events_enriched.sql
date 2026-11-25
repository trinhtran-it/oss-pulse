-- Business rules in one place (see docs/DATA_NOTES.md): event category, bot flag and the metric
-- flags every mart counts. A view: callers filter created_at, and the filter is pushed down to
-- raw.events' partitions.

with events as (

    select * from {{ ref('stg_events') }}

),

event_types as (

    select
        event_type,
        category
    from {{ ref('event_type_lookup') }}

)

select
    events.event_id,
    events.event_type,
    coalesce(event_types.category, 'other') as event_category,
    events.created_at,
    events.event_date,
    events.event_hour,

    events.actor_id,
    events.actor_login,
    {{ is_bot('events.actor_login') }} as is_bot,

    events.repo_id,
    events.repo_name,
    events.repo_owner,
    events.org_id,

    events.payload_action,
    events.pr_number,
    events.issue_number,

    -- Metric flags. PR merges changed format around Oct 2025: before, closed + pr_merged = true;
    -- after, payload_action = 'merged'. Both are counted.
    events.event_type = 'WatchEvent' as is_star,
    events.event_type = 'ForkEvent' as is_fork,
    events.event_type = 'PushEvent' as is_push,
    events.event_type = 'PullRequestEvent' and events.payload_action = 'opened' as is_pr_opened,
    coalesce(
        events.event_type = 'PullRequestEvent'
        and (
            events.payload_action = 'merged'
            or (events.payload_action = 'closed' and events.pr_merged)
        ),
        false
    ) as is_pr_merged,
    events.event_type = 'IssuesEvent' and events.payload_action = 'opened' as is_issue_opened

from events
left join event_types
    on events.event_type = event_types.event_type
-- A star is a WatchEvent with action "started" (the only action GitHub emits today); anything
-- else would not be a star, so it is dropped rather than miscounted.
where events.event_type != 'WatchEvent' or events.payload_action = 'started'
