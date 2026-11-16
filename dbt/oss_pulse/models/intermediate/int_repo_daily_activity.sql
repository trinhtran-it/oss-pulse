select
    event_date,
    repo_id,
    repo_name,
    countif(event_type = 'WatchEvent') as stars,
    countif(event_type = 'ForkEvent') as forks,
    countif(event_type = 'PushEvent') as pushes,
    countif(event_type = 'PullRequestEvent') as pull_requests,
    count(distinct actor_id) as unique_actors
from {{ ref('stg_events') }}
group by 1, 2, 3
