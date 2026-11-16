-- Fails if any activity count is negative.
select *
from {{ ref('fct_repo_daily') }}
where stars < 0 or forks < 0 or pushes < 0 or pull_requests < 0
