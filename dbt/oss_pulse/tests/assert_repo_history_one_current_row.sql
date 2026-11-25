-- SCD2 rule: every repository has exactly one current period. Returns the repos that don't.

select
    repo_id,
    countif(is_current) as current_rows
from {{ ref('dim_repo_history') }}
group by repo_id
having countif(is_current) != 1
