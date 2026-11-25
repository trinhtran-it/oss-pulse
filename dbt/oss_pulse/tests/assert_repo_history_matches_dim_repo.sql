-- The current period's name must be dim_repo's name: both take the name at the most recent event
-- (ties broken by event_id). Returns repos where the two models disagree.

select
    h.repo_id,
    h.repo_name as history_name,
    d.repo_name as dim_repo_name
from {{ ref('dim_repo_history') }} as h
inner join {{ ref('dim_repo') }} as d
    on h.repo_id = d.repo_id
where
    h.is_current
    and h.repo_name != d.repo_name
