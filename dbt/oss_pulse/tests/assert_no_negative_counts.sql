-- Counts can never be negative; a negative value means a broken calculation upstream.
-- A singular test fails if this query returns any rows.

select *
from {{ ref('agg_repo_daily') }}
where
    stars < 0
    or forks < 0
    or pushes < 0
    or prs_opened < 0
    or prs_merged < 0
    or issues_opened < 0
    or unique_actors < 0
    or events < 0
