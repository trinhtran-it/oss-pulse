-- SCD2 rule: a repo's periods follow each other with no gap and no overlap: each valid_to is the
-- next period's valid_from, and no period ends before it starts. Returns the broken periods.

with ordered as (

    select
        repo_id,
        repo_version,
        valid_from,
        valid_to,
        lead(valid_from) over (partition by repo_id order by repo_version asc) as next_valid_from,
        lead(repo_version) over (partition by repo_id order by repo_version asc) as next_version
    from {{ ref('dim_repo_history') }}

)

select *
from ordered
where
    valid_to < valid_from
    or next_valid_from < valid_from
    or (next_version is not null and next_version != repo_version + 1)
    or coalesce(cast(valid_to as string), 'null') != coalesce(cast(next_valid_from as string), 'null')
