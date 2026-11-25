-- One row per repository: the latest GitHub API answer, from raw.github_repo_metadata (an
-- append-only log written by ingestion/github_repos.py).
-- Metadata comes from the latest successful call (status 'ok'): a 304 "not modified" means it is
-- still current. last_status / last_checked_at describe the latest call of any kind, so a repo
-- deleted after it was enriched keeps its metadata but shows last_status = 'not_found'.

with calls as (

    select
        repo_id,
        fetched_at,
        status,
        full_name,
        description,
        language,
        topics,
        license_spdx,
        stargazers_count,
        forks_count,
        open_issues_count,
        is_fork,
        is_archived,
        repo_created_at,
        pushed_at
    from {{ source('raw', 'github_repo_metadata') }}

),

latest_call as (

    select
        repo_id,
        status as last_status,
        fetched_at as last_checked_at
    from calls
    qualify row_number() over (partition by repo_id order by fetched_at desc) = 1

),

latest_metadata as (

    select *
    from calls
    where status = 'ok'
    qualify row_number() over (partition by repo_id order by fetched_at desc) = 1

)

select
    latest_call.repo_id,
    latest_call.last_status,
    latest_call.last_checked_at,
    latest_metadata.fetched_at as metadata_fetched_at,
    latest_metadata.full_name as github_full_name,
    latest_metadata.description,
    latest_metadata.language,
    latest_metadata.topics,
    latest_metadata.license_spdx,
    latest_metadata.stargazers_count as stars_total,
    latest_metadata.forks_count as forks_total,
    latest_metadata.open_issues_count,
    latest_metadata.is_fork,
    latest_metadata.is_archived,
    latest_metadata.repo_created_at,
    latest_metadata.pushed_at
from latest_call
left join latest_metadata
    on latest_call.repo_id = latest_metadata.repo_id
