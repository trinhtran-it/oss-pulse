-- GitHub attributes for the enriched repositories (top 1,000 by stars, refreshed weekly):
-- language, topics, license and totals as GitHub reports them. One row per repo_id; join to
-- dim_repo / fct_events on repo_id. Repos never enriched have no row: use a left join.
-- stars_total is all-time (GitHub); the marts count stars per day from events.

{{ config(
    materialized='table',
    cluster_by=['repo_id'],
) }}

select
    repo_id,
    github_full_name,
    description,
    language,
    topics,
    license_spdx,
    stars_total,
    forks_total,
    open_issues_count,
    is_fork,
    is_archived,
    repo_created_at,
    pushed_at,
    last_status,
    last_status in ('ok', 'not_modified') as is_available,
    metadata_fetched_at,
    last_checked_at
from {{ ref('stg_github_repos') }}
