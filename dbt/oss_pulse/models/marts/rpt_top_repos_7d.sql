-- Top 10 repositories by stars over the 7 most recent days loaded into agg_repo_daily (bots excluded).
-- Rebuilt on every run. The dashboard reads these 10 rows instead of joining agg_repo_daily
-- (9M rows) to dim_repo (5.8M rows) on every refresh.
--
-- The window ends at the latest loaded day, not at "today": a paused pipeline or a historical
-- backfill still shows a full week. window_start / window_end are columns so the tile can say which week.
-- window_end comes from partition metadata (macros/latest_partition_date.sql), so the 7-day
-- filter is a constant and BigQuery prunes agg_repo_daily to 7 partitions.
-- language: from the GitHub API (dim_repo_github); NULL until the repo has been enriched.

{{ config(materialized='table') }}

{%- set window_end = latest_partition_date(ref('agg_repo_daily')) %}

with repo_totals as (

    select
        repo_id,
        sum(stars) as stars,
        sum(forks) as forks,
        sum(prs_merged) as prs_merged,
        sum(events) as events
    from {{ ref('agg_repo_daily') }}
    {%- if window_end %}
    where event_date between date_sub(date '{{ window_end }}', interval 6 day) and date '{{ window_end }}'
    {%- else %}
        where false  -- nothing loaded yet
    {%- endif %}
    group by repo_id

),

top_repos as (

    select
        *,
        -- ties broken by repo_id so the ranking is deterministic
        row_number() over (order by stars desc, repo_id asc) as stars_rank
    from repo_totals
    where stars > 0
    qualify stars_rank <= 10

)

select
    top_repos.stars_rank,
    top_repos.repo_id,
    dim_repo.repo_name,
    -- built here rather than read from dim_repo.repo_url: halves the bytes scanned (216 vs 496 MB)
    'https://github.com/' || dim_repo.repo_name as repo_url,
    github.language,
    top_repos.stars,
    top_repos.forks,
    top_repos.prs_merged,
    top_repos.events,
    {%- if window_end %}
    date_sub(date '{{ window_end }}', interval 6 day) as window_start,
    date '{{ window_end }}' as window_end
    {%- else %}
        cast(null as date) as window_start,
        cast(null as date) as window_end
    {%- endif %}
from top_repos
inner join {{ ref('dim_repo') }} as dim_repo
    on top_repos.repo_id = dim_repo.repo_id
left join {{ ref('dim_repo_github') }} as github
    on top_repos.repo_id = github.repo_id
