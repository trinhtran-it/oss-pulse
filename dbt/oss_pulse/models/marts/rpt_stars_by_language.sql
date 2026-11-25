-- Stars received by language over the 30 most recent loaded days, bots excluded, for the
-- repositories enriched from the GitHub API (the top 1,000 by stars). One row per language;
-- 'unknown' = no primary language (e.g. docs or awesome-lists), 'not enriched' = every other repo.
-- share_of_stars is over ALL stars in the window, so the 'not enriched' row shows the coverage.

{{ config(materialized='table') }}

{%- set window_end = latest_partition_date(ref('agg_repo_daily')) %}

with stars as (

    select
        repo_id,
        sum(stars) as stars
    from {{ ref('agg_repo_daily') }}
    {%- if window_end %}
    where event_date between date_sub(date '{{ window_end }}', interval 29 day) and date '{{ window_end }}'
    {%- else %}
        where false  -- nothing loaded yet
    {%- endif %}
    group by repo_id
    having stars > 0  -- the alias: sum(stars) here would be read as sum(sum(stars))

),

labelled as (

    select
        stars.stars,
        stars.repo_id,
        case
            when github.repo_id is null then 'not enriched'
            else coalesce(lower(github.language), 'unknown')
        end as language
    from stars
    left join {{ ref('dim_repo_github') }} as github
        on stars.repo_id = github.repo_id

)

select
    language,
    count(*) as repos,
    sum(stars) as stars,
    round(sum(stars) / sum(sum(stars)) over (), 4) as share_of_stars,
    {%- if window_end %}
    date_sub(date '{{ window_end }}', interval 29 day) as window_start,
    date '{{ window_end }}' as window_end
    {%- else %}
        cast(null as date) as window_start,
        cast(null as date) as window_end
    {%- endif %}
from labelled
group by language
