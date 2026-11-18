-- 1:1 with raw.events (same rows): renamed, typed and lowercased. A view, so it costs nothing to
-- build; queries on it must still filter created_at (raw.events requires a partition filter).
-- payload_json is left out on purpose: it is ~78% of the table's bytes. Extract payload fields in
-- a dedicated incremental model when one is needed.

with source as (

    select
        event_id,
        event_type,
        created_at,
        event_date,
        event_hour,
        actor_id,
        actor_login,
        repo_id,
        repo_name,
        repo_owner,
        org_id,
        org_login,
        is_public,
        payload_action,
        pr_number,
        pr_merged,
        issue_number,
        push_size,
        push_distinct_size,
        ref,
        ref_type,
        release_tag,
        source_file
    from {{ source('raw', 'events') }}

)

select
    cast(event_id as string) as event_id,
    cast(event_type as string) as event_type,
    cast(created_at as timestamp) as created_at,
    cast(event_date as date) as event_date,
    cast(event_hour as int64) as event_hour,

    cast(actor_id as int64) as actor_id,
    -- GitHub logins and repo names are case-insensitive: lowercase so they join and match reliably
    lower(actor_login) as actor_login,
    cast(repo_id as int64) as repo_id,
    lower(repo_name) as repo_name,
    lower(repo_owner) as repo_owner,
    cast(org_id as int64) as org_id,
    lower(org_login) as org_login,
    cast(is_public as bool) as is_public,

    lower(payload_action) as payload_action,
    cast(pr_number as int64) as pr_number,
    cast(pr_merged as bool) as pr_merged,
    cast(issue_number as int64) as issue_number,
    cast(push_size as int64) as push_size,
    cast(push_distinct_size as int64) as push_distinct_size,
    ref as git_ref,
    lower(ref_type) as git_ref_type,
    release_tag,
    source_file

from source
