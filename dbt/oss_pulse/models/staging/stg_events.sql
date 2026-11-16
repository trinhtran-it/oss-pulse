select
    event_id,
    event_type,
    created_at,
    event_date,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    org_login,
    payload_json
from {{ source('raw', 'events') }}
