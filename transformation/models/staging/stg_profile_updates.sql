select
    user_id_hash,
    occurred_at,
    operation
from {{ source('raw', 'profile_updates') }}
