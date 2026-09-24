select
    user_id_hash || '::' || record_key as block_id,
    user_id_hash,
    record_key,
    occurred_at,
    occurred_at::date as occurred_date,
    target_user_id_hash
from {{ source('raw', 'blocks') }}
