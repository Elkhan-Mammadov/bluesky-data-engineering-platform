select
    user_id_hash || '::' || record_key as post_id,
    user_id_hash,
    record_key,
    occurred_at,
    occurred_at::date as occurred_date,
    language,
    text_length,
    hashtags,
    has_link,
    has_media,
    is_reply
from {{ source('raw', 'posts') }}
