-- Grain: one row per post.
select
    post_id,
    user_id_hash,
    record_key,
    occurred_at,
    occurred_date,
    language,
    text_length,
    hashtags,
    has_link,
    has_media,
    is_reply
from {{ ref('stg_posts') }}
