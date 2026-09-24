-- Grain: one row per (user, day).
select
    user_id_hash,
    activity_date,
    post_count,
    like_count,
    repost_count,
    follow_count,
    block_count,
    total_activity_count
from {{ ref('int_user_daily_activity') }}
