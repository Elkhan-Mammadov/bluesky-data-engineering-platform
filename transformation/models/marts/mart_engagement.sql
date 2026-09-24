-- Likes and reposts per post. Reply counts are not included: the
-- ingestor only keeps is_reply as a flag, never the parent post it
-- targets (see fct_interactions.sql for the same limitation, explained).
with likes as (
    select target_user_id_hash, target_record_key, count(*) as like_count
    from {{ ref('stg_likes') }}
    where target_user_id_hash is not null and target_record_key is not null
    group by 1, 2
),
reposts as (
    select target_user_id_hash, target_record_key, count(*) as repost_count
    from {{ ref('stg_reposts') }}
    where target_user_id_hash is not null and target_record_key is not null
    group by 1, 2
)
select
    posts.post_id,
    posts.user_id_hash,
    posts.record_key,
    posts.occurred_at,
    coalesce(likes.like_count, 0) as like_count,
    coalesce(reposts.repost_count, 0) as repost_count,
    coalesce(likes.like_count, 0) + coalesce(reposts.repost_count, 0) as engagement_count
from {{ ref('fct_posts') }} as posts
left join likes
    on likes.target_user_id_hash = posts.user_id_hash
    and likes.target_record_key = posts.record_key
left join reposts
    on reposts.target_user_id_hash = posts.user_id_hash
    and reposts.target_record_key = posts.record_key
