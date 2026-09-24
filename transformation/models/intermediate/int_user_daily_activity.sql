-- Grain: one row per (user, day). Counts each event type separately so
-- fct_user_daily_activity (marts) can expose them, and totals feed
-- int_user_activity_summary's segment classification.
with posts as (
    select user_id_hash, occurred_date, count(*) as post_count
    from {{ ref('stg_posts') }}
    group by 1, 2
),
likes as (
    select user_id_hash, occurred_date, count(*) as like_count
    from {{ ref('stg_likes') }}
    group by 1, 2
),
reposts as (
    select user_id_hash, occurred_date, count(*) as repost_count
    from {{ ref('stg_reposts') }}
    group by 1, 2
),
follows as (
    select user_id_hash, occurred_date, count(*) as follow_count
    from {{ ref('stg_follows') }}
    group by 1, 2
),
blocks as (
    select user_id_hash, occurred_date, count(*) as block_count
    from {{ ref('stg_blocks') }}
    group by 1, 2
),
-- Every (user, day) pair that had at least one event of any type.
all_days as (
    select user_id_hash, occurred_date from posts
    union
    select user_id_hash, occurred_date from likes
    union
    select user_id_hash, occurred_date from reposts
    union
    select user_id_hash, occurred_date from follows
    union
    select user_id_hash, occurred_date from blocks
)
select
    all_days.user_id_hash,
    all_days.occurred_date as activity_date,
    coalesce(posts.post_count, 0)     as post_count,
    coalesce(likes.like_count, 0)     as like_count,
    coalesce(reposts.repost_count, 0) as repost_count,
    coalesce(follows.follow_count, 0) as follow_count,
    coalesce(blocks.block_count, 0)   as block_count,
    coalesce(posts.post_count, 0) + coalesce(likes.like_count, 0)
        + coalesce(reposts.repost_count, 0) + coalesce(follows.follow_count, 0)
        + coalesce(blocks.block_count, 0) as total_activity_count
from all_days
left join posts   on posts.user_id_hash = all_days.user_id_hash and posts.occurred_date = all_days.occurred_date
left join likes    on likes.user_id_hash = all_days.user_id_hash and likes.occurred_date = all_days.occurred_date
left join reposts  on reposts.user_id_hash = all_days.user_id_hash and reposts.occurred_date = all_days.occurred_date
left join follows  on follows.user_id_hash = all_days.user_id_hash and follows.occurred_date = all_days.occurred_date
left join blocks   on blocks.user_id_hash = all_days.user_id_hash and blocks.occurred_date = all_days.occurred_date
