-- Grain: one row per user - all-time totals, main language, and a
-- rule-based (not ML) activity segment. This is the source dbt snapshots
-- for dim_user's SCD2 history (snapshots/dim_user_snapshot.sql).
with totals as (
    select
        user_id_hash,
        sum(post_count)   as total_posts,
        sum(like_count)   as total_likes,
        sum(repost_count) as total_reposts,
        sum(follow_count) as total_follows,
        sum(block_count)  as total_blocks
    from {{ ref('int_user_daily_activity') }}
    group by 1
),
-- The language a user posts in most often; ties broken alphabetically so
-- the result is deterministic.
main_language as (
    select user_id_hash, language
    from (
        select
            user_id_hash,
            language,
            count(*) as post_count,
            row_number() over (
                partition by user_id_hash
                order by count(*) desc, language
            ) as rn
        from {{ ref('stg_posts') }}
        where language is not null
        group by user_id_hash, language
    ) ranked
    where rn = 1
)
select
    users.user_id_hash,
    users.first_seen_at,
    users.last_seen_at,
    coalesce(totals.total_posts, 0)   as total_posts,
    coalesce(totals.total_likes, 0)   as total_likes,
    coalesce(totals.total_reposts, 0) as total_reposts,
    coalesce(totals.total_follows, 0) as total_follows,
    coalesce(totals.total_blocks, 0)  as total_blocks,
    main_language.language as main_language,
    case
        when coalesce(totals.total_posts, 0) >= 5 then 'creator'
        when coalesce(totals.total_likes, 0) + coalesce(totals.total_reposts, 0)
            + coalesce(totals.total_follows, 0) >= 5 then 'engager'
        else 'lurker'
    end as segment
from {{ ref('stg_users') }} as users
left join totals on totals.user_id_hash = users.user_id_hash
left join main_language on main_language.user_id_hash = users.user_id_hash
