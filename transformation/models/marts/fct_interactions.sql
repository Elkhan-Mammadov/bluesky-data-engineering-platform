-- Grain: one row per action (like, repost, follow, block). Incremental
-- with a lookback window so CDC events that arrive a little late still
-- get picked up on the next run instead of being silently missed.
--
-- "reply" is intentionally not included here: the ingestor only keeps a
-- post's is_reply flag (see fct_posts), never which specific post it
-- replies to (privacy design, Stage 3 / docs/PRIVACY.md), so a reply
-- cannot be attributed to a target the way a like/repost/follow/block can.
with unioned as (

    select
        'like' as interaction_type,
        user_id_hash,
        record_key,
        occurred_at,
        target_user_id_hash,
        target_record_key
    from {{ ref('stg_likes') }}

    union all

    select
        'repost' as interaction_type,
        user_id_hash,
        record_key,
        occurred_at,
        target_user_id_hash,
        target_record_key
    from {{ ref('stg_reposts') }}

    union all

    select
        'follow' as interaction_type,
        user_id_hash,
        record_key,
        occurred_at,
        target_user_id_hash,
        cast(null as text) as target_record_key
    from {{ ref('stg_follows') }}

    union all

    select
        'block' as interaction_type,
        user_id_hash,
        record_key,
        occurred_at,
        target_user_id_hash,
        cast(null as text) as target_record_key
    from {{ ref('stg_blocks') }}

)

select *
from unioned

{% if is_incremental() %}
where occurred_at > (
    select coalesce(max(occurred_at), '1970-01-01'::timestamptz) - interval '2 hours'
    from {{ this }}
)
{% endif %}
