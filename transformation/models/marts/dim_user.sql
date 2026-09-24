-- SCD2: one row per (user, validity period). is_current makes "give me
-- today's segment for each user" a one-line filter for Grafana/BI.
select
    user_id_hash,
    first_seen_at,
    last_seen_at,
    total_posts,
    total_likes,
    total_reposts,
    total_follows,
    total_blocks,
    main_language,
    segment,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_current
from {{ ref('dim_user_snapshot') }}
