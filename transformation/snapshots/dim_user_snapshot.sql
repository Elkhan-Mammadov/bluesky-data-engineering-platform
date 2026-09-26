{#
  SCD2 history for dim_user: a new row is added whenever a user's segment
  or main_language changes (CLAUDE.md 8.7). The "check" strategy compares
  those two columns on every run; dbt adds dbt_valid_from/dbt_valid_to
  automatically, which marts/dim_user.sql turns into is_current.
#}
{% snapshot dim_user_snapshot %}

{{
    config(
        unique_key='user_id_hash',
        strategy='check',
        check_cols=['segment', 'main_language'],
        target_schema='snapshots',
    )
}}

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
    segment
from {{ ref('int_user_activity_summary') }}

{% endsnapshot %}
