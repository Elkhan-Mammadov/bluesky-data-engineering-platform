-- Rule-based (not ML) anomaly detection for Trust & Safety, per
-- docs/PROJECT_PLAN.md section 1: accounts that follow or like at a rate
-- no real person plausibly sustains.
with base as (
    select
        user_id_hash,
        first_seen_at,
        last_seen_at,
        total_follows,
        total_likes,
        -- Never divide by zero: an account seen for under a minute still
        -- gets an hours value (1/60), not a division error.
        greatest(extract(epoch from (last_seen_at - first_seen_at)) / 3600.0, 1.0 / 60) as account_age_hours
    from {{ ref('int_user_activity_summary') }}
),
scored as (
    select
        *,
        total_follows / account_age_hours as follows_per_hour,
        total_likes / account_age_hours as likes_per_hour
    from base
)
select
    user_id_hash,
    total_follows,
    total_likes,
    round(account_age_hours::numeric, 2) as account_age_hours,
    round(follows_per_hour::numeric, 2) as follows_per_hour,
    round(likes_per_hour::numeric, 2) as likes_per_hour,
    (case when follows_per_hour > 30 then 1 else 0 end)
        + (case when likes_per_hour > 60 then 1 else 0 end) as anomaly_score,
    nullif(
        trim(both ', ' from
            (case when follows_per_hour > 30 then 'high follow rate, ' else '' end)
            || (case when likes_per_hour > 60 then 'high like rate, ' else '' end)
        ),
        ''
    ) as reason,
    (follows_per_hour > 30 or likes_per_hour > 60) as is_anomalous
from scored
