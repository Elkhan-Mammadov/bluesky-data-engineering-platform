-- Grain: one row per (hashtag, hour).
select
    lower(hashtag) as hashtag,
    date_trunc('hour', occurred_at) as hour_bucket,
    count(*) as usage_count
from {{ ref('stg_posts') }}, unnest(hashtags) as hashtag
where hashtag is not null and hashtag <> ''
group by 1, 2
