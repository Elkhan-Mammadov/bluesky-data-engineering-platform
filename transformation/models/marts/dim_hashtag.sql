select distinct
    lower(hashtag) as hashtag
from {{ ref('stg_posts') }}, unnest(hashtags) as hashtag
where hashtag is not null and hashtag <> ''
