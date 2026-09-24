select distinct
    language
from {{ ref('stg_posts') }}
where language is not null
