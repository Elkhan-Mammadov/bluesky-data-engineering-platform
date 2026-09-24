-- One row per hashed user ever seen. Deduplication is guaranteed
-- upstream: raw.users has a PRIMARY KEY on user_id_hash, kept in sync by
-- the ingestor's upsert (Stage 3) and Spark's upsert (Stage 5).
select
    user_id_hash,
    first_seen_at,
    last_seen_at
from {{ source('raw', 'users') }}
