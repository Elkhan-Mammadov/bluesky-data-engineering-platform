#!/bin/bash
# Runs once, automatically, the first time source-db's data volume is
# created (standard Postgres docker-entrypoint-initdb.d behaviour).
#
# Stage 3 (Ingestion): the OLTP tables the ingestor writes into, plus the
# logical replication PUBLICATION Debezium reads from in Stage 4.
#
# Privacy (see docs/PRIVACY.md): every user reference is a salted hash
# (user_id_hash / target_user_id_hash) - never a raw Bluesky DID, handle,
# or post text. Only the non-identifying fields listed in CLAUDE.md 8.1
# are derived from post text before the text itself is discarded.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    -- One row per hashed user ever seen, used as the anchor for dim_user
    -- (Stage 6). Upserted on every event so last_seen_at stays current.
    CREATE TABLE IF NOT EXISTS users (
        user_id_hash  TEXT PRIMARY KEY,
        first_seen_at TIMESTAMPTZ NOT NULL,
        last_seen_at  TIMESTAMPTZ NOT NULL
    );

    CREATE TABLE IF NOT EXISTS posts (
        user_id_hash TEXT NOT NULL,
        record_key   TEXT NOT NULL,
        occurred_at  TIMESTAMPTZ NOT NULL,
        language     TEXT,
        text_length  INTEGER,
        hashtags     TEXT[] NOT NULL DEFAULT '{}',
        has_link     BOOLEAN NOT NULL DEFAULT false,
        has_media    BOOLEAN NOT NULL DEFAULT false,
        is_reply     BOOLEAN NOT NULL DEFAULT false,
        PRIMARY KEY (user_id_hash, record_key)
    );

    -- likes/reposts target a POST, identified by the hash of its author
    -- and its record key. The target may belong to a user we never
    -- sampled - that's expected, see docs/PROJECT_PLAN.md section 2.
    CREATE TABLE IF NOT EXISTS likes (
        user_id_hash        TEXT NOT NULL,
        record_key          TEXT NOT NULL,
        occurred_at         TIMESTAMPTZ NOT NULL,
        target_user_id_hash TEXT,
        target_record_key   TEXT,
        PRIMARY KEY (user_id_hash, record_key)
    );

    CREATE TABLE IF NOT EXISTS reposts (
        user_id_hash        TEXT NOT NULL,
        record_key          TEXT NOT NULL,
        occurred_at         TIMESTAMPTZ NOT NULL,
        target_user_id_hash TEXT,
        target_record_key   TEXT,
        PRIMARY KEY (user_id_hash, record_key)
    );

    -- follows/blocks target a whole ACCOUNT (no record key), also hashed.
    CREATE TABLE IF NOT EXISTS follows (
        user_id_hash        TEXT NOT NULL,
        record_key          TEXT NOT NULL,
        occurred_at         TIMESTAMPTZ NOT NULL,
        target_user_id_hash TEXT NOT NULL,
        PRIMARY KEY (user_id_hash, record_key)
    );

    CREATE TABLE IF NOT EXISTS blocks (
        user_id_hash        TEXT NOT NULL,
        record_key          TEXT NOT NULL,
        occurred_at         TIMESTAMPTZ NOT NULL,
        target_user_id_hash TEXT NOT NULL,
        PRIMARY KEY (user_id_hash, record_key)
    );

    -- app.bsky.actor.profile is a singleton record per account (rkey is
    -- always "self"), so one row per user is enough - we only track that
    -- an update happened, never its content (no handle/bio stored).
    CREATE TABLE IF NOT EXISTS profile_updates (
        user_id_hash TEXT PRIMARY KEY,
        occurred_at  TIMESTAMPTZ NOT NULL,
        operation    TEXT NOT NULL
    );

    -- Single-row table: the time_us of the last event the ingestor
    -- successfully wrote, so a restart can resume a few seconds earlier
    -- instead of replaying (or skipping) the whole feed.
    CREATE TABLE IF NOT EXISTS ingestion_cursor (
        id           SMALLINT PRIMARY KEY DEFAULT 1,
        last_time_us BIGINT,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT ingestion_cursor_single_row CHECK (id = 1)
    );
    INSERT INTO ingestion_cursor (id, last_time_us)
        VALUES (1, NULL) ON CONFLICT (id) DO NOTHING;

    -- Debezium needs SELECT for its initial snapshot, plus the
    -- REPLICATION attribute (already granted in 01-roles-and-replication.sh)
    -- for streaming changes afterwards.
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO "${SOURCE_DB_REPLICATION_USER}";
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT ON TABLES TO "${SOURCE_DB_REPLICATION_USER}";

    -- ingestion_cursor is internal bookkeeping - it is not published, so
    -- Debezium/Kafka never sees it.
    CREATE PUBLICATION dbz_publication FOR TABLE
        users, posts, likes, reposts, follows, blocks, profile_updates;

EOSQL

echo "[source-init] tables, publication and grants ready"
