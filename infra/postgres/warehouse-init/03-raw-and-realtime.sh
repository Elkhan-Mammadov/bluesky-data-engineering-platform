#!/bin/bash
# Runs once, automatically, the first time warehouse-db's data volume is
# created (standard Postgres docker-entrypoint-initdb.d behaviour).
#
# Stage 5 (Spark streaming): the raw CDC mirror (with lineage columns),
# the realtime per-minute aggregate tables, and the two dq tables Spark
# writes to every micro-batch.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    -- ---------------------------------------------------------------
    -- raw: 1:1 mirror of source-db, plus CDC lineage columns.
    -- ---------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS raw.users (
        user_id_hash  TEXT PRIMARY KEY,
        first_seen_at TIMESTAMPTZ NOT NULL,
        last_seen_at  TIMESTAMPTZ NOT NULL,
        kafka_offset  BIGINT,
        cdc_op        TEXT,
        ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS raw.posts (
        user_id_hash TEXT NOT NULL,
        record_key   TEXT NOT NULL,
        occurred_at  TIMESTAMPTZ NOT NULL,
        language     TEXT,
        text_length  INTEGER,
        hashtags     TEXT[] NOT NULL DEFAULT '{}',
        has_link     BOOLEAN NOT NULL DEFAULT false,
        has_media    BOOLEAN NOT NULL DEFAULT false,
        is_reply     BOOLEAN NOT NULL DEFAULT false,
        kafka_offset BIGINT,
        cdc_op       TEXT,
        ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (user_id_hash, record_key)
    );

    CREATE TABLE IF NOT EXISTS raw.likes (
        user_id_hash        TEXT NOT NULL,
        record_key          TEXT NOT NULL,
        occurred_at         TIMESTAMPTZ NOT NULL,
        target_user_id_hash TEXT,
        target_record_key   TEXT,
        kafka_offset        BIGINT,
        cdc_op              TEXT,
        ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (user_id_hash, record_key)
    );

    CREATE TABLE IF NOT EXISTS raw.reposts (
        user_id_hash        TEXT NOT NULL,
        record_key          TEXT NOT NULL,
        occurred_at         TIMESTAMPTZ NOT NULL,
        target_user_id_hash TEXT,
        target_record_key   TEXT,
        kafka_offset        BIGINT,
        cdc_op              TEXT,
        ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (user_id_hash, record_key)
    );

    CREATE TABLE IF NOT EXISTS raw.follows (
        user_id_hash        TEXT NOT NULL,
        record_key          TEXT NOT NULL,
        occurred_at         TIMESTAMPTZ NOT NULL,
        target_user_id_hash TEXT NOT NULL,
        kafka_offset        BIGINT,
        cdc_op              TEXT,
        ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (user_id_hash, record_key)
    );

    CREATE TABLE IF NOT EXISTS raw.blocks (
        user_id_hash        TEXT NOT NULL,
        record_key          TEXT NOT NULL,
        occurred_at         TIMESTAMPTZ NOT NULL,
        target_user_id_hash TEXT NOT NULL,
        kafka_offset        BIGINT,
        cdc_op              TEXT,
        ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (user_id_hash, record_key)
    );

    CREATE TABLE IF NOT EXISTS raw.profile_updates (
        user_id_hash TEXT PRIMARY KEY,
        occurred_at  TIMESTAMPTZ NOT NULL,
        operation    TEXT NOT NULL,
        kafka_offset BIGINT,
        cdc_op       TEXT,
        ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    -- ---------------------------------------------------------------
    -- realtime: per-minute aggregates for the Grafana Row 1 panels.
    -- ---------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS realtime.event_counts_by_minute (
        minute_bucket TIMESTAMPTZ NOT NULL,
        event_type    TEXT NOT NULL,
        operation     TEXT NOT NULL,
        event_count   INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (minute_bucket, event_type, operation)
    );

    -- Membership table (not a running count) so COUNT(DISTINCT user_id_hash)
    -- stays accurate even across several 5s batches in the same minute.
    CREATE TABLE IF NOT EXISTS realtime.active_users_by_minute (
        minute_bucket TIMESTAMPTZ NOT NULL,
        user_id_hash  TEXT NOT NULL,
        PRIMARY KEY (minute_bucket, user_id_hash)
    );

    CREATE TABLE IF NOT EXISTS realtime.hashtag_counts_by_minute (
        minute_bucket TIMESTAMPTZ NOT NULL,
        hashtag       TEXT NOT NULL,
        tag_count     INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (minute_bucket, hashtag)
    );

    CREATE TABLE IF NOT EXISTS realtime.language_counts_by_minute (
        minute_bucket TIMESTAMPTZ NOT NULL,
        language      TEXT NOT NULL,
        post_count    INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (minute_bucket, language)
    );

    -- ---------------------------------------------------------------
    -- dq: quarantine + per-batch stream stats.
    -- ---------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS dq.quarantine (
        id             BIGSERIAL PRIMARY KEY,
        source_table   TEXT,
        reason         TEXT NOT NULL,
        raw_payload    JSONB,
        quarantined_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS dq.stream_batches (
        id              BIGSERIAL PRIMARY KEY,
        batch_id        BIGINT NOT NULL,
        row_count       INTEGER NOT NULL,
        valid_row_count INTEGER NOT NULL,
        duration_ms     INTEGER NOT NULL,
        latency_ms      INTEGER,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );

EOSQL

echo "[warehouse-init] raw, realtime and dq (quarantine/stream_batches) tables ready"
