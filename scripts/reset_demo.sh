#!/usr/bin/env bash
# Host-side equivalent of DAG 99_reset_demo: turns off both control
# switches, deletes the Debezium connector, and truncates every data
# table (source-db + every warehouse schema except dim_date, a static
# calendar). Safe to run more than once.
#
# Kafka topic contents are left untouched - run `make clean` if you need
# a fully empty Kafka too (it removes every volume, including Kafka's).
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

echo "[reset_demo.sh] turning off control switches..."
docker compose exec -T warehouse-db psql -U "$WAREHOUSE_DB_USER" -d "$WAREHOUSE_DB_NAME" -c "
    UPDATE control.pipeline_switches SET is_enabled = false, updated_at = now();
"

echo "[reset_demo.sh] deleting the Debezium connector (if it exists)..."
curl -s -X DELETE "http://localhost:${KAFKA_CONNECT_HOST_PORT:-8083}/connectors/source-db-connector" > /dev/null || true

echo "[reset_demo.sh] truncating source-db tables..."
docker compose exec -T source-db psql -U "$SOURCE_DB_USER" -d "$SOURCE_DB_NAME" -c "
    DO \$\$
    DECLARE
        t text;
    BEGIN
        FOREACH t IN ARRAY ARRAY['users','posts','likes','reposts','follows','blocks','profile_updates'] LOOP
            IF to_regclass(t) IS NOT NULL THEN
                EXECUTE 'TRUNCATE TABLE ' || t || ' CASCADE';
            END IF;
        END LOOP;
        UPDATE ingestion_cursor SET last_time_us = NULL, updated_at = now();
    END
    \$\$;
"

echo "[reset_demo.sh] truncating warehouse-db tables..."
docker compose exec -T warehouse-db psql -U "$WAREHOUSE_DB_USER" -d "$WAREHOUSE_DB_NAME" -c "
    DO \$\$
    DECLARE
        t text;
    BEGIN
        FOREACH t IN ARRAY ARRAY[
            'raw.users','raw.posts','raw.likes','raw.reposts','raw.follows','raw.blocks','raw.profile_updates',
            'realtime.event_counts_by_minute','realtime.active_users_by_minute',
            'realtime.hashtag_counts_by_minute','realtime.language_counts_by_minute',
            'dq.quarantine','dq.stream_batches',
            'snapshots.dim_user_snapshot',
            'marts.dim_user','marts.dim_hashtag','marts.dim_language','marts.fct_posts',
            'marts.fct_interactions','marts.fct_user_daily_activity','marts.mart_engagement',
            'marts.mart_trending_hashtags','marts.mart_anomalous_accounts'
        ] LOOP
            IF to_regclass(t) IS NOT NULL THEN
                EXECUTE 'TRUNCATE TABLE ' || t || ' CASCADE';
            END IF;
        END LOOP;
    END
    \$\$;
"

echo "[reset_demo.sh] done - everything is empty."
echo "                 Kafka topic contents are untouched; run 'make clean' for a fully empty Kafka too."
