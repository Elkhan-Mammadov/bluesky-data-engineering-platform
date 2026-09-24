#!/usr/bin/env bash
# Prints row counts per layer (source-db -> raw -> marts) every 5 seconds,
# using psql inside the running containers (no local Postgres client
# needed on the host). Press Ctrl+C to stop.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

source_count() {
    docker compose exec -T source-db psql -U "$SOURCE_DB_USER" -d "$SOURCE_DB_NAME" -t -A -c "
        SELECT (SELECT count(*) FROM users) + (SELECT count(*) FROM posts) + (SELECT count(*) FROM likes)
             + (SELECT count(*) FROM reposts) + (SELECT count(*) FROM follows) + (SELECT count(*) FROM blocks)
             + (SELECT count(*) FROM profile_updates);
    " 2>/dev/null | tr -d '[:space:]'
}

raw_count() {
    docker compose exec -T warehouse-db psql -U "$WAREHOUSE_DB_USER" -d "$WAREHOUSE_DB_NAME" -t -A -c "
        SELECT (SELECT count(*) FROM raw.users) + (SELECT count(*) FROM raw.posts) + (SELECT count(*) FROM raw.likes)
             + (SELECT count(*) FROM raw.reposts) + (SELECT count(*) FROM raw.follows) + (SELECT count(*) FROM raw.blocks)
             + (SELECT count(*) FROM raw.profile_updates);
    " 2>/dev/null | tr -d '[:space:]'
}

marts_count() {
    docker compose exec -T warehouse-db psql -U "$WAREHOUSE_DB_USER" -d "$WAREHOUSE_DB_NAME" -t -A -c "
        SELECT (SELECT count(*) FROM marts.fct_posts) + (SELECT count(*) FROM marts.fct_interactions);
    " 2>/dev/null | tr -d '[:space:]'
}

echo "Watching row counts per layer - press Ctrl+C to stop."
printf "%-10s %12s %12s %12s\n" "time" "source-db" "raw" "marts"

while true; do
    printf "%-10s %12s %12s %12s\n" \
        "$(date +%H:%M:%S)" \
        "$(source_count || echo '?')" \
        "$(raw_count || echo '?')" \
        "$(marts_count || echo '?')"
    sleep 5
done
