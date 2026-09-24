#!/bin/bash
# Runs once, automatically, the first time source-db's data volume is
# created (standard Postgres docker-entrypoint-initdb.d behaviour).
#
# Scope for Stage 2 (Infrastructure) only: create the dedicated replication
# role Debezium will use. Logical replication itself (wal_level=logical) is
# turned on via the `command:` flags on the source-db service in
# docker-compose.yml, not here.
#
# The actual OLTP tables and the publication Debezium reads from are
# created in Stage 3 (Ingestion), once the ingestor's schema is designed.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM pg_catalog.pg_roles WHERE rolname = '${SOURCE_DB_REPLICATION_USER}'
        ) THEN
            CREATE ROLE "${SOURCE_DB_REPLICATION_USER}"
                WITH REPLICATION LOGIN PASSWORD '${SOURCE_DB_REPLICATION_PASSWORD}';
        END IF;
    END
    \$\$;
EOSQL

echo "[source-init] replication role '${SOURCE_DB_REPLICATION_USER}' ready"
