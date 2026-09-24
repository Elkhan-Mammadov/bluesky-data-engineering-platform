#!/bin/bash
# Runs once, automatically, the first time warehouse-db's data volume is
# created (standard Postgres docker-entrypoint-initdb.d behaviour).
#
# Scope for Stage 2 (Infrastructure) only: open the empty schema
# namespaces every later stage writes into. No tables yet - those are
# created by the stage that owns them:
#   - control.pipeline_switches, dq.pipeline_runs -> Stage 3 (Ingestion)
#   - raw, dq.quarantine, dq.stream_batches       -> Stage 5 (Spark streaming)
#   - realtime                                     -> Stage 5 (Spark streaming)
#   - staging/intermediate/snapshots/marts         -> Stage 6 (dbt, self-managed)
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS realtime;
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS intermediate;
    CREATE SCHEMA IF NOT EXISTS snapshots;
    CREATE SCHEMA IF NOT EXISTS marts;
    CREATE SCHEMA IF NOT EXISTS dq;
    CREATE SCHEMA IF NOT EXISTS control;
EOSQL

echo "[warehouse-init] schemas created: raw, realtime, staging, intermediate, snapshots, marts, dq, control"
