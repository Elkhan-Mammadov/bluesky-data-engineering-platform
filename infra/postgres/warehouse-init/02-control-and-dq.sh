#!/bin/bash
# Runs once, automatically, the first time warehouse-db's data volume is
# created (standard Postgres docker-entrypoint-initdb.d behaviour).
#
# Stage 3 (Ingestion): the control-switch table Airflow flips and the
# ingestor/Spark launcher poll, plus the run-log table every DAG writes to.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    CREATE TABLE IF NOT EXISTS control.pipeline_switches (
        switch_name TEXT PRIMARY KEY,
        is_enabled  BOOLEAN NOT NULL,
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    INSERT INTO control.pipeline_switches (switch_name, is_enabled) VALUES
        ('ingestion_enabled', false),
        ('spark_enabled', false)
    ON CONFLICT (switch_name) DO NOTHING;

    CREATE TABLE IF NOT EXISTS dq.pipeline_runs (
        id       BIGSERIAL PRIMARY KEY,
        dag_id   TEXT NOT NULL,
        task_id  TEXT NOT NULL,
        status   TEXT NOT NULL,
        detail   TEXT,
        run_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );

EOSQL

echo "[warehouse-init] control.pipeline_switches and dq.pipeline_runs ready"
