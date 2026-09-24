#!/usr/bin/env bash
# Manual/debug helper: registers the Debezium connector from the host,
# idempotently (skips if it already exists). Run from the project root.
#
# The automated path is DAG 02 (make start-cdc), which does the same thing
# from inside the airflow-scheduler container. This script is for when you
# want to register the connector without going through Airflow.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

# Reachable from the host via the published port (127.0.0.1:8083), unlike
# KAFKA_CONNECT_URL in .env which only resolves inside the Docker network.
CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="source-db-connector"

if curl -sf "$CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
    echo "[register_connector.sh] '$CONNECTOR_NAME' already registered - skipping"
    exit 0
fi

if ! command -v envsubst > /dev/null 2>&1; then
    echo "[register_connector.sh] envsubst not found - install gettext-base" >&2
    exit 1
fi

RENDERED_CONFIG="$(mktemp)"
envsubst < ingestion/cdc/debezium-source.json > "$RENDERED_CONFIG"

curl -sf -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d @"$RENDERED_CONFIG"
echo
rm -f "$RENDERED_CONFIG"

echo "[register_connector.sh] '$CONNECTOR_NAME' registered"
