#!/usr/bin/env bash
# Prints the UI links (make urls) or the SSH tunnel command (make tunnel)
# with the host ports actually in use: the *_HOST_PORT values from .env,
# falling back to the same defaults as docker-compose.yml.
#
#   scripts/urls.sh [urls | tunnel] [user@host]
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

INGESTOR=${INGESTOR_HOST_PORT:-8000}
SIMULATOR=${SIMULATOR_HOST_PORT:-8001}
KAFKA_UI=${KAFKA_UI_HOST_PORT:-8085}
KAFKA_CONNECT=${KAFKA_CONNECT_HOST_PORT:-8083}
SPARK_MASTER=${SPARK_MASTER_UI_HOST_PORT:-18080}
SPARK_UI=${SPARK_UI_HOST_PORT:-4040}
AIRFLOW=${AIRFLOW_UI_HOST_PORT:-8081}
DBT_DOCS=${DBT_DOCS_HOST_PORT:-8088}
GRAFANA=${GRAFANA_HOST_PORT:-3000}
SOURCE_DB=${SOURCE_DB_HOST_PORT:-5432}
WAREHOUSE_DB=${WAREHOUSE_DB_HOST_PORT:-5433}

case "${1:-urls}" in
    urls)
        echo "Ingestor status  : http://localhost:$INGESTOR/docs"
        echo "Simulator        : http://localhost:$SIMULATOR/docs"
        echo "Kafka UI         : http://localhost:$KAFKA_UI"
        echo "Kafka Connect    : http://localhost:$KAFKA_CONNECT/connectors"
        echo "Spark Master     : http://localhost:$SPARK_MASTER"
        echo "Spark Streaming  : http://localhost:$SPARK_UI"
        echo "Airflow          : http://localhost:$AIRFLOW"
        echo "dbt docs         : http://localhost:$DBT_DOCS"
        echo "Grafana          : http://localhost:$GRAFANA"
        echo "source-db        : localhost:$SOURCE_DB"
        echo "warehouse-db     : localhost:$WAREHOUSE_DB"
        ;;
    tunnel)
        args=""
        for port in "$INGESTOR" "$SIMULATOR" "$KAFKA_UI" "$KAFKA_CONNECT" "$SPARK_MASTER" \
                    "$SPARK_UI" "$AIRFLOW" "$DBT_DOCS" "$GRAFANA" "$SOURCE_DB" "$WAREHOUSE_DB"; do
            args="$args -L $port:localhost:$port"
        done
        echo "ssh -N$args ${2:-<user>@<remote-host>}"
        ;;
    *)
        echo "usage: $0 [urls | tunnel] [user@host]" >&2
        exit 2
        ;;
esac
