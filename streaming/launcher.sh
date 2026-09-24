#!/usr/bin/env bash
# Submits the Spark Structured Streaming job once
# control.pipeline_switches.spark_enabled is on, polling every 5 seconds.
# Always touches a heartbeat file so the spark-streaming container's
# HEALTHCHECK (docker-compose.yml) reports healthy, whether or not the
# switch is on yet.
set -euo pipefail

HEARTBEAT_FILE="/tmp/launcher_heartbeat"
POLL_INTERVAL_SECONDS=5

is_spark_enabled() {
    python3 -c "
import os
import psycopg2

conn = psycopg2.connect(
    host=os.environ['WAREHOUSE_DB_HOST'],
    port=os.environ.get('WAREHOUSE_DB_PORT', '5432'),
    dbname=os.environ['WAREHOUSE_DB_NAME'],
    user=os.environ['WAREHOUSE_DB_USER'],
    password=os.environ['WAREHOUSE_DB_PASSWORD'],
)
cur = conn.cursor()
cur.execute(\"SELECT is_enabled FROM control.pipeline_switches WHERE switch_name = 'spark_enabled'\")
row = cur.fetchone()
print('yes' if row and row[0] else 'no')
" 2>/dev/null || echo "no"
}

echo "[launcher.sh] waiting for control.pipeline_switches.spark_enabled"

while true; do
    date +%s > "$HEARTBEAT_FILE"

    if [ "$(is_spark_enabled)" = "yes" ]; then
        echo "[launcher.sh] spark_enabled is on - submitting the streaming job"
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
            /opt/app/streaming/spark_jobs/cdc_to_warehouse.py \
            || echo "[launcher.sh] streaming job exited (see logs above) - will retry after the next poll"
    fi

    sleep "$POLL_INTERVAL_SECONDS"
done
