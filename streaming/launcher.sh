#!/usr/bin/env bash
# Submits the Spark Structured Streaming job once
# control.pipeline_switches.spark_enabled is on, polling every 5 seconds.
#
# Runs spark-submit in the BACKGROUND, not in the foreground: a running
# streaming query never returns (awaitTermination() blocks forever, by
# design), so if we ran it in the foreground this script's own loop - and
# with it, the heartbeat file below - would freeze the moment the job
# started, even though the job itself is perfectly healthy. Running it in
# the background keeps this loop (and the heartbeat) alive for as long as
# the container runs, and lets us notice + resubmit if the job ever dies.
set -euo pipefail

HEARTBEAT_FILE="/tmp/launcher_heartbeat"
POLL_INTERVAL_SECONDS=5
SPARK_PID=""

cleanup() {
    if [ -n "$SPARK_PID" ]; then
        kill "$SPARK_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

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

is_job_running() {
    [ -n "$SPARK_PID" ] && kill -0 "$SPARK_PID" 2>/dev/null
}

echo "[launcher.sh] waiting for control.pipeline_switches.spark_enabled"

while true; do
    date +%s > "$HEARTBEAT_FILE"

    if [ "$(is_spark_enabled)" = "yes" ] && ! is_job_running; then
        echo "[launcher.sh] spark_enabled is on - submitting the streaming job"
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
            /opt/app/streaming/spark_jobs/cdc_to_warehouse.py &
        SPARK_PID=$!
    fi

    sleep "$POLL_INTERVAL_SECONDS"
done
