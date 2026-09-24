#!/usr/bin/env bash
# Submits the Spark Structured Streaming job once control.pipeline_switches
# .spark_enabled is on. That table does not exist until Stage 3, and the
# real spark-submit call is written in Stage 5 - so for now this script
# just stays alive and healthy, touching a heartbeat file every 5 seconds.
# The spark-streaming container's HEALTHCHECK (docker-compose.yml) checks
# that this heartbeat file was updated recently.
set -euo pipefail

HEARTBEAT_FILE="/tmp/launcher_heartbeat"

echo "[launcher.sh] waiting for control.pipeline_switches.spark_enabled (Stage 3+)"
echo "[launcher.sh] real spark-submit wiring lands in Stage 5"

while true; do
    date +%s > "$HEARTBEAT_FILE"
    sleep 5
done
