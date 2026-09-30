#!/usr/bin/env bash
# Runs the Phase 1 pipeline (DAG 07_daily_batch_report) for one logical
# date through Airflow, waits for it to finish and prints every task's
# state. Run from the project root (or via make pipeline).
#
#   scripts/run_pipeline.sh [YYYY-MM-DD] [--fail | --fail-dq]
#
# Date defaults to today (UTC). If a run for that date already exists it
# is cleared and re-run (Airflow allows one run per logical date), which
# is also how to check idempotency: run the same date twice and compare.
#   --fail     deliberate failure: start_ingestion fails, downstream tasks
#              show upstream_failed, the run is marked failed
#   --fail-dq  sets min_partition_rows to 1,000,000,000 so the data quality
#              check fails and fails the run
# A failure run keeps its conf when cleared, so demo it on a date you do
# not need for real data (e.g. 2026-01-01).
set -euo pipefail

cd "$(dirname "$0")/.."

DAG_ID="07_daily_batch_report"
TIMEOUT_SECONDS=1200
POLL_SECONDS=10

DATE="$(date -u +%F)"
CONF=""
for arg in "$@"; do
    case "$arg" in
        --fail) CONF='{"force_ingestion_failure": true}' ;;
        --fail-dq) CONF='{"min_partition_rows": 1000000000}' ;;
        [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]) DATE="$arg" ;;
        *) echo "usage: $0 [YYYY-MM-DD] [--fail | --fail-dq]" >&2; exit 2 ;;
    esac
done

airflow() {
    docker compose exec -T airflow-scheduler airflow "$@"
}

run_state() {
    # `airflow dags state` prints log lines first; the state is the last line.
    airflow dags state "$DAG_ID" "$DATE" 2>/dev/null | tail -n 1 | tr -d '\r'
}

echo "[run_pipeline] logical date: $DATE"
airflow dags unpause "$DAG_ID" >/dev/null 2>&1

existing="$(run_state)"
if [ "$existing" != "None" ]; then
    if [ -n "$CONF" ]; then
        echo "[run_pipeline] a run for $DATE already exists ($existing); pick another date for a failure demo" >&2
        exit 2
    fi
    echo "[run_pipeline] run for $DATE exists ($existing) - clearing it to re-run"
    airflow tasks clear "$DAG_ID" -s "$DATE" -e "$DATE" -y >/dev/null 2>&1
    # Clearing only matches the DAG's current tasks. A run created by an
    # older version of this DAG has none of them, so nothing is cleared.
    if [ "$(run_state)" = "$existing" ]; then
        echo "[run_pipeline] nothing was cleared - the $DATE run was made by an older version of $DAG_ID." >&2
        echo "[run_pipeline] remove the old runs with: docker compose exec airflow-scheduler airflow dags delete $DAG_ID -y" >&2
        exit 1
    fi
elif [ -n "$CONF" ]; then
    echo "[run_pipeline] triggering with conf $CONF"
    airflow dags trigger "$DAG_ID" -e "$DATE" -c "$CONF" >/dev/null 2>&1
else
    echo "[run_pipeline] triggering"
    airflow dags trigger "$DAG_ID" -e "$DATE" >/dev/null 2>&1
fi

deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
state=""
while :; do
    new_state="$(run_state)"
    if [ "$new_state" != "$state" ]; then
        echo "[run_pipeline] $(date -u +%T) state: $new_state"
        state="$new_state"
    fi
    case "$state" in success | failed) break ;; esac
    if [ "$(date +%s)" -gt "$deadline" ]; then
        echo "[run_pipeline] still '$state' after ${TIMEOUT_SECONDS}s - check the Airflow UI" >&2
        exit 1
    fi
    sleep "$POLL_SECONDS"
done

echo
airflow tasks states-for-dag-run "$DAG_ID" "$DATE" 2>/dev/null
echo

if [ "$state" = "success" ]; then
    echo "[run_pipeline] SUCCESS - run 'make smoke SMOKE_LOGICAL_DATE=$DATE' to verify every layer"
elif [ -n "$CONF" ]; then
    echo "[run_pipeline] FAILED, as requested by the failure switch - see the task states above"
else
    echo "[run_pipeline] FAILED - open the failed task's log in the Airflow UI (localhost:8081)" >&2
    exit 1
fi
