#!/usr/bin/env bash
# Reports the health of every Docker Compose service.
# Long-running services are judged by their Docker HEALTHCHECK status.
# One-shot init services (no healthcheck) are judged by their exit code.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Checking health of all Docker Compose services..."
echo

all_healthy=true

for service in $(docker compose ps --services); do
    container_id=$(docker compose ps -q "$service")

    if [ -z "$container_id" ]; then
        printf "  %-20s %s\n" "$service" "NOT RUNNING"
        all_healthy=false
        continue
    fi

    has_healthcheck=$(docker inspect --format='{{if .State.Health}}yes{{else}}no{{end}}' "$container_id")

    if [ "$has_healthcheck" = "yes" ]; then
        status=$(docker inspect --format='{{.State.Health.Status}}' "$container_id")
    else
        # One-shot services such as kafka-init and airflow-init have no
        # healthcheck - judge them by whether they exited cleanly instead.
        state=$(docker inspect --format='{{.State.Status}}' "$container_id")
        exit_code=$(docker inspect --format='{{.State.ExitCode}}' "$container_id")
        if [ "$state" = "exited" ] && [ "$exit_code" = "0" ]; then
            status="completed"
        elif [ "$state" = "running" ]; then
            status="running"
        else
            status="failed (exit $exit_code)"
        fi
    fi

    printf "  %-20s %s\n" "$service" "$status"

    case "$status" in
        healthy|completed|running) ;;
        *) all_healthy=false ;;
    esac
done

echo
if [ "$all_healthy" = true ]; then
    echo "All services are healthy."
    exit 0
else
    echo "One or more services are NOT healthy - see above."
    exit 1
fi
