"""Phase 1 integration smoke test.

Hits the REAL running services (docker compose, not mocks) and checks
that one logical date's data went through every layer:
raw -> staging -> curated -> serving, and that Airflow recorded the
pipeline run for that date as a success.

Run with: make smoke                                (today, UTC)
          make smoke SMOKE_LOGICAL_DATE=2026-09-29  (another date)

Prerequisites: the stack is up (`make up`) and the pipeline has run for
the date (`make pipeline DATE=<date>`).

Connection notes: this test runs on the HOST (outside any container), so
it uses the *published* ports (warehouse-db 127.0.0.1:5433, Grafana
127.0.0.1:3000), not the in-network hosts services use. Database names,
users and passwords come from .env (make smoke loads it); override the
host side with SMOKE_WAREHOUSE_HOST/SMOKE_WAREHOUSE_PORT/SMOKE_GRAFANA_URL.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import urllib.request
from datetime import datetime, timezone

import psycopg2

LOGICAL_DATE = os.environ.get("SMOKE_LOGICAL_DATE") or datetime.now(timezone.utc).date().isoformat()
GRAFANA_URL = os.environ.get("SMOKE_GRAFANA_URL", "http://localhost:3000").rstrip("/")
PIPELINE_DAG_ID = "07_daily_batch_report"


def _warehouse_conn():
    return psycopg2.connect(
        host=os.environ.get("SMOKE_WAREHOUSE_HOST", "localhost"),
        port=os.environ.get("SMOKE_WAREHOUSE_PORT", "5433"),
        dbname=os.environ["WAREHOUSE_DB_NAME"],
        user=os.environ["WAREHOUSE_DB_USER"],
        password=os.environ["WAREHOUSE_DB_PASSWORD"],
    )


def _count(sql: str) -> int:
    with _warehouse_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (LOGICAL_DATE,))
        return cur.fetchone()[0]


def _raw_posts() -> int:
    return _count("SELECT count(*) FROM raw.posts WHERE occurred_at::date = %s")


def test_raw_layer_has_data_for_logical_date():
    count = _raw_posts()
    print(f"\n[smoke] raw.posts rows for {LOGICAL_DATE}: {count}")
    assert count > 0, f"raw.posts has no rows for {LOGICAL_DATE} - has the pipeline run for it?"


def test_staging_layer_has_data_for_logical_date():
    count = _count("SELECT count(*) FROM staging.stg_posts WHERE occurred_date = %s")
    print(f"\n[smoke] staging.stg_posts rows for {LOGICAL_DATE}: {count}")
    assert count > 0, f"staging.stg_posts has no rows for {LOGICAL_DATE}"


def test_curated_layer_has_data_for_logical_date():
    activity = _count("SELECT count(*) FROM marts.fct_user_daily_activity WHERE activity_date = %s")
    summary = _count("SELECT count(*) FROM marts.mart_daily_summary WHERE report_date = %s")
    print(
        f"\n[smoke] marts.fct_user_daily_activity rows for {LOGICAL_DATE}: {activity}, "
        f"marts.mart_daily_summary rows: {summary}"
    )
    assert activity > 0, f"marts.fct_user_daily_activity has no rows for {LOGICAL_DATE}"
    # Exactly one: delete-then-insert on report_date, so re-runs never duplicate.
    assert summary == 1, f"expected exactly one mart_daily_summary row for {LOGICAL_DATE}, got {summary}"


def test_serving_layer_is_queryable_through_grafana():
    """Query the published mart through Grafana's own datasource API - the
    same path the dashboard panel "Daily batch summary" uses."""
    body = json.dumps(
        {
            "from": "now-1h",
            "to": "now",
            "queries": [
                {
                    "refId": "A",
                    "datasource": {"type": "postgres", "uid": "warehouse-db-postgres"},
                    "format": "table",
                    "rawSql": (
                        "SELECT report_date, total_posts, active_users FROM marts.mart_daily_summary "
                        f"WHERE report_date = '{LOGICAL_DATE}'"
                    ),
                }
            ],
        }
    ).encode("utf-8")
    credentials = f"{os.environ['GRAFANA_ADMIN_USER']}:{os.environ['GRAFANA_ADMIN_PASSWORD']}"
    request = urllib.request.Request(
        f"{GRAFANA_URL}/api/ds/query",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic " + base64.b64encode(credentials.encode()).decode(),
        },
    )
    with urllib.request.urlopen(request, timeout=10) as resp:
        assert resp.status == 200, f"Grafana query returned {resp.status}"
        result = json.loads(resp.read())

    frame = result["results"]["A"]["frames"][0]
    columns = frame["data"]["values"]
    rows = len(columns[0]) if columns else 0
    print(f"\n[smoke] Grafana /api/ds/query -> {rows} row(s): {columns}")
    assert rows == 1, f"Grafana returned {rows} rows of mart_daily_summary for {LOGICAL_DATE}"


def test_row_counts_reconcile():
    raw_count = _raw_posts()
    staging_count = _count("SELECT count(*) FROM staging.stg_posts WHERE occurred_date = %s")
    marts_count = _count("SELECT count(*) FROM marts.fct_posts WHERE occurred_date = %s")
    summary_posts = _count("SELECT coalesce(max(total_posts), 0) FROM marts.mart_daily_summary WHERE report_date = %s")

    print(
        f"\n[smoke] row counts for {LOGICAL_DATE} -> raw.posts={raw_count} "
        f"staging.stg_posts={staging_count} marts.fct_posts={marts_count} "
        f"mart_daily_summary.total_posts={summary_posts} (marts lag: {raw_count - marts_count} rows)"
    )
    # staging.stg_posts is an unfiltered VIEW over raw.posts: always equal.
    assert staging_count == raw_count, "staging.stg_posts should mirror raw.posts exactly"
    # marts.fct_posts is a TABLE refreshed only when dbt runs, while ingestion
    # keeps writing to raw.posts in between, so for a date still receiving
    # data marts can only trail raw - never exceed it (no filtering in
    # fct_posts.sql). For a closed past date they are equal.
    assert marts_count <= raw_count, "marts.fct_posts has MORE rows than raw.posts - should be impossible"


def test_orchestrator_run_succeeded():
    """Ask Airflow itself for the state of the pipeline run for this date."""
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "airflow-scheduler",
         "airflow", "dags", "state", PIPELINE_DAG_ID, LOGICAL_DATE],
        capture_output=True, text=True, timeout=120, check=True,
    )
    # Last line is the state, followed by ", <conf>" if the run has a conf.
    state = result.stdout.strip().splitlines()[-1].split(",")[0].strip()
    print(f"\n[smoke] Airflow run state of {PIPELINE_DAG_ID} for {LOGICAL_DATE}: {state}")
    assert state == "success", f"{PIPELINE_DAG_ID} run for {LOGICAL_DATE} is '{state}', expected 'success'"
