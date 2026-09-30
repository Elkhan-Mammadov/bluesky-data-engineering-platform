"""Phase 1 integration smoke test.

Hits the REAL running services (docker compose, not mocks) and checks
that data actually flows raw -> staging -> curated -> serving for a
specific logical date.

Run with: make smoke   (equivalent to: pytest tests/smoke -v)

Prerequisites (see docs/RUNBOOK.md for the full sequence):
  - The stack is up (`make up`, `make health` all healthy).
  - DAGs 01/02/03 have run at least once (real data in raw/staging/marts).
  - DAG 07_daily_batch_report has run for today's date (the "logical
    date" this test checks by default; override with SMOKE_LOGICAL_DATE).

Connection notes: this test runs on the HOST (outside any container), so
it uses the *published* warehouse-db port (127.0.0.1:5433), not the
in-network host/port (`warehouse-db:5432`) that services inside
docker-compose use. Only the database name/user/password are shared with
.env - override host/port with SMOKE_WAREHOUSE_HOST/SMOKE_WAREHOUSE_PORT
if your setup differs.
"""

from __future__ import annotations

import os
import urllib.request
from datetime import date

import psycopg2

LOGICAL_DATE = os.environ.get("SMOKE_LOGICAL_DATE", date.today().isoformat())


def _warehouse_conn():
    return psycopg2.connect(
        host=os.environ.get("SMOKE_WAREHOUSE_HOST", "localhost"),
        port=os.environ.get("SMOKE_WAREHOUSE_PORT", "5433"),
        dbname=os.environ["WAREHOUSE_DB_NAME"],
        user=os.environ["WAREHOUSE_DB_USER"],
        password=os.environ["WAREHOUSE_DB_PASSWORD"],
    )


def _count(cur, sql: str, params: tuple = ()) -> int:
    cur.execute(sql, params)
    return cur.fetchone()[0]


def test_raw_layer_has_data():
    with _warehouse_conn() as conn, conn.cursor() as cur:
        count = _count(cur, "SELECT count(*) FROM raw.posts")
    print(f"[smoke] raw.posts row count: {count}")
    assert count > 0, "raw.posts is empty - has ingestion/CDC/Spark run yet?"


def test_staging_layer_has_data():
    with _warehouse_conn() as conn, conn.cursor() as cur:
        count = _count(cur, "SELECT count(*) FROM staging.stg_posts")
    print(f"[smoke] staging.stg_posts row count: {count}")
    assert count > 0, "staging.stg_posts is empty - has dbt run yet (make run-dbt)?"


def test_curated_layer_has_data_for_logical_date():
    """The "curated" layer, checked for a SPECIFIC logical date - this is
    what DAG 07_daily_batch_report (the Phase 1 batch slice) produces."""
    with _warehouse_conn() as conn, conn.cursor() as cur:
        count = _count(
            cur,
            "SELECT count(*) FROM marts.mart_daily_summary WHERE report_date = %s",
            (LOGICAL_DATE,),
        )
    print(f"[smoke] marts.mart_daily_summary rows for {LOGICAL_DATE}: {count}")
    assert count == 1, (
        f"expected exactly one mart_daily_summary row for {LOGICAL_DATE} - "
        "has DAG 07_daily_batch_report run for this date? "
        "(docker compose exec airflow-scheduler airflow dags trigger 07_daily_batch_report)"
    )


def test_serving_layer_is_queryable():
    """Grafana is the consumer-facing serving layer; its own datasource is
    the same warehouse-db we already checked above."""
    with urllib.request.urlopen("http://localhost:3000/api/health", timeout=5) as resp:
        assert resp.status == 200, "Grafana health check did not return 200"
    print("[smoke] Grafana /api/health: 200 OK")


def test_row_counts_reconcile_and_orchestrator_run_succeeded():
    """Report row counts per layer together, and confirm Airflow itself
    recorded a successful DAG 07 run - the orchestrator's own proof that
    the batch slice finished successfully."""
    with _warehouse_conn() as conn, conn.cursor() as cur:
        raw_count = _count(cur, "SELECT count(*) FROM raw.posts")
        staging_count = _count(cur, "SELECT count(*) FROM staging.stg_posts")
        marts_count = _count(cur, "SELECT count(*) FROM marts.fct_posts")
        successful_batch_runs = _count(
            cur,
            "SELECT count(*) FROM dq.pipeline_runs WHERE dag_id = %s AND status = 'success'",
            ("07_daily_batch_report",),
        )

    lag = raw_count - marts_count
    print(
        "[smoke] row counts -> "
        f"raw.posts={raw_count} staging.stg_posts={staging_count} marts.fct_posts={marts_count} "
        f"(marts lag: {lag} rows - explained below)"
    )
    # staging.stg_posts is a VIEW with no filtering over raw.posts, so it
    # always reflects the exact same live count.
    assert staging_count == raw_count, "staging.stg_posts should mirror raw.posts exactly"
    # marts.fct_posts is a materialized TABLE, only refreshed when dbt runs
    # (DAG 04, every DBT_RUN_INTERVAL_MINUTES). Ingestion keeps writing to
    # raw.posts continuously in between runs, so marts can only ever be
    # <= raw at this instant, lagging by however many rows arrived since
    # the last dbt run - never more, never a mismatch in the other
    # direction (fct_posts.sql applies no filtering either).
    assert marts_count <= raw_count, (
        "marts.fct_posts has MORE rows than raw.posts - that should be impossible "
        "since fct_posts.sql applies no filtering on top of stg_posts"
    )
    assert successful_batch_runs > 0, "no successful 07_daily_batch_report run found in dq.pipeline_runs"
