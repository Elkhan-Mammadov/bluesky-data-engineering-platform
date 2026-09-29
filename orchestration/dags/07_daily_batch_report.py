"""DAG 07: daily_batch_report.

Phase 1 requirement: a genuinely BATCH, logical-date-parameterized slice,
separate from this platform's core streaming pipeline. See
docs/decisions/0001-streaming-vs-batch-architecture.md for why the core
pipeline (DAGs 01-06, 99) is deliberately NOT date-partitioned - it is a
continuous stream, not a daily batch load.

For the Airflow logical date this run is for (the `ds` template, e.g.
"2026-09-29"), (re)computes exactly one row of marts.mart_daily_summary
via delete-then-insert:
  - Running this DAG twice for the same date never creates duplicates
    (the DELETE removes the old row for that date first).
  - Running it for two different dates never overwrites each other
    (each DELETE/INSERT only touches its own report_date).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import _common

DAG_ID = "07_daily_batch_report"

default_args = {
    "owner": "bluesky-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def ensure_table_exists() -> None:
    with _common.get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS marts.mart_daily_summary (
                    report_date   DATE PRIMARY KEY,
                    total_posts   INTEGER NOT NULL,
                    total_likes   INTEGER NOT NULL,
                    total_reposts INTEGER NOT NULL,
                    total_follows INTEGER NOT NULL,
                    total_blocks  INTEGER NOT NULL,
                    active_users  INTEGER NOT NULL,
                    computed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
        conn.commit()


def compute_daily_summary(ds: str, **_context) -> None:
    """ds = Airflow's logical date for this run (e.g. "2026-09-29"),
    templated by Airflow from the DAG run's data interval."""
    with _common.get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    coalesce(sum(post_count), 0),
                    coalesce(sum(like_count), 0),
                    coalesce(sum(repost_count), 0),
                    coalesce(sum(follow_count), 0),
                    coalesce(sum(block_count), 0),
                    count(*) FILTER (WHERE total_activity_count > 0)
                FROM marts.fct_user_daily_activity
                WHERE activity_date = %s
                """,
                (ds,),
            )
            totals = cur.fetchone()

            # Delete-then-insert on the report_date key: the idempotency
            # pattern this task deliberately uses (see module docstring).
            cur.execute("DELETE FROM marts.mart_daily_summary WHERE report_date = %s", (ds,))
            cur.execute(
                """
                INSERT INTO marts.mart_daily_summary
                    (report_date, total_posts, total_likes, total_reposts,
                     total_follows, total_blocks, active_users)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (ds, *totals),
            )
        conn.commit()

    _common.log_pipeline_run(DAG_ID, "compute_daily_summary", "success", f"report_date={ds}")


with DAG(
    dag_id=DAG_ID,
    description="Daily batch aggregation into marts.mart_daily_summary, parameterized by logical date",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["batch", "phase-1"],
) as dag:
    ensure_table = PythonOperator(task_id="ensure_table_exists", python_callable=ensure_table_exists)
    compute = PythonOperator(
        task_id="compute_daily_summary",
        python_callable=compute_daily_summary,
        op_kwargs={"ds": "{{ ds }}"},
    )
    ensure_table >> compute
