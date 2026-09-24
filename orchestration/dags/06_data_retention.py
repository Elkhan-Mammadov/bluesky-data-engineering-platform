"""DAG 06: data_retention.

Runs daily. Deletes rows older than DATA_RETENTION_DAYS (default 7) from
source-db and the warehouse's operational layers (raw, realtime,
dq.quarantine/stream_batches), then logs exactly how many rows were
deleted per table.

marts (and dq.pipeline_runs) are deliberately NOT pruned here: marts are
curated, aggregated business results (e.g. mart_trending_hashtags) meant
to be kept as analytical history, and pipeline_runs is the audit trail
that records this very DAG's own runs.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import _common

DAG_ID = "06_data_retention"

_SOURCE_TABLES = ["posts", "likes", "reposts", "follows", "blocks", "profile_updates"]
_RAW_TABLES = [f"raw.{t}" for t in _SOURCE_TABLES]
_TIMESTAMP_COLUMN_BY_TABLE = {table: "occurred_at" for table in _RAW_TABLES}
_TIMESTAMP_COLUMN_BY_TABLE.update({
    "realtime.event_counts_by_minute": "minute_bucket",
    "realtime.active_users_by_minute": "minute_bucket",
    "realtime.hashtag_counts_by_minute": "minute_bucket",
    "realtime.language_counts_by_minute": "minute_bucket",
    "dq.quarantine": "quarantined_at",
    "dq.stream_batches": "created_at",
})

default_args = {"owner": "bluesky-platform", "retries": 1}


def _retention_days() -> int:
    return int(os.environ.get("DATA_RETENTION_DAYS", "7"))


def _delete_old_rows(cur, table: str, timestamp_column: str, retention_days: int) -> int:
    cur.execute(
        f"DELETE FROM {table} WHERE {timestamp_column} < now() - interval '{retention_days} days'"
    )
    return cur.rowcount


def delete_old_data() -> None:
    retention_days = _retention_days()
    deleted_rows: dict[str, int] = {}

    with _common.get_source_conn() as conn:
        with conn.cursor() as cur:
            for table in _SOURCE_TABLES:
                deleted_rows[table] = _delete_old_rows(cur, table, "occurred_at", retention_days)
        conn.commit()

    with _common.get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            for table, ts_column in _TIMESTAMP_COLUMN_BY_TABLE.items():
                deleted_rows[table] = _delete_old_rows(cur, table, ts_column, retention_days)
        conn.commit()

    detail = f"retention={retention_days}d; deleted rows: " + ", ".join(
        f"{table}={count}" for table, count in deleted_rows.items()
    )
    _common.log_pipeline_run(DAG_ID, "delete_old_data", "success", detail)


with DAG(
    dag_id=DAG_ID,
    description="Delete data older than the configured retention window",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["maintenance"],
) as dag:
    PythonOperator(task_id="delete_old_data", python_callable=delete_old_data)
