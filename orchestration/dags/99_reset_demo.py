"""DAG 99: reset_demo.

On demand. Turns off both control switches, deletes the Debezium
connector, and truncates every data table (source-db and every warehouse
schema except dim_date, which is a static calendar, not activity data) so
a demo can start from a genuinely empty state. Safe to run more than once
- missing tables/connectors are skipped rather than raising.

Kafka topic contents are NOT purged here: that needs a Kafka admin client
(e.g. kafka-python), which isn't part of this project's approved Python
dependencies. `make clean` removes them entirely, along with every other
volume, if a fully empty Kafka is needed too.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.request
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import _common

DAG_ID = "99_reset_demo"
CONNECTOR_NAME = "source-db-connector"

_SOURCE_TABLES = ["users", "posts", "likes", "reposts", "follows", "blocks", "profile_updates"]

_WAREHOUSE_TABLES = [
    "raw.users", "raw.posts", "raw.likes", "raw.reposts", "raw.follows", "raw.blocks", "raw.profile_updates",
    "realtime.event_counts_by_minute", "realtime.active_users_by_minute",
    "realtime.hashtag_counts_by_minute", "realtime.language_counts_by_minute",
    "dq.quarantine", "dq.stream_batches",
    "snapshots.dim_user_snapshot",
    "marts.dim_user", "marts.dim_hashtag", "marts.dim_language",
    "marts.fct_posts", "marts.fct_interactions", "marts.fct_user_daily_activity",
    "marts.mart_engagement", "marts.mart_trending_hashtags", "marts.mart_anomalous_accounts",
]

default_args = {"owner": "bluesky-platform", "retries": 0}


def _connect_url(path: str) -> str:
    base = os.environ["KAFKA_CONNECT_URL"].rstrip("/")
    return f"{base}{path}"


def _truncate_if_exists(cur, table: str) -> bool:
    """Truncate `table` if it exists; return whether it did. Safe to call
    on a table that hasn't been created yet (e.g. dbt never ran)."""
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (table,))
    exists = cur.fetchone()[0]
    if exists:
        cur.execute(f"TRUNCATE TABLE {table} CASCADE")
    return exists


def turn_off_switches() -> None:
    _common.set_switch("ingestion_enabled", False)
    _common.set_switch("spark_enabled", False)


def delete_connector() -> None:
    request = urllib.request.Request(_connect_url(f"/connectors/{CONNECTOR_NAME}"), method="DELETE")
    try:
        urllib.request.urlopen(request, timeout=15)
    except urllib.error.HTTPError as exc:
        if exc.code != 404:  # already gone - fine, DAG must be safe to re-run
            raise


def truncate_source_tables() -> None:
    with _common.get_source_conn() as conn:
        with conn.cursor() as cur:
            for table in _SOURCE_TABLES:
                _truncate_if_exists(cur, table)
            cur.execute("UPDATE ingestion_cursor SET last_time_us = NULL, updated_at = now()")
        conn.commit()


def truncate_warehouse_tables() -> None:
    with _common.get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            for table in _WAREHOUSE_TABLES:
                _truncate_if_exists(cur, table)
        conn.commit()
    _common.log_pipeline_run(DAG_ID, "reset_everything", "success")


with DAG(
    dag_id=DAG_ID,
    description="Reset the whole pipeline to a clean, empty state",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["maintenance", "demo"],
) as dag:
    switches = PythonOperator(task_id="turn_off_switches", python_callable=turn_off_switches)
    connector = PythonOperator(task_id="delete_connector", python_callable=delete_connector)
    source_tables = PythonOperator(task_id="truncate_source_tables", python_callable=truncate_source_tables)
    warehouse_tables = PythonOperator(task_id="truncate_warehouse_tables", python_callable=truncate_warehouse_tables)

    switches >> connector >> source_tables >> warehouse_tables
