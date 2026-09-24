"""Shared helpers for Airflow DAGs: warehouse-db/source-db connections,
control-switch updates and dq.pipeline_runs logging.

Plain module, no DAG object defined here - Airflow's DagFileProcessor
scans this file while looking for DAGs, finds none, and simply skips it.
"""

from __future__ import annotations

import os

import psycopg2


def get_warehouse_conn():
    return psycopg2.connect(
        host=os.environ["WAREHOUSE_DB_HOST"],
        port=os.environ.get("WAREHOUSE_DB_PORT", "5432"),
        dbname=os.environ["WAREHOUSE_DB_NAME"],
        user=os.environ["WAREHOUSE_DB_USER"],
        password=os.environ["WAREHOUSE_DB_PASSWORD"],
    )


def get_source_conn():
    return psycopg2.connect(
        host=os.environ["SOURCE_DB_HOST"],
        port=os.environ.get("SOURCE_DB_PORT", "5432"),
        dbname=os.environ["SOURCE_DB_NAME"],
        user=os.environ["SOURCE_DB_USER"],
        password=os.environ["SOURCE_DB_PASSWORD"],
    )


def set_switch(switch_name: str, is_enabled: bool) -> None:
    with get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE control.pipeline_switches
                SET is_enabled = %s, updated_at = now()
                WHERE switch_name = %s
                """,
                (is_enabled, switch_name),
            )
        conn.commit()


def get_ingestion_cursor() -> int | None:
    with get_source_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT last_time_us FROM ingestion_cursor WHERE id = 1")
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else None


def log_pipeline_run(dag_id: str, task_id: str, status: str, detail: str | None = None) -> None:
    with get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dq.pipeline_runs (dag_id, task_id, status, detail)
                VALUES (%s, %s, %s, %s)
                """,
                (dag_id, task_id, status, detail),
            )
        conn.commit()
