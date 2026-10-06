"""Shared helpers for Airflow DAGs: warehouse-db/source-db connections,
control-switch updates, the Debezium connector, dq.pipeline_runs logging
and per-task run logging (task_run).

Plain module, no DAG object defined here - Airflow's DagFileProcessor
scans this file while looking for DAGs, finds none, and simply skips it.
"""

from __future__ import annotations

import json
import logging
import os
import string
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path

import psycopg2

log = logging.getLogger("airflow.task")

CDC_CONNECTOR_NAME = "source-db-connector"
CDC_CONNECTOR_CONFIG_PATH = Path("/opt/airflow/cdc/debezium-source.json")


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


@contextmanager
def task_run(dag_id: str, task_id: str, *, input_location: str, output_location: str):
    """Log a task's start, end, input/output location and rows read/written.

    The task fills in the yielded dict (rows_read / rows_written / note).
    Lines go to the task log (Airflow UI -> task -> Logs) and one summary
    row goes to dq.pipeline_runs, which Grafana's annotations read.
    """
    stats = {"rows_read": None, "rows_written": None, "note": None}
    started = time.monotonic()
    log.info("[%s] START input=%s output=%s", task_id, input_location, output_location)
    try:
        yield stats
    except Exception as exc:
        log.error("[%s] FAILED after %.1fs: %s", task_id, time.monotonic() - started, exc)
        log_pipeline_run(dag_id, task_id, "failed", f"input={input_location} output={output_location} error={exc}")
        raise
    summary = (
        f"input={input_location} output={output_location} "
        f"rows_read={stats['rows_read']} rows_written={stats['rows_written']}"
    )
    if stats["note"]:
        summary += f" {stats['note']}"
    log.info("[%s] END in %.1fs %s", task_id, time.monotonic() - started, summary)
    log_pipeline_run(dag_id, task_id, "success", summary)


def _connect_url(path: str) -> str:
    return os.environ["KAFKA_CONNECT_URL"].rstrip("/") + path


def cdc_connector_exists() -> bool:
    try:
        urllib.request.urlopen(_connect_url(f"/connectors/{CDC_CONNECTOR_NAME}"), timeout=10)
        return True
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise


def register_cdc_connector() -> bool:
    """Register the Debezium connector unless it exists. True if created."""
    if cdc_connector_exists():
        return False
    template = string.Template(CDC_CONNECTOR_CONFIG_PATH.read_text())
    payload = json.dumps(json.loads(template.substitute(os.environ))).encode("utf-8")
    request = urllib.request.Request(
        _connect_url("/connectors"),
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(request, timeout=15)
    return True


def wait_cdc_connector_running(attempts: int = 10, delay_seconds: int = 3) -> str:
    """Poll until the connector and all its tasks are RUNNING. Returns the
    last status line; raises RuntimeError if it never gets there."""
    detail = ""
    for _ in range(attempts):
        with urllib.request.urlopen(
            _connect_url(f"/connectors/{CDC_CONNECTOR_NAME}/status"), timeout=10
        ) as response:
            status = json.loads(response.read())
        connector_state = status.get("connector", {}).get("state")
        task_states = [task.get("state") for task in status.get("tasks", [])]
        detail = f"connector={connector_state} tasks={task_states}"
        if connector_state == "RUNNING" and task_states and all(s == "RUNNING" for s in task_states):
            return detail
        time.sleep(delay_seconds)
    raise RuntimeError(f"connector not healthy after {attempts} attempts: {detail}")
