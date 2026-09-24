"""DAG 03: start_spark_streaming.

Manually triggered, once. Turns on control.pipeline_switches.spark_enabled
so streaming/launcher.sh submits the Spark job, then verifies a new row
appears in dq.stream_batches (i.e. a real micro-batch actually ran).
"""

from __future__ import annotations

import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import _common

DAG_ID = "03_start_spark_streaming"
# Spark needs a few seconds to pick up the switch, resolve --packages on
# first run, and complete at least one 5s micro-batch.
GROWTH_CHECK_WAIT_SECONDS = 40

default_args = {"owner": "bluesky-platform", "retries": 2}


def enable_spark_streaming() -> None:
    _common.set_switch("spark_enabled", True)
    _common.log_pipeline_run(DAG_ID, "enable_spark_streaming", "success")


def _latest_batch_time():
    with _common.get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT max(created_at) FROM dq.stream_batches")
            return cur.fetchone()[0]


def check_warehouse_growth() -> None:
    before = _latest_batch_time()
    time.sleep(GROWTH_CHECK_WAIT_SECONDS)
    after = _latest_batch_time()

    grew = after is not None and (before is None or after > before)
    status = "success" if grew else "failed"
    _common.log_pipeline_run(DAG_ID, "check_warehouse_growth", status, f"{before} -> {after}")
    if not grew:
        raise RuntimeError(f"dq.stream_batches did not grow: {before} -> {after}")


with DAG(
    dag_id=DAG_ID,
    description="Turn on Spark streaming and verify the warehouse is growing",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["control-switch", "spark"],
) as dag:
    enable = PythonOperator(task_id="enable_spark_streaming", python_callable=enable_spark_streaming)
    check = PythonOperator(task_id="check_warehouse_growth", python_callable=check_warehouse_growth)
    enable >> check
