"""DAG 03: start_spark_streaming.

Manually triggered, once. Turns on control.pipeline_switches.spark_enabled
so streaming/launcher.sh submits the Spark job, then verifies a new row
appears in dq.stream_batches (i.e. a real micro-batch actually ran).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

import _common

DAG_ID = "03_start_spark_streaming"
# Spark needs a few seconds to pick up the switch, resolve --packages on
# first run, and complete at least one 5s micro-batch.
GROWTH_CHECK_WAIT_SECONDS = 40

default_args = {"owner": "bluesky-platform", "retries": 2}


def _latest_batch_time():
    with _common.get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT max(created_at) FROM dq.stream_batches")
            return cur.fetchone()[0]


def enable_spark_streaming(ti=None, **_) -> None:
    before = _latest_batch_time()
    if ti:
        ti.xcom_push(key="batch_time_before", value=before.isoformat() if before else None)
    _common.set_switch("spark_enabled", True)
    _common.log_pipeline_run(DAG_ID, "enable_spark_streaming", "success")


def check_warehouse_growth(ti=None, **_) -> bool:
    before_str = ti.xcom_pull(task_ids="enable_spark_streaming", key="batch_time_before") if ti else None
    after = _latest_batch_time()
    grew = after is not None and (before_str is None or after.isoformat() > before_str)
    if grew:
        _common.log_pipeline_run(DAG_ID, "check_warehouse_growth", "success", f"{before_str} -> {after}")
    return grew


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
    check = PythonSensor(
        task_id="check_warehouse_growth",
        python_callable=check_warehouse_growth,
        mode="reschedule",
        poke_interval=10,
        timeout=120,
    )
    enable >> check
