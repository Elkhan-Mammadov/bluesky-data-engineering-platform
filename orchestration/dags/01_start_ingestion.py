"""DAG 01: start_ingestion.

Manually triggered, once. Turns on control.pipeline_switches.ingestion_enabled
so the ingestor starts writing to source-db, then self-checks that the
ingestion_cursor actually advanced (i.e. new events were really written).
"""

from __future__ import annotations

import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import _common

DAG_ID = "01_start_ingestion"
# Two ingestor write cycles (5s each), plus a margin, before we judge growth.
GROWTH_CHECK_WAIT_SECONDS = 15

default_args = {
    "owner": "bluesky-platform",
    "retries": 2,
}


def enable_ingestion() -> None:
    _common.set_switch("ingestion_enabled", True)
    _common.log_pipeline_run(DAG_ID, "enable_ingestion", "success")


def check_source_db_growth() -> None:
    before = _common.get_ingestion_cursor()
    time.sleep(GROWTH_CHECK_WAIT_SECONDS)
    after = _common.get_ingestion_cursor()

    grew = after is not None and (before is None or after > before)
    status = "success" if grew else "failed"
    _common.log_pipeline_run(
        DAG_ID, "check_source_db_growth", status, f"cursor {before} -> {after}"
    )
    if not grew:
        raise RuntimeError(f"ingestion_cursor did not advance: {before} -> {after}")


with DAG(
    dag_id=DAG_ID,
    description="Turn on ingestion and verify source-db is growing",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["control-switch", "ingestion"],
) as dag:
    enable = PythonOperator(task_id="enable_ingestion", python_callable=enable_ingestion)
    check = PythonOperator(task_id="check_source_db_growth", python_callable=check_source_db_growth)
    enable >> check
