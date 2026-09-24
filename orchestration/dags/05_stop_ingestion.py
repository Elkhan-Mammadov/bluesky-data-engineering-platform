"""DAG 05: stop_ingestion.

On demand. Turns off control.pipeline_switches.ingestion_enabled so the
ingestor idles (stays connected, stops writing) and source-db growth stops.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import _common

DAG_ID = "05_stop_ingestion"

default_args = {"owner": "bluesky-platform", "retries": 2}


def disable_ingestion() -> None:
    _common.set_switch("ingestion_enabled", False)
    _common.log_pipeline_run(DAG_ID, "disable_ingestion", "success")


with DAG(
    dag_id=DAG_ID,
    description="Turn off ingestion",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["control-switch", "ingestion"],
) as dag:
    PythonOperator(task_id="disable_ingestion", python_callable=disable_ingestion)
