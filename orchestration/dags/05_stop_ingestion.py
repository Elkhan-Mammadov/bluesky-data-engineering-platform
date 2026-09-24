"""DAG 05: stop_ingestion.

On demand. Turns off control.pipeline_switches.ingestion_enabled so the
ingestor idles and source-db growth stops.

Full task logic lands in Stage 3 (Ingestion).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "bluesky-platform", "retries": 2}


def disable_ingestion() -> None:
    """TODO (Stage 3): UPDATE control.pipeline_switches SET
    ingestion_enabled = false."""
    raise NotImplementedError("Implemented in Stage 3 (Ingestion)")


with DAG(
    dag_id="05_stop_ingestion",
    description="Turn off ingestion",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["control-switch", "ingestion"],
) as dag:
    PythonOperator(task_id="disable_ingestion", python_callable=disable_ingestion)
