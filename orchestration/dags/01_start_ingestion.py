"""DAG 01: start_ingestion.

Manually triggered, once. Turns on control.pipeline_switches.ingestion_enabled
so the ingestor starts writing to source-db, then self-checks that source-db
row counts are actually increasing.

Full task logic lands in Stage 3 (Ingestion).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "bluesky-platform",
    "retries": 2,
}


def enable_ingestion() -> None:
    """TODO (Stage 3): UPDATE control.pipeline_switches SET ingestion_enabled
    = true, and write the run result to dq.pipeline_runs."""
    raise NotImplementedError("Implemented in Stage 3 (Ingestion)")


def check_source_db_growth() -> None:
    """TODO (Stage 3): confirm source-db row counts increased after
    enabling ingestion."""
    raise NotImplementedError("Implemented in Stage 3 (Ingestion)")


with DAG(
    dag_id="01_start_ingestion",
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
