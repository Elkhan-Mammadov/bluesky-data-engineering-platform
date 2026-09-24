"""DAG 99: reset_demo.

On demand. Turns off all switches, deletes the Debezium connector and Kafka
topics, and truncates warehouse and source tables so a demo can start clean.

Full task logic lands in Stage 8 (Reliability & final check).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "bluesky-platform", "retries": 0}


def reset_everything() -> None:
    """TODO (Stage 8): switches off, delete connector and topics, truncate
    tables. Must be safe to run multiple times."""
    raise NotImplementedError("Implemented in Stage 8 (Reliability & final check)")


with DAG(
    dag_id="99_reset_demo",
    description="Reset the whole pipeline to a clean, empty state",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["maintenance", "demo"],
) as dag:
    PythonOperator(task_id="reset_everything", python_callable=reset_everything)
