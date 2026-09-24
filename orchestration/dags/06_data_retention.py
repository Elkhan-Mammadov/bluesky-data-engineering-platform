"""DAG 06: data_retention.

Runs daily. Deletes data older than DATA_RETENTION_DAYS (default 7) from the
warehouse and verifies row counts dropped.

Full task logic lands in Stage 8 (Reliability & final check).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "bluesky-platform", "retries": 1}


def delete_old_data() -> None:
    """TODO (Stage 8): delete rows older than DATA_RETENTION_DAYS across
    raw, staging and marts as appropriate."""
    raise NotImplementedError("Implemented in Stage 8 (Reliability & final check)")


with DAG(
    dag_id="06_data_retention",
    description="Delete data older than the configured retention window",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["maintenance"],
) as dag:
    PythonOperator(task_id="delete_old_data", python_callable=delete_old_data)
