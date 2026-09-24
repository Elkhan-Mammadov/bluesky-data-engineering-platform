"""DAG 02: start_cdc_kafka.

Manually triggered, once. Registers the Debezium connector (idempotent,
skips if it already exists) and verifies it reaches the RUNNING state with
messages flowing into Kafka topics.

Full task logic lands in Stage 4 (CDC & Kafka).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "bluesky-platform", "retries": 2}


def register_connector() -> None:
    """TODO (Stage 4): POST ingestion/cdc/debezium-source.json to Kafka
    Connect, skipping if a connector with the same name already exists."""
    raise NotImplementedError("Implemented in Stage 4 (CDC & Kafka)")


def check_connector_running() -> None:
    """TODO (Stage 4): GET connector status and confirm RUNNING state plus
    messages present in the source-db topics."""
    raise NotImplementedError("Implemented in Stage 4 (CDC & Kafka)")


with DAG(
    dag_id="02_start_cdc_kafka",
    description="Register the Debezium connector and verify CDC is flowing",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["control-switch", "cdc"],
) as dag:
    register = PythonOperator(task_id="register_connector", python_callable=register_connector)
    check = PythonOperator(task_id="check_connector_running", python_callable=check_connector_running)
    register >> check
