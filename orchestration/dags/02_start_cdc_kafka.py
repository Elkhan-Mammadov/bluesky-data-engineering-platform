"""DAG 02: start_cdc_kafka.

Manually triggered, once. Registers the Debezium connector (idempotent -
skips if it already exists) and verifies it reaches the RUNNING state with
its task also RUNNING, meaning CDC messages should now be flowing into
Kafka topics (one per source-db table, see ingestion/cdc/debezium-source.json).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import _common

DAG_ID = "02_start_cdc_kafka"

default_args = {"owner": "bluesky-platform", "retries": 2}


def register_connector() -> None:
    if _common.register_cdc_connector():
        _common.log_pipeline_run(DAG_ID, "register_connector", "success")
    else:
        _common.log_pipeline_run(DAG_ID, "register_connector", "skipped", "connector already exists")


def check_connector_running() -> None:
    try:
        detail = _common.wait_cdc_connector_running()
    except RuntimeError as exc:
        _common.log_pipeline_run(DAG_ID, "check_connector_running", "failed", str(exc))
        raise
    _common.log_pipeline_run(DAG_ID, "check_connector_running", "success", detail)


with DAG(
    dag_id=DAG_ID,
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
