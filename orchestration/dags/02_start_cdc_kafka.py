"""DAG 02: start_cdc_kafka.

Manually triggered, once. Registers the Debezium connector (idempotent -
skips if it already exists) and verifies it reaches the RUNNING state with
its task also RUNNING, meaning CDC messages should now be flowing into
Kafka topics (one per source-db table, see ingestion/cdc/debezium-source.json).
"""

from __future__ import annotations

import json
import os
import string
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

import _common

DAG_ID = "02_start_cdc_kafka"
CONNECTOR_NAME = "source-db-connector"
CONNECTOR_CONFIG_PATH = Path("/opt/airflow/cdc/debezium-source.json")
STATUS_CHECK_RETRIES = 10
STATUS_CHECK_DELAY_SECONDS = 3

default_args = {"owner": "bluesky-platform", "retries": 2}


def _connect_url(path: str) -> str:
    base = os.environ["KAFKA_CONNECT_URL"].rstrip("/")
    return f"{base}{path}"


def _connector_exists() -> bool:
    try:
        urllib.request.urlopen(_connect_url(f"/connectors/{CONNECTOR_NAME}"), timeout=10)
        return True
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise


def register_connector() -> None:
    if _connector_exists():
        _common.log_pipeline_run(DAG_ID, "register_connector", "skipped", "connector already exists")
        return

    template = string.Template(CONNECTOR_CONFIG_PATH.read_text())
    rendered = template.substitute(os.environ)
    payload = json.dumps(json.loads(rendered)).encode("utf-8")

    request = urllib.request.Request(
        _connect_url("/connectors"),
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(request, timeout=15)
    _common.log_pipeline_run(DAG_ID, "register_connector", "success")


def check_connector_running() -> None:
    detail = ""
    for _ in range(STATUS_CHECK_RETRIES):
        with urllib.request.urlopen(
            _connect_url(f"/connectors/{CONNECTOR_NAME}/status"), timeout=10
        ) as response:
            status = json.loads(response.read())

        connector_state = status.get("connector", {}).get("state")
        task_states = [task.get("state") for task in status.get("tasks", [])]
        detail = f"connector={connector_state} tasks={task_states}"

        if connector_state == "RUNNING" and task_states and all(s == "RUNNING" for s in task_states):
            _common.log_pipeline_run(DAG_ID, "check_connector_running", "success", detail)
            return

        time.sleep(STATUS_CHECK_DELAY_SECONDS)

    _common.log_pipeline_run(DAG_ID, "check_connector_running", "failed", detail)
    raise RuntimeError(f"connector not healthy after {STATUS_CHECK_RETRIES} attempts: {detail}")


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
