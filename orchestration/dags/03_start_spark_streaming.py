"""DAG 03: start_spark_streaming.

Manually triggered, once. Turns on control.pipeline_switches.spark_enabled
so streaming/launcher.sh submits the Spark job, then verifies the warehouse
raw schema is growing.

Full task logic lands in Stage 5 (Spark streaming).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "bluesky-platform", "retries": 2}


def enable_spark_streaming() -> None:
    """TODO (Stage 5): UPDATE control.pipeline_switches SET spark_enabled =
    true."""
    raise NotImplementedError("Implemented in Stage 5 (Spark streaming)")


def check_warehouse_growth() -> None:
    """TODO (Stage 5): confirm warehouse raw schema row counts increased."""
    raise NotImplementedError("Implemented in Stage 5 (Spark streaming)")


with DAG(
    dag_id="03_start_spark_streaming",
    description="Turn on Spark streaming and verify the warehouse is growing",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["control-switch", "spark"],
) as dag:
    enable = PythonOperator(task_id="enable_spark_streaming", python_callable=enable_spark_streaming)
    check = PythonOperator(task_id="check_warehouse_growth", python_callable=check_warehouse_growth)
    enable >> check
