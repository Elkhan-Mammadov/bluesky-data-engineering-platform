"""DAG 04: dbt_transform.

Runs on a schedule (DBT_RUN_INTERVAL_MINUTES, default 5 minutes):
dbt snapshot, then dbt run, then dbt test. If tests fail, the DAG fails and
marts are not refreshed.

Full task logic lands in Stage 6 (dbt).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"owner": "bluesky-platform", "retries": 1}

DBT_PROJECT_DIR = "/opt/airflow/transformation"
INTERVAL_MINUTES = int(os.getenv("DBT_RUN_INTERVAL_MINUTES", "5"))

with DAG(
    dag_id="04_dbt_transform",
    description="Run dbt snapshot, run and test on a schedule",
    schedule=timedelta(minutes=INTERVAL_MINUTES),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dbt"],
) as dag:
    # TODO (Stage 6): confirm the dbt executable/venv path once the Airflow
    # image (infra/airflow/Dockerfile) installs dbt in Stage 2.
    snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd " + DBT_PROJECT_DIR + " && dbt snapshot",
    )
    run = BashOperator(
        task_id="dbt_run",
        bash_command="cd " + DBT_PROJECT_DIR + " && dbt run",
    )
    test = BashOperator(
        task_id="dbt_test",
        bash_command="cd " + DBT_PROJECT_DIR + " && dbt test",
    )
    snapshot >> run >> test
