"""DAG 04: dbt_transform.

Runs on a schedule (DBT_RUN_INTERVAL_MINUTES, default 5 minutes):
dbt source freshness (warns only, never fails the DAG), then dbt snapshot,
dbt run, and dbt test. If tests fail, the DAG fails and marts are not
refreshed.
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
    # dbt lives in its own virtualenv (infra/airflow/Dockerfile, Stage 2) so
    # its dependencies never collide with Airflow's own.
    DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"

    # dbt source freshness only warns (warn_after is set, error_after is
    # not - see models/staging/_sources.yml), so this never fails the DAG.
    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command="cd " + DBT_PROJECT_DIR + " && " + DBT_BIN + " source freshness || true",
    )
    snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd " + DBT_PROJECT_DIR + " && " + DBT_BIN + " snapshot",
    )
    run = BashOperator(
        task_id="dbt_run",
        bash_command="cd " + DBT_PROJECT_DIR + " && " + DBT_BIN + " run",
    )
    test = BashOperator(
        task_id="dbt_test",
        bash_command="cd " + DBT_PROJECT_DIR + " && " + DBT_BIN + " test",
    )
    source_freshness >> snapshot >> run >> test
