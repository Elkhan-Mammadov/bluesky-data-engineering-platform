"""DAG 04: dbt_transform.

Runs on a schedule (DBT_RUN_INTERVAL_MINUTES, default 5 minutes):
dbt source freshness (warns only, never fails the DAG), then a two-phase
run/snapshot/run, then dbt test. If tests fail, the DAG fails and marts
are not refreshed.

Why two `dbt run` phases: marts.dim_user is built FROM the
dim_user_snapshot snapshot, but that snapshot's own source
(intermediate.int_user_activity_summary) only exists after `dbt run` has
built it. Running `dbt snapshot` before any `dbt run` fails on a fresh
warehouse ("relation intermediate.int_user_activity_summary does not
exist"). So: run everything except dim_user, then snapshot (its source
now exists), then run just dim_user (its source, the snapshot, now
exists too).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"owner": "bluesky-platform", "retries": 1}

DBT_PROJECT_DIR = "/opt/airflow/transformation"
INTERVAL_MINUTES = int(os.getenv("DBT_RUN_INTERVAL_MINUTES", "5"))
# Shared one-slot pool with DAG 07's dbt tasks (created by airflow-init):
# two dbt invocations rebuilding the same tables at once fail on the swap.

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
        pool="dbt",
        bash_command="cd " + DBT_PROJECT_DIR + " && " + DBT_BIN + " source freshness || true",
    )
    run_before_snapshot = BashOperator(
        task_id="dbt_run_before_snapshot",
        pool="dbt",
        bash_command="cd " + DBT_PROJECT_DIR + " && " + DBT_BIN + " run --exclude dim_user",
    )
    snapshot = BashOperator(
        task_id="dbt_snapshot",
        pool="dbt",
        bash_command="cd " + DBT_PROJECT_DIR + " && " + DBT_BIN + " snapshot",
    )
    run_dim_user = BashOperator(
        task_id="dbt_run_dim_user",
        pool="dbt",
        bash_command="cd " + DBT_PROJECT_DIR + " && " + DBT_BIN + " run --select dim_user",
    )
    test = BashOperator(
        task_id="dbt_test",
        pool="dbt",
        bash_command="cd " + DBT_PROJECT_DIR + " && " + DBT_BIN + " test",
    )
    source_freshness >> run_before_snapshot >> snapshot >> run_dim_user >> test
