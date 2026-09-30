"""DAG 01: start_ingestion.

Manually triggered, once. Turns on control.pipeline_switches.ingestion_enabled
so the ingestor starts writing to source-db, then self-checks that the
ingestion_cursor actually advanced (i.e. new events were really written).

Deliberate-failure demo (Phase 1 requirement): set FORCE_INGESTION_FAILURE=true
to make enable_ingestion() fail (no retries) instead of flipping the switch:
    docker compose exec -e FORCE_INGESTION_FAILURE=true airflow-scheduler \
        airflow dags test 01_start_ingestion
`dags test`, not `dags trigger`: trigger only queues the run, and the task
then executes in the scheduler's process, which never sees the -e variable.
The downstream check_source_db_growth task then shows "upstream_failed"
and the whole DAG run is marked failed - see docs/RUNBOOK.md.
"""

from __future__ import annotations

import os
import time
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

import _common

DAG_ID = "01_start_ingestion"
# Two ingestor write cycles (5s each), plus a margin, before we judge growth.
GROWTH_CHECK_WAIT_SECONDS = 15

default_args = {
    "owner": "bluesky-platform",
    "retries": 2,
}


def enable_ingestion() -> None:
    if os.environ.get("FORCE_INGESTION_FAILURE", "false").lower() == "true":
        _common.log_pipeline_run(
            DAG_ID, "enable_ingestion", "failed", "FORCE_INGESTION_FAILURE=true (deliberate demo failure)"
        )
        # AirflowFailException skips the 2 retries (5 min apart) - retrying a
        # forced failure can only fail again.
        raise AirflowFailException(
            "Deliberate failure: FORCE_INGESTION_FAILURE=true is set. "
            "Unset it (or omit -e) to run normally."
        )
    _common.set_switch("ingestion_enabled", True)
    _common.log_pipeline_run(DAG_ID, "enable_ingestion", "success")


def check_source_db_growth() -> None:
    before = _common.get_ingestion_cursor()
    time.sleep(GROWTH_CHECK_WAIT_SECONDS)
    after = _common.get_ingestion_cursor()

    grew = after is not None and (before is None or after > before)
    status = "success" if grew else "failed"
    _common.log_pipeline_run(
        DAG_ID, "check_source_db_growth", status, f"cursor {before} -> {after}"
    )
    if not grew:
        raise RuntimeError(f"ingestion_cursor did not advance: {before} -> {after}")


with DAG(
    dag_id=DAG_ID,
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
