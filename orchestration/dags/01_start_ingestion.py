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
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

import _common

DAG_ID = "01_start_ingestion"
# Two ingestor write cycles (5s each), plus a margin, before we judge growth.
GROWTH_CHECK_WAIT_SECONDS = 15

default_args = {
    "owner": "bluesky-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def enable_ingestion(ti=None, **_) -> None:
    if os.environ.get("FORCE_INGESTION_FAILURE", "false").lower() == "true":
        _common.log_pipeline_run(
            DAG_ID, "enable_ingestion", "failed", "FORCE_INGESTION_FAILURE=true (deliberate demo failure)"
        )
        # AirflowFailException skips the 2 retries - retrying a
        # forced failure can only fail again.
        raise AirflowFailException(
            "Deliberate failure: FORCE_INGESTION_FAILURE=true is set. "
            "Unset it (or omit -e) to run normally."
        )
    if ti:
        ti.xcom_push(key="cursor_before", value=_common.get_ingestion_cursor())
    _common.set_switch("ingestion_enabled", True)
    _common.log_pipeline_run(DAG_ID, "enable_ingestion", "success")


def check_source_db_growth(ti=None, **_) -> bool:
    before = ti.xcom_pull(task_ids="enable_ingestion", key="cursor_before") if ti else None
    after = _common.get_ingestion_cursor()
    grew = after is not None and (before is None or after > before)
    if grew:
        _common.log_pipeline_run(
            DAG_ID, "check_source_db_growth", "success", f"cursor {before} -> {after}"
        )
    return grew


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
    check = PythonSensor(
        task_id="check_source_db_growth",
        python_callable=check_source_db_growth,
        mode="reschedule",
        poke_interval=5,
        timeout=60,
    )
    enable >> check
