"""DAG 07: daily_batch_report - the Phase 1 end-to-end batch pipeline.

One DAG, parameterized by Airflow's logical date (`ds`), that walks one
day's partition through every component, each hop a separate task:

  ingest     start_ingestion -> start_cdc
  load raw   start_spark -> check_source_partition -> load_raw_partition
  transform  dbt_run_before_snapshot -> dbt_snapshot -> dbt_run_dim_user
  quality    dbt_test -> check_partition_quality
  publish    publish_daily_summary  (marts.mart_daily_summary, shown in Grafana)

The data itself arrives continuously (Jetstream -> ingestor -> source-db
-> Debezium -> Kafka -> Spark -> raw.*), see
docs/decisions/0001-streaming-vs-batch-architecture.md. This DAG turns
those components on (idempotently - a no-op if they already run), then
proves that the partition for `ds` really reached source-db and raw.*,
transforms it with dbt, checks it and publishes it. Nothing is run by
hand between stages: a fresh clone needs only `make up` and one trigger.

Re-runnable: raw.* is upserted on its natural key, staging/marts are
rebuilt by dbt, and publish_daily_summary uses delete-then-insert on
report_date - re-running a date never duplicates, and two dates never
overwrite each other.

Deliberate failure (Phase 1 requirement), set in the run's conf so it
reaches the task whichever process runs it:
    airflow dags trigger 07_daily_batch_report -e <date> -c '{"force_ingestion_failure": true}'
start_ingestion fails immediately (no retries) and every downstream task
shows upstream_failed. `min_partition_rows` in the conf raises the
threshold of the row-count data quality check, to see a DQ failure fail
the run the same way. scripts/run_pipeline.sh wraps both.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import _common

DAG_ID = "07_daily_batch_report"
DBT_PROJECT_DIR = "/opt/airflow/transformation"
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
# One slot, shared with DAG 04: two dbt invocations rebuilding the same
# tables at once fail on the table swap. Created by airflow-init.
DBT_POOL = "dbt"

ENTITIES = ("posts", "likes", "reposts", "follows", "blocks")
POLL_SECONDS = 5
# Today's partition must receive rows within this long of ingestion being on.
SOURCE_WAIT_SECONDS = 90
# Spark micro-batches are 5s; a batch finishing this long after the source
# snapshot has read every CDC event that existed at snapshot time.
RAW_CATCH_UP_MARGIN_SECONDS = 15
RAW_CATCH_UP_TIMEOUT_SECONDS = 300

default_args = {
    "owner": "bluesky-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def _conf(dag_run) -> dict:
    return (dag_run.conf or {}) if dag_run else {}


def _is_true(value) -> bool:
    return value is True or str(value).lower() in ("true", "1", "yes")


def _partition_counts(conn, schema_prefix: str, ds: str) -> dict[str, int]:
    counts = {}
    with conn.cursor() as cur:
        for entity in ENTITIES:
            cur.execute(f"SELECT count(*) FROM {schema_prefix}{entity} WHERE occurred_at::date = %s", (ds,))
            counts[entity] = cur.fetchone()[0]
    return counts


def start_ingestion(dag_run=None, **_context) -> None:
    forced = _is_true(_conf(dag_run).get("force_ingestion_failure")) or _is_true(
        os.environ.get("FORCE_INGESTION_FAILURE", "false")
    )
    with _common.task_run(
        DAG_ID, "start_ingestion",
        input_location="Bluesky Jetstream (ingestor)", output_location="source-db",
    ) as stats:
        if forced:
            # AirflowFailException skips retries - a forced failure can only fail again.
            raise AirflowFailException(
                "Deliberate failure: force_ingestion_failure is set for this run."
            )
        _common.set_switch("ingestion_enabled", True)
        stats["note"] = "control.pipeline_switches.ingestion_enabled=true"


def start_cdc(**_context) -> None:
    with _common.task_run(
        DAG_ID, "start_cdc",
        input_location="source-db (logical replication)", output_location="Kafka topics bluesky.public.*",
    ) as stats:
        created = _common.register_cdc_connector()
        status = _common.wait_cdc_connector_running()
        stats["note"] = ("connector registered" if created else "connector already registered") + f", {status}"


def start_spark(**_context) -> None:
    with _common.task_run(
        DAG_ID, "start_spark",
        input_location="Kafka topics bluesky.public.*", output_location="warehouse-db raw.*",
    ) as stats:
        _common.set_switch("spark_enabled", True)
        stats["note"] = "control.pipeline_switches.spark_enabled=true"


def check_source_partition(ds: str, **_context) -> dict:
    """Count the logical date's rows in source-db. Today's partition must
    be receiving data; a past date may legitimately be empty (before the
    project ran, or past DATA_RETENTION_DAYS)."""
    is_today = ds == datetime.now(timezone.utc).date().isoformat()
    with _common.task_run(
        DAG_ID, "check_source_partition",
        input_location=f"source-db public.{{{','.join(ENTITIES)}}} occurred_at::date={ds}",
        output_location="XCom (partition row counts)",
    ) as stats:
        deadline = time.monotonic() + SOURCE_WAIT_SECONDS
        while True:
            with _common.get_source_conn() as conn:
                counts = _partition_counts(conn, "", ds)
            total = sum(counts.values())
            if total > 0 or not is_today or time.monotonic() > deadline:
                break
            time.sleep(POLL_SECONDS)
        if is_today and total == 0:
            raise RuntimeError(f"no rows for today ({ds}) in source-db after {SOURCE_WAIT_SECONDS}s - is ingestion running?")

        with _common.get_warehouse_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT now()")
            snapshot_at = cur.fetchone()[0]
        stats["rows_read"] = total
        stats["note"] = f"per_entity={counts}" + ("" if total else " (empty partition)")
    return {"counts": counts, "total": total, "snapshot_at": snapshot_at.isoformat()}


def load_raw_partition(ds: str, ti=None, **_context) -> None:
    """Wait until Spark has written a micro-batch that started after the
    source snapshot, then reconcile the partition's raw.* counts with it."""
    source = ti.xcom_pull(task_ids="check_source_partition")
    with _common.task_run(
        DAG_ID, "load_raw_partition",
        input_location="Kafka topics bluesky.public.* (via Spark)",
        output_location=f"warehouse-db raw.{{{','.join(ENTITIES)}}} occurred_at::date={ds}",
    ) as stats:
        deadline = time.monotonic() + RAW_CATCH_UP_TIMEOUT_SECONDS
        while True:
            with _common.get_warehouse_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT coalesce(max(created_at) > %s::timestamptz + %s * interval '1 second', false)"
                    " FROM dq.stream_batches",
                    (source["snapshot_at"], RAW_CATCH_UP_MARGIN_SECONDS),
                )
                caught_up = cur.fetchone()[0]
            if caught_up:
                break
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"Spark wrote no micro-batch within {RAW_CATCH_UP_TIMEOUT_SECONDS}s of the source snapshot"
                )
            time.sleep(POLL_SECONDS)

        with _common.get_warehouse_conn() as conn:
            counts = _partition_counts(conn, "raw.", ds)
        total = sum(counts.values())
        if source["total"] > 0 and total == 0:
            raise RuntimeError(f"source-db has {source['total']} rows for {ds} but raw.* has none")
        stats["rows_read"] = source["total"]
        stats["rows_written"] = total
        # raw can exceed the snapshot (rows that arrived since) or trail it
        # (deletes replicated since) - both are expected on a live stream.
        stats["note"] = f"per_entity={counts} difference_vs_source_snapshot={total - source['total']}"


def check_partition_quality(ds: str, ti=None, dag_run=None, **_context) -> None:
    """Three checks on the curated partition; any failure fails the run
    (on top of the dbt tests in dbt_test)."""
    source = ti.xcom_pull(task_ids="check_source_partition")
    min_rows = int(_conf(dag_run).get("min_partition_rows", 1 if source["total"] > 0 else 0))
    with _common.task_run(
        DAG_ID, "check_partition_quality",
        input_location=f"marts.fct_user_daily_activity activity_date={ds}",
        output_location="dq.pipeline_runs",
    ) as stats:
        with _common.get_warehouse_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*),
                       count(*) FILTER (WHERE user_id_hash IS NULL),
                       count(*) - count(DISTINCT user_id_hash)
                FROM marts.fct_user_daily_activity
                WHERE activity_date = %s
                """,
                (ds,),
            )
            rows, null_keys, duplicate_keys = cur.fetchone()
        stats["rows_read"] = rows
        checks = {
            f"row_count>={min_rows}": rows >= min_rows,
            "user_id_hash_not_null": null_keys == 0,
            "user_id_hash_unique_per_day": duplicate_keys == 0,
        }
        stats["note"] = f"checks={checks}"
        failed = [name for name, passed in checks.items() if not passed]
        if failed:
            raise AirflowFailException(
                f"data quality failed for {ds}: {failed} (rows={rows}, null_keys={null_keys}, duplicates={duplicate_keys})"
            )


def publish_daily_summary(ds: str, **_context) -> None:
    with _common.task_run(
        DAG_ID, "publish_daily_summary",
        input_location=f"marts.fct_user_daily_activity activity_date={ds}",
        output_location=f"marts.mart_daily_summary report_date={ds}",
    ) as stats:
        with _common.get_warehouse_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS marts.mart_daily_summary (
                        report_date   DATE PRIMARY KEY,
                        total_posts   INTEGER NOT NULL,
                        total_likes   INTEGER NOT NULL,
                        total_reposts INTEGER NOT NULL,
                        total_follows INTEGER NOT NULL,
                        total_blocks  INTEGER NOT NULL,
                        active_users  INTEGER NOT NULL,
                        computed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    """
                    SELECT
                        count(*),
                        coalesce(sum(post_count), 0),
                        coalesce(sum(like_count), 0),
                        coalesce(sum(repost_count), 0),
                        coalesce(sum(follow_count), 0),
                        coalesce(sum(block_count), 0),
                        count(*) FILTER (WHERE total_activity_count > 0)
                    FROM marts.fct_user_daily_activity
                    WHERE activity_date = %s
                    """,
                    (ds,),
                )
                rows_read, *totals = cur.fetchone()

                # Delete-then-insert on the report_date key: re-running a date
                # replaces its row, other dates are never touched.
                cur.execute("DELETE FROM marts.mart_daily_summary WHERE report_date = %s", (ds,))
                cur.execute(
                    """
                    INSERT INTO marts.mart_daily_summary
                        (report_date, total_posts, total_likes, total_reposts,
                         total_follows, total_blocks, active_users)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (ds, *totals),
                )
            conn.commit()
        stats["rows_read"] = rows_read
        stats["rows_written"] = 1
        stats["note"] = f"report_date={ds}"


def _dbt(task_id: str, args: str, input_location: str, output_location: str) -> BashOperator:
    # dbt prints each model's rows written ("SELECT n") into this task's log.
    return BashOperator(
        task_id=task_id,
        pool=DBT_POOL,
        bash_command=(
            f'echo "[{task_id}] START input={input_location} output={output_location}"'
            f" && cd {DBT_PROJECT_DIR} && {DBT_BIN} {args}"
            f' && echo "[{task_id}] END"'
        ),
    )


with DAG(
    dag_id=DAG_ID,
    description="Phase 1 end-to-end pipeline for one logical date: ingest -> raw -> dbt -> quality -> publish",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["batch", "phase-1"],
) as dag:
    ingest = PythonOperator(
        task_id="start_ingestion",
        python_callable=start_ingestion,
        retries=3,
        retry_delay=timedelta(seconds=30),
    )
    cdc = PythonOperator(task_id="start_cdc", python_callable=start_cdc)
    spark = PythonOperator(task_id="start_spark", python_callable=start_spark)
    source_partition = PythonOperator(task_id="check_source_partition", python_callable=check_source_partition)
    raw_partition = PythonOperator(task_id="load_raw_partition", python_callable=load_raw_partition)
    run_before_snapshot = _dbt(
        "dbt_run_before_snapshot", "run --exclude dim_user", "raw.*", "staging.*, intermediate.*, marts.*"
    )
    snapshot = _dbt("dbt_snapshot", "snapshot", "intermediate.int_user_activity_summary", "snapshots.dim_user_snapshot")
    run_dim_user = _dbt("dbt_run_dim_user", "run --select dim_user", "snapshots.dim_user_snapshot", "marts.dim_user")
    dbt_test = _dbt("dbt_test", "test", "staging.*, marts.*", "test results (fails the run on error)")
    quality = PythonOperator(task_id="check_partition_quality", python_callable=check_partition_quality)
    publish = PythonOperator(task_id="publish_daily_summary", python_callable=publish_daily_summary)

    (
        ingest >> cdc >> spark >> source_partition >> raw_partition
        >> run_before_snapshot >> snapshot >> run_dim_user
        >> dbt_test >> quality >> publish
    )
