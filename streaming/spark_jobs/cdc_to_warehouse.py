"""Entry point for the Spark Structured Streaming job.

Reads Debezium CDC messages from Kafka (one topic per source-db table),
cleans them, upserts into the `raw` warehouse schema (deletes become real
DELETEs), computes `realtime` per-minute aggregates, and records
per-batch stats in dq.stream_batches - all on a 5-second trigger.

Course-scale design note: sampled Bluesky traffic keeps each micro-batch
small, so batches are collected to the driver and written with one
psycopg2 connection rather than distributed per-partition writes. That
keeps the logic easy to read and debug, which matters more here than raw
write throughput. See docs/PROJECT_PLAN.md section 6 for why this
streaming job runs outside Airflow.
"""

from __future__ import annotations

import os
import time
from typing import Any

import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession

from streaming.spark_jobs import cleaning, debezium_parser, realtime_aggregates

TOPICS = list(debezium_parser.TABLE_BY_TOPIC.keys())
CHECKPOINT_DIR = "/opt/app/checkpoints/cdc_to_warehouse"

# Column lists mirror the source-db / raw table shape exactly (see
# infra/postgres/source-init/02-tables.sh and
# infra/postgres/warehouse-init/03-raw-and-realtime.sh).
_COLUMNS_BY_TABLE = {
    "users": ["user_id_hash", "first_seen_at", "last_seen_at"],
    "posts": ["user_id_hash", "record_key", "occurred_at", "language", "text_length",
              "hashtags", "has_link", "has_media", "is_reply"],
    "likes": ["user_id_hash", "record_key", "occurred_at", "target_user_id_hash", "target_record_key"],
    "reposts": ["user_id_hash", "record_key", "occurred_at", "target_user_id_hash", "target_record_key"],
    "follows": ["user_id_hash", "record_key", "occurred_at", "target_user_id_hash"],
    "blocks": ["user_id_hash", "record_key", "occurred_at", "target_user_id_hash"],
    "profile_updates": ["user_id_hash", "occurred_at", "operation"],
}

_CONFLICT_KEY_BY_TABLE = {
    "users": ["user_id_hash"],
    "posts": ["user_id_hash", "record_key"],
    "likes": ["user_id_hash", "record_key"],
    "reposts": ["user_id_hash", "record_key"],
    "follows": ["user_id_hash", "record_key"],
    "blocks": ["user_id_hash", "record_key"],
    "profile_updates": ["user_id_hash"],
}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("bluesky-cdc-to-warehouse")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )


def _warehouse_conn():
    return psycopg2.connect(
        host=os.environ["WAREHOUSE_DB_HOST"],
        port=os.environ.get("WAREHOUSE_DB_PORT", "5432"),
        dbname=os.environ["WAREHOUSE_DB_NAME"],
        user=os.environ["WAREHOUSE_DB_USER"],
        password=os.environ["WAREHOUSE_DB_PASSWORD"],
    )


def _upsert_raw_row(cur, row: dict[str, Any]) -> None:
    table = row["table"]
    conflict_key = _CONFLICT_KEY_BY_TABLE[table]

    if row["op"] == "d":
        payload = row["before"]
        where_clause = " AND ".join(f"{col} = %s" for col in conflict_key)
        cur.execute(
            f"DELETE FROM raw.{table} WHERE {where_clause}",
            [payload[col] for col in conflict_key],
        )
        return

    payload = row["after"]
    columns = _COLUMNS_BY_TABLE[table]
    update_columns = [c for c in columns if c not in conflict_key]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)

    cur.execute(
        f"""
        INSERT INTO raw.{table} ({", ".join(columns)}, kafka_offset, cdc_op, ingested_at)
        VALUES ({", ".join(["%s"] * len(columns))}, %s, %s, now())
        ON CONFLICT ({", ".join(conflict_key)}) DO UPDATE SET
            {set_clause},
            kafka_offset = EXCLUDED.kafka_offset,
            cdc_op       = EXCLUDED.cdc_op,
            ingested_at  = now()
        """,
        [payload.get(col) for col in columns] + [row.get("kafka_offset"), row["op"]],
    )


def _insert_quarantine_row(cur, row: dict[str, Any], reason: str) -> None:
    cur.execute(
        "INSERT INTO dq.quarantine (source_table, reason, raw_payload) VALUES (%s, %s, %s)",
        (row.get("table"), reason, psycopg2.extras.Json(row)),
    )


def _log_batch_stats(cur, batch_id: int, total_rows: int, valid_rows: int, duration_ms: int, latency_ms: int | None) -> None:
    cur.execute(
        """
        INSERT INTO dq.stream_batches (batch_id, row_count, valid_row_count, duration_ms, latency_ms)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (batch_id, total_rows, valid_rows, duration_ms, latency_ms),
    )


def _compute_latency_ms(valid_rows: list[dict[str, Any]]) -> int | None:
    """How far behind 'now' the newest row in this batch is (ms) - a rough
    end-to-end latency signal for the Grafana pipeline-health row."""
    source_times = [r["source_ts_ms"] for r in valid_rows if r.get("source_ts_ms")]
    if not source_times:
        return None
    return int(time.time() * 1000) - max(source_times)


def _process_batch(batch_df, batch_id: int) -> None:
    start = time.monotonic()
    kafka_rows = [row.asDict() for row in batch_df.collect()]

    parsed_rows = [debezium_parser.parse_envelope(r) for r in kafka_rows]
    valid_rows, quarantined_rows = cleaning.split_valid_and_quarantined(parsed_rows)

    with _warehouse_conn() as conn:
        with conn.cursor() as cur:
            for row in valid_rows:
                _upsert_raw_row(cur, row)
            for row, reason in quarantined_rows:
                _insert_quarantine_row(cur, row, reason)

            duration_ms = int((time.monotonic() - start) * 1000)
            latency_ms = _compute_latency_ms(valid_rows)
            _log_batch_stats(cur, batch_id, len(kafka_rows), len(valid_rows), duration_ms, latency_ms)
        conn.commit()

    realtime_aggregates.update_realtime_aggregates(valid_rows)


def run() -> None:
    spark = build_spark_session()

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        .option("subscribe", ",".join(TOPICS))
        .option("startingOffsets", "earliest")
        .load()
    )

    query = (
        stream.selectExpr("topic", "CAST(value AS STRING) as value", "offset")
        .writeStream.foreachBatch(_process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
