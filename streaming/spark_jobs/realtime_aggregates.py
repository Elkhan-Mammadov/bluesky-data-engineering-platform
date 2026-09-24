"""Compute per-minute real-time aggregates for the `realtime` schema.

Called once per micro-batch (cdc_to_warehouse.py) with only the rows that
passed cleaning.validate_row(). Every bucket is upserted (ON CONFLICT ...
DO UPDATE), so re-processing the same batch after a checkpoint replay
never double-counts.
"""

from __future__ import annotations

import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

import psycopg2


def _warehouse_conn():
    return psycopg2.connect(
        host=os.environ["WAREHOUSE_DB_HOST"],
        port=os.environ.get("WAREHOUSE_DB_PORT", "5432"),
        dbname=os.environ["WAREHOUSE_DB_NAME"],
        user=os.environ["WAREHOUSE_DB_USER"],
        password=os.environ["WAREHOUSE_DB_PASSWORD"],
    )


def _minute_bucket(row: dict[str, Any]) -> datetime:
    source_ts_ms = row.get("source_ts_ms")
    if source_ts_ms:
        ts = datetime.fromtimestamp(source_ts_ms / 1000, tz=timezone.utc)
    else:
        ts = datetime.now(tz=timezone.utc)
    return ts.replace(second=0, microsecond=0)


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    return row["before"] if row["op"] == "d" else row["after"]


def _aggregate(valid_rows: list[dict[str, Any]]):
    event_counts: Counter[tuple[datetime, str, str]] = Counter()
    active_users: dict[datetime, set[str]] = defaultdict(set)
    hashtag_counts: Counter[tuple[datetime, str]] = Counter()
    language_counts: Counter[tuple[datetime, str]] = Counter()

    for row in valid_rows:
        if row["table"] == "users":
            continue  # bookkeeping table, not a user-facing event

        bucket = _minute_bucket(row)
        payload = _payload(row)

        event_counts[(bucket, row["table"], row["op"])] += 1
        active_users[bucket].add(payload.get("user_id_hash"))

        if row["table"] == "posts" and row["op"] != "d":
            for hashtag in payload.get("hashtags") or []:
                hashtag_counts[(bucket, hashtag)] += 1
            if payload.get("language"):
                language_counts[(bucket, payload["language"])] += 1

    return event_counts, active_users, hashtag_counts, language_counts


def update_realtime_aggregates(valid_rows: list[dict[str, Any]]) -> None:
    if not valid_rows:
        return

    event_counts, active_users, hashtag_counts, language_counts = _aggregate(valid_rows)

    with _warehouse_conn() as conn:
        with conn.cursor() as cur:
            for (bucket, event_type, op), count in event_counts.items():
                cur.execute(
                    """
                    INSERT INTO realtime.event_counts_by_minute
                        (minute_bucket, event_type, operation, event_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (minute_bucket, event_type, operation) DO UPDATE
                    SET event_count = realtime.event_counts_by_minute.event_count + EXCLUDED.event_count
                    """,
                    (bucket, event_type, op, count),
                )

            for bucket, user_hashes in active_users.items():
                for user_hash in user_hashes:
                    if not user_hash:
                        continue
                    cur.execute(
                        """
                        INSERT INTO realtime.active_users_by_minute (minute_bucket, user_id_hash)
                        VALUES (%s, %s)
                        ON CONFLICT (minute_bucket, user_id_hash) DO NOTHING
                        """,
                        (bucket, user_hash),
                    )

            for (bucket, hashtag), count in hashtag_counts.items():
                cur.execute(
                    """
                    INSERT INTO realtime.hashtag_counts_by_minute (minute_bucket, hashtag, tag_count)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (minute_bucket, hashtag) DO UPDATE
                    SET tag_count = realtime.hashtag_counts_by_minute.tag_count + EXCLUDED.tag_count
                    """,
                    (bucket, hashtag, count),
                )

            for (bucket, language), count in language_counts.items():
                cur.execute(
                    """
                    INSERT INTO realtime.language_counts_by_minute (minute_bucket, language, post_count)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (minute_bucket, language) DO UPDATE
                    SET post_count = realtime.language_counts_by_minute.post_count + EXCLUDED.post_count
                    """,
                    (bucket, language, count),
                )
        conn.commit()
