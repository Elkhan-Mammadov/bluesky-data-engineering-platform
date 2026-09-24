"""Validate parsed CDC rows before they are upserted into `raw`.

Anything that fails validation goes to dq.quarantine instead - the
pipeline never stops because of one bad row (see docs/PROJECT_PLAN.md
section 7, Data quality plan).

Deduplication is not a separate step here: raw.* upserts are keyed by the
same primary key as source-db (cdc_to_warehouse.py, ON CONFLICT DO
UPDATE), so a message Kafka redelivers (at-least-once delivery) simply
overwrites the same row instead of creating a duplicate.
"""

from __future__ import annotations

import time
from typing import Any

_KNOWN_TABLES = {"users", "posts", "likes", "reposts", "follows", "blocks", "profile_updates"}
_KNOWN_OPS = {"c", "u", "d", "r"}
_FUTURE_TOLERANCE_MS = 5 * 60 * 1000        # "more than 5 minutes in the future"
_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000      # "far in the past" - 30 days


def validate_row(row: dict[str, Any]) -> tuple[bool, str | None]:
    """Return (is_valid, rejection_reason)."""
    if row.get("parse_error"):
        return False, f"unparseable message: {row['parse_error']}"

    if row.get("table") not in _KNOWN_TABLES:
        return False, f"unknown event type: {row.get('table')}"

    if row.get("op") not in _KNOWN_OPS:
        return False, f"unknown operation: {row.get('op')}"

    payload = row.get("before") if row["op"] == "d" else row.get("after")
    if not payload or not payload.get("user_id_hash"):
        return False, "missing required field: user_id_hash"

    source_ts_ms = row.get("source_ts_ms")
    if source_ts_ms is not None:
        now_ms = int(time.time() * 1000)
        if source_ts_ms - now_ms > _FUTURE_TOLERANCE_MS:
            return False, "timestamp too far in the future"
        if now_ms - source_ts_ms > _MAX_AGE_MS:
            return False, "timestamp too far in the past"

    return True, None


def split_valid_and_quarantined(
    rows: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], str]]]:
    """Partition parsed rows into (valid_rows, [(row, reason), ...])."""
    valid: list[dict[str, Any]] = []
    quarantined: list[tuple[dict[str, Any], str]] = []

    for row in rows:
        is_valid, reason = validate_row(row)
        if is_valid:
            valid.append(row)
        else:
            quarantined.append((row, reason))

    return valid, quarantined
