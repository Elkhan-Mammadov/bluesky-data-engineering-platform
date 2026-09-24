"""Unpack a Debezium change event into (table, op, before, after, ...).

Debezium envelope shape. The connector (ingestion/cdc/debezium-source.json)
sets key/value schemas.enable=false, so the Kafka message value IS the
envelope directly - there is no separate "schema"/"payload" wrapper:
{
  "before": {...} | null,
  "after": {...} | null,
  "source": {"ts_ms": ..., "table": ..., ...},
  "op": "c" | "u" | "d" | "r",
  "ts_ms": ...
}
"""

from __future__ import annotations

import json
from typing import Any

# Kafka topic name -> the raw/source-db table it carries changes for.
# Matches topic.prefix=bluesky and table.include.list in the connector config.
TABLE_BY_TOPIC = {
    "bluesky.public.users": "users",
    "bluesky.public.posts": "posts",
    "bluesky.public.likes": "likes",
    "bluesky.public.reposts": "reposts",
    "bluesky.public.follows": "follows",
    "bluesky.public.blocks": "blocks",
    "bluesky.public.profile_updates": "profile_updates",
}


def parse_envelope(kafka_row: dict[str, Any]) -> dict[str, Any]:
    """Turn one raw Kafka row ({"topic", "value", "offset"}) into a
    structured dict. Never raises - a malformed message gets
    parse_error set instead, so cleaning.py can quarantine it and the
    batch keeps going."""
    table = TABLE_BY_TOPIC.get(kafka_row.get("topic"))

    try:
        envelope = json.loads(kafka_row["value"]) if kafka_row.get("value") else {}
    except (json.JSONDecodeError, TypeError) as exc:
        return {
            "table": table,
            "kafka_offset": kafka_row.get("offset"),
            "op": None,
            "before": None,
            "after": None,
            "source_ts_ms": None,
            "parse_error": str(exc),
        }

    source = envelope.get("source") or {}

    return {
        "table": table,
        "op": envelope.get("op"),
        "before": envelope.get("before"),
        "after": envelope.get("after"),
        "source_ts_ms": source.get("ts_ms") or envelope.get("ts_ms"),
        "kafka_offset": kafka_row.get("offset"),
        "parse_error": None,
    }
