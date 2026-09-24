"""Unpack a Debezium change event into (before, after, op, source_ts).

Full implementation lands in Stage 5 (Spark streaming).

Debezium envelope shape (Postgres connector):
{
  "payload": {
    "before": {...} | null,
    "after": {...} | null,
    "source": {"ts_ms": ..., "table": ...},
    "op": "c" | "u" | "d" | "r"
  }
}
"""

from __future__ import annotations

from typing import Any


def parse_envelope(raw_value: dict) -> dict[str, Any]:
    """TODO (Stage 5): extract before/after/op/source_ts from the Debezium
    JSON envelope produced by Kafka Connect."""
    raise NotImplementedError("Implemented in Stage 5 (Spark streaming)")
