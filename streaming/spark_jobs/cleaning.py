"""Validate and deduplicate parsed CDC rows before upserting into `raw`.

Full implementation lands in Stage 5 (Spark streaming). Rows that fail
validation go to dq.quarantine; the pipeline never stops because of bad data.
"""

from __future__ import annotations

from typing import Any


def validate_row(row: dict[str, Any]) -> tuple[bool, str | None]:
    """Return (is_valid, rejection_reason).

    TODO (Stage 5):
      - required fields present
      - event timestamp not more than 5 minutes in the future
      - event timestamp not absurdly old
      - event type is one of the tracked collections
    """
    raise NotImplementedError("Implemented in Stage 5 (Spark streaming)")
