"""Turn a raw Jetstream event dict into a row (or row-change) for source-db.

Full implementation lands in Stage 3 (Ingestion).

Expected Jetstream v1 event shape (verified against the official repo):
{
  "did": "did:plc:...",
  "time_us": 1725911162329308,
  "kind": "commit",              # or "identity" / "account"
  "commit": {
    "rev": "...",
    "operation": "create",       # "create" | "update" | "delete"
    "collection": "app.bsky.feed.post",
    "rkey": "...",
    "cid": "...",                # absent on delete
    "record": { "...": "..." }   # absent on delete
  }
}
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

Operation = Literal["create", "update", "delete"]


@dataclass
class ParsedEvent:
    """Normalized event ready to be written to source-db."""

    event_type: str          # post | like | repost | follow | block | profile_update
    operation: Operation
    user_did: str
    record_key: str          # Jetstream rkey, used to build the row's unique key
    occurred_at_us: int      # time_us from Jetstream
    fields: dict[str, Any]   # derived, non-sensitive fields only (see privacy.py)


def parse_event(raw_event: dict, tracked_collections: dict[str, str]) -> ParsedEvent | None:
    """Parse one raw Jetstream event, or return None if it should be skipped
    (wrong kind, unknown collection, malformed payload).

    TODO (Stage 3):
      - Only handle kind == "commit".
      - Map commit.collection back to our event_type via tracked_collections.
      - For "post" events, derive language/length/hashtags/has_link/
        has_media/is_reply from record text - never store the raw text.
      - Return None for anything that does not match a tracked collection.
    """
    raise NotImplementedError("Implemented in Stage 3 (Ingestion)")
