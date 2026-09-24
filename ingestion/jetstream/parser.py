"""Turn a raw Jetstream event dict into a row (or row-change) for source-db.

Expected Jetstream v1 event shape (verified against the official repo):
{
  "did": "did:plc:...",
  "time_us": 1725911162329308,
  "kind": "commit",              # or "identity" / "account" (ignored here)
  "commit": {
    "rev": "...",
    "operation": "create",       # "create" | "update" | "delete"
    "collection": "app.bsky.feed.post",
    "rkey": "...",
    "cid": "...",                # absent on delete
    "record": { "...": "..." }   # absent on delete
  }
}

Only shape-mapping happens here - hashing user IDs is the writer's job
(ingestion/jetstream/writer.py), so the salt never needs to reach this
module.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

Operation = Literal["create", "update", "delete"]

_HASHTAG_RE = re.compile(r"#(\w+)")
_LINK_RE = re.compile(r"https?://")
_MEDIA_EMBED_TYPES = {"app.bsky.embed.images", "app.bsky.embed.video"}


@dataclass
class ParsedEvent:
    """Normalized event ready to be written to source-db."""

    event_type: str          # post | like | repost | follow | block | profile_update
    operation: Operation
    user_did: str
    record_key: str          # Jetstream rkey, used to build the row's unique key
    occurred_at_us: int      # time_us from Jetstream
    fields: dict[str, Any]   # derived, non-sensitive fields only (see privacy.py)


def _extract_text_features(record: dict) -> dict[str, Any]:
    """Derive only what CLAUDE.md 8.1 allows from a post's text - the text
    itself is never returned or stored."""
    text = record.get("text") or ""
    langs = record.get("langs") or []
    embed_type = (record.get("embed") or {}).get("$type", "")

    return {
        "language": langs[0] if langs else None,
        "text_length": len(text),
        "hashtags": sorted(set(_HASHTAG_RE.findall(text))),
        "has_link": bool(_LINK_RE.search(text)) or embed_type == "app.bsky.embed.external",
        "has_media": embed_type in _MEDIA_EMBED_TYPES,
        "is_reply": "reply" in record,
    }


def _split_at_uri(uri: str) -> tuple[str | None, str | None]:
    """at://did:plc:xxx/collection/rkey -> (did, rkey)."""
    if not uri or not uri.startswith("at://"):
        return None, None
    parts = uri[len("at://") :].split("/")
    if len(parts) < 3:
        return None, None
    return parts[0], parts[2]


def parse_event(raw_event: dict, tracked_collections: dict[str, str]) -> ParsedEvent | None:
    """Parse one raw Jetstream event, or return None if it should be
    skipped (wrong kind, unknown collection, malformed payload)."""
    if raw_event.get("kind") != "commit":
        return None

    commit = raw_event.get("commit") or {}
    collection = commit.get("collection")
    event_type = next(
        (name for name, nsid in tracked_collections.items() if nsid == collection), None
    )
    if event_type is None:
        return None

    operation = commit.get("operation")
    if operation not in ("create", "update", "delete"):
        return None

    user_did = raw_event.get("did")
    record_key = commit.get("rkey")
    occurred_at_us = raw_event.get("time_us")
    if not user_did or not record_key or occurred_at_us is None:
        return None

    record = commit.get("record") or {}
    fields: dict[str, Any] = {}

    if event_type == "post":
        fields.update(_extract_text_features(record))
    elif event_type in ("like", "repost"):
        target_did, target_rkey = _split_at_uri((record.get("subject") or {}).get("uri", ""))
        fields["target_user_did"] = target_did
        fields["target_record_key"] = target_rkey
    elif event_type in ("follow", "block"):
        fields["target_user_did"] = record.get("subject")

    return ParsedEvent(
        event_type=event_type,
        operation=operation,
        user_did=user_did,
        record_key=record_key,
        occurred_at_us=occurred_at_us,
        fields=fields,
    )
