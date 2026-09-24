"""Generate synthetic Jetstream-shaped events as a fallback data source.

Used when SOURCE_MODE=simulator. Every event has the exact same shape a
real Jetstream commit event would have (see ingestion/jetstream/parser.py),
so the ingestor cannot tell the difference - only ingestion/simulator/server.py
knows this data is synthetic.
"""

from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Iterator

from faker import Faker

from ingestion.common.config import AppConfig

_fake = Faker()
_LANGUAGES = ["en", "es", "pt", "ja", "fr", "de"]
_HASHTAGS = ["bluesky", "atproto", "python", "data", "news", "art", "music"]


def _new_did() -> str:
    return f"did:plc:{uuid.uuid4().hex[:24]}"


def _new_rkey() -> str:
    # Real Jetstream rkeys are TIDs; a random hex string only needs to look
    # plausible and be unique for a synthetic fallback source.
    return uuid.uuid4().hex[:13]


def _fake_post_record() -> dict:
    text = _fake.sentence()
    if random.random() < 0.3:
        text += " #" + random.choice(_HASHTAGS)
    record: dict = {
        "$type": "app.bsky.feed.post",
        "text": text,
        "createdAt": _fake.iso8601(),
    }
    if random.random() < 0.8:
        record["langs"] = [random.choice(_LANGUAGES)]
    if random.random() < 0.1:
        record["reply"] = {"parent": {"uri": f"at://{_new_did()}/app.bsky.feed.post/{_new_rkey()}"}}
    return record


def _fake_subject_record(target_did: str) -> dict:
    return {"subject": {"uri": f"at://{target_did}/app.bsky.feed.post/{_new_rkey()}"}}


def _fake_account_subject_record(target_did: str) -> dict:
    return {"subject": target_did}


def _fake_profile_record() -> dict:
    return {"$type": "app.bsky.actor.profile", "description": _fake.sentence()}


@dataclass
class EventGenerator:
    """Produces an endless stream of events in the same shape as real
    Jetstream commit events, with configurable dirty records, schema
    drift and bot-like accounts (config/settings.yaml: simulator.*)."""

    config: AppConfig
    _user_pool: list[str] = field(default_factory=list, init=False)
    _bot_users: set[str] = field(default_factory=set, init=False)
    _started_at: float = field(default_factory=time.monotonic, init=False)

    def __post_init__(self) -> None:
        sim_cfg = self.config.settings["simulator"]
        pool_size = max(50, sim_cfg["events_per_second"] * 5)
        self._user_pool = [_new_did() for _ in range(pool_size)]
        bot_count = max(1, int(pool_size * sim_cfg.get("bot_account_rate", 0.01)))
        self._bot_users = set(random.sample(self._user_pool, bot_count))

    def _pick_event_type(self) -> tuple[str, str]:
        """Return (event_type, collection_nsid), biased towards bot
        behaviour (mass follows) for bot accounts."""
        return random.choice(list(self.config.tracked_collections.items()))

    def _pick_user(self) -> tuple[str, bool]:
        """Return (user_did, is_bot)."""
        if self._bot_users and random.random() < 0.2:
            return random.choice(list(self._bot_users)), True
        return random.choice(self._user_pool), False

    def _build_record(self, event_type: str) -> dict:
        if event_type == "post":
            return _fake_post_record()
        if event_type in ("like", "repost"):
            return _fake_subject_record(random.choice(self._user_pool))
        if event_type in ("follow", "block"):
            return _fake_account_subject_record(random.choice(self._user_pool))
        return _fake_profile_record()

    def _apply_schema_drift(self, record: dict) -> None:
        drift_after = self.config.settings["simulator"]["schema_drift_after_seconds"]
        if time.monotonic() - self._started_at > drift_after:
            record["experimentalField"] = "unexpected-value"

    def _make_clean_event(self, user_did: str, event_type: str, collection: str) -> dict:
        record = self._build_record(event_type)
        self._apply_schema_drift(record)
        return {
            "did": user_did,
            "time_us": int(time.time() * 1_000_000),
            "kind": "commit",
            "commit": {
                "rev": uuid.uuid4().hex[:10],
                "operation": "create",
                "collection": collection,
                "rkey": _new_rkey(),
                "record": record,
            },
        }

    def _apply_dirty_mutation(self, event: dict, dirty_type: str) -> dict:
        if dirty_type == "missing_required_field":
            event["commit"].pop("record", None)
        elif dirty_type == "future_timestamp":
            event["time_us"] += 3600 * 1_000_000
        elif dirty_type == "unknown_event_type":
            event["commit"]["collection"] = "app.bsky.unknown.type"
        # "duplicate_event" is handled by the caller (generate()), which
        # yields the same clean event twice instead of mutating it.
        return event

    def generate_one(self) -> tuple[dict, str | None]:
        """Build one event. Returns (event, dirty_type_or_None)."""
        sim_cfg = self.config.settings["simulator"]
        user_did, is_bot = self._pick_user()

        if is_bot:
            event_type, collection = "follow", self.config.tracked_collections["follow"]
        else:
            event_type, collection = self._pick_event_type()

        event = self._make_clean_event(user_did, event_type, collection)

        dirty_type = None
        if random.random() < sim_cfg["dirty_record_rate"]:
            dirty_type = random.choice(sim_cfg["dirty_record_types"])
            if dirty_type != "duplicate_event":
                event = self._apply_dirty_mutation(event, dirty_type)

        return event, dirty_type

    def generate(self) -> Iterator[dict]:
        """Yield an endless stream of synthetic events."""
        while True:
            event, dirty_type = self.generate_one()
            yield event
            if dirty_type == "duplicate_event":
                yield event
