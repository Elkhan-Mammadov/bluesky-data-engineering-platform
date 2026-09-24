"""Generate synthetic Jetstream-shaped events as a fallback data source.

Full implementation lands in Stage 3 (Ingestion), using the tuning values
in config/settings.yaml (simulator: events_per_second, dirty_record_rate,
schema_drift_after_seconds, bot_account_rate, ...).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

from ingestion.common.config import AppConfig


@dataclass
class EventGenerator:
    """Produces events in the same shape as real Jetstream commit events."""

    config: AppConfig
    started_at_us: int = 0

    def generate(self) -> Iterator[dict]:
        """Yield an endless stream of synthetic events.

        TODO (Stage 3):
          - Emit valid events for each tracked collection (post/like/repost/
            follow/block/profile_update) using Faker for realistic-looking
            (but fake) content.
          - Occasionally emit dirty records per simulator.dirty_record_rate:
            missing fields, duplicates, future timestamps, unknown types.
          - After simulator.schema_drift_after_seconds, start adding one new
            field to records (schema drift).
          - Make simulator.bot_account_rate of accounts act like bots
            (simulator.bot_actions_per_minute follows/likes per minute).
        """
        raise NotImplementedError("Implemented in Stage 3 (Ingestion)")
