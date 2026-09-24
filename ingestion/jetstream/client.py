"""WebSocket client for the Bluesky Jetstream feed.

Full implementation lands in Stage 3 (Ingestion). For now this module only
defines the shape of the client so other modules can import it safely.

Reference: https://github.com/bluesky-social/jetstream
  - Connect to one of the public hosts (see config/settings.yaml) at
    the `/subscribe` path.
  - Filter server-side with repeated `wantedCollections` query params.
  - Resume with `?cursor=<unix_microseconds>` after a restart.
  - On disconnect, retry with exponential backoff and fail over to the
    next configured host.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncIterator

from ingestion.common.config import AppConfig


@dataclass
class JetstreamClient:
    """Connects to Jetstream and yields raw event dicts."""

    config: AppConfig
    cursor_us: int | None = None

    def build_url(self, host: str) -> str:
        """Build the `/subscribe` URL for one host with wantedCollections
        and, if we have one, a resume cursor.

        TODO (Stage 3): implement using urllib.parse and
        self.config.tracked_collections.
        """
        raise NotImplementedError("Implemented in Stage 3 (Ingestion)")

    async def events(self) -> AsyncIterator[dict]:
        """Yield decoded JSON events from Jetstream, reconnecting and
        failing over to other hosts as needed.

        TODO (Stage 3): implement with the `websockets` library, exponential
        backoff (config/settings.yaml: jetstream.reconnect_backoff) and host
        failover across config/settings.yaml: jetstream.hosts.
        """
        raise NotImplementedError("Implemented in Stage 3 (Ingestion)")
        yield {}  # pragma: no cover - keeps this an async generator
