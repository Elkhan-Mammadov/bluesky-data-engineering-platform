"""WebSocket client for the Bluesky Jetstream feed (or the simulator, when
SOURCE_MODE=simulator - both speak the exact same protocol, see
ingestion/simulator/server.py).

Reference: https://github.com/bluesky-social/jetstream
  - Connect to one of the public hosts (config/settings.yaml) at the
    `/subscribe` path.
  - Filter server-side with repeated `wantedCollections` query params.
  - Resume with `?cursor=<unix_microseconds>` after a restart.
  - On disconnect, retry with exponential backoff and fail over to the
    next configured host.
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

import websockets
from websockets.exceptions import ConnectionClosed

from ingestion.common.config import AppConfig

logger = logging.getLogger(__name__)


@dataclass
class JetstreamClient:
    """Connects to Jetstream (or the simulator) and yields raw event dicts.

    Reconnects forever - callers decide what to do with the events (e.g.
    ingestion/jetstream/runner.py only persists them while the
    ingestion_enabled switch is on).
    """

    config: AppConfig
    cursor_us: int | None = None
    current_host: str | None = field(default=None, init=False)

    def _hosts(self) -> list[str]:
        if self.config.source_mode == "simulator":
            return [self.config.settings["simulator"]["subscribe_url"]]
        return self.config.jetstream_hosts

    def build_url(self, host: str) -> str:
        """Append wantedCollections (and cursor, if we have one) to a host."""
        params = [("wantedCollections", nsid) for nsid in self.config.tracked_collections.values()]
        if self.cursor_us is not None:
            params.append(("cursor", str(self.cursor_us)))
        query = urllib.parse.urlencode(params)
        return f"{host}?{query}" if query else host

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        """Yield decoded JSON events forever, reconnecting with exponential
        backoff and failing over to the next host on every disconnect."""
        backoff_cfg = self.config.settings["jetstream"]["reconnect_backoff"]
        delay = backoff_cfg["initial_seconds"]
        host_index = 0

        while True:
            hosts = self._hosts()
            host = hosts[host_index % len(hosts)]
            self.current_host = host
            url = self.build_url(host)

            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    logger.info("connected to %s", host)
                    delay = backoff_cfg["initial_seconds"]  # reset backoff on success
                    async for raw_message in ws:
                        try:
                            yield json.loads(raw_message)
                        except json.JSONDecodeError:
                            logger.warning("dropped a non-JSON message from %s", host)
            except (ConnectionClosed, OSError) as exc:
                logger.warning("disconnected from %s: %s", host, exc)

            host_index += 1
            await asyncio.sleep(delay)
            delay = min(delay * backoff_cfg["multiplier"], backoff_cfg["max_seconds"])
