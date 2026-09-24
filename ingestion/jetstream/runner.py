"""Ties the Jetstream client, parser and writer together into the
ingestor's main loop, and exposes the live state status_api.py serves.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

import psycopg2

from ingestion.common.config import AppConfig
from ingestion.jetstream.client import JetstreamClient
from ingestion.jetstream.parser import parse_event
from ingestion.jetstream.writer import SourceDbWriter

logger = logging.getLogger(__name__)

_EVENTS_PER_SECOND_WINDOW = 5.0


@dataclass
class IngestorState:
    """Snapshot of the ingestor's current state, read by status_api.py."""

    source_mode: str
    connected: bool = False
    ingestion_enabled: bool = False
    current_host: str | None = None
    cursor_us: int | None = None
    events_per_second: float = 0.0
    latest_event_types: list[str] = field(default_factory=list)


class IngestorRunner:
    """Runs forever: reads events, and only persists them while
    control.pipeline_switches.ingestion_enabled is on (checked every
    ingestor.switch_poll_interval_seconds)."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.state = IngestorState(source_mode=config.source_mode)
        self.client = JetstreamClient(config)
        self.writer = SourceDbWriter(config)
        self._recent_event_times: list[float] = []

    def _switch_enabled(self) -> bool:
        try:
            with psycopg2.connect(self.config.warehouse_db.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT is_enabled FROM control.pipeline_switches "
                        "WHERE switch_name = 'ingestion_enabled'"
                    )
                    row = cur.fetchone()
                    return bool(row[0]) if row else False
        except Exception:
            logger.exception("could not read ingestion_enabled switch")
            return False

    def _refresh_events_per_second(self, now: float) -> None:
        cutoff = now - _EVENTS_PER_SECOND_WINDOW
        self._recent_event_times = [t for t in self._recent_event_times if t >= cutoff]
        self.state.events_per_second = len(self._recent_event_times) / _EVENTS_PER_SECOND_WINDOW

    async def run(self) -> None:
        self.client.cursor_us = self.writer.load_cursor()
        self.state.cursor_us = self.client.cursor_us

        ingestor_cfg = self.config.settings["ingestor"]
        write_interval = ingestor_cfg["write_interval_seconds"]
        switch_poll_interval = ingestor_cfg["switch_poll_interval_seconds"]

        last_flush = time.monotonic()
        last_switch_check = 0.0

        async for raw_event in self.client.events():
            self.state.connected = True
            self.state.current_host = self.client.current_host
            now = time.monotonic()

            if now - last_switch_check >= switch_poll_interval:
                self.state.ingestion_enabled = self._switch_enabled()
                last_switch_check = now

            if self.state.ingestion_enabled:
                parsed = parse_event(raw_event, self.config.tracked_collections)
                if parsed is not None:
                    self.writer.add(parsed)
                    self._recent_event_times.append(now)
                    self.state.latest_event_types = (
                        [parsed.event_type] + self.state.latest_event_types
                    )[:20]

            if now - last_flush >= write_interval:
                if self.state.ingestion_enabled:
                    self.writer.flush()
                    self.client.cursor_us = self.writer.load_cursor()
                    self.state.cursor_us = self.client.cursor_us
                last_flush = now
                self._refresh_events_per_second(now)
