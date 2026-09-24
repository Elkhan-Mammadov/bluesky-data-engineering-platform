"""Status API for the ingestor, served at localhost:8000/docs.

Starts the IngestorRunner as a background task and exposes its live state.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from ingestion.common.config import load_config
from ingestion.jetstream.runner import IngestorRunner

_config = load_config()
_runner = IngestorRunner(_config)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    task = asyncio.create_task(_runner.run())
    yield
    task.cancel()


app = FastAPI(title="Bluesky Ingestor Status", version="0.2.0", lifespan=_lifespan)


class IngestorStatus(BaseModel):
    source_mode: str
    connected: bool
    ingestion_enabled: bool
    current_host: str | None
    cursor_us: int | None
    events_per_second: float
    latest_event_types: list[str]


@app.get("/status", response_model=IngestorStatus)
def get_status() -> IngestorStatus:
    s = _runner.state
    return IngestorStatus(
        source_mode=s.source_mode,
        connected=s.connected,
        ingestion_enabled=s.ingestion_enabled,
        current_host=s.current_host,
        cursor_us=s.cursor_us,
        events_per_second=s.events_per_second,
        latest_event_types=s.latest_event_types,
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
