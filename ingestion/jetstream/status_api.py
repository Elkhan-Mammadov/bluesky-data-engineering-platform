"""Status API for the ingestor, served at localhost:8000/docs.

Full implementation (real connection state, cursor, events/sec, latest
events) lands in Stage 3 (Ingestion). For now this exposes the planned
shape of the API with placeholder data so the service can already be
started and inspected.
"""

from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Bluesky Ingestor Status", version="0.1.0")


class IngestorStatus(BaseModel):
    source_mode: str
    connected: bool
    current_host: str | None
    cursor_us: int | None
    events_per_second: float
    latest_event_types: list[str]


@app.get("/status", response_model=IngestorStatus)
def get_status() -> IngestorStatus:
    """Return the ingestor's current state.

    TODO (Stage 3): read real values from the running JetstreamClient /
    SourceDbWriter instead of returning placeholder data.
    """
    return IngestorStatus(
        source_mode="unknown",
        connected=False,
        current_host=None,
        cursor_us=None,
        events_per_second=0.0,
        latest_event_types=[],
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
