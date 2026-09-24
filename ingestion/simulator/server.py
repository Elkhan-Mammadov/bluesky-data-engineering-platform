"""Simulator API, served at localhost:8001/docs.

Exposes a `/subscribe` WebSocket that streams synthetic events in the
exact shape Jetstream uses, so ingestion/jetstream/client.py can connect
to it exactly like it would connect to a real Jetstream host (only the URL
differs - see config/settings.yaml: simulator.subscribe_url).
"""

from __future__ import annotations

import asyncio
import json

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from ingestion.common.config import load_config
from ingestion.simulator.generator import EventGenerator

_config = load_config()
_generator = EventGenerator(_config)

app = FastAPI(title="Bluesky Simulator", version="0.2.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/config")
def get_config() -> dict:
    """Return the simulator's current tuning parameters."""
    return _config.settings["simulator"]


@app.websocket("/subscribe")
async def subscribe(websocket: WebSocket) -> None:
    await websocket.accept()
    events_per_second = _config.settings["simulator"]["events_per_second"]
    delay_seconds = 1.0 / events_per_second if events_per_second > 0 else 1.0

    try:
        for event in _generator.generate():
            await websocket.send_text(json.dumps(event))
            await asyncio.sleep(delay_seconds)
    except WebSocketDisconnect:
        pass
