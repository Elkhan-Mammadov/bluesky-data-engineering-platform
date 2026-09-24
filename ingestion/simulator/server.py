"""Simulator API, served at localhost:8001/docs.

Full implementation lands in Stage 3 (Ingestion): starting/stopping the
generator and exposing its live output/config for inspection.
"""

from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(title="Bluesky Simulator", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/config")
def get_config() -> dict[str, str]:
    """Return the simulator's current tuning parameters.

    TODO (Stage 3): read real values from config/settings.yaml via
    ingestion.common.config.load_config().
    """
    return {"status": "not_configured_yet"}
