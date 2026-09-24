"""Load configuration for the ingestor and the simulator.

Two sources are combined:
  1. `.env` (secrets and per-environment values, via python-dotenv)
  2. `config/settings.yaml` (non-secret, shared tuning values)

Both are exposed through a single `AppConfig` object so the rest of the
code never reads os.environ or the YAML file directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
SETTINGS_PATH = REPO_ROOT / "config" / "settings.yaml"


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@dataclass
class DbConfig:
    """Connection details for one Postgres database."""

    host: str
    port: int
    name: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.name} "
            f"user={self.user} password={self.password}"
        )


@dataclass
class AppConfig:
    source_mode: str
    hash_salt: str
    sampling_rate: float
    source_db: DbConfig
    # Only needed to poll control.pipeline_switches.ingestion_enabled -
    # the ingestor never writes business data into the warehouse.
    warehouse_db: DbConfig
    settings: dict[str, Any] = field(default_factory=dict)

    @property
    def jetstream_hosts(self) -> list[str]:
        return self.settings["jetstream"]["hosts"]

    @property
    def tracked_collections(self) -> dict[str, str]:
        return self.settings["tracked_collections"]


def load_config(env_file: str | Path | None = None) -> AppConfig:
    """Load .env (if present) and settings.yaml into one AppConfig."""
    load_dotenv(dotenv_path=env_file or (REPO_ROOT / ".env"), override=False)
    settings = _load_yaml(SETTINGS_PATH)

    source_db = DbConfig(
        host=_env("SOURCE_DB_HOST", "source-db"),
        port=int(_env("SOURCE_DB_PORT", "5432")),
        name=_env("SOURCE_DB_NAME", "bluesky_source"),
        user=_env("SOURCE_DB_USER", "source_app"),
        password=_env("SOURCE_DB_PASSWORD", ""),
    )

    warehouse_db = DbConfig(
        host=_env("WAREHOUSE_DB_HOST", "warehouse-db"),
        port=int(_env("WAREHOUSE_DB_PORT", "5432")),
        name=_env("WAREHOUSE_DB_NAME", "bluesky_warehouse"),
        user=_env("WAREHOUSE_DB_USER", "warehouse_app"),
        password=_env("WAREHOUSE_DB_PASSWORD", ""),
    )

    return AppConfig(
        source_mode=_env("SOURCE_MODE", "jetstream"),
        hash_salt=_env("USER_ID_HASH_SALT", ""),
        sampling_rate=float(
            os.getenv("SAMPLING_RATE", settings.get("sampling", {}).get("default_rate", 0.10))
        ),
        source_db=source_db,
        warehouse_db=warehouse_db,
        settings=settings,
    )
