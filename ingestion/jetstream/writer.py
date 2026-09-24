"""Buffer parsed events and flush them to source-db every 5 seconds.

Full implementation lands in Stage 3 (Ingestion).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ingestion.common.config import AppConfig
from ingestion.jetstream.parser import ParsedEvent


@dataclass
class SourceDbWriter:
    """Accumulates ParsedEvent objects and writes them in micro-batches."""

    config: AppConfig
    _buffer: list[ParsedEvent] = field(default_factory=list)

    def add(self, event: ParsedEvent) -> None:
        """Queue one parsed event for the next flush."""
        self._buffer.append(event)

    def flush(self) -> int:
        """Write buffered events to source-db and clear the buffer.

        TODO (Stage 3):
          - Open a psycopg2 connection using self.config.source_db.dsn.
          - INSERT for create, UPDATE for update, DELETE for delete,
            keyed by (user_did_hash, record_key) so retries are idempotent.
          - Update the ingestion_cursor table with the latest occurred_at_us.
          - Return the number of rows written.
        """
        raise NotImplementedError("Implemented in Stage 3 (Ingestion)")

    def load_cursor(self) -> int | None:
        """Read the last saved cursor (time_us) from source-db.

        TODO (Stage 3): SELECT from ingestion_cursor; return None if empty.
        """
        raise NotImplementedError("Implemented in Stage 3 (Ingestion)")
