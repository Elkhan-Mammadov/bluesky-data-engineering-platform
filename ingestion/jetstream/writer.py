"""Buffer parsed events and flush them to source-db every 5 seconds.

This is the one place a raw Bluesky DID is hashed before it touches a
database - see docs/PRIVACY.md and ingestion/common/privacy.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg2

from ingestion.common.config import AppConfig
from ingestion.common.privacy import hash_user_id, is_sampled
from ingestion.jetstream.parser import ParsedEvent

logger = logging.getLogger(__name__)

_TABLE_BY_TYPE = {
    "post": "posts",
    "like": "likes",
    "repost": "reposts",
    "follow": "follows",
    "block": "blocks",
    "profile_update": "profile_updates",
}


@dataclass
class SourceDbWriter:
    """Accumulates ParsedEvent objects and writes them in micro-batches."""

    config: AppConfig
    _buffer: list[ParsedEvent] = field(default_factory=list)

    def add(self, event: ParsedEvent) -> None:
        """Queue one parsed event for the next flush."""
        self._buffer.append(event)

    def _connect(self):
        return psycopg2.connect(self.config.source_db.dsn)

    def flush(self) -> int:
        """Write buffered events to source-db, update the cursor, and
        clear the buffer. Returns the number of rows actually written
        (unsampled users are silently dropped, not counted)."""
        if not self._buffer:
            return 0

        batch, self._buffer = self._buffer, []
        salt = self.config.hash_salt
        rate = self.config.sampling_rate
        written = 0
        latest_time_us: int | None = None

        with self._connect() as conn:
            with conn.cursor() as cur:
                for event in batch:
                    if not is_sampled(event.user_did, salt, rate):
                        continue

                    user_hash = hash_user_id(event.user_did, salt)
                    occurred_at = datetime.fromtimestamp(
                        event.occurred_at_us / 1_000_000, tz=timezone.utc
                    )

                    self._upsert_user(cur, user_hash, occurred_at)
                    self._write_event(cur, event, user_hash, occurred_at, salt)

                    written += 1
                    latest_time_us = (
                        event.occurred_at_us
                        if latest_time_us is None
                        else max(latest_time_us, event.occurred_at_us)
                    )

            if latest_time_us is not None:
                self._save_cursor(conn, latest_time_us)

            conn.commit()

        logger.info("flushed %d/%d events", written, len(batch))
        return written

    def _upsert_user(self, cur, user_hash: str, occurred_at: datetime) -> None:
        cur.execute(
            """
            INSERT INTO users (user_id_hash, first_seen_at, last_seen_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id_hash)
            DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at
            """,
            (user_hash, occurred_at, occurred_at),
        )

    def _save_cursor(self, conn, latest_time_us: int) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_cursor (id, last_time_us, updated_at)
                VALUES (1, %s, now())
                ON CONFLICT (id) DO UPDATE
                SET last_time_us = EXCLUDED.last_time_us, updated_at = now()
                """,
                (latest_time_us,),
            )

    def _write_event(self, cur, event: ParsedEvent, user_hash: str, occurred_at: datetime, salt: str) -> None:
        table = _TABLE_BY_TYPE[event.event_type]

        if event.operation == "delete":
            if event.event_type == "profile_update":
                cur.execute(f"DELETE FROM {table} WHERE user_id_hash = %s", (user_hash,))
            else:
                cur.execute(
                    f"DELETE FROM {table} WHERE user_id_hash = %s AND record_key = %s",
                    (user_hash, event.record_key),
                )
            return

        if event.event_type == "post":
            self._upsert_post(cur, user_hash, event, occurred_at)
        elif event.event_type in ("like", "repost"):
            self._upsert_like_or_repost(cur, table, user_hash, event, occurred_at, salt)
        elif event.event_type in ("follow", "block"):
            self._upsert_follow_or_block(cur, table, user_hash, event, occurred_at, salt)
        elif event.event_type == "profile_update":
            self._upsert_profile_update(cur, user_hash, event, occurred_at)

    def _upsert_post(self, cur, user_hash: str, event: ParsedEvent, occurred_at: datetime) -> None:
        f = event.fields
        cur.execute(
            """
            INSERT INTO posts
                (user_id_hash, record_key, occurred_at, language, text_length,
                 hashtags, has_link, has_media, is_reply)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id_hash, record_key) DO UPDATE SET
                occurred_at = EXCLUDED.occurred_at,
                language    = EXCLUDED.language,
                text_length = EXCLUDED.text_length,
                hashtags    = EXCLUDED.hashtags,
                has_link    = EXCLUDED.has_link,
                has_media   = EXCLUDED.has_media,
                is_reply    = EXCLUDED.is_reply
            """,
            (
                user_hash, event.record_key, occurred_at,
                f.get("language"), f.get("text_length"), f.get("hashtags", []),
                f.get("has_link", False), f.get("has_media", False), f.get("is_reply", False),
            ),
        )

    def _upsert_like_or_repost(self, cur, table: str, user_hash: str, event: ParsedEvent, occurred_at: datetime, salt: str) -> None:
        target_did = event.fields.get("target_user_did")
        target_hash = hash_user_id(target_did, salt) if target_did else None
        cur.execute(
            f"""
            INSERT INTO {table}
                (user_id_hash, record_key, occurred_at, target_user_id_hash, target_record_key)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id_hash, record_key) DO UPDATE SET
                occurred_at          = EXCLUDED.occurred_at,
                target_user_id_hash  = EXCLUDED.target_user_id_hash,
                target_record_key    = EXCLUDED.target_record_key
            """,
            (user_hash, event.record_key, occurred_at, target_hash, event.fields.get("target_record_key")),
        )

    def _upsert_follow_or_block(self, cur, table: str, user_hash: str, event: ParsedEvent, occurred_at: datetime, salt: str) -> None:
        target_did = event.fields.get("target_user_did")
        target_hash = hash_user_id(target_did, salt) if target_did else None
        cur.execute(
            f"""
            INSERT INTO {table} (user_id_hash, record_key, occurred_at, target_user_id_hash)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id_hash, record_key) DO UPDATE SET
                occurred_at         = EXCLUDED.occurred_at,
                target_user_id_hash = EXCLUDED.target_user_id_hash
            """,
            (user_hash, event.record_key, occurred_at, target_hash),
        )

    def _upsert_profile_update(self, cur, user_hash: str, event: ParsedEvent, occurred_at: datetime) -> None:
        cur.execute(
            """
            INSERT INTO profile_updates (user_id_hash, occurred_at, operation)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id_hash) DO UPDATE SET
                occurred_at = EXCLUDED.occurred_at,
                operation   = EXCLUDED.operation
            """,
            (user_hash, occurred_at, event.operation),
        )

    def load_cursor(self) -> int | None:
        """Read the last saved cursor (time_us) from source-db."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT last_time_us FROM ingestion_cursor WHERE id = 1")
                row = cur.fetchone()
                return row[0] if row and row[0] is not None else None
