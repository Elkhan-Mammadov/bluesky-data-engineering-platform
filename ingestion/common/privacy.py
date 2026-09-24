"""Privacy helpers: turn a Bluesky user DID into an anonymous, stable hash.

Rules (see docs/PRIVACY.md):
  - We never store a user's raw DID, handle, display name or avatar.
  - The same DID always hashes to the same value (so activity per user can
    still be tracked over time), but the hash cannot be reversed to the
    original DID without knowing the secret salt.
  - The salt lives only in `.env` (USER_ID_HASH_SALT) and is never committed.
"""

from __future__ import annotations

import hashlib


def hash_user_id(user_did: str, salt: str) -> str:
    """Return a stable, salted SHA-256 hex digest for a Bluesky DID."""
    if not user_did:
        raise ValueError("user_did must not be empty")
    if not salt:
        raise ValueError("salt must not be empty")
    payload = f"{salt}:{user_did}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def is_sampled(user_did: str, salt: str, sampling_rate: float) -> bool:
    """Deterministically decide whether a user's events should be kept.

    Uses the first 8 hex chars of the hashed id as a 32-bit integer and
    compares it against sampling_rate, so the same user is always either
    fully in or fully out of the sample (never a mix of kept/dropped events).
    """
    if not 0.0 <= sampling_rate <= 1.0:
        raise ValueError("sampling_rate must be between 0 and 1")
    digest = hash_user_id(user_did, salt)
    bucket = int(digest[:8], 16) / 0xFFFFFFFF
    return bucket < sampling_rate
