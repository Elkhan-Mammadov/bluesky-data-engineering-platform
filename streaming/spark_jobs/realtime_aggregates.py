"""Compute per-minute real-time aggregates for the `realtime` schema.

Full implementation lands in Stage 5 (Spark streaming): events/min by type,
active users, hashtag and language counts, deletions.
"""

from __future__ import annotations


def compute_aggregates(streaming_df):
    """TODO (Stage 5): windowed aggregation over the cleaned event stream,
    written to the realtime schema every 5 seconds."""
    raise NotImplementedError("Implemented in Stage 5 (Spark streaming)")
