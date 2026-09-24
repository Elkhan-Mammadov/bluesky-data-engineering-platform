"""Unit tests for ingestion/jetstream/parser.py.

Real test cases (mapping Jetstream commit events to ParsedEvent, filtering
unknown collections, deriving text features) are added in Stage 3
(Ingestion), once parse_event() is implemented.
"""

import pytest


@pytest.mark.skip(reason="parser.parse_event() is implemented in Stage 3 (Ingestion)")
def test_parse_event_maps_known_collection():
    pass


@pytest.mark.skip(reason="parser.parse_event() is implemented in Stage 3 (Ingestion)")
def test_parse_event_skips_unknown_collection():
    pass
