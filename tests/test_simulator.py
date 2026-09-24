"""Unit tests for ingestion/simulator/generator.py.

Real test cases (event shape, dirty-record rate, schema drift, bot
behaviour) are added in Stage 3 (Ingestion), once EventGenerator.generate()
is implemented.
"""

import pytest


@pytest.mark.skip(reason="EventGenerator.generate() is implemented in Stage 3 (Ingestion)")
def test_generated_events_match_jetstream_shape():
    pass


@pytest.mark.skip(reason="EventGenerator.generate() is implemented in Stage 3 (Ingestion)")
def test_dirty_record_rate_is_respected():
    pass
