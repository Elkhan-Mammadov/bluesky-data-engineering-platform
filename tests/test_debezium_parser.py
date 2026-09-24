"""Unit tests for streaming/spark_jobs/debezium_parser.py.

Real test cases (before/after/op extraction from the Debezium envelope) are
added in Stage 5 (Spark streaming), once parse_envelope() is implemented.
"""

import pytest


@pytest.mark.skip(reason="parse_envelope() is implemented in Stage 5 (Spark streaming)")
def test_parse_envelope_extracts_before_after_op():
    pass
