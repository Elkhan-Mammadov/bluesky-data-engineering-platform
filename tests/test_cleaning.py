"""Unit tests for streaming/spark_jobs/cleaning.py.

Real test cases (required fields, timestamp bounds, unknown event types)
are added in Stage 5 (Spark streaming), once validate_row() is implemented.
"""

import pytest


@pytest.mark.skip(reason="validate_row() is implemented in Stage 5 (Spark streaming)")
def test_validate_row_rejects_missing_fields():
    pass


@pytest.mark.skip(reason="validate_row() is implemented in Stage 5 (Spark streaming)")
def test_validate_row_rejects_future_timestamp():
    pass
