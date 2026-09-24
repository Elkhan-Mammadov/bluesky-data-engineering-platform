"""Unit tests for streaming/spark_jobs/cleaning.py."""

import time

from streaming.spark_jobs.cleaning import split_valid_and_quarantined, validate_row


def _valid_row(**overrides) -> dict:
    row = {
        "table": "posts",
        "op": "c",
        "before": None,
        "after": {"user_id_hash": "abc123", "record_key": "r1"},
        "source_ts_ms": int(time.time() * 1000),
        "kafka_offset": 1,
        "parse_error": None,
    }
    row.update(overrides)
    return row


def test_valid_row_passes():
    is_valid, reason = validate_row(_valid_row())

    assert is_valid is True
    assert reason is None


def test_row_with_parse_error_is_rejected():
    row = _valid_row(parse_error="bad json")

    is_valid, reason = validate_row(row)

    assert is_valid is False
    assert "unparseable" in reason


def test_row_with_unknown_table_is_rejected():
    row = _valid_row(table="not_a_real_table")

    is_valid, reason = validate_row(row)

    assert is_valid is False
    assert "unknown event type" in reason


def test_row_with_unknown_operation_is_rejected():
    row = _valid_row(op="x")

    is_valid, reason = validate_row(row)

    assert is_valid is False
    assert "unknown operation" in reason


def test_row_missing_user_id_hash_is_rejected():
    row = _valid_row(after={"record_key": "r1"})

    is_valid, reason = validate_row(row)

    assert is_valid is False
    assert "missing required field" in reason


def test_row_with_future_timestamp_is_rejected():
    row = _valid_row(source_ts_ms=int(time.time() * 1000) + 3_600_000)

    is_valid, reason = validate_row(row)

    assert is_valid is False
    assert "future" in reason


def test_row_with_ancient_timestamp_is_rejected():
    row = _valid_row(source_ts_ms=1)  # 1970

    is_valid, reason = validate_row(row)

    assert is_valid is False
    assert "past" in reason


def test_delete_row_checks_before_not_after():
    row = _valid_row(op="d", after=None, before={"user_id_hash": "abc123", "record_key": "r1"})

    is_valid, reason = validate_row(row)

    assert is_valid is True
    assert reason is None


def test_split_valid_and_quarantined_separates_rows():
    rows = [_valid_row(), _valid_row(table="bad_table")]

    valid, quarantined = split_valid_and_quarantined(rows)

    assert len(valid) == 1
    assert len(quarantined) == 1
    assert quarantined[0][1] == "unknown event type: bad_table"
