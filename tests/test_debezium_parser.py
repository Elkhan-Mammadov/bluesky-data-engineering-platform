"""Unit tests for streaming/spark_jobs/debezium_parser.py."""

import json

from streaming.spark_jobs.debezium_parser import parse_envelope


def _kafka_row(topic: str, envelope: dict, offset: int = 42) -> dict:
    return {"topic": topic, "value": json.dumps(envelope), "offset": offset}


def test_parse_envelope_extracts_before_after_op():
    envelope = {
        "before": None,
        "after": {"user_id_hash": "abc", "record_key": "r1"},
        "source": {"ts_ms": 1_700_000_000_000, "table": "posts"},
        "op": "c",
    }

    parsed = parse_envelope(_kafka_row("bluesky.public.posts", envelope))

    assert parsed["table"] == "posts"
    assert parsed["op"] == "c"
    assert parsed["after"] == {"user_id_hash": "abc", "record_key": "r1"}
    assert parsed["before"] is None
    assert parsed["source_ts_ms"] == 1_700_000_000_000
    assert parsed["kafka_offset"] == 42
    assert parsed["parse_error"] is None


def test_parse_envelope_handles_delete():
    envelope = {
        "before": {"user_id_hash": "abc", "record_key": "r1"},
        "after": None,
        "source": {"ts_ms": 1_700_000_000_000},
        "op": "d",
    }

    parsed = parse_envelope(_kafka_row("bluesky.public.likes", envelope))

    assert parsed["op"] == "d"
    assert parsed["before"]["user_id_hash"] == "abc"
    assert parsed["after"] is None


def test_parse_envelope_maps_unknown_topic_to_none_table():
    parsed = parse_envelope({"topic": "some.other.topic", "value": "{}", "offset": 1})

    assert parsed["table"] is None


def test_parse_envelope_sets_parse_error_on_invalid_json():
    parsed = parse_envelope({"topic": "bluesky.public.posts", "value": "not-json", "offset": 1})

    assert parsed["parse_error"] is not None
