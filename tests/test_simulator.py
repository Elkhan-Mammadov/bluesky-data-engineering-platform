"""Unit tests for ingestion/simulator/generator.py."""

from ingestion.common.config import AppConfig, DbConfig
from ingestion.simulator.generator import EventGenerator

_TRACKED_COLLECTIONS = {
    "post": "app.bsky.feed.post",
    "like": "app.bsky.feed.like",
    "repost": "app.bsky.feed.repost",
    "follow": "app.bsky.graph.follow",
    "block": "app.bsky.graph.block",
    "profile_update": "app.bsky.actor.profile",
}


def _make_config() -> AppConfig:
    dummy_db = DbConfig(host="x", port=5432, name="x", user="x", password="x")
    return AppConfig(
        source_mode="simulator",
        hash_salt="salt",
        sampling_rate=1.0,
        source_db=dummy_db,
        warehouse_db=dummy_db,
        settings={
            "tracked_collections": _TRACKED_COLLECTIONS,
            "simulator": {
                "events_per_second": 10,
                "dirty_record_rate": 0.0,
                "dirty_record_types": ["missing_required_field"],
                "schema_drift_after_seconds": 999_999,
                "bot_account_rate": 0.1,
                "bot_actions_per_minute": 200,
            },
        },
    )


def test_generated_event_matches_jetstream_shape():
    generator = EventGenerator(_make_config())

    event, _ = generator.generate_one()

    assert "did" in event
    assert "time_us" in event
    assert event["kind"] == "commit"
    assert event["commit"]["collection"] in _TRACKED_COLLECTIONS.values()
    assert "rkey" in event["commit"]


def test_dirty_record_rate_zero_never_mutates():
    generator = EventGenerator(_make_config())

    for _ in range(50):
        _, dirty_type = generator.generate_one()
        assert dirty_type is None


def test_dirty_record_rate_one_always_mutates():
    config = _make_config()
    config.settings["simulator"]["dirty_record_rate"] = 1.0
    generator = EventGenerator(config)

    _, dirty_type = generator.generate_one()

    assert dirty_type is not None


def test_generate_yields_duplicate_event_twice():
    config = _make_config()
    config.settings["simulator"]["dirty_record_rate"] = 1.0
    config.settings["simulator"]["dirty_record_types"] = ["duplicate_event"]
    generator = EventGenerator(config)

    stream = generator.generate()
    first = next(stream)
    second = next(stream)

    assert first == second


def test_schema_drift_adds_extra_field_once_triggered():
    config = _make_config()
    config.settings["simulator"]["schema_drift_after_seconds"] = -1  # already "elapsed"
    generator = EventGenerator(config)

    event, _ = generator.generate_one()

    assert "experimentalField" in event["commit"]["record"]
