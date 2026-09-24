"""Unit tests for ingestion/jetstream/parser.py."""

from ingestion.jetstream.parser import parse_event

TRACKED_COLLECTIONS = {
    "post": "app.bsky.feed.post",
    "like": "app.bsky.feed.like",
    "repost": "app.bsky.feed.repost",
    "follow": "app.bsky.graph.follow",
    "block": "app.bsky.graph.block",
    "profile_update": "app.bsky.actor.profile",
}


def _commit_event(collection: str, operation: str = "create", record: dict | None = None) -> dict:
    return {
        "did": "did:plc:author123",
        "time_us": 1_725_911_162_329_308,
        "kind": "commit",
        "commit": {
            "rev": "abc",
            "operation": operation,
            "collection": collection,
            "rkey": "rkey123",
            "record": record if record is not None else {},
        },
    }


def test_parse_post_extracts_text_features():
    event = _commit_event(
        "app.bsky.feed.post",
        record={"text": "hello #bluesky #python world https://example.com", "langs": ["en"]},
    )

    parsed = parse_event(event, TRACKED_COLLECTIONS)

    assert parsed is not None
    assert parsed.event_type == "post"
    assert parsed.user_did == "did:plc:author123"
    assert parsed.record_key == "rkey123"
    assert parsed.fields["language"] == "en"
    assert parsed.fields["hashtags"] == ["bluesky", "python"]
    assert parsed.fields["has_link"] is True
    assert parsed.fields["is_reply"] is False


def test_parse_post_without_langs_has_none_language():
    event = _commit_event("app.bsky.feed.post", record={"text": "no language tag"})

    parsed = parse_event(event, TRACKED_COLLECTIONS)

    assert parsed is not None
    assert parsed.fields["language"] is None


def test_parse_like_extracts_target():
    event = _commit_event(
        "app.bsky.feed.like",
        record={"subject": {"uri": "at://did:plc:target456/app.bsky.feed.post/postkey789"}},
    )

    parsed = parse_event(event, TRACKED_COLLECTIONS)

    assert parsed is not None
    assert parsed.event_type == "like"
    assert parsed.fields["target_user_did"] == "did:plc:target456"
    assert parsed.fields["target_record_key"] == "postkey789"


def test_parse_follow_extracts_target_did():
    event = _commit_event("app.bsky.graph.follow", record={"subject": "did:plc:target456"})

    parsed = parse_event(event, TRACKED_COLLECTIONS)

    assert parsed is not None
    assert parsed.event_type == "follow"
    assert parsed.fields["target_user_did"] == "did:plc:target456"


def test_parse_event_skips_unknown_collection():
    event = _commit_event("app.bsky.unknown.type")

    assert parse_event(event, TRACKED_COLLECTIONS) is None


def test_parse_event_skips_non_commit_kind():
    event = {"did": "did:plc:x", "time_us": 1, "kind": "identity"}

    assert parse_event(event, TRACKED_COLLECTIONS) is None


def test_parse_event_skips_missing_required_fields():
    event = _commit_event("app.bsky.feed.post")
    del event["did"]

    assert parse_event(event, TRACKED_COLLECTIONS) is None


def test_parse_delete_operation():
    event = _commit_event("app.bsky.feed.post", operation="delete", record=None)
    event["commit"].pop("record")

    parsed = parse_event(event, TRACKED_COLLECTIONS)

    assert parsed is not None
    assert parsed.operation == "delete"
