"""Unit tests for ingestion/common/privacy.py."""

import pytest

from ingestion.common.privacy import hash_user_id, is_sampled

SALT = "test-salt"


def test_hash_is_deterministic():
    assert hash_user_id("did:plc:abc123", SALT) == hash_user_id("did:plc:abc123", SALT)


def test_hash_differs_per_user():
    assert hash_user_id("did:plc:abc123", SALT) != hash_user_id("did:plc:xyz789", SALT)


def test_hash_differs_per_salt():
    assert hash_user_id("did:plc:abc123", "salt-a") != hash_user_id("did:plc:abc123", "salt-b")


def test_hash_rejects_empty_input():
    with pytest.raises(ValueError):
        hash_user_id("", SALT)
    with pytest.raises(ValueError):
        hash_user_id("did:plc:abc123", "")


def test_sampling_is_deterministic_per_user():
    result_1 = is_sampled("did:plc:abc123", SALT, 0.5)
    result_2 = is_sampled("did:plc:abc123", SALT, 0.5)
    assert result_1 == result_2


def test_sampling_rate_zero_keeps_nobody():
    assert is_sampled("did:plc:abc123", SALT, 0.0) is False


def test_sampling_rate_one_keeps_everybody():
    assert is_sampled("did:plc:abc123", SALT, 1.0) is True


def test_sampling_rejects_invalid_rate():
    with pytest.raises(ValueError):
        is_sampled("did:plc:abc123", SALT, 1.5)
