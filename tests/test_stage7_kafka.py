"""Verifies the Stage 7 Kafka topics + producer/consumer against the live broker."""
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from streaming.config import TOPIC_BEHAVIORAL_EVENTS, TOPIC_MEDIA_STREAM  # noqa: E402
from streaming.consumer import consume_one  # noqa: E402
from streaming.producer import produce  # noqa: E402
from streaming.topics import create_topics  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def _ensure_topics():
    create_topics()


def _fresh_group() -> str:
    return f"stage7-test-{uuid.uuid4()}"


def test_round_trip_media_stream():
    payload = str(uuid.uuid4())
    produce(TOPIC_MEDIA_STREAM, payload)
    assert consume_one(TOPIC_MEDIA_STREAM, _fresh_group(), timeout=10.0, expected_value=payload) == payload


def test_round_trip_behavioral_events():
    payload = str(uuid.uuid4())
    produce(TOPIC_BEHAVIORAL_EVENTS, payload)
    assert consume_one(TOPIC_BEHAVIORAL_EVENTS, _fresh_group(), timeout=10.0, expected_value=payload) == payload


def test_topic_isolation():
    payload = str(uuid.uuid4())
    produce(TOPIC_MEDIA_STREAM, payload)
    # a fresh consumer on the *other* topic must never see this specific
    # payload, regardless of whatever unrelated history that topic already
    # has from earlier test runs
    assert consume_one(TOPIC_BEHAVIORAL_EVENTS, _fresh_group(), timeout=3.0, expected_value=payload) is None
