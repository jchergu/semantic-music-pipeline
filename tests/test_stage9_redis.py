"""Verifies the Stage 9 Redis session cache against the live stack."""
import json
import sys
import uuid
from pathlib import Path

import pytest
import redis

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from streaming.config import REDIS_HOST, REDIS_PORT, SESSION_TTL_SECONDS, TOPIC_BEHAVIORAL_EVENTS  # noqa: E402
from streaming.producer import produce  # noqa: E402
from streaming.session_consumer import consume_and_cache_one  # noqa: E402
from streaming.session_state import get_session_events, record_event  # noqa: E402
from streaming.topics import create_topics  # noqa: E402

_redis = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)


@pytest.fixture(scope="module", autouse=True)
def _ensure_topics():
    create_topics()


def _fresh_session_id() -> str:
    return f"stage9-test-{uuid.uuid4()}"


def _fresh_group() -> str:
    return f"stage9-test-{uuid.uuid4()}"


def test_record_and_get_session_events():
    session_id = _fresh_session_id()
    event = {"event_type": "play", "track_id": "abc123"}
    record_event(session_id, event)
    assert get_session_events(session_id) == [event]


def test_session_ttl_is_set():
    session_id = _fresh_session_id()
    record_event(session_id, {"event_type": "play"})
    ttl = _redis.ttl(f"session:{session_id}:events")
    assert 0 < ttl <= SESSION_TTL_SECONDS


def test_consume_and_cache_from_behavioral_events():
    session_id = _fresh_session_id()
    event = {"session_id": session_id, "event_type": "skip", "track_id": "xyz789"}
    produce(TOPIC_BEHAVIORAL_EVENTS, json.dumps(event))

    result = consume_and_cache_one(_fresh_group(), timeout=10.0, expected_session_id=session_id)

    assert result == event
    assert get_session_events(session_id) == [event]
