"""Verifies the Stage 10 Postgres sessions/events durability against the live stack."""
import json
import sys
import uuid
from pathlib import Path

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "platform"))

import redis as redis_lib  # noqa: E402

from streaming.config import PG_DSN, REDIS_HOST, REDIS_PORT, TOPIC_BEHAVIORAL_EVENTS  # noqa: E402
from streaming.producer import produce  # noqa: E402
from streaming.session_consumer import (  # noqa: E402
    consume_and_cache_many,
    consume_and_cache_one,
    rebuild_session_state,
)
from streaming.session_state import events_key, get_session_events, profile_key  # noqa: E402
from streaming.session_store import apply_schema, get_events, persist_event  # noqa: E402
from streaming.topics import create_topics  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def _ensure_topics():
    create_topics()


@pytest.fixture()
def pg_conn():
    conn = psycopg2.connect(PG_DSN)
    apply_schema(conn)
    yield conn
    conn.close()


def _fresh_session_id() -> str:
    return f"stage10-test-{uuid.uuid4()}"


def _fresh_group() -> str:
    return f"stage10-test-{uuid.uuid4()}"


def test_persist_and_get_events(pg_conn):
    session_id = _fresh_session_id()
    event = {"event_type": "play", "track_id": "abc123"}
    persist_event(pg_conn, session_id, event)
    assert get_events(pg_conn, session_id) == [event]


def test_persist_event_upserts_session(pg_conn):
    session_id = _fresh_session_id()
    persist_event(pg_conn, session_id, {"event_type": "play"})
    persist_event(pg_conn, session_id, {"event_type": "skip"})

    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM sessions WHERE session_id = %s", (session_id,))
        assert cur.fetchone()[0] == 1

    assert len(get_events(pg_conn, session_id)) == 2


def test_consume_and_cache_persists_to_postgres(pg_conn):
    session_id = _fresh_session_id()
    event = {"session_id": session_id, "event_type": "skip", "track_id": "xyz789"}
    produce(TOPIC_BEHAVIORAL_EVENTS, json.dumps(event))

    result = consume_and_cache_one(
        _fresh_group(), timeout=10.0, expected_session_id=session_id, pg_conn=pg_conn
    )

    assert result == event
    assert get_session_events(session_id) == [event]
    assert get_events(pg_conn, session_id) == [event]


# --- Decision C, 2026-08-31: consumer ownership boundary ---


def test_events_and_profile_namespaces_do_not_collide():
    session_id = _fresh_session_id()
    events = events_key(session_id)
    profile = profile_key(session_id)

    assert events != profile
    assert events.endswith(":events")
    assert profile.endswith(":profile")
    # same session_id prefix, genuinely different keys -- not just different
    # suffixes on an otherwise-colliding string
    assert events.rsplit(":", 1)[0] == profile.rsplit(":", 1)[0]


def test_rebuild_session_state_recovers_from_postgres_after_redis_flush(pg_conn):
    session_id = _fresh_session_id()
    posted = [
        {"session_id": session_id, "event_type": "play", "track_id": "1"},
        {"session_id": session_id, "event_type": "skip", "track_id": "1"},
        {"session_id": session_id, "event_type": "play", "track_id": "2"},
    ]
    for event in posted:
        produce(TOPIC_BEHAVIORAL_EVENTS, json.dumps(event))
    # consume_and_cache_one never commits offsets (by design -- see its own
    # docstring), so a loop reusing it would just re-read the same first
    # match every time (the exact bug found and fixed in stage 12) --
    # consume_and_cache_many drains the whole batch with one Consumer.
    drained = consume_and_cache_many(
        _fresh_group(), count=len(posted), timeout=10.0, expected_session_id=session_id, pg_conn=pg_conn
    )
    assert drained == posted

    assert get_session_events(session_id) == posted
    assert get_events(pg_conn, session_id) == posted

    # flush Redis mid-session
    redis_client = redis_lib.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)
    redis_client.delete(events_key(session_id))
    assert get_session_events(session_id) == []

    rebuilt = rebuild_session_state(session_id, pg_conn)

    assert rebuilt == posted
    assert get_session_events(session_id) == get_events(pg_conn, session_id) == posted
