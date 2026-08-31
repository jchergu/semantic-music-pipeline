"""Verifies the Stage 10 Postgres sessions/events durability against the live stack."""
import json
import sys
import uuid
from pathlib import Path

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from streaming.config import PG_DSN, TOPIC_BEHAVIORAL_EVENTS  # noqa: E402
from streaming.producer import produce  # noqa: E402
from streaming.session_consumer import consume_and_cache_one  # noqa: E402
from streaming.session_state import get_session_events  # noqa: E402
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
