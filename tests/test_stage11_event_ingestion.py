"""Verifies the Stage 11 event ingestion service against the live stack."""
import sys
import uuid
from pathlib import Path

import httpx
import psycopg2
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from streaming.config import PG_DSN  # noqa: E402
from streaming.session_consumer import consume_and_cache_one  # noqa: E402
from streaming.session_state import get_session_events  # noqa: E402
from streaming.session_store import apply_schema, get_events  # noqa: E402
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
    return f"stage11-test-{uuid.uuid4()}"


def _fresh_group() -> str:
    return f"stage11-test-{uuid.uuid4()}"


def test_health(event_ingestion_server):
    response = httpx.get(f"{event_ingestion_server}/health", timeout=10.0)
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_post_event_produces_to_kafka(event_ingestion_server):
    session_id = _fresh_session_id()
    event = {"session_id": session_id, "event_type": "play", "track_id": "abc123"}

    response = httpx.post(f"{event_ingestion_server}/events", json=event, timeout=10.0)
    assert response.status_code == 202

    result = consume_and_cache_one(_fresh_group(), timeout=10.0, expected_session_id=session_id)
    assert result == event


def test_end_to_end_http_to_redis_and_postgres(event_ingestion_server, pg_conn):
    session_id = _fresh_session_id()
    event = {"session_id": session_id, "event_type": "skip", "track_id": "xyz789"}

    response = httpx.post(f"{event_ingestion_server}/events", json=event, timeout=10.0)
    assert response.status_code == 202

    result = consume_and_cache_one(
        _fresh_group(), timeout=10.0, expected_session_id=session_id, pg_conn=pg_conn
    )

    assert result == event
    assert get_session_events(session_id) == [event]
    assert get_events(pg_conn, session_id) == [event]
