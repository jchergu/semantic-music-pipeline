"""Verifies the Stage 16 consumer daemons against the live stack.

Two daemons, one per Decision C ownership half:
`session_consumer_daemon` maintains RAW state (`session:{id}:events` plus
the Postgres log), `refresh_daemon` maintains DERIVED recommendations
(`session:{id}:recs`). They run as separate processes in separate Kafka
consumer groups, and both are required for the pipeline to produce
anything.

These drive the daemons' real `run()` loops from worker threads with a
bounded `max_events`, rather than reimplementing them -- the same reason
those parameters exist. Signal handling is not exercised here (Python only
permits installing handlers on the main thread); `run()` takes its stop
Event as a parameter precisely so the loop is testable without them.
"""
import json
import sys
import threading
import uuid
from pathlib import Path

import httpx
import psycopg2
import pytest
import redis as redis_lib

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from streaming import refresh_daemon, session_consumer_daemon  # noqa: E402
from streaming.config import (  # noqa: E402
    PG_DSN,
    RECS_REFRESH_GROUP_ID,
    REDIS_HOST,
    REDIS_PORT,
    SESSION_CONSUMER_GROUP_ID,
)
from streaming.session_state import events_key, recs_key  # noqa: E402
from streaming.session_store import get_events  # noqa: E402
from streaming.topics import create_topics  # noqa: E402

SESSION_KEY_SUFFIXES = ("events", "profile", "profile_meta", "recs", "refresh_meta")


@pytest.fixture(scope="module", autouse=True)
def _ensure_topics():
    create_topics()


@pytest.fixture()
def redis_client():
    return redis_lib.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)


@pytest.fixture()
def pg_conn():
    conn = psycopg2.connect(PG_DSN)
    yield conn
    conn.close()


@pytest.fixture()
def session_id(redis_client):
    sid = f"stage16-daemon-{uuid.uuid4()}"
    yield sid
    redis_client.delete(*[f"session:{sid}:{suffix}" for suffix in SESSION_KEY_SUFFIXES])


def _post(ingestion_url: str, event: dict) -> None:
    httpx.post(f"{ingestion_url}/events", json=event, timeout=10.0).raise_for_status()


def test_daemons_default_to_the_canonical_consumer_groups():
    """A real deployment must use the documented group ids, not a string a
    caller made up -- Decision C's point about SESSION_CONSUMER_GROUP_ID.
    Tests below deliberately pass throwaway ids instead, so that committed
    offsets never interfere across runs of the shared, never-purged topic.
    """
    import inspect

    raw_default = inspect.signature(session_consumer_daemon.run).parameters["group_id"].default
    refresh_default = inspect.signature(refresh_daemon.run).parameters["group_id"].default
    assert raw_default == SESSION_CONSUMER_GROUP_ID
    assert refresh_default == RECS_REFRESH_GROUP_ID
    assert raw_default != refresh_default


def test_raw_daemon_drains_a_multi_event_session(event_ingestion_server, redis_client, pg_conn, session_id):
    """The daemon must advance past its first message. consume_and_cache_many
    with a *fresh* Consumer per call rescans from "earliest" every time, so
    a naive loop would re-handle event one forever -- the daemon reuses one
    Consumer for its whole life."""
    events = [
        {"session_id": session_id, "event_type": "play", "track_id": "1"},
        {"session_id": session_id, "event_type": "skip", "track_id": "1"},
        {"session_id": session_id, "event_type": "play", "track_id": "2"},
    ]
    for event in events:
        _post(event_ingestion_server, event)

    stats = session_consumer_daemon.run(
        group_id=f"stage16-raw-{uuid.uuid4()}", poll_timeout=2.0, max_events=len(events),
        expected_session_id=session_id,
    )

    assert stats["events"] == len(events)
    assert session_id in stats["sessions"]
    cached = [json.loads(raw) for raw in redis_client.lrange(events_key(session_id), 0, -1)]
    assert cached == events
    assert get_events(pg_conn, session_id) == events


def test_raw_daemon_stops_on_its_stop_event(event_ingestion_server, session_id):
    """Shutdown is cooperative: the loop finishes the message it holds and
    closes its connections rather than being torn down mid-write."""
    _post(event_ingestion_server, {"session_id": session_id, "event_type": "play", "track_id": "1"})

    stop = threading.Event()
    result: dict = {}

    def _run():
        result["stats"] = session_consumer_daemon.run(
            group_id=f"stage16-raw-{uuid.uuid4()}", poll_timeout=1.0, stop=stop,
            expected_session_id=session_id,
        )

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    stop.set()
    thread.join(timeout=30)

    assert not thread.is_alive()
    assert "stats" in result


def test_raw_daemon_runs_without_postgres(event_ingestion_server, redis_client, session_id):
    """--no-postgres is Redis-only: stage 9's behaviour, before stage 10
    added the durable log to the same consumer."""
    event = {"session_id": session_id, "event_type": "play", "track_id": "7"}
    _post(event_ingestion_server, event)

    stats = session_consumer_daemon.run(
        group_id=f"stage16-raw-{uuid.uuid4()}", poll_timeout=2.0, max_events=1, with_postgres=False,
        expected_session_id=session_id,
    )

    assert stats["events"] == 1
    assert redis_client.llen(events_key(session_id)) >= 1


def test_refresh_daemon_produces_recommendations(
    event_ingestion_server, semantic_api_server, redis_client, session_id
):
    """Both daemons together, which is the only configuration that produces
    anything: refresh_recommendations() reads session:{id}:events and
    returns skip_insufficient_data below two events, and only the raw-state
    daemon writes that key."""
    events = [
        {"session_id": session_id, "event_type": "play", "track_id": "2"},
        {"session_id": session_id, "event_type": "complete", "track_id": "2"},
        {"session_id": session_id, "event_type": "play", "track_id": "3"},
    ]

    suffix = uuid.uuid4()
    stop = threading.Event()
    results: dict = {}

    def _raw():
        results["raw"] = session_consumer_daemon.run(
            group_id=f"stage16-raw-{suffix}", poll_timeout=2.0, stop=stop, max_events=len(events),
            expected_session_id=session_id,
        )

    def _refresh():
        results["refresh"] = refresh_daemon.run(
            semantic_api_url=semantic_api_server, group_id=f"stage16-recs-{suffix}",
            poll_timeout=2.0, stop=stop, max_events=len(events),
            expected_session_id=session_id,
        )

    threads = [threading.Thread(target=_raw, daemon=True),
               threading.Thread(target=_refresh, daemon=True)]
    for thread in threads:
        thread.start()
    for event in events:
        _post(event_ingestion_server, event)
    for thread in threads:
        thread.join(timeout=90)
    stop.set()
    for thread in threads:
        thread.join(timeout=20)

    stats = results["refresh"]
    assert stats["processed"] == len(events)
    assert stats["refreshed"] >= 1
    # How many events get skipped for insufficient data is a RACE, not a
    # property: the two daemons are independent consumer groups, so the raw
    # one may already have cached all three events by the time the refresh
    # one processes the first. Asserting a skip here would be asserting a
    # scheduling outcome. What must hold is that the three buckets partition
    # everything processed -- a new action landing in the wrong one is
    # exactly the miscount this assertion caught.
    assert stats["refreshed"] + stats["debounced"] + stats["skipped"] == stats["processed"]
    assert stats["errors"] == 0

    recs = json.loads(redis_client.get(recs_key(session_id)))
    assert len(recs) > 0
    assert {"track_id", "title", "score"} <= set(recs[0])


# --- regression: the bug stage 16 found in stage 14's refresh ---


def test_as_track_id_skips_unparseable_ids_instead_of_raising():
    """Behavioral events carry a free-form `track_id` (the ingestion
    service's schema is extra="allow" and never validates against the
    catalog), but the refresh path indexes Postgres integer ids with it.
    Before stage 16 that was a bare int(), which raised ValueError straight
    out of refresh_recommendations()."""
    from streaming.recommendation_refresh import as_track_id

    assert as_track_id("42") == 42
    assert as_track_id(42) == 42
    assert as_track_id("xyz789") is None
    assert as_track_id(None) is None
    assert as_track_id("") is None


def test_skip_actions_never_count_as_a_refresh():
    """process_one_event() marks an event as real work by checking that the
    action is not a skip. Stage 16 added a second skip action, so the check
    is by prefix -- if a future skip stops matching, it would silently spend
    the debounce budget and be counted as a refresh that never happened."""
    import inspect

    from streaming import recommendation_refresh

    source = inspect.getsource(recommendation_refresh.process_one_event)
    assert 'did_real_work = not result["action"].startswith("skip_")' in source

    skip_actions = {
        action
        for action in ("skip_insufficient_data", "skip_unseedable_cold_start")
    }
    refresh_source = inspect.getsource(recommendation_refresh.refresh_recommendations)
    for action in skip_actions:
        assert f'"{action}"' in refresh_source
        assert action.startswith("skip_")


def test_refresh_daemon_survives_an_unprocessable_event(
    event_ingestion_server, semantic_api_server, redis_client, session_id
):
    """A daemon reads every session on the topic by design, so it is the
    first consumer exposed to arbitrary payloads -- including a track_id
    that is not a catalog id at all (stage 11's own test left `"xyz789"` on
    the never-purged topic, which is how this was found). It must keep
    running."""
    events = [
        {"session_id": session_id, "event_type": "play", "track_id": "xyz789"},
        {"session_id": session_id, "event_type": "play", "track_id": "not-a-track"},
        {"session_id": session_id, "event_type": "play", "track_id": "5"},
    ]

    suffix = uuid.uuid4()
    stop = threading.Event()
    results: dict = {}

    def _raw():
        results["raw"] = session_consumer_daemon.run(
            group_id=f"stage16-raw-{suffix}", poll_timeout=2.0, stop=stop,
            max_events=len(events), expected_session_id=session_id,
        )

    def _refresh():
        results["refresh"] = refresh_daemon.run(
            semantic_api_url=semantic_api_server, group_id=f"stage16-recs-{suffix}",
            poll_timeout=2.0, stop=stop, max_events=len(events),
            expected_session_id=session_id,
        )

    threads = [threading.Thread(target=_raw, daemon=True),
               threading.Thread(target=_refresh, daemon=True)]
    for thread in threads:
        thread.start()
    for event in events:
        _post(event_ingestion_server, event)
    for thread in threads:
        thread.join(timeout=90)
    stop.set()
    for thread in threads:
        thread.join(timeout=20)

    # Every event was processed and the loop reached its bound -- it did not
    # die on the unparseable ones.
    assert results["refresh"]["processed"] == len(events)
    assert results["refresh"]["errors"] == 0
    assert results["raw"]["events"] == len(events)
