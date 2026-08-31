"""Verifies the stage 14 recommendation refresh loop: the debounce/context
pure logic (unit tests, no live services) and the end-to-end refresh
behavior against the live stack, using the stage 12 simulator to produce
real events."""
import json
import sys
import time
import uuid
from pathlib import Path

import psycopg2
import pytest
import redis as redis_lib

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from simulator import events as sim_events  # noqa: E402
from simulator import scripts_io  # noqa: E402
from streaming.config import PG_DSN, REDIS_HOST, REDIS_PORT, RECS_REFRESH_GROUP_ID  # noqa: E402
from streaming.recommendation_refresh import (  # noqa: E402
    connect_milvus,
    connect_postgres,
    determine_context,
    disconnect_milvus,
    merge_current_event,
    new_consumer,
    process_one_event,
    should_refresh,
)
from streaming.session_state import profile_key, record_event, recs_key  # noqa: E402
from streaming.topics import create_topics  # noqa: E402

import httpx  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def _ensure_topics():
    create_topics()


@pytest.fixture()
def redis_client():
    return redis_lib.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)


def _fresh_session_id() -> str:
    return f"stage14-test-{uuid.uuid4()}"


def _fresh_group() -> str:
    return f"stage14-test-{uuid.uuid4()}"


# --- unit tests: should_refresh, pure, no I/O ---


def test_should_refresh_true_when_never_refreshed():
    assert should_refresh(None, 0, now=1000.0) is True


def test_should_refresh_true_after_interval_elapses():
    assert should_refresh(last_refresh_ts=994.0, events_since_refresh=0, now=1000.0) is True  # 6s elapsed


def test_should_refresh_false_before_interval_or_event_count():
    assert should_refresh(last_refresh_ts=998.0, events_since_refresh=1, now=1000.0) is False  # 2s, 1 event


def test_should_refresh_true_after_event_count_even_if_interval_not_elapsed():
    assert should_refresh(last_refresh_ts=999.0, events_since_refresh=3, now=1000.0) is True  # 1s, 3 events


def test_should_refresh_respects_custom_thresholds():
    assert should_refresh(999.0, 1, now=1000.0, min_interval_seconds=1.0, min_events=1) is True


# --- unit tests: determine_context, pure, no I/O ---


def test_determine_context_empty_session():
    anchor, played = determine_context([])
    assert anchor is None
    assert played == set()


def test_determine_context_anchor_is_most_recent():
    events = [{"track_id": "1"}, {"track_id": "2"}, {"track_id": "3"}]
    anchor, played = determine_context(events)
    assert anchor == "3"
    assert played == {"1", "2", "3"}


def test_determine_context_played_dedupes_repeated_tracks():
    events = [{"track_id": "1"}, {"track_id": "1"}, {"track_id": "2"}]
    _, played = determine_context(events)
    assert played == {"1", "2"}


# --- unit tests: merge_current_event, pure, no I/O ---


def test_merge_current_event_appends_when_missing():
    events = [{"track_id": "1"}]
    current = {"track_id": "2"}
    merged = merge_current_event(events, current)
    assert merged == [{"track_id": "1"}, {"track_id": "2"}]
    assert events == [{"track_id": "1"}]  # original not mutated


def test_merge_current_event_no_duplicate_when_already_last():
    events = [{"track_id": "1"}, {"track_id": "2"}]
    current = {"track_id": "2"}
    merged = merge_current_event(events, current)
    assert merged == events


def test_merge_current_event_none_is_a_no_op():
    events = [{"track_id": "1"}]
    assert merge_current_event(events, None) == events


# --- live: debounce + refresh against the real stack ---


@pytest.fixture()
def live_pg_conn():
    conn = connect_postgres()
    yield conn
    conn.close()


@pytest.fixture()
def live_milvus_collection():
    collection = connect_milvus()
    yield collection
    disconnect_milvus()


@pytest.fixture()
def live_http_client(event_ingestion_server, semantic_api_server):
    with httpx.Client(base_url=semantic_api_server, timeout=10.0) as client:
        yield client


def _post_event(ingestion_url: str, event: dict) -> None:
    resp = httpx.post(f"{ingestion_url}/events", json=event, timeout=10.0)
    assert resp.status_code == 202


def test_recs_visibly_change_after_a_skip_and_stay_stable_on_a_debounced_noop(
    event_ingestion_server, redis_client, live_pg_conn, live_milvus_collection, live_http_client
):
    session_id = _fresh_session_id()
    script = scripts_io.load_script(ROOT / "platform" / "simulator" / "scripts" / "three_early_skips.yaml")
    track_ids = [t["track_id"] for t in script["tracks"]]
    with connect_postgres() as durations_conn:
        with durations_conn.cursor() as cur:
            cur.execute("SELECT id, duration_sec FROM tracks WHERE id = ANY(%s)", (track_ids,))
            durations = dict(cur.fetchall())

    import datetime as dt

    planned = sim_events.build_session_events(script, session_id, durations, dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc))
    all_events = [p["event"] for p in planned]

    group_id = _fresh_group()
    # One Consumer, reused across every call below -- see
    # process_one_event's own docstring: offsets are never committed, so a
    # fresh Consumer per call would always rescan from "earliest" and
    # never advance past the first matching message.
    consumer = new_consumer(group_id)

    def _post_and_record(event: dict) -> None:
        # In production, the independent stages-9-10 consumer group caches
        # every event into session:{id}:events (Decision C) -- this test
        # doesn't run that consumer, so it stands in for it directly. Only
        # the *current* event's own race with that consumer is what
        # merge_current_event exists to cover (see its unit tests above);
        # history needs to actually be there for determine_context to see
        # more than one event at a time.
        _post_event(event_ingestion_server, event)
        record_event(session_id, event)

    # Event 1 (play track 1): fewer than 2 raw events -> should_refresh is
    # True (never refreshed), but refresh_recommendations short-circuits
    # on insufficient data -- confirms the cold-start guard, not a crash.
    # refreshed=False here because no real work happened (no recs were
    # written) -- and, just as importantly, this must NOT consume debounce
    # budget, or the very next (real) refresh would be wrongly suppressed.
    _post_and_record(all_events[0])
    result_1 = process_one_event(group_id, redis_client, live_pg_conn, live_milvus_collection, live_http_client, expected_session_id=session_id, consumer=consumer)
    assert result_1["refreshed"] is False
    assert result_1["action"] == "skip_insufficient_data"
    assert redis_client.get(recs_key(session_id)) is None

    # Event 2 (skip track 1, position 2500ms): now >= 2 raw events -> cold
    # start fallback fires for real, writes recs for the first time.
    _post_and_record(all_events[1])
    result_2 = process_one_event(group_id, redis_client, live_pg_conn, live_milvus_collection, live_http_client, expected_session_id=session_id, consumer=consumer)
    assert result_2["refreshed"] is True
    assert result_2["action"] == "cold_start"
    recs_after_skip = json.loads(redis_client.get(recs_key(session_id)))
    assert len(recs_after_skip) > 0

    # Event 3 (play track 4): events_since_refresh=1, well under 5s and
    # under 3 events -> debounced no-op. recs must NOT change.
    _post_and_record(all_events[2])
    result_3 = process_one_event(group_id, redis_client, live_pg_conn, live_milvus_collection, live_http_client, expected_session_id=session_id, consumer=consumer)
    assert result_3["refreshed"] is False
    recs_after_noop = json.loads(redis_client.get(recs_key(session_id)))
    assert recs_after_noop == recs_after_skip

    # Two more events (skip track 4, play track 7) push events_since_refresh
    # to 3 -> debounce threshold crossed on event count alone -> the
    # refresh mechanism re-arms and actually runs again (refreshed=True),
    # proving the count-based debounce branch independently of the
    # interval-based one already proven above. The *content* is expected
    # to be identical: cold_start always seeds from events[0] (the
    # session's first track), not the evolving "active context" the warm
    # path uses -- still only 5 raw events, nowhere near enough for a
    # stage-13 profile to exist for a session this short-lived, so the
    # seed genuinely hasn't changed between this refresh and the last one.
    _post_and_record(all_events[3])
    result_4 = process_one_event(group_id, redis_client, live_pg_conn, live_milvus_collection, live_http_client, expected_session_id=session_id, consumer=consumer)
    assert result_4["refreshed"] is False

    _post_and_record(all_events[4])
    result_5 = process_one_event(group_id, redis_client, live_pg_conn, live_milvus_collection, live_http_client, expected_session_id=session_id, consumer=consumer)
    assert result_5["refreshed"] is True
    assert result_5["action"] == "cold_start"
    assert result_5["seed_track_id"] == result_2["seed_track_id"]
    recs_after_third_event = json.loads(redis_client.get(recs_key(session_id)))
    assert recs_after_third_event == recs_after_noop

    consumer.close()


def test_warm_path_excludes_already_played_tracks(
    event_ingestion_server, redis_client, live_pg_conn, live_milvus_collection, live_http_client
):
    """Exercises the actual new logic this stage adds (cold_start above is
    just reused 8.1 logic): Milvus search over a profile vector, excluding
    already-played tracks, re-ranked against active context. Seeds a fake
    session:{id}:profile directly rather than waiting on stage 13's Flink
    job, which needs real wall-clock window time to produce one -- the
    warm path's mechanics don't care where the vector came from."""
    session_id = _fresh_session_id()
    events = [
        {"session_id": session_id, "event_type": "play", "track_id": "1", "event_time": "2026-01-01T00:00:00+00:00", "position_ms": 0},
        {"session_id": session_id, "event_type": "complete", "track_id": "3", "event_time": "2026-01-01T00:05:00+00:00", "position_ms": 200000},
    ]
    for event in events:
        _post_event(event_ingestion_server, event)
        record_event(session_id, event)

    query_result = live_milvus_collection.query(expr="track_id == 2", output_fields=["embedding"])
    fake_profile = [float(v) for v in query_result[0]["embedding"]]
    redis_client.set(profile_key(session_id), json.dumps(fake_profile))

    current_event = {
        "session_id": session_id,
        "event_type": "play",
        "track_id": "3",
        "event_time": "2026-01-01T00:05:01+00:00",
        "position_ms": 0,
    }
    _post_event(event_ingestion_server, current_event)

    group_id = _fresh_group()
    consumer = new_consumer(group_id)
    try:
        result = process_one_event(
            group_id, redis_client, live_pg_conn, live_milvus_collection, live_http_client, expected_session_id=session_id, consumer=consumer
        )
    finally:
        consumer.close()

    assert result["refreshed"] is True
    assert result["action"] == "warm"
    recs = json.loads(redis_client.get(recs_key(session_id)))
    assert len(recs) > 0
    played_ids = {"1", "3"}
    assert all(str(r["track_id"]) not in played_ids for r in recs)
