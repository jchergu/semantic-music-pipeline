"""Verifies the Stage 16 Session API against the live stack.

Decision E: recommendation delivery is a separate, read-only service over
Redis, request/response rather than WebSocket, and it computes nothing --
`streaming/refresh_daemon.py` produces, this serves. These tests hold that
line: the endpoint shapes are checked against state seeded directly into
Redis, and the last test drives the whole real chain (HTTP event -> Kafka
-> both daemons -> Redis -> this API) to prove the delivery path works end
to end rather than only against fixtures.
"""
import json
import sys
import threading
import uuid
from pathlib import Path

import httpx
import pytest
import redis as redis_lib

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from streaming import refresh_daemon, session_consumer_daemon  # noqa: E402
from streaming.config import REDIS_HOST, REDIS_PORT  # noqa: E402
from streaming.session_state import (  # noqa: E402
    events_key,
    profile_key,
    profile_meta_key,
    recs_key,
)
from streaming.topics import create_topics  # noqa: E402

SESSION_KEY_SUFFIXES = ("events", "profile", "profile_meta", "recs", "refresh_meta")

SAMPLE_RECS = [
    {"track_id": 14, "title": "Gates", "artist_name": "Bellevue", "similarity": 0.8499,
     "genre_sibling": True, "same_artist": False, "score": 0.8999},
    {"track_id": 59, "title": "You Can't Have Everything", "artist_name": "Chasing Eidolon",
     "similarity": 0.8449, "genre_sibling": False, "same_artist": False, "score": 0.8449},
]


@pytest.fixture(scope="module", autouse=True)
def _ensure_topics():
    create_topics()


@pytest.fixture()
def redis_client():
    return redis_lib.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)


@pytest.fixture()
def session_id(redis_client):
    sid = f"stage16-test-{uuid.uuid4()}"
    yield sid
    redis_client.delete(*[f"session:{sid}:{suffix}" for suffix in SESSION_KEY_SUFFIXES])


def _seed_raw_event(redis_client, session_id: str, track_id: str = "1") -> None:
    redis_client.rpush(
        events_key(session_id),
        json.dumps({"session_id": session_id, "event_type": "play", "track_id": track_id}),
    )
    redis_client.expire(events_key(session_id), 1800)


def test_health(session_api_server):
    response = httpx.get(f"{session_api_server}/health", timeout=10.0)
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_recommendations_returns_what_the_refresh_wrote(session_api_server, redis_client, session_id):
    redis_client.set(recs_key(session_id), json.dumps(SAMPLE_RECS))

    response = httpx.get(f"{session_api_server}/sessions/{session_id}/recommendations", timeout=10.0)

    assert response.status_code == 200
    body = response.json()
    assert body["session_id"] == session_id
    assert body["count"] == 2
    assert [r["track_id"] for r in body["recommendations"]] == [14, 59]
    # Served verbatim, not re-enriched: every field the ranker persisted
    # comes back, which is why this service needs no Postgres.
    assert body["recommendations"][0] == SAMPLE_RECS[0]


def test_recommendations_tolerate_a_null_artist_name(session_api_server, redis_client, session_id):
    """ranking.py's _touch() creates candidates from a genre-sibling or
    same-artist hit that Milvus never returned, and those carry no artist
    name -- a non-optional field here would 500 on a real row."""
    row = {**SAMPLE_RECS[0], "artist_name": None}
    redis_client.set(recs_key(session_id), json.dumps([row]))

    response = httpx.get(f"{session_api_server}/sessions/{session_id}/recommendations", timeout=10.0)

    assert response.status_code == 200
    assert response.json()["recommendations"][0]["artist_name"] is None


def test_profile_returns_the_centroid_with_its_provenance(session_api_server, redis_client, session_id):
    vector = [0.1, -0.2, 0.3]
    redis_client.set(profile_key(session_id), json.dumps(vector))
    redis_client.hset(profile_meta_key(session_id),
                      mapping={"computed_at": "1788687246.1255796", "n_events": "4"})

    response = httpx.get(f"{session_api_server}/sessions/{session_id}/profile", timeout=10.0)

    assert response.status_code == 200
    body = response.json()
    assert body["vector"] == vector
    assert body["dimension"] == 3
    assert body["n_events"] == 4
    assert body["computed_at"] == pytest.approx(1788687246.1255796)


def test_unknown_session_is_404_everywhere(session_api_server):
    unknown = f"stage16-never-existed-{uuid.uuid4()}"
    for path in ("", "/recommendations", "/profile"):
        response = httpx.get(f"{session_api_server}/sessions/{unknown}{path}", timeout=10.0)
        assert response.status_code == 404, path
        assert "unknown session" in response.json()["detail"], path


def test_a_known_session_without_recs_is_not_reported_as_unknown(
    session_api_server, redis_client, session_id
):
    """A wrong session id and a session whose first events haven't cleared
    the debounce are completely different situations for a client;
    collapsing both into one 404 would make the second look like a bug."""
    _seed_raw_event(redis_client, session_id)

    response = httpx.get(f"{session_api_server}/sessions/{session_id}/recommendations", timeout=10.0)

    assert response.status_code == 404
    detail = response.json()["detail"]
    assert "exists but has no recommendations yet" in detail
    assert "unknown session" not in detail


def test_status_reports_which_state_exists_and_why_recs_are_missing(
    session_api_server, redis_client, session_id
):
    _seed_raw_event(redis_client, session_id)

    body = httpx.get(f"{session_api_server}/sessions/{session_id}", timeout=10.0).json()

    assert body["raw_events"] == {"present": True, "count": 1, "ttl_seconds": 1800}
    assert body["profile"]["present"] is False
    assert body["recommendations"] == {"present": False, "count": 0}
    assert body["raw_state_expired"] is False
    assert any("No recommendations yet" in note for note in body["notes"])
    assert any("No session profile yet" in note for note in body["notes"])


def test_status_flags_derived_state_outliving_expired_raw_state(
    session_api_server, redis_client, session_id
):
    """The stage 16 finding, deferred to 15D: session:{id}:events carries a
    30-minute sliding TTL while :recs and :profile have none, so this
    service can serve recommendations for a session whose raw state is long
    gone. Reported rather than hidden."""
    redis_client.set(recs_key(session_id), json.dumps(SAMPLE_RECS))

    body = httpx.get(f"{session_api_server}/sessions/{session_id}", timeout=10.0).json()

    assert body["raw_events"]["present"] is False
    assert body["recommendations"]["present"] is True
    assert body["raw_state_expired"] is True
    assert any("30-minute sliding TTL" in note for note in body["notes"])


def test_delivery_path_end_to_end(
    session_api_server, event_ingestion_server, semantic_api_server, redis_client, session_id
):
    """The stage 16 exit criterion, over the real chain and nothing seeded:
    POST behavioral events -> Kafka -> BOTH daemons -> Redis -> GET
    /recommendations returns a populated ranked list.

    Both daemons are required, which is the point of running them together
    here. refresh_recommendations() reads session:{id}:events and returns
    skip_insufficient_data below two events, and only the raw-state daemon
    ever writes that key -- with refresh_daemon alone this would return 404
    forever.
    """
    events = [
        {"session_id": session_id, "event_type": "play", "track_id": "2"},
        {"session_id": session_id, "event_type": "complete", "track_id": "2"},
        {"session_id": session_id, "event_type": "play", "track_id": "3"},
    ]

    group_suffix = uuid.uuid4()
    stop = threading.Event()
    results: dict = {}

    def _raw():
        results["raw"] = session_consumer_daemon.run(
            group_id=f"stage16-raw-{group_suffix}", poll_timeout=2.0,
            stop=stop, max_events=len(events), expected_session_id=session_id,
        )

    def _refresh():
        results["refresh"] = refresh_daemon.run(
            semantic_api_url=semantic_api_server, group_id=f"stage16-recs-{group_suffix}",
            poll_timeout=2.0, stop=stop, max_events=len(events),
            expected_session_id=session_id,
        )

    threads = [threading.Thread(target=_raw, daemon=True),
               threading.Thread(target=_refresh, daemon=True)]
    for thread in threads:
        thread.start()

    for event in events:
        httpx.post(f"{event_ingestion_server}/events", json=event, timeout=10.0).raise_for_status()

    for thread in threads:
        thread.join(timeout=90)
    stop.set()
    for thread in threads:
        thread.join(timeout=20)

    assert results["raw"]["events"] == len(events)
    assert results["raw"]["sessions"] == [session_id]
    assert results["refresh"]["refreshed"] >= 1

    response = httpx.get(f"{session_api_server}/sessions/{session_id}/recommendations", timeout=10.0)
    assert response.status_code == 200
    body = response.json()
    assert body["count"] > 0
    played = {"2", "3"}
    assert not played & {str(r["track_id"]) for r in body["recommendations"]}

    status = httpx.get(f"{session_api_server}/sessions/{session_id}", timeout=10.0).json()
    assert status["raw_events"]["count"] == len(events)
    assert status["recommendations"]["present"] is True
