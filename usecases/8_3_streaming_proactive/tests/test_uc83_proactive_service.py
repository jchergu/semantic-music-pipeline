"""Integration test for 8.3's proactive_service.process_now_playing()
against the live stack (real Redis, real Semantic API via
semantic_api_server, real precomputed CLAP embeddings/labels) -- no mocks,
same standard this repo holds every other live-service test to.

Does not start proactive_service.py's own FastAPI app / Kafka consumer
loop; it calls process_now_playing() directly, the same way stage 14's
tests call recommendation_refresh.refresh_recommendations() directly
rather than driving it through a running daemon.
"""
import json
import sys
import uuid
from pathlib import Path

import httpx
import pytest
import redis

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent.parent / "platform"))

from proactive_service import load_label_embeddings, process_now_playing  # noqa: E402
from streaming.config import REDIS_HOST, REDIS_PORT  # noqa: E402
from streaming.session_state import live_tags_key, now_playing_key, proactive_suggestion_key  # noqa: E402

# Track 16 ("Give Me Hope", Modern Pitch) -- part of the running example's
# rock/pop/indie thread; already embedded (stage 3) and preloaded to Redis
# (stage 13's preload_embeddings_to_redis.py must have already run against
# this stack, same precondition every eval/8_2 test carries).
RUNNING_EXAMPLE_TRACK_ID = 16


@pytest.fixture
def redis_client():
    client = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)
    yield client
    client.close()


def test_process_now_playing_writes_tags_and_suggestion(semantic_api_server, redis_client):
    if redis_client.get(f"track:{RUNNING_EXAMPLE_TRACK_ID}:embedding") is None:
        pytest.skip("preload_embeddings_to_redis.py has not been run against this stack")

    session_id = f"test-uc83-{uuid.uuid4()}"
    event = {"session_id": session_id, "track_id": str(RUNNING_EXAMPLE_TRACK_ID), "event_time": "2000-01-01T00:00:00Z"}
    label_embeddings = load_label_embeddings()

    with httpx.Client(base_url=semantic_api_server, timeout=30.0) as http_client:
        result = process_now_playing(event, redis_client, http_client, label_embeddings)

    try:
        assert result is not None
        assert result["session_id"] == session_id
        assert result["now_playing"]["track_id"] == RUNNING_EXAMPLE_TRACK_ID
        assert result["now_playing"]["title"]

        assert len(result["live_tags"]) == 3
        for label, score in result["live_tags"]:
            assert isinstance(label, str)
            assert -1.0 <= score <= 1.0

        suggestion = result["proactive_suggestion"]
        assert suggestion is not None
        assert suggestion["track_id"] != RUNNING_EXAMPLE_TRACK_ID

        # Redis was actually written, not just returned.
        assert json.loads(redis_client.get(now_playing_key(session_id)))["track_id"] == RUNNING_EXAMPLE_TRACK_ID
        # JSON has no tuple type -- Redis round-trips live_tags as lists.
        assert json.loads(redis_client.get(live_tags_key(session_id))) == [list(t) for t in result["live_tags"]]
        assert json.loads(redis_client.get(proactive_suggestion_key(session_id))) == suggestion
    finally:
        redis_client.delete(now_playing_key(session_id), live_tags_key(session_id), proactive_suggestion_key(session_id))


def test_process_now_playing_returns_none_for_untracked_track(semantic_api_server, redis_client):
    session_id = f"test-uc83-{uuid.uuid4()}"
    event = {"session_id": session_id, "track_id": "999999", "event_time": "2000-01-01T00:00:00Z"}
    label_embeddings = load_label_embeddings()

    with httpx.Client(base_url=semantic_api_server, timeout=30.0) as http_client:
        result = process_now_playing(event, redis_client, http_client, label_embeddings)

    assert result is None
