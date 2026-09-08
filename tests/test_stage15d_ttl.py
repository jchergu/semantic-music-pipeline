"""Stage 15D: every derived session key expires, and an expired raw list no
longer makes a live session look unknown.

The finding this closes was surfaced by stage 16 and deliberately deferred
here (docs/platform/stage16-session-api.md): `session:{id}:events` carried a
30-minute sliding TTL while `:profile`, `:profile_meta`, `:recs` and
`:refresh_meta` carried none, so session_api could serve recommendations for
a session whose raw state expired hours earlier and the derived keyspace grew
without bound.

These run against the live stack, like every other non-pure test here. The
outage arms of stage 15D's failure injection live in `eval/8_2` instead --
they stop database containers, which would make this suite slow and
order-dependent.
"""
import json
import sys
import uuid
from pathlib import Path

import httpx
import pytest
import redis as redis_lib

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from streaming import recommendation_refresh  # noqa: E402
from streaming.config import DERIVED_TTL_SECONDS, REDIS_HOST, REDIS_PORT  # noqa: E402
from streaming.session_state import (  # noqa: E402
    events_key, profile_key, profile_meta_key, recs_key, refresh_meta_key,
)

SESSION_KEY_SUFFIXES = ("events", "profile", "profile_meta", "recs", "refresh_meta")

SAMPLE_RECS = [
    {"track_id": 14, "title": "Gates", "artist_name": "Bellevue", "similarity": 0.8499,
     "genre_sibling": True, "same_artist": False, "score": 0.8999},
]


@pytest.fixture()
def redis_client():
    return redis_lib.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)


@pytest.fixture()
def session_id(redis_client):
    sid = f"stage15d-test-{uuid.uuid4()}"
    yield sid
    redis_client.delete(*[f"session:{sid}:{suffix}" for suffix in SESSION_KEY_SUFFIXES])


def test_refresh_meta_key_is_canonical_and_distinct(session_id):
    """It was the one session key with no definition in session_state.py --
    built inline in recommendation_refresh.py -- which is how it came to be
    a derived key nobody noticed had no TTL."""
    assert refresh_meta_key(session_id) == f"session:{session_id}:refresh_meta"
    assert len({refresh_meta_key(session_id), profile_key(session_id),
                profile_meta_key(session_id), recs_key(session_id),
                events_key(session_id)}) == 5


def test_write_recs_sets_a_ttl(redis_client, session_id):
    recommendation_refresh._write_recs(redis_client, session_id, SAMPLE_RECS)

    ttl = redis_client.ttl(recs_key(session_id))
    # -1 means "exists, never expires" -- the exact state this stage closes.
    assert ttl > 0, "session:{id}:recs was written with no expiry"
    assert ttl <= DERIVED_TTL_SECONDS


def test_derived_ttl_matches_the_raw_session_ttl(session_id):
    """One constant, not two: derived state is meaningless without the
    session it derives from."""
    from streaming.config import SESSION_TTL_SECONDS
    assert DERIVED_TTL_SECONDS == SESSION_TTL_SECONDS


def test_expire_derived_applies_to_every_key_it_is_given(redis_client, session_id):
    from streaming.session_state import expire_derived
    redis_client.set(recs_key(session_id), "[]")
    redis_client.hset(refresh_meta_key(session_id), mapping={"events_since_refresh": 1})
    assert redis_client.ttl(recs_key(session_id)) == -1

    expire_derived(redis_client, session_id, recs_key(session_id), refresh_meta_key(session_id))

    assert redis_client.ttl(recs_key(session_id)) > 0
    assert redis_client.ttl(refresh_meta_key(session_id)) > 0


def test_flink_job_derived_ttl_matches_config():
    """The job's UDF workers have no platform/ on their path inside the
    container, so it duplicates the constant inline. Same cross-check
    arrangement test_stage13_session_profile.py applies to the key strings."""
    src = (ROOT / "platform" / "streaming" / "flink_session_profile_job.py").read_text()
    assert f"DERIVED_TTL_SECONDS = {DERIVED_TTL_SECONDS}" in src
    assert "ex=DERIVED_TTL_SECONDS" in src
    assert 'self._redis.expire(f"session:{key}:profile_meta", DERIVED_TTL_SECONDS)' in src


def test_expired_raw_state_no_longer_makes_a_live_session_unknown(
    session_api_server, redis_client, session_id
):
    """The `_missing()` collapse: with only :recs alive (raw events expired),
    /profile used to answer "unknown session" -- the exact conflation that
    function's docstring says it exists to prevent.

    Written against the pre-fix code first and confirmed to fail there.
    """
    redis_client.set(recs_key(session_id), json.dumps(SAMPLE_RECS), ex=DERIVED_TTL_SECONDS)
    assert redis_client.exists(events_key(session_id)) == 0

    response = httpx.get(f"{session_api_server}/sessions/{session_id}/profile", timeout=10.0)

    assert response.status_code == 404
    detail = response.json()["detail"]
    assert "unknown session" not in detail, detail
    assert "has no profile yet" in detail
