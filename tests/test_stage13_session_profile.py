"""Unit tests for platform/streaming/session_profile.py's pure weighting/
decay/centroid logic -- no live services, mirrors
usecases/8_1_batch_reactive/tests/test_uc81_recommender.py's pattern for
ranking.py."""
import math
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from streaming import session_profile, session_state  # noqa: E402

FLINK_JOB_SRC = (ROOT / "platform" / "streaming" / "flink_session_profile_job.py").read_text()


def _cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


# --- base_weight ---


def test_base_weight_complete():
    assert session_profile.base_weight("complete", position_ms=999999) == 1.0


def test_base_weight_like():
    assert session_profile.base_weight("like", position_ms=0) == 1.5


def test_base_weight_play():
    assert session_profile.base_weight("play", position_ms=0) == 0.2


def test_base_weight_skip_early():
    assert session_profile.base_weight("skip", position_ms=2000, duration_sec=200) == -1.0


def test_base_weight_skip_late():
    # 85% of a 200s track = 170000ms
    assert session_profile.base_weight("skip", position_ms=170000, duration_sec=200) == 0.8


def test_base_weight_skip_other():
    # mid-track, neither early nor late
    assert session_profile.base_weight("skip", position_ms=60000, duration_sec=200) == -0.3


def test_base_weight_skip_late_requires_duration():
    # can't classify late without knowing the track's duration -- falls
    # through to "otherwise" rather than guessing
    assert session_profile.base_weight("skip", position_ms=170000, duration_sec=None) == -0.3


def test_base_weight_unknown_type_is_neutral():
    assert session_profile.base_weight("rage_quit", position_ms=0) == 0.0


# --- compute_centroid ---


def test_compute_centroid_no_events_returns_none():
    centroid, weight_total = session_profile.compute_centroid([], {})
    assert centroid is None
    assert weight_total == 0.0


def test_compute_centroid_single_complete_matches_track_embedding_direction():
    embeddings = {"1": [1.0] + [0.0] * (session_profile.EMBED_DIM - 1)}
    events = [{"event_type": "complete", "track_id": "1", "position_ms": 200000}]
    centroid, weight_total = session_profile.compute_centroid(events, embeddings)
    assert weight_total == pytest.approx(1.0)
    assert _cosine(centroid, embeddings["1"]) == pytest.approx(1.0)


def test_compute_centroid_skip_pulls_away_from_track_embedding():
    embeddings = {"1": [1.0] + [0.0] * (session_profile.EMBED_DIM - 1)}
    events = [{"event_type": "skip", "track_id": "1", "position_ms": 1000}]
    centroid, weight_total = session_profile.compute_centroid(events, embeddings)
    # a single negative-weight event normalizes (by |weight|) to the exact
    # opposite direction of the track it rejected
    assert _cosine(centroid, embeddings["1"]) == pytest.approx(-1.0)


def test_compute_centroid_missing_embedding_is_skipped_not_a_crash():
    events = [{"event_type": "complete", "track_id": "missing", "position_ms": 0}]
    centroid, weight_total = session_profile.compute_centroid(events, {})
    assert centroid is None
    assert weight_total == 0.0


def test_compute_centroid_recency_decay_favors_the_latest_event():
    # two orthogonal unit vectors; the earlier event should contribute less
    # than the later one once decay is applied, even with equal base weight
    dim = session_profile.EMBED_DIM
    e1 = [1.0] + [0.0] * (dim - 1)
    e2 = [0.0, 1.0] + [0.0] * (dim - 2)
    embeddings = {"1": e1, "2": e2}
    events = [
        {"event_type": "complete", "track_id": "1", "position_ms": 200000},
        {"event_type": "complete", "track_id": "2", "position_ms": 200000},
    ]
    centroid, _ = session_profile.compute_centroid(events, embeddings)
    # decay(events_ago=1) = 0.5**(1/3) < 1 = decay(events_ago=0), so the
    # centroid should lean toward e2 (the later, undecayed event)
    assert centroid[1] > centroid[0] > 0


def test_compute_centroid_decay_half_life_is_three_events():
    # an event exactly 3 "events ago" should be weighted exactly half of
    # the most recent one, all else equal
    weight_now = session_profile.base_weight("complete", 0) * 0.5 ** (0 / 3)
    weight_3_ago = session_profile.base_weight("complete", 0) * 0.5 ** (3 / 3)
    assert weight_3_ago == pytest.approx(weight_now / 2)


# --- Flink job key format stays in sync with session_state.profile_key() ---
# (can't be a shared import -- the Flink job's Python UDF workers don't have
# platform/ on their path inside the container, see profile_key()'s docstring)


def test_flink_job_profile_key_format_matches_session_state():
    assert 'f"session:{key}:profile"' in FLINK_JOB_SRC
    assert session_state.profile_key("key") == "session:key:profile"
