"""Verifies the Stage 5 Recommender Engine: ranking.py's pure scoring logic
(unit tests, no live services) and the end-to-end batch pipeline against a
live Postgres + a live, subprocess-managed Semantic API instance
(integration test; see `semantic_api_server` in tests/conftest.py)."""
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from recommender import ranking  # noqa: E402
from recommender import recommend as recommend_cli  # noqa: E402


# --- unit tests: ranking.py, pure function, synthetic candidates, no I/O ---


def test_ranking_orders_by_similarity_by_default():
    similar = [
        {"track_id": 2, "title": "B", "artist_name": "X", "score": 0.9},
        {"track_id": 3, "title": "C", "artist_name": "Y", "score": 0.5},
    ]
    result = ranking.score_recommendations(1, similar, [], [], top_k=10)
    assert [r["track_id"] for r in result] == [2, 3]
    assert result[0]["score"] == pytest.approx(0.9)


def test_ranking_applies_genre_and_artist_boosts_additively():
    similar = [{"track_id": 2, "title": "B", "artist_name": "X", "score": 0.5}]
    genre_siblings = [{"track_id": 2, "title": "B"}]
    same_artist = [{"track_id": 2, "title": "B"}]
    result = ranking.score_recommendations(
        1,
        similar,
        genre_siblings,
        same_artist,
        genre_boost=0.05,
        artist_boost=0.15,
        top_k=10,
    )
    assert result[0]["score"] == pytest.approx(0.5 + 0.05 + 0.15)


def test_ranking_includes_candidates_with_no_similarity_score():
    same_artist = [{"track_id": 5, "title": "Only via artist"}]
    result = ranking.score_recommendations(1, [], [], same_artist, artist_boost=0.15, top_k=10)
    assert result[0]["track_id"] == 5
    assert result[0]["score"] == pytest.approx(0.15)


def test_ranking_dedupes_when_a_track_appears_in_multiple_sources():
    similar = [{"track_id": 2, "title": "B", "artist_name": "X", "score": 0.4}]
    genre_siblings = [{"track_id": 2, "title": "B"}]
    result = ranking.score_recommendations(1, similar, genre_siblings, [], genre_boost=0.05, top_k=10)
    assert len(result) == 1


def test_ranking_excludes_seed_track_defensively():
    similar = [{"track_id": 1, "title": "Seed itself", "artist_name": "X", "score": 1.0}]
    result = ranking.score_recommendations(1, similar, [], [], top_k=10)
    assert result == []


def test_ranking_respects_top_k():
    similar = [
        {"track_id": i, "title": str(i), "artist_name": "X", "score": 1.0 / i} for i in range(2, 20)
    ]
    result = ranking.score_recommendations(1, similar, [], [], top_k=5)
    assert len(result) == 5


def test_ranking_tie_breaks_deterministically_by_track_id():
    similar = [
        {"track_id": 3, "title": "C", "artist_name": "X", "score": 0.5},
        {"track_id": 2, "title": "B", "artist_name": "Y", "score": 0.5},
    ]
    result = ranking.score_recommendations(1, similar, [], [], top_k=10)
    assert [r["track_id"] for r in result] == [2, 3]


# --- integration test: full batch pipeline, live stack + live API subprocess ---


def test_recommend_batch_writes_valid_rows(pg_conn, semantic_api_server):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT id FROM tracks ORDER BY id LIMIT 1")
        (seed_track_id,) = cur.fetchone()

    run_id = uuid.uuid4()
    top_k = 5
    try:
        recommend_cli.main(
            [
                "--seed-track-id",
                str(seed_track_id),
                "--api-base-url",
                semantic_api_server,
                "--run-id",
                str(run_id),
                "--top-k",
                str(top_k),
            ]
        )

        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT recommended_track_id, rank, score FROM recommendations "
                "WHERE run_id = %s AND seed_track_id = %s ORDER BY rank",
                (str(run_id), seed_track_id),
            )
            rows = cur.fetchall()

        assert len(rows) == top_k
        assert all(rec_id != seed_track_id for rec_id, _, _ in rows)
        assert [r[1] for r in rows] == list(range(1, top_k + 1))
        scores = [r[2] for r in rows]
        assert scores == sorted(scores, reverse=True)
    finally:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM recommendations WHERE run_id = %s", (str(run_id),))
        pg_conn.commit()
