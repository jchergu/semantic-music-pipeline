"""Verifies the Stage 4 Semantic API against the live Postgres/Milvus/Neo4j state."""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from fastapi.testclient import TestClient  # noqa: E402

from semantic_api.main import app  # noqa: E402


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {
        "status": "ok",
        "services": {"postgres": True, "milvus": True, "neo4j": True},
    }


def test_get_track(client, pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT id, title, artist_name FROM tracks ORDER BY id LIMIT 1")
        track_id, title, artist_name = cur.fetchone()

    resp = client.get(f"/tracks/{track_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == track_id
    assert body["title"] == title
    assert body["artist_name"] == artist_name
    assert body["enriched"] is True


def test_get_track_not_found(client):
    resp = client.get("/tracks/999999999")
    assert resp.status_code == 404


def test_similar_tracks_excludes_self_and_respects_k(client, pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT id FROM tracks ORDER BY id LIMIT 1")
        (track_id,) = cur.fetchone()

    resp = client.get(f"/tracks/{track_id}/similar", params={"k": 5})
    assert resp.status_code == 200
    results = resp.json()
    assert len(results) == 5
    assert all(r["track_id"] != track_id for r in results)
    scores = [r["score"] for r in results]
    assert scores == sorted(scores, reverse=True)


def test_similar_tracks_not_found(client):
    resp = client.get("/tracks/999999999/similar")
    assert resp.status_code == 404


def test_track_graph_matches_postgres(client, pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT id, artist_name, genre_tags FROM tracks "
            "WHERE genre_tags != '{}' ORDER BY id LIMIT 1"
        )
        track_id, artist_name, genre_tags = cur.fetchone()

    resp = client.get(f"/tracks/{track_id}/graph")
    assert resp.status_code == 200
    body = resp.json()
    assert body["artist"] == artist_name
    assert set(body["genres"]) == set(genre_tags)


def test_track_graph_related_by_genre_ordered_by_shared_genre_count(client, pg_conn):
    """Stage 15A regression: related_by_genre used to be LIMIT 10 with no
    ORDER BY, so on a track with >10 genre siblings the 10 returned were
    an arbitrary cut. The fixed query orders by shared-genre-count
    descending, track_id ascending as tiebreak -- reconstructed here from
    Postgres genre_tags, which test_track_graph_matches_postgres already
    established mirrors Neo4j's HAS_GENRE edges exactly."""
    with pg_conn.cursor() as cur:
        cur.execute("SELECT id, genre_tags FROM tracks WHERE genre_tags != '{}'")
        genre_tags_by_id = {tid: set(tags) for tid, tags in cur.fetchall()}

    def shared_count(seed_id, other_id):
        return len(genre_tags_by_id[seed_id] & genre_tags_by_id[other_id])

    seed_id = next(
        (
            tid
            for tid in genre_tags_by_id
            if sum(1 for other in genre_tags_by_id if other != tid and shared_count(tid, other) > 0) > 10
        ),
        None,
    )
    assert seed_id is not None, "expected at least one seed track with >10 genre siblings in the dataset"

    resp = client.get(f"/tracks/{seed_id}/graph")
    assert resp.status_code == 200
    related = resp.json()["related_by_genre"]
    assert len(related) == 10

    shared_counts = [shared_count(seed_id, r["track_id"]) for r in related]
    assert shared_counts == sorted(shared_counts, reverse=True)

    expected_ids = sorted(
        (other for other in genre_tags_by_id if other != seed_id and shared_count(seed_id, other) > 0),
        key=lambda oid: (-shared_count(seed_id, oid), oid),
    )[:10]
    assert [r["track_id"] for r in related] == expected_ids


def test_artist_tracks_lookup(client, pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT artist_name FROM tracks ORDER BY id LIMIT 1")
        (artist_name,) = cur.fetchone()

    resp = client.get(f"/artists/{artist_name}/tracks")
    assert resp.status_code == 200
    assert len(resp.json()) >= 1


def test_genre_tracks_lookup(client, pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT genre_tags[1] FROM tracks WHERE genre_tags != '{}' ORDER BY id LIMIT 1"
        )
        (genre,) = cur.fetchone()

    resp = client.get(f"/genres/{genre}/tracks")
    assert resp.status_code == 200
    assert len(resp.json()) >= 1
