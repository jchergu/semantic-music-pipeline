"""Verifies the Stage 4 Semantic API against the live Postgres/Milvus/Neo4j state."""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient  # noqa: E402

from api.main import app  # noqa: E402


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
