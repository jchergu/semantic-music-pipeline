"""Stage 6 (8.1): end-to-end test.

Traces a small set of golden tracks through every layer of the pipeline —
Postgres/MinIO (L1) -> Milvus/Neo4j (L2) -> Semantic API (L3) ->
Recommender Engine (L3 demo) — asserting data consistency at each hop.
Runs entirely against the live, already-populated stack (stages 2-5 must
have already run); makes no destructive changes to existing data — the
recommender step writes its own rows under a fresh run_id and cleans them
up after.
"""
import sys
import uuid
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from recommender import recommend as recommend_cli  # noqa: E402

GOLDEN_TRACK_LIMIT = 3


@pytest.fixture(scope="module")
def golden_tracks(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT id, jamendo_id, title, artist_name, genre_tags, "
            "minio_bucket, minio_object_key FROM tracks ORDER BY id LIMIT %s",
            (GOLDEN_TRACK_LIMIT,),
        )
        cols = [
            "id",
            "jamendo_id",
            "title",
            "artist_name",
            "genre_tags",
            "minio_bucket",
            "minio_object_key",
        ]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def test_golden_tracks_exist_in_postgres_and_minio(golden_tracks, s3_client):
    for t in golden_tracks:
        assert t["title"]
        assert t["artist_name"]
        assert t["jamendo_id"]
        head = s3_client.head_object(Bucket=t["minio_bucket"], Key=t["minio_object_key"])
        assert head["ContentLength"] > 0


def test_golden_tracks_embedded_in_milvus(golden_tracks, milvus_collection):
    ids = [t["id"] for t in golden_tracks]
    results = milvus_collection.query(expr=f"track_id in {ids}", output_fields=["track_id"])
    found_ids = {r["track_id"] for r in results}
    assert found_ids == set(ids)


def test_golden_tracks_in_neo4j_graph_match_postgres(golden_tracks, neo4j_driver):
    with neo4j_driver.session() as session:
        for t in golden_tracks:
            result = session.run(
                "MATCH (a:Artist)-[:PERFORMED]->(track:Track {track_id: $id}) RETURN a.name AS artist",
                id=t["id"],
            ).single()
            assert result is not None, f"no Track/Artist edge in Neo4j for track {t['id']}"
            assert result["artist"] == t["artist_name"]


def test_golden_tracks_via_semantic_api_match_postgres(golden_tracks, semantic_api_server):
    with httpx.Client(base_url=semantic_api_server, timeout=10.0) as client:
        for t in golden_tracks:
            resp = client.get(f"/tracks/{t['id']}")
            assert resp.status_code == 200
            body = resp.json()
            assert body["title"] == t["title"]
            assert body["artist_name"] == t["artist_name"]
            assert set(body["genre_tags"]) == set(t["genre_tags"] or [])
            assert body["enriched"] is True

            graph_resp = client.get(f"/tracks/{t['id']}/graph")
            assert graph_resp.status_code == 200
            graph = graph_resp.json()
            assert graph["artist"] == t["artist_name"]

            similar_resp = client.get(f"/tracks/{t['id']}/similar", params={"k": 5})
            assert similar_resp.status_code == 200
            similar = similar_resp.json()
            assert len(similar) == 5
            assert all(s["track_id"] != t["id"] for s in similar)


def test_recommender_produces_valid_recommendations_for_golden_tracks(
    golden_tracks, pg_conn, semantic_api_server
):
    run_id = uuid.uuid4()
    top_k = 5
    try:
        for t in golden_tracks:
            recommend_cli.main(
                [
                    "--seed-track-id",
                    str(t["id"]),
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
                "SELECT r.seed_track_id, r.recommended_track_id, r.rank, r.score, t.title "
                "FROM recommendations r JOIN tracks t ON t.id = r.recommended_track_id "
                "WHERE r.run_id = %s ORDER BY r.seed_track_id, r.rank",
                (str(run_id),),
            )
            rows = cur.fetchall()

        # The JOIN itself is the real FK-integrity check: if a
        # recommended_track_id didn't correspond to a real `tracks` row,
        # that row would silently drop out of an inner join, so a full
        # row count here proves every recommendation traces back to a
        # genuine, existing track all the way through Postgres.
        assert len(rows) == len(golden_tracks) * top_k

        for seed_id, rec_id, rank, score, title in rows:
            assert rec_id != seed_id
            assert title
        scores_by_seed: dict[int, list[float]] = {}
        for seed_id, _, _, score, _ in rows:
            scores_by_seed.setdefault(seed_id, []).append(score)
        for scores in scores_by_seed.values():
            assert scores == sorted(scores, reverse=True)
    finally:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM recommendations WHERE run_id = %s", (str(run_id),))
        pg_conn.commit()
