"""
Fresh per-stage latency measurement for eval/8_1's metric 4.

Not a replay of the original stage-5 batch run's timing -- that run never
recorded per-stage numbers, only the aggregate "~8.8s / 411 seeds" figure in
CLAUDE.md exists historically. These are new measurements against the
current environment, timing the same three logical stages recommend.py
performs for each seed: a Milvus ANN similarity search, a Neo4j graph query
(genre siblings + same-artist lookup), and the pure-Python ranking merge.
Mirrors platform/semantic_api/main.py's search()/graph query shape exactly,
so the timed calls are the real ones a live request would make, not a
simplified stand-in.
"""
import time

from pymilvus import Collection

CANDIDATE_K = 25  # matches usecases/8_1_batch_reactive/recommender/recommend.py's DEFAULT_CANDIDATE_K
TOP_K = 10


def time_one_seed(collection: Collection, neo4j_session, ranking_fn, seed_track_id: int) -> dict:
    t0 = time.perf_counter()

    query_result = collection.query(expr=f"track_id == {seed_track_id}", output_fields=["embedding"])
    embedding = query_result[0]["embedding"]
    search_result = collection.search(
        data=[embedding],
        anns_field="embedding",
        param={"metric_type": "COSINE", "params": {"nprobe": 16}},
        limit=CANDIDATE_K,
        expr=f"track_id != {seed_track_id}",
        output_fields=["track_id"],
    )
    similar = [
        {
            "track_id": hit.entity.get("track_id"),
            "title": None,
            "artist_name": None,
            "score": hit.distance,
        }
        for hit in search_result[0]
    ]
    t1 = time.perf_counter()

    neo4j_row = neo4j_session.run(
        """
        MATCH (t:Track {track_id: $id})
        OPTIONAL MATCH (t)-[:HAS_GENRE]->(:Genre)<-[:HAS_GENRE]-(sibling:Track) WHERE sibling <> t
        OPTIONAL MATCH (a:Artist)-[:PERFORMED]->(t)
        OPTIONAL MATCH (a)-[:PERFORMED]->(same:Track) WHERE same <> t
        RETURN collect(DISTINCT sibling.track_id) AS genre_siblings,
               collect(DISTINCT same.track_id) AS same_artist
        """,
        id=seed_track_id,
    ).single()
    genre_siblings = [{"track_id": tid, "title": None} for tid in neo4j_row["genre_siblings"]]
    same_artist = [{"track_id": tid, "title": None} for tid in neo4j_row["same_artist"]]
    t2 = time.perf_counter()

    ranking_fn(seed_track_id, similar, genre_siblings, same_artist, top_k=TOP_K)
    t3 = time.perf_counter()

    return {
        "milvus_ms": (t1 - t0) * 1000,
        "neo4j_ms": (t2 - t1) * 1000,
        "ranking_ms": (t3 - t2) * 1000,
        "total_ms": (t3 - t0) * 1000,
    }


def percentiles(values: list[float]) -> dict:
    if not values:
        return {"p50": None, "p95": None, "mean": None}
    sorted_vals = sorted(values)
    n = len(sorted_vals)

    def pct(p):
        idx = min(n - 1, int(round(p * (n - 1))))
        return round(sorted_vals[idx], 3)

    return {"p50": pct(0.50), "p95": pct(0.95), "mean": round(sum(sorted_vals) / n, 3)}


def summarize(per_seed_timings: list[dict]) -> dict:
    stages = ["milvus_ms", "neo4j_ms", "ranking_ms", "total_ms"]
    return {stage: percentiles([t[stage] for t in per_seed_timings]) for stage in stages}
