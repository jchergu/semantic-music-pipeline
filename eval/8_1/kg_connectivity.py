"""
Neo4j degree-distribution queries for eval/8_1's KG connectivity metric
(metric 5). Same Cypher shapes as platform/semantic_api/main.py's graph
endpoints (:PERFORMED, :HAS_GENRE).
"""


def _distribution_stats(values: list[int]) -> dict:
    if not values:
        return {"count": 0, "min": None, "max": None, "mean": None, "median": None}
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mean = sum(sorted_vals) / n
    median = sorted_vals[n // 2] if n % 2 else (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
    return {
        "count": n,
        "min": sorted_vals[0],
        "max": sorted_vals[-1],
        "mean": round(mean, 3),
        "median": median,
    }


def track_degree_rows(session) -> list[dict]:
    result = session.run(
        """
        MATCH (t:Track)
        OPTIONAL MATCH (t)-[:HAS_GENRE]->(g:Genre)
        WITH t, count(g) AS genre_degree
        OPTIONAL MATCH (a:Artist)-[:PERFORMED]->(t)
        RETURN t.track_id AS track_id, genre_degree, count(a) AS artist_degree
        """
    )
    return [dict(r) for r in result]


def artist_degree_rows(session) -> list[dict]:
    result = session.run(
        "MATCH (a:Artist)-[:PERFORMED]->(t:Track) RETURN a.name AS artist_name, count(t) AS track_count"
    )
    return [dict(r) for r in result]


def genre_degree_rows(session) -> list[dict]:
    result = session.run(
        "MATCH (g:Genre)<-[:HAS_GENRE]-(t:Track) RETURN g.name AS genre_name, count(t) AS track_count"
    )
    return [dict(r) for r in result]


def capped_genre_sibling_ids(session, seed_track_ids: list[int], limit: int = 10) -> dict:
    """Reproduces platform/semantic_api/main.py's get_track_graph exactly
    (same MATCH pattern, no ORDER BY, LIMIT 10) -- the actual capped
    related_by_genre candidate set context_builder.py received and
    ranking.py applied GENRE_BOOST to at generation time, batched per seed
    via a Cypher subquery so LIMIT applies per-seed rather than globally.
    Ground truth for eval/8_1/metrics.py::reconstruct_signals's
    genre_sibling flag -- the complete tracks.genre_tags overlap is a
    different, broader fact (see metrics.py), not this one."""
    result = session.run(
        """
        UNWIND $seed_ids AS sid
        MATCH (t:Track {track_id: sid})
        CALL (t) {
            MATCH (t)-[:HAS_GENRE]->(:Genre)<-[:HAS_GENRE]-(other:Track)
            WHERE other <> t
            RETURN DISTINCT other.track_id AS track_id
            LIMIT $limit
        }
        RETURN sid AS seed_id, collect(track_id) AS sibling_ids
        """,
        seed_ids=seed_track_ids,
        limit=limit,
    )
    return {r["seed_id"]: set(r["sibling_ids"]) for r in result}


def genre_sibling_counts(session, seed_track_ids: list[int]) -> dict:
    """track_id -> number of distinct other tracks sharing at least one genre."""
    result = session.run(
        """
        UNWIND $seed_ids AS sid
        MATCH (t:Track {track_id: sid})
        OPTIONAL MATCH (t)-[:HAS_GENRE]->(:Genre)<-[:HAS_GENRE]-(other:Track)
        WHERE other <> t
        RETURN sid AS track_id, count(DISTINCT other) AS sibling_count
        """,
        seed_ids=seed_track_ids,
    )
    return {r["track_id"]: r["sibling_count"] for r in result}


def summarize(session, seed_track_ids: list[int]) -> dict:
    track_rows = track_degree_rows(session)
    artist_rows = artist_degree_rows(session)
    genre_rows = genre_degree_rows(session)
    sibling_counts = genre_sibling_counts(session, seed_track_ids)
    zero_sibling_ids = sorted(tid for tid, c in sibling_counts.items() if c == 0)

    return {
        "total_tracks": len(track_rows),
        "total_artists": len(artist_rows),
        "total_genres": len(genre_rows),
        "track_genre_out_degree": _distribution_stats([r["genre_degree"] for r in track_rows]),
        "track_artist_in_degree": _distribution_stats([r["artist_degree"] for r in track_rows]),
        "artist_track_out_degree": _distribution_stats([r["track_count"] for r in artist_rows]),
        "genre_track_in_degree": _distribution_stats([r["track_count"] for r in genre_rows]),
        "zero_genre_sibling_seed_count": len(zero_sibling_ids),
        "zero_genre_sibling_seed_ids": zero_sibling_ids,
    }
