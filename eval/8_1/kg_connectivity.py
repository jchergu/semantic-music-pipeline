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
    """Reproduces platform/semantic_api/main.py's CURRENT get_track_graph
    query exactly (same MATCH pattern, ORDER BY shared genre count
    descending / track_id ascending, LIMIT 10, per the Stage 15A fix) --
    the capped related_by_genre candidate set context_builder.py receives
    and ranking.py applies GENRE_BOOST to today. Ground truth for
    eval/8_1/metrics.py::reconstruct_signals's genre_sibling flag against
    eval/8_1/regenerate_recommendations.py's regenerated table -- the
    complete tracks.genre_tags overlap is a different, broader fact (see
    metrics.py), not this one.

    Un-frozen from the Stage 15A version (which deliberately reproduced
    the pre-fix query to reconcile against the historical frozen 4110-row
    `recommendations` table). As of Stage 15A.2 (2026-09-01) this is the
    sole canonical reconstruction function -- eval/8_1/results.json /
    tables.md / figures/*.png are generated against
    eval/8_1/regenerated_recommendations.json, validated bit-identical to
    a real recommend.py run (eval/8_1/path_validation.json). This function
    no longer matches what the ORIGINAL frozen table saw at generation
    time (2026-08-07) -- see capped_genre_sibling_ids_legacy() below for
    that, and eval/8_1/frozen_legacy_results.json for its output. Calling
    eval/8_1/run.py::main() (still reads the raw frozen Postgres table
    directly) against this function will show fresh reconstruction
    inconsistency for that reason, not a new bug -- it isn't part of the
    canonical pack anymore.
    """
    result = session.run(
        """
        UNWIND $seed_ids AS sid
        MATCH (t:Track {track_id: sid})
        CALL (t) {
            MATCH (t)-[:HAS_GENRE]->(:Genre)<-[:HAS_GENRE]-(other:Track)
            WHERE other <> t
            WITH other, count(*) AS shared_genres
            RETURN other.track_id AS track_id
            ORDER BY shared_genres DESC, other.track_id ASC
            LIMIT $limit
        }
        RETURN sid AS seed_id, collect(track_id) AS sibling_ids
        """,
        seed_ids=seed_track_ids,
        limit=limit,
    )
    return {r["seed_id"]: set(r["sibling_ids"]) for r in result}


def capped_genre_sibling_ids_legacy(session, seed_track_ids: list[int], limit: int = 10) -> dict:
    """LEGACY -- do not use for anything but reconciling against
    eval/8_1/frozen_legacy_results.json. Reproduces the ORIGINAL
    pre-Stage-15A get_track_graph query: same MATCH pattern, LIMIT 10,
    no ORDER BY -- an arbitrary, non-principled cut on skewed/large
    genres. This is what the frozen 4110-row `recommendations` table
    (run_id 3a7ffa23-..., generated 2026-08-07) actually saw; it is NOT
    what platform/semantic_api/main.py's live endpoint does today (see
    capped_genre_sibling_ids() above, the sole canonical function as of
    Stage 15A.2)."""
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
