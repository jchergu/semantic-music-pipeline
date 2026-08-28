"""
Results & evaluation metrics: goes beyond "the pipeline ran end to end"
(stage 6) to ask "are the recommendations it produced any good" — the
question stage 5/6 deliberately left open (no ground-truth relevance
labels exist for this dataset; see docs/results-evaluation.md).

Computes, against the live 411-seed / 4110-row `recommendations` table:
  - same-artist rate in the top-10, split by whether the seed's artist has
    a small (<5) or large (>=5) catalog in the 411-track dataset — checks
    for a catalog-size "filter bubble" effect the additive ARTIST_BOOST
    could produce
  - genre-tag overlap rate across all recommendation rows
  - rank-1 same-artist rate
  - recommendation quality (score) for the 57 seeds with no genre tags at
    all, vs. the dataset-wide average — checks the CLAP-only fallback
    isn't degenerate
  - full top-10 detail for 8 hand-picked, spread-out seeds (varied catalog
    size, genre-tag richness, one with zero genre tags) for a qualitative
    spot-check

Writes reports/results_evaluation/results.json.

Requires: docker compose stack up (reads Postgres only, no live API needed).
"""
import json

import psycopg2.extras

from _db import ROOT, connect

OUT_DIR = ROOT / "reports" / "results_evaluation"
OUT_DIR.mkdir(parents=True, exist_ok=True)

BIG_CATALOG_THRESHOLD = 5

SPOTCHECK_SEED_IDS = [1, 45, 90, 150, 210, 246, 275, 400]


def _pct(numerator: int, denominator: int):
    return round(100.0 * numerator / denominator, 1) if denominator else None


def _bucket_stats(catalog_buckets: dict, key: bool) -> dict:
    row = catalog_buckets.get(key, {"same_artist_rows": 0, "total_rows": 0})
    return {
        "same_artist_rows": row["same_artist_rows"],
        "total_rows": row["total_rows"],
        "pct_same_artist": _pct(row["same_artist_rows"], row["total_rows"]),
    }


def main() -> None:
    conn = connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(
        """
        SELECT
            count(*) AS total_rows,
            count(*) FILTER (WHERE s.artist_name = t.artist_name) AS same_artist_rows,
            count(*) FILTER (WHERE s.genre_tags && t.genre_tags) AS genre_overlap_rows
        FROM recommendations r
        JOIN tracks s ON s.id = r.seed_track_id
        JOIN tracks t ON t.id = r.recommended_track_id;
        """
    )
    overall = cur.fetchone()

    cur.execute(
        """
        SELECT count(*) AS rank1_same_artist
        FROM recommendations r
        JOIN tracks s ON s.id = r.seed_track_id
        JOIN tracks t ON t.id = r.recommended_track_id
        WHERE r.rank = 1 AND s.artist_name = t.artist_name;
        """
    )
    rank1 = cur.fetchone()
    cur.execute("SELECT count(*) AS total_seeds FROM tracks;")
    total_seeds = cur.fetchone()["total_seeds"]

    cur.execute("SELECT artist_name, count(*) AS n FROM tracks GROUP BY artist_name;")
    artist_counts = cur.fetchall()
    big_catalog_names = [r["artist_name"] for r in artist_counts if r["n"] >= BIG_CATALOG_THRESHOLD]
    big_catalog_artists = sorted(
        (r for r in artist_counts if r["n"] >= BIG_CATALOG_THRESHOLD),
        key=lambda r: r["n"],
        reverse=True,
    )

    cur.execute(
        """
        SELECT
            (s.artist_name = ANY(%(big_artists)s)) AS big_catalog_artist,
            count(*) FILTER (WHERE s.artist_name = t.artist_name) AS same_artist_rows,
            count(*) AS total_rows
        FROM recommendations r
        JOIN tracks s ON s.id = r.seed_track_id
        JOIN tracks t ON t.id = r.recommended_track_id
        GROUP BY s.artist_name = ANY(%(big_artists)s);
        """,
        {"big_artists": big_catalog_names},
    )
    catalog_buckets = {row["big_catalog_artist"]: row for row in cur.fetchall()}

    cur.execute(
        """
        SELECT
            count(*) AS empty_genre_seed_count,
            (SELECT round(avg(score)::numeric, 4) FROM recommendations r
                JOIN tracks s ON s.id = r.seed_track_id
                WHERE s.genre_tags = '{}' OR s.genre_tags IS NULL) AS avg_score_empty_genre_seeds,
            (SELECT round(avg(score)::numeric, 4) FROM recommendations) AS avg_score_overall
        FROM tracks WHERE genre_tags = '{}' OR genre_tags IS NULL;
        """
    )
    empty_genre = cur.fetchone()

    cur.execute(
        "SELECT id, title, artist_name, genre_tags FROM tracks WHERE id = ANY(%s);",
        (SPOTCHECK_SEED_IDS,),
    )
    seeds_by_id = {row["id"]: row for row in cur.fetchall()}

    cur.execute(
        """
        SELECT r.seed_track_id, r.rank, r.score, t.id AS track_id, t.title, t.artist_name, t.genre_tags
        FROM recommendations r JOIN tracks t ON t.id = r.recommended_track_id
        WHERE r.seed_track_id = ANY(%s) ORDER BY r.seed_track_id, r.rank;
        """,
        (SPOTCHECK_SEED_IDS,),
    )
    recs_by_seed: dict[int, list] = {}
    for row in cur.fetchall():
        recs_by_seed.setdefault(row["seed_track_id"], []).append(row)

    spotcheck = []
    for seed_id in SPOTCHECK_SEED_IDS:
        seed = seeds_by_id[seed_id]
        recs = []
        for row in recs_by_seed.get(seed_id, []):
            recs.append(
                {
                    "rank": row["rank"],
                    "score": round(row["score"], 4),
                    "track_id": row["track_id"],
                    "title": row["title"],
                    "artist_name": row["artist_name"],
                    "genre_tags": row["genre_tags"],
                    "same_artist": row["artist_name"] == seed["artist_name"],
                    "genre_overlap": bool(set(row["genre_tags"] or []) & set(seed["genre_tags"] or [])),
                }
            )
        spotcheck.append(
            {
                "seed_track_id": seed["id"],
                "seed_title": seed["title"],
                "seed_artist_name": seed["artist_name"],
                "seed_genre_tags": seed["genre_tags"],
                "recommendations": recs,
            }
        )

    avg_empty = empty_genre["avg_score_empty_genre_seeds"]
    avg_overall = empty_genre["avg_score_overall"]

    result = {
        "overall": {
            "total_rows": overall["total_rows"],
            "same_artist_rows": overall["same_artist_rows"],
            "pct_same_artist": _pct(overall["same_artist_rows"], overall["total_rows"]),
            "genre_overlap_rows": overall["genre_overlap_rows"],
            "pct_genre_overlap": _pct(overall["genre_overlap_rows"], overall["total_rows"]),
        },
        "rank1_same_artist": {
            "count": rank1["rank1_same_artist"],
            "total": total_seeds,
            "pct": _pct(rank1["rank1_same_artist"], total_seeds),
        },
        "catalog_skew": {
            "big_catalog_threshold": BIG_CATALOG_THRESHOLD,
            "big_catalog_artist_count": len(big_catalog_artists),
            "small_catalog": _bucket_stats(catalog_buckets, False),
            "big_catalog": _bucket_stats(catalog_buckets, True),
            "top_big_catalog_artists": [{"artist_name": r["artist_name"], "n": r["n"]} for r in big_catalog_artists],
        },
        "empty_genre_fallback": {
            "empty_genre_seed_count": empty_genre["empty_genre_seed_count"],
            "total_seeds": total_seeds,
            "avg_score_empty_genre_seeds": float(avg_empty) if avg_empty is not None else None,
            "avg_score_overall": float(avg_overall) if avg_overall is not None else None,
        },
        "spotcheck": spotcheck,
    }

    with open(OUT_DIR / "results.json", "w") as f:
        json.dump(result, f, indent=2)
    print(f"Wrote {OUT_DIR / 'results.json'}")
    print(json.dumps({k: v for k, v in result.items() if k != "spotcheck"}, indent=2))


if __name__ == "__main__":
    main()
