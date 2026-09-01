"""
Pure metric functions for eval/8_1 -- no I/O, testable against synthetic
data, mirroring usecases/8_1_batch_reactive/recommender/ranking.py's
testability pattern.

The `recommendations` table (usecases/8_1_batch_reactive/recommender/schema.sql)
only persists (run_id, seed_track_id, recommended_track_id, rank, score) --
ranking.py's per-candidate similarity/genre_sibling/same_artist booleans were
never written to Postgres. reconstruct_signals() rebuilds them from the
stores that still hold the ground truth: tracks.artist_name / tracks.genre_tags
(Postgres) for same_artist/genre_sibling, and true CLAP cosine similarity
(computed here from Milvus-fetched embedding vectors, not re-run through
Milvus's ANN index). expected_score is then cross-checked against the stored
score as a sanity check, not blind trust in the reconstruction.
"""
import itertools
from collections import Counter

import numpy as np

GENRE_BOOST = 0.05  # usecases/8_1_batch_reactive/recommender/ranking.py::DEFAULT_GENRE_BOOST
ARTIST_BOOST = 0.15  # ranking.py::DEFAULT_ARTIST_BOOST
SCORE_EPSILON = 1e-4


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    denom = float(np.linalg.norm(a) * np.linalg.norm(b))
    if denom == 0:
        return 0.0
    return float(np.dot(a, b) / denom)


def reconstruct_signals(
    rows: list[dict],
    track_meta: dict,
    embeddings: dict,
    capped_genre_siblings: dict | None = None,
) -> list[dict]:
    """
    rows: [{"seed_track_id", "recommended_track_id", "score", ...}, ...]
    track_meta: track_id -> {"artist_name": str | None, "genre_tags": list[str] | None}
    embeddings: track_id -> np.ndarray (512,)
    capped_genre_siblings: seed_track_id -> set(track_id), from
        kg_connectivity.capped_genre_sibling_ids() -- the EXACT capped
        related_by_genre candidate set context_builder.py received at
        generation time (LIMIT 10, ordered by shared-genre-count descending
        as of Stage 15A, same Cypher as platform/semantic_api/main.py's
        /tracks/{id}/graph). This is the real ground truth for whether
        GENRE_BOOST was applied to a given row -- confirmed empirically: for
        a sample of rows where the broader tracks.genre_tags-overlap proxy
        said genre_sibling=True, the stored score equalled the true CLAP
        cosine similarity with NO boost added, and the recommended track was
        absent from this capped list every time. Without it, reconstruction
        falls back to the complete tracks.genre_tags overlap, which is
        measurably wrong for genres with more than 10 tracks (median
        tracks/genre is 3, but the mean is ~9.9 -- a few large genres skew
        the distribution and are exactly where the cap bites). The Stage 15A
        ordering fix changed WHICH 10 siblings get selected but not the cap
        itself -- the coverage gap this causes is still there (see
        genre_boost_coverage_gap below), just no longer arbitrary.

    ranking.py's score is `similarity + boosts`, but `similarity` there is
    0.0 for any candidate that was NOT in the original candidate_k=25 Milvus
    search results -- a track pulled in purely via the genre/artist graph
    signals contributes similarity=0.0 to its score even though its *true*
    cosine similarity to the seed is some nonzero value. The residual
    recovered as `score - boosts` is what ranking.py actually used; the true
    cosine (from embeddings) classifies whether the row's candidate was
    inside that top-25 pool (`in_similarity_pool`). Given accurate same_artist/
    genre_sibling flags, residual_similarity should always match either the
    true cosine or 0 -- `reconstruction_inconsistent` flags the rare
    remainder (e.g. float precision in the stored REAL column) rather than
    absorbing it silently.
    """
    enriched = []
    for row in rows:
        seed_id = row["seed_track_id"]
        rec_id = row["recommended_track_id"]
        seed = track_meta[seed_id]
        rec = track_meta[rec_id]

        same_artist = seed["artist_name"] is not None and seed["artist_name"] == rec["artist_name"]
        seed_genres = set(seed["genre_tags"] or [])
        rec_genres = set(rec["genre_tags"] or [])
        genre_tag_overlap = bool(seed_genres & rec_genres)
        if capped_genre_siblings is not None:
            genre_sibling = rec_id in capped_genre_siblings.get(seed_id, set())
        else:
            genre_sibling = genre_tag_overlap

        true_similarity = cosine_similarity(embeddings[seed_id], embeddings[rec_id])
        boosts = (GENRE_BOOST if genre_sibling else 0.0) + (ARTIST_BOOST if same_artist else 0.0)
        residual_similarity = row["score"] - boosts

        matches_true = abs(residual_similarity - true_similarity) <= SCORE_EPSILON
        matches_zero = abs(residual_similarity - 0.0) <= SCORE_EPSILON
        # When true_similarity is itself ~0, both hypotheses collapse to the
        # same residual -- not a real ambiguity, so prefer "in pool" (no
        # positive evidence of exclusion) rather than treat it as inconsistent.
        in_similarity_pool = matches_true
        reconstruction_inconsistent = not (matches_true or matches_zero)

        enriched.append(
            {
                **row,
                "same_artist": same_artist,
                "genre_sibling": genre_sibling,
                "genre_tag_overlap": genre_tag_overlap,
                "genre_boost_missed_by_cap": genre_tag_overlap and not genre_sibling,
                "true_similarity": round(true_similarity, 6),
                "residual_similarity": round(residual_similarity, 6),
                "in_similarity_pool": in_similarity_pool,
                "reconstruction_inconsistent": reconstruction_inconsistent,
            }
        )
    return enriched


def signal_contribution_summary(enriched_rows: list[dict]) -> dict:
    total = len(enriched_rows)
    combo_counts = {"similarity_only": 0, "genre_only": 0, "artist_only": 0, "genre_and_artist": 0}
    for r in enriched_rows:
        g, a = r["genre_sibling"], r["same_artist"]
        if g and a:
            combo_counts["genre_and_artist"] += 1
        elif g:
            combo_counts["genre_only"] += 1
        elif a:
            combo_counts["artist_only"] += 1
        else:
            combo_counts["similarity_only"] += 1

    def _pct(n):
        return round(100.0 * n / total, 2) if total else None

    in_pool = sum(1 for r in enriched_rows if r["in_similarity_pool"])
    inconsistent = sum(1 for r in enriched_rows if r["reconstruction_inconsistent"])
    missed_by_cap = sum(1 for r in enriched_rows if r["genre_boost_missed_by_cap"])
    tag_overlap_total = sum(1 for r in enriched_rows if r["genre_tag_overlap"])
    return {
        "total_rows": total,
        "combination_counts": combo_counts,
        "combination_pct": {k: _pct(v) for k, v in combo_counts.items()},
        "pct_genre_sibling": _pct(sum(1 for r in enriched_rows if r["genre_sibling"])),
        "pct_same_artist": _pct(sum(1 for r in enriched_rows if r["same_artist"])),
        "pct_in_similarity_pool": _pct(in_pool),
        "reconstruction_inconsistent_count": inconsistent,
        "reconstruction_inconsistent_pct": _pct(inconsistent),
        "genre_boost_coverage_gap": {
            "note": (
                "genre_sibling is the exact capped related_by_genre set (LIMIT 10, ordered by "
                "shared-genre-count descending as of Stage 15A) the batch run actually applied "
                "GENRE_BOOST from; genre_tag_overlap is the complete tracks.genre_tags overlap "
                "fact. This counts rows where the two disagree -- a real, confirmed pipeline "
                "behavior (large genres still lose most of their true siblings to the cap itself, "
                "independent of ordering -- the Stage 15A fix changed which 10 get selected, not "
                "how many can fit), not a reconstruction error. Unaddressed: raising the cap is a "
                "separate, larger change than this fix and was out of scope for it."
            ),
            "genre_tag_overlap_rows": tag_overlap_total,
            "missed_by_cap_count": missed_by_cap,
            "missed_by_cap_pct_of_tag_overlap": (
                round(100.0 * missed_by_cap / tag_overlap_total, 2) if tag_overlap_total else None
            ),
        },
    }


def gini_coefficient(values: list[float]) -> float:
    """Standard Gini coefficient over a list of non-negative counts.
    0 = perfect equality, approaches 1 as inequality grows (the exact
    theoretical max for n items concentrated in one is (n-1)/n, not 1).
    An all-zero input returns 0.0 rather than dividing by zero -- "nobody
    appears" is not the same claim as "maximal inequality"."""
    n = len(values)
    if n == 0:
        return 0.0
    total = sum(values)
    if total == 0:
        return 0.0
    sorted_vals = sorted(values)
    weighted_sum = sum(i * v for i, v in enumerate(sorted_vals, start=1))
    return (2 * weighted_sum) / (n * total) - (n + 1) / n


def catalog_coverage(rows: list[dict], all_track_ids: list[int]) -> dict:
    counts = Counter(r["recommended_track_id"] for r in rows)
    appearance_counts = [counts.get(tid, 0) for tid in all_track_ids]
    covered = sum(1 for c in appearance_counts if c > 0)
    return {
        "total_tracks": len(all_track_ids),
        "covered_tracks": covered,
        "pct_covered": round(100.0 * covered / len(all_track_ids), 2) if all_track_ids else None,
        "gini_coefficient": round(gini_coefficient(appearance_counts), 4),
        "max_appearances": max(appearance_counts) if appearance_counts else 0,
        "min_appearances": min(appearance_counts) if appearance_counts else 0,
        "appearance_counts_by_track_id": {
            str(tid): c for tid, c in zip(all_track_ids, appearance_counts)
        },
    }


def intra_list_diversity(rows_by_seed: dict, embeddings: dict) -> dict:
    """rows_by_seed: seed_track_id -> [recommended_track_id, ...] (that seed's top-K).
    Diversity is undefined for a list of fewer than 2 items; such seeds are
    counted separately, not silently dropped."""
    per_seed = {}
    for seed_id, rec_ids in rows_by_seed.items():
        if len(rec_ids) < 2:
            continue
        dists = [1.0 - cosine_similarity(embeddings[a], embeddings[b]) for a, b in itertools.combinations(rec_ids, 2)]
        per_seed[seed_id] = sum(dists) / len(dists)

    values = sorted(per_seed.values())
    n = len(values)

    def pct(p):
        if n == 0:
            return None
        idx = min(n - 1, int(round(p * (n - 1))))
        return round(values[idx], 4)

    return {
        "seeds_evaluated": n,
        "seeds_skipped_lt_2_recs": len(rows_by_seed) - n,
        "mean": round(sum(values) / n, 4) if n else None,
        "median": pct(0.5),
        "p10": pct(0.10),
        "p90": pct(0.90),
        "per_seed": {str(k): round(v, 4) for k, v in per_seed.items()},
    }


def failure_edge_cases(rows_by_seed: dict, expected_top_k: int = 10) -> dict:
    short_seeds = {str(seed_id): len(rec_ids) for seed_id, rec_ids in rows_by_seed.items() if len(rec_ids) < expected_top_k}
    return {
        "expected_top_k": expected_top_k,
        "seeds_with_fewer_recommendations": short_seeds,
        "count": len(short_seeds),
    }
