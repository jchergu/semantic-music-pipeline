"""Unit tests for eval/8_1/metrics.py -- pure functions, synthetic data, no
live services, mirroring usecases/8_1_batch_reactive/tests/test_uc81_recommender.py's
pattern for ranking.py."""
import sys
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import metrics  # noqa: E402


# --- cosine_similarity ---


def test_cosine_similarity_identical_vectors():
    v = np.array([1.0, 2.0, 3.0])
    assert metrics.cosine_similarity(v, v) == pytest.approx(1.0)


def test_cosine_similarity_orthogonal_vectors():
    a = np.array([1.0, 0.0])
    b = np.array([0.0, 1.0])
    assert metrics.cosine_similarity(a, b) == pytest.approx(0.0)


def test_cosine_similarity_zero_vector_no_crash():
    a = np.array([0.0, 0.0])
    b = np.array([1.0, 0.0])
    assert metrics.cosine_similarity(a, b) == 0.0


# --- reconstruct_signals + signal_contribution_summary ---

TRACK_META = {
    1: {"artist_name": "A", "genre_tags": ["rock"]},
    2: {"artist_name": "A", "genre_tags": ["rock"]},  # same artist + genre sibling of 1
    3: {"artist_name": "B", "genre_tags": ["rock"]},  # genre sibling of 1 only
    4: {"artist_name": "B", "genre_tags": ["jazz"]},  # neither, similarity only
}
EMBEDDINGS = {
    1: np.array([1.0, 0.0, 0.0]),
    2: np.array([1.0, 0.0, 0.0]),  # identical -> similarity 1.0
    3: np.array([0.0, 1.0, 0.0]),  # orthogonal -> similarity 0.0
    4: np.array([0.7, 0.7, 0.0]),  # partial similarity
}


def _expected_score(seed_id, rec_id):
    sim = metrics.cosine_similarity(EMBEDDINGS[seed_id], EMBEDDINGS[rec_id])
    genre_sibling = bool(set(TRACK_META[seed_id]["genre_tags"]) & set(TRACK_META[rec_id]["genre_tags"]))
    same_artist = TRACK_META[seed_id]["artist_name"] == TRACK_META[rec_id]["artist_name"]
    return sim + (metrics.GENRE_BOOST if genre_sibling else 0) + (metrics.ARTIST_BOOST if same_artist else 0)


def test_reconstruct_signals_flags_and_pool_membership():
    rows = [
        {"seed_track_id": 1, "recommended_track_id": 2, "score": _expected_score(1, 2)},
        {"seed_track_id": 1, "recommended_track_id": 3, "score": _expected_score(1, 3)},
        {"seed_track_id": 1, "recommended_track_id": 4, "score": _expected_score(1, 4)},
    ]
    enriched = metrics.reconstruct_signals(rows, TRACK_META, EMBEDDINGS)

    by_rec = {r["recommended_track_id"]: r for r in enriched}
    assert by_rec[2]["same_artist"] is True
    assert by_rec[2]["genre_sibling"] is True
    assert by_rec[2]["in_similarity_pool"] is True
    assert by_rec[2]["reconstruction_inconsistent"] is False

    assert by_rec[3]["same_artist"] is False
    assert by_rec[3]["genre_sibling"] is True
    assert by_rec[3]["reconstruction_inconsistent"] is False

    assert by_rec[4]["same_artist"] is False
    assert by_rec[4]["genre_sibling"] is False
    assert by_rec[4]["reconstruction_inconsistent"] is False


def test_reconstruct_signals_detects_pool_exclusion():
    # rec_id 2's true cosine similarity to seed 1 is 1.0, but if the stored
    # score only reflects the artist+genre boosts (as if the candidate was
    # NOT in the original top-K similarity pool), the residual should read
    # ~0, not ~1.0 -- and that must not be flagged as inconsistent, since
    # 0 is one of the two valid residual values.
    rows = [{"seed_track_id": 1, "recommended_track_id": 2, "score": metrics.GENRE_BOOST + metrics.ARTIST_BOOST}]
    enriched = metrics.reconstruct_signals(rows, TRACK_META, EMBEDDINGS)
    assert enriched[0]["in_similarity_pool"] is False
    assert enriched[0]["reconstruction_inconsistent"] is False


def test_reconstruct_signals_flags_real_inconsistency():
    # A stored score whose residual (after subtracting the reconstructed
    # boosts) matches neither the true cosine similarity nor zero -- the
    # only two values ranking.py could actually have produced. This means
    # the reconstructed same_artist/genre_sibling flags disagree with what
    # the original batch run actually used.
    rows = [{"seed_track_id": 1, "recommended_track_id": 3, "score": 999.0}]
    enriched = metrics.reconstruct_signals(rows, TRACK_META, EMBEDDINGS)
    assert enriched[0]["reconstruction_inconsistent"] is True


def test_reconstruct_signals_uses_capped_genre_siblings_when_given():
    # track 3 shares a genre tag with seed 1 (genre_tag_overlap=True), but is
    # excluded from the capped ground-truth set -- genre_sibling must follow
    # the capped set, not the broader tag overlap, and the gap must be
    # visible via genre_boost_missed_by_cap.
    capped = {1: set()}  # seed 1 has no capped genre siblings at all
    row_score = metrics.cosine_similarity(EMBEDDINGS[1], EMBEDDINGS[3])  # no boosts applied
    rows = [{"seed_track_id": 1, "recommended_track_id": 3, "score": row_score}]
    enriched = metrics.reconstruct_signals(rows, TRACK_META, EMBEDDINGS, capped_genre_siblings=capped)

    assert enriched[0]["genre_sibling"] is False
    assert enriched[0]["genre_tag_overlap"] is True
    assert enriched[0]["genre_boost_missed_by_cap"] is True
    assert enriched[0]["reconstruction_inconsistent"] is False


def test_signal_contribution_summary_combinations():
    rows = [
        {"seed_track_id": 1, "recommended_track_id": 2, "score": _expected_score(1, 2)},
        {"seed_track_id": 1, "recommended_track_id": 3, "score": _expected_score(1, 3)},
        {"seed_track_id": 1, "recommended_track_id": 4, "score": _expected_score(1, 4)},
    ]
    enriched = metrics.reconstruct_signals(rows, TRACK_META, EMBEDDINGS)
    summary = metrics.signal_contribution_summary(enriched)

    assert summary["total_rows"] == 3
    assert summary["combination_counts"] == {
        "similarity_only": 1,
        "genre_only": 1,
        "artist_only": 0,
        "genre_and_artist": 1,
    }
    assert summary["reconstruction_inconsistent_count"] == 0


def test_signal_contribution_summary_empty_input():
    summary = metrics.signal_contribution_summary([])
    assert summary["total_rows"] == 0
    assert summary["pct_genre_sibling"] is None


# --- gini_coefficient ---


def test_gini_coefficient_perfect_equality():
    assert metrics.gini_coefficient([1, 1, 1, 1]) == pytest.approx(0.0)


def test_gini_coefficient_known_inequality_value():
    # n=4, one item holds everything: theoretical max Gini for n=4 is (n-1)/n = 0.75
    assert metrics.gini_coefficient([0, 0, 0, 4]) == pytest.approx(0.75)


def test_gini_coefficient_all_zero_returns_zero_not_nan():
    assert metrics.gini_coefficient([0, 0, 0]) == 0.0


def test_gini_coefficient_empty_returns_zero():
    assert metrics.gini_coefficient([]) == 0.0


# --- catalog_coverage ---


def test_catalog_coverage_counts_and_gini():
    rows = [
        {"recommended_track_id": 1},
        {"recommended_track_id": 1},
        {"recommended_track_id": 2},
    ]
    result = metrics.catalog_coverage(rows, all_track_ids=[1, 2, 3])
    assert result["total_tracks"] == 3
    assert result["covered_tracks"] == 2
    assert result["pct_covered"] == pytest.approx(66.67, abs=0.01)
    assert result["appearance_counts_by_track_id"] == {"1": 2, "2": 1, "3": 0}
    assert result["max_appearances"] == 2
    assert result["min_appearances"] == 0


# --- intra_list_diversity ---


def test_intra_list_diversity_basic():
    rows_by_seed = {1: [2, 3]}  # cosine(2,3) = 0 -> distance 1.0
    result = metrics.intra_list_diversity(rows_by_seed, EMBEDDINGS)
    assert result["seeds_evaluated"] == 1
    assert result["mean"] == pytest.approx(1.0)


def test_intra_list_diversity_skips_lists_shorter_than_two():
    rows_by_seed = {1: [2], 2: [3, 4]}
    result = metrics.intra_list_diversity(rows_by_seed, EMBEDDINGS)
    assert result["seeds_evaluated"] == 1
    assert result["seeds_skipped_lt_2_recs"] == 1
    assert "1" not in result["per_seed"]


# --- failure_edge_cases ---


def test_failure_edge_cases_finds_short_lists():
    rows_by_seed = {1: list(range(10)), 2: [1, 2, 3]}
    result = metrics.failure_edge_cases(rows_by_seed, expected_top_k=10)
    assert result["count"] == 1
    assert result["seeds_with_fewer_recommendations"] == {"2": 3}


def test_failure_edge_cases_none_found_is_a_valid_result():
    rows_by_seed = {1: list(range(10)), 2: list(range(10))}
    result = metrics.failure_edge_cases(rows_by_seed, expected_top_k=10)
    assert result["count"] == 0
    assert result["seeds_with_fewer_recommendations"] == {}
