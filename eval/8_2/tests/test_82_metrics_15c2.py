"""Pure unit tests for eval/8_2's stage 15C.2 metrics (4, 5, 6, 7).

Same split as test_82_metrics.py and eval/8_1: every definition in
METRICS.md sections 5-8 is exercised against synthetic input, so a changed
definition fails here in milliseconds rather than at the end of an
18-minute live run.

The file name carries the `82_` prefix and a stage suffix for the reason
README.md gives: `8_1`/`8_2` are not valid Python identifiers, so pytest
resolves same-named files across the two eval packages to the same module
and silently collects only one of them.
"""
import math
import sys
from importlib import import_module
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT))

metrics = import_module("eval.8_2.metrics")


# --- metric 4: coherence ---


def _post(index, wall, event_time, track_id, event_type="play"):
    return {"index": index, "wall_clock": wall, "event_time": event_time,
            "track_id": track_id, "event_type": event_type}


BASE = "2030-01-01T00:00:00+00:00"


def test_cosine_is_none_for_a_zero_vector_not_zero():
    """A zero-norm vector means "no information", not "orthogonal". Returning
    0.0 would report the second as the first."""
    assert metrics.cosine([1.0, 0.0], [0.0, 0.0]) is None
    assert metrics.cosine([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)
    assert metrics.cosine([1.0, 0.0], [-1.0, 0.0]) == pytest.approx(-1.0)


def test_cosine_rejects_a_dimension_mismatch():
    with pytest.raises(ValueError):
        metrics.cosine([1.0, 0.0], [1.0, 0.0, 0.0])


def test_mean_vector_is_unweighted_and_rejects_ragged_input():
    assert metrics.mean_vector([[0.0, 2.0], [2.0, 0.0]]) == [1.0, 1.0]
    assert metrics.mean_vector([]) is None
    with pytest.raises(ValueError):
        metrics.mean_vector([[1.0], [1.0, 2.0]])


def test_coherence_series_windows_on_session_time_not_wall_clock():
    """The two windows are defined in SESSION time. Here wall-clock advances
    1s per event while session time advances 200s, exactly the decoupling
    --speed produces -- a wall-clock window would put every event in the
    "last 5 minutes" and the metric would be flat by construction."""
    posts = [
        _post(0, 1000.0, "2030-01-01T00:00:00+00:00", "1"),
        _post(1, 1001.0, "2030-01-01T00:03:20+00:00", "2"),   # +200s session
        _post(2, 1002.0, "2030-01-01T00:06:40+00:00", "3"),   # +400s session
        _post(3, 1003.0, "2030-01-01T00:10:00+00:00", "4"),   # +600s session
    ]
    embeddings = {"1": [1.0, 0.0], "2": [1.0, 0.0], "3": [0.0, 1.0], "4": [0.0, 1.0]}
    samples = [{"computed_at": 1003.5, "vector": [0.0, 1.0], "n_events": 4}]

    series = metrics.coherence_series(samples, posts, embeddings)
    assert len(series) == 1
    point = series[0]
    # Session position is the last event posted at or before computed_at:
    # t=600s, so the 5-min window is (300, 600] -> tracks 3 and 4.
    assert point["recent_track_ids"] == ["3", "4"]
    # The first 5 minutes is [0, 300] -> tracks 1 and 2.
    assert point["early_track_ids"] == ["1", "2"]
    assert point["session_elapsed_seconds"] == pytest.approx(600.0)
    assert point["cos_recent"] == pytest.approx(1.0)
    assert point["cos_early"] == pytest.approx(0.0)


def test_coherence_series_skips_samples_that_predate_the_first_post():
    posts = [_post(0, 1000.0, BASE, "1")]
    samples = [{"computed_at": 999.0, "vector": [1.0, 0.0]},
               {"computed_at": 1000.5, "vector": [1.0, 0.0]}]
    series = metrics.coherence_series(samples, posts, {"1": [1.0, 0.0]})
    assert [s["computed_at"] for s in series] == [1000.5]


def test_coherence_series_leaves_a_cosine_none_when_no_embedding_exists():
    posts = [_post(0, 1000.0, BASE, "1")]
    series = metrics.coherence_series([{"computed_at": 1001.0, "vector": [1.0, 0.0]}], posts, {})
    assert series[0]["cos_recent"] is None and series[0]["cos_early"] is None


def test_coherence_summary_reports_the_recency_claim_as_a_rate():
    series = [
        {"cos_recent": 0.9, "cos_early": 0.2},
        {"cos_recent": 0.8, "cos_early": 0.1},
        {"cos_recent": 0.1, "cos_early": 0.5},
        {"cos_recent": None, "cos_early": 0.4},
    ]
    summary = metrics.coherence_summary(series)
    assert summary["n"] == 3
    assert summary["samples_closer_to_recent_than_early"] == 2
    assert summary["fraction_closer_to_recent"] == pytest.approx(2 / 3)
    assert summary["cos_recent_first"] == 0.9 and summary["cos_recent_last"] == 0.1
    assert metrics.coherence_summary([])["n"] == 0


# --- metric 5: coverage ---


def _snap(index, ids, scores=None):
    scores = scores or [1.0] * len(ids)
    return {"refresh_index": index, "track_ids": list(ids),
            "recs": [{"track_id": t, "score": s} for t, s in zip(ids, scores)]}


def test_cumulative_unique_curve_counts_only_newly_seen_tracks():
    curve = metrics.cumulative_unique_curve([
        _snap(1, ["1", "2"]), _snap(2, ["2", "3"]), _snap(3, ["1", "3"]),
    ])
    assert [c["cumulative_unique"] for c in curve] == [2, 3, 3]
    assert [c["new_this_refresh"] for c in curve] == [2, 1, 0]


def test_catalog_coverage_reports_the_flat_tail_that_is_attractor_collapse():
    """A curve that stops growing well before the session ends is the
    collapse METRICS.md section 6 anticipates -- reported as a number, not
    left to be read off a plot."""
    cov = metrics.catalog_coverage(
        [_snap(1, ["1", "2"]), _snap(2, ["3"]), _snap(3, ["1"]), _snap(4, ["2"])],
        catalog_size=10,
    )
    assert cov["unique_recommended"] == 3
    assert cov["coverage_fraction"] == pytest.approx(0.3)
    assert cov["last_refresh_contributing_new_tracks"] == 2
    assert cov["refreshes_after_last_new_track"] == 2
    assert cov["flat_tail_fraction"] == pytest.approx(0.5)


def test_catalog_coverage_handles_a_run_with_no_refreshes():
    assert metrics.catalog_coverage([])["coverage_fraction"] == 0.0


# --- metric 6: determinism ---


def test_snapshots_as_ranked_rows_matches_diff_runs_input_shape():
    rows = metrics.snapshots_as_ranked_rows([_snap(1, ["7", "9"], [0.5, 0.25])])
    assert rows == {1: [{"recommended_track_id": 7, "score": 0.5},
                        {"recommended_track_id": 9, "score": 0.25}]}


def _comparison(identical=0, order_only=0, membership_changed=0, diverging=(), total=None):
    counts = {"identical": identical, "order_only": order_only,
              "membership_changed": membership_changed}
    return {
        "classification_counts": counts,
        "affected_seed_ids": list(diverging),
        "total_seeds": total if total is not None else sum(counts.values()),
    }


def test_determinism_passes_only_on_exact_reproduction():
    verdict = metrics.determinism_verdict((5, 5), _comparison(identical=5), 0.0)
    assert verdict["pass"] is True


def test_determinism_fails_on_a_refresh_count_mismatch():
    verdict = metrics.determinism_verdict((5, 6), _comparison(identical=5), 0.0)
    assert verdict["pass"] is False
    assert verdict["refresh_count_check"]["pass"] is False


def test_determinism_fails_on_an_order_only_difference():
    """Same ten tracks in a different order is not a reproduction: the recs
    list is ranked and a client reads it in order."""
    verdict = metrics.determinism_verdict((5, 5), _comparison(identical=4, order_only=1), 0.0)
    assert verdict["pass"] is False
    assert verdict["snapshot_identity_check"]["pass"] is False


def test_determinism_admits_no_score_tolerance():
    """eval/8_1's Stage 15A.2 measured this scoring path's repeated-run noise
    floor at exactly 0.0, so any drift is a new effect and must not be
    absorbed by a tolerance invented after seeing it."""
    verdict = metrics.determinism_verdict((5, 5), _comparison(identical=5), 1e-12)
    assert verdict["pass"] is False
    assert verdict["score_delta_check"]["pass"] is False


# --- metric 7: late events ---


def test_late_delivery_count_measures_the_dose_not_the_setting():
    """--jitter is a probability, so how many events it actually pushes past
    the 5s watermark bound is a draw. Delivering event_times 0, 200, 100
    puts the third 100s behind the high-water mark."""
    posts = [
        _post(0, 1.0, "2030-01-01T00:00:00+00:00", "1"),
        _post(1, 2.0, "2030-01-01T00:03:20+00:00", "2"),
        _post(2, 3.0, "2030-01-01T00:01:40+00:00", "3"),
    ]
    dose = metrics.late_delivery_count(posts)
    assert dose["events_delivered_late"] == 1
    assert dose["max_lateness_seconds"] == pytest.approx(100.0)


def test_late_delivery_count_is_zero_for_event_time_ordered_delivery():
    posts = [
        _post(0, 1.0, "2030-01-01T00:00:00+00:00", "1"),
        _post(1, 2.0, "2030-01-01T00:03:20+00:00", "2"),
    ]
    assert metrics.late_delivery_count(posts)["events_delivered_late"] == 0


def test_late_delivery_count_ignores_reordering_inside_the_watermark_bound():
    """A 2s inversion is within for_bounded_out_of_orderness(5s) -- the
    window has not fired yet, so nothing is dropped."""
    posts = [
        _post(0, 1.0, "2030-01-01T00:00:10+00:00", "1"),
        _post(1, 2.0, "2030-01-01T00:00:08+00:00", "2"),
    ]
    assert metrics.late_delivery_count(posts)["events_delivered_late"] == 0


def _arm(events_to_adaptation, refresh_count, curve, coherence):
    return {"events_to_adaptation": events_to_adaptation, "refresh_count": refresh_count,
            "reactivity_curve": curve, "coherence": coherence}


def _coh(points):
    return [{"session_elapsed_seconds": t, "cos_recent": v} for t, v in points]


def test_late_event_effect_below_the_replicate_noise_is_not_distinguishable():
    """Two jitter-0 replicates differ by 2 refreshes; the jittered run differs
    by 1. The jitter effect is smaller than the pipeline's own run-to-run
    variability, so it is reported as noise -- METRICS.md section 8 admits a
    null result explicitly."""
    a = _arm(3, 10, {1: 0.5}, _coh([(0.0, 0.9)]))
    b = _arm(3, 12, {1: 0.5}, _coh([(0.0, 0.9)]))
    t = _arm(3, 11, {1: 0.5}, _coh([(0.0, 0.9)]))
    verdict = metrics.late_event_verdict(a, b, t)
    assert verdict["measures"]["refresh_count"]["baseline_vs_baseline"] == 2
    assert verdict["measures"]["refresh_count"]["baseline_vs_jittered"] == 1
    assert verdict["measures"]["refresh_count"]["distinguishable_from_noise"] is False
    assert verdict["any_measure_distinguishable"] is False


def test_late_event_effect_above_the_replicate_noise_is_distinguishable():
    a = _arm(3, 10, {1: 0.5}, _coh([(0.0, 0.90)]))
    b = _arm(3, 10, {1: 0.5}, _coh([(0.0, 0.89)]))
    t = _arm(7, 10, {1: 0.5}, _coh([(0.0, 0.40)]))
    verdict = metrics.late_event_verdict(a, b, t)
    assert verdict["measures"]["events_to_adaptation"]["distinguishable_from_noise"] is True
    assert verdict["measures"]["max_coherence_cos_recent_delta"]["distinguishable_from_noise"] is True
    assert "events_to_adaptation" in verdict["measures_distinguishable"]


def test_late_event_coherence_is_aligned_on_session_time_not_sample_index():
    """Window fires are not under the harness's control, so two runs produce
    different sample counts. Only the session-time positions they share are
    comparable; an index-aligned comparison would pair unrelated points."""
    a = _arm(3, 10, {1: 0.5}, _coh([(0.0, 0.9), (300.0, 0.8), (600.0, 0.7)]))
    b = _arm(3, 10, {1: 0.5}, _coh([(0.0, 0.9), (600.0, 0.7)]))
    t = _arm(3, 10, {1: 0.5}, _coh([(300.0, 0.2), (600.0, 0.7)]))
    verdict = metrics.late_event_verdict(a, b, t)
    assert verdict["measures"]["max_coherence_cos_recent_delta"]["baseline_vs_baseline"] == pytest.approx(0.0)
    assert verdict["measures"]["max_coherence_cos_recent_delta"]["baseline_vs_jittered"] == pytest.approx(0.6)


def test_late_event_measure_is_undecidable_when_a_run_never_crossed():
    a = _arm(None, 10, {1: 0.5}, _coh([(0.0, 0.9)]))
    b = _arm(3, 10, {1: 0.5}, _coh([(0.0, 0.9)]))
    t = _arm(3, 10, {1: 0.5}, _coh([(0.0, 0.9)]))
    verdict = metrics.late_event_verdict(a, b, t)
    assert verdict["measures"]["events_to_adaptation"]["distinguishable_from_noise"] is None


def test_late_event_measure_is_undecidable_when_curves_share_no_position():
    a = _arm(3, 10, {1: 0.5}, [])
    b = _arm(3, 10, {2: 0.5}, [])
    t = _arm(3, 10, {3: 0.5}, [])
    verdict = metrics.late_event_verdict(a, b, t)
    assert verdict["measures"]["max_reactivity_jaccard_delta"]["distinguishable_from_noise"] is None
    assert verdict["measures"]["max_coherence_cos_recent_delta"]["distinguishable_from_noise"] is None


def test_watermark_advancing_events_is_the_ceiling_on_resolvable_samples():
    """One profile vector per watermark advance is observable at any poll
    rate: a single advance releases several 30s slides that the job fires
    milliseconds apart over one overwritten key. Here two events share a
    session timestamp, so only three of four advance anything."""
    posts = [
        _post(0, 1.0, "2030-01-01T00:00:00+00:00", "1"),
        _post(1, 2.0, "2030-01-01T00:00:00+00:00", "1", "complete"),
        _post(2, 3.0, "2030-01-01T00:03:20+00:00", "2"),
        _post(3, 4.0, "2030-01-01T00:06:40+00:00", "2", "complete"),
    ]
    assert metrics.watermark_advancing_events(posts) == 3


def test_watermark_advancing_events_ignores_events_delivered_behind_it():
    """A jittered event arriving behind the high-water mark advances nothing,
    which is why this is defined on delivery order rather than event type."""
    posts = [
        _post(0, 1.0, "2030-01-01T00:06:40+00:00", "1"),
        _post(1, 2.0, "2030-01-01T00:00:00+00:00", "2"),
        _post(2, 3.0, "2030-01-01T00:10:00+00:00", "3"),
    ]
    assert metrics.watermark_advancing_events(posts) == 2
    assert metrics.watermark_advancing_events([]) == 0


def test_cold_warm_handoff_reports_rank_change_a_set_jaccard_cannot_see():
    """Observed live in the long session: the first warm refresh returned the
    same ten tracks as the cold start with the top one moved to position six.
    METRICS.md section 4's set-based Jaccard reads that as 1.000, so the rank
    comparison rides alongside it rather than replacing it."""
    log = [
        {"refresh_index": 1, "action": "cold_start", "wall_clock": 10.0,
         "track_ids": ["1", "2", "3"]},
        {"refresh_index": 2, "action": "warm", "wall_clock": 20.0,
         "track_ids": ["2", "3", "1"]},
    ]
    result = metrics.cold_warm_transition(log, session_start_wall=0.0)
    assert result["handoff_jaccard"] == pytest.approx(1.0)
    assert result["handoff_rank_identical"] is False

    same = [log[0], {**log[1], "track_ids": ["1", "2", "3"]}]
    assert metrics.cold_warm_transition(same, 0.0)["handoff_rank_identical"] is True
