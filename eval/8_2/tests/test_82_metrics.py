"""Unit tests for eval/8_2/metrics.py -- pure functions, synthetic data, no
live services. These pin the definitions METRICS.md signs off on, so a
10-minute live run isn't the only place they're checked.

Filenames in this directory carry an 82_ prefix deliberately. pytest derives
module names from the file path, and the package directories here ("8_1",
"8_2") aren't valid Python identifiers, so same-named test files across the
two eval packages collide: before the rename, a full-suite run silently
collected eval/8_1/tests/test_metrics.py TWICE (once under each path) and
never ran this file at all. Keep new test files in eval/ uniquely named.
"""
import datetime as dt
import sys
from importlib import import_module
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT))

metrics = import_module("eval.8_2.metrics")


# --- jaccard ---


def test_jaccard_identical_sets():
    assert metrics.jaccard({1, 2, 3}, {1, 2, 3}) == 1.0


def test_jaccard_disjoint_sets():
    assert metrics.jaccard({1, 2}, {3, 4}) == 0.0


def test_jaccard_partial_overlap():
    assert metrics.jaccard({1, 2, 3}, {3, 4, 5}) == pytest.approx(1 / 5)


def test_jaccard_two_empty_sets_are_identical_not_zero():
    assert metrics.jaccard(set(), set()) == 1.0


# --- events_to_adaptation ---


def test_events_to_adaptation_returns_first_crossing_index():
    curve = [(1, 0.9), (2, 0.5), (3, 0.2), (4, 0.1)]
    assert metrics.events_to_adaptation(curve) == 3


def test_events_to_adaptation_is_none_when_never_crossing():
    """A pivot that never turns the set over is a finding, not a failure --
    the threshold is pre-registered and does not get retuned."""
    curve = [(1, 0.9), (2, 0.8), (3, 0.75)]
    assert metrics.events_to_adaptation(curve) is None


def test_events_to_adaptation_empty_curve():
    assert metrics.events_to_adaptation([]) is None


def test_events_to_adaptation_threshold_is_strict_less_than():
    assert metrics.events_to_adaptation([(1, metrics.ADAPTATION_JACCARD_THRESHOLD)]) is None


# --- percentiles ---


def test_percentiles_nearest_rank_returns_observed_values():
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    out = metrics.percentiles(values)
    assert out["n"] == 5
    assert out["min"] == 1.0 and out["max"] == 5.0
    assert out["mean"] == pytest.approx(3.0)
    for key in ("p50", "p95", "p99"):
        assert out[key] in values


def test_percentiles_empty_sample():
    assert metrics.percentiles([]) == {"n": 0}


def test_percentiles_single_sample():
    out = metrics.percentiles([0.5])
    assert out["p50"] == out["p99"] == 0.5


# --- cold_warm_transition ---


def _snap(index, action, wall, ids):
    return {"refresh_index": index, "action": action, "wall_clock": wall, "track_ids": ids}


def test_cold_warm_transition_reports_the_handoff_discontinuity():
    log = [
        _snap(1, "cold_start", 100.0, ["1", "2", "3"]),
        _snap(2, "cold_start", 110.0, ["1", "2", "3"]),
        _snap(3, "warm", 120.0, ["3", "4", "5"]),
    ]
    out = metrics.cold_warm_transition(log, session_start_wall=90.0)
    assert out["reached_warm_path"] is True
    assert out["cold_start_refresh_count"] == 2
    assert out["seconds_to_first_warm"] == pytest.approx(30.0)
    assert out["first_warm_refresh_index"] == 3
    assert out["preceded_by_cold_start"] is True
    assert out["handoff_jaccard"] == pytest.approx(1 / 5)


def test_cold_warm_transition_when_warm_path_never_reached():
    log = [_snap(1, "cold_start", 100.0, ["1"])]
    out = metrics.cold_warm_transition(log, session_start_wall=90.0)
    assert out["reached_warm_path"] is False
    assert out["seconds_to_first_warm"] is None
    assert out["handoff_jaccard"] is None


def test_cold_warm_transition_ignores_skip_insufficient_data_entries():
    log = [
        {"refresh_index": 0, "action": "skip_insufficient_data", "wall_clock": 95.0, "track_ids": []},
        _snap(1, "warm", 120.0, ["1"]),
    ]
    out = metrics.cold_warm_transition(log, session_start_wall=90.0)
    assert out["reached_warm_path"] is True
    assert out["preceded_by_cold_start"] is False
    assert out["handoff_jaccard"] is None


# --- anchor_schedule ---


def test_anchor_schedule_is_strictly_increasing():
    anchors = metrics.anchor_schedule([600.0, 900.0, 1200.0])
    assert anchors == sorted(anchors)
    assert len(set(anchors)) == 3


def test_anchor_schedule_leaves_at_least_the_gap_after_each_scenario():
    spans = [600.0, 900.0]
    anchors = metrics.anchor_schedule(spans)
    end_of_first = anchors[0] + dt.timedelta(seconds=spans[0])
    assert (anchors[1] - end_of_first).total_seconds() >= metrics.SCENARIO_GAP_SECONDS


def test_anchor_schedule_keeps_every_anchor_window_aligned():
    """Anchors must be whole multiples of the 5-minute window size from the
    epoch, or sliding-window boundaries fall differently relative to each
    scenario's events and the runs stop being comparable."""
    anchors = metrics.anchor_schedule([137.0, 941.0, 3.0, 6000.0])
    for anchor in anchors:
        offset = (anchor - metrics.EVAL_EPOCH).total_seconds()
        assert offset % metrics.WINDOW_ALIGNMENT_SECONDS == 0


def test_anchor_schedule_first_anchor_is_the_epoch():
    assert metrics.anchor_schedule([100.0])[0] == metrics.EVAL_EPOCH
