"""Pure unit tests for eval/8_2's stage 15D metric 8 (failure injection).

Same split as test_82_metrics.py / test_82_metrics_15c2.py: METRICS.md
section 9's verdict rule is exercised against synthetic arm records, so a
changed definition fails here in milliseconds rather than after a live run
that stops containers.

The `82_` filename prefix is required -- see README.md: `8_1`/`8_2` are not
valid Python identifiers, so pytest resolves same-named files across the two
eval packages to the same module and silently collects only one of them.
"""
import sys
from importlib import import_module
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT))

metrics = import_module("eval.8_2.metrics")


def _sample(phase, session=200, recs=200, profile=200):
    return {"phase": phase, "session_status": session,
            "recommendations_status": recs, "profile_status": profile}


def _record(arm="control", events_lost=0, recovers=True, stale=False, samples=None):
    return {
        "arm": arm,
        "events_lost": events_lost,
        "recovers_without_restart": recovers,
        "staleness_detectable": stale,
        "client_samples": samples if samples is not None else [_sample("before"), _sample("after")],
    }


def test_control_matching_replicates_report_no_effect():
    control = _record()
    verdict = metrics.failure_injection_verdict(control, [_record(arm="x"), _record(arm="x")])
    assert verdict["pass"] is True
    assert verdict["effects_beyond_control"] == []
    assert verdict["unstable_measures"] == []


def test_effect_absent_from_control_is_reported():
    control = _record()
    arm = _record(arm="redis_outage", events_lost=4)
    verdict = metrics.failure_injection_verdict(control, [arm, dict(arm)])
    assert verdict["effects_beyond_control"] == ["events_lost"]
    assert verdict["measures"]["events_lost"]["effect"] == 4
    assert verdict["measures"]["events_lost"]["differs_from_control"] is True


def test_effect_already_present_in_control_does_not_count():
    """Section 9.5's first condition: a measure that already differs from
    nominal in the control arm is a property of the pipeline, not of the
    injected failure, so it must not be attributed to the failure."""
    control = _record(events_lost=4)
    arm = _record(arm="redis_outage", events_lost=4)
    verdict = metrics.failure_injection_verdict(control, [arm, dict(arm)])
    assert verdict["effects_beyond_control"] == []
    assert verdict["measures"]["events_lost"]["differs_from_control"] is False


def test_disagreeing_replicates_make_the_measure_unstable_and_claimless():
    """Section 9.5's second condition: replicates that disagree yield NO
    claim -- not an average, not a pick. `differs_from_control` must be None
    rather than False, so an unstable measure can never be misread as a
    null effect."""
    control = _record()
    verdict = metrics.failure_injection_verdict(
        control, [_record(arm="a", events_lost=4), _record(arm="a", events_lost=7)]
    )
    assert verdict["pass"] is False
    assert verdict["unstable_measures"] == ["events_lost"]
    assert verdict["measures"]["events_lost"]["effect"] == "unstable"
    assert verdict["measures"]["events_lost"]["differs_from_control"] is None
    assert "events_lost" not in verdict["effects_beyond_control"]


def test_pass_is_about_measurement_soundness_not_system_health():
    """An arm that loses events AND exposes no staleness signal still passes:
    the verdict says the measurement is sound, the finding says what it
    found. Section 9.5 is explicit that these are separate judgements."""
    control = _record()
    arm = _record(arm="semantic_api_outage", events_lost=9, recovers=False, stale=False)
    verdict = metrics.failure_injection_verdict(control, [arm, dict(arm)])
    assert verdict["pass"] is True
    assert set(verdict["effects_beyond_control"]) == {"events_lost", "recovers_without_restart"}
    assert verdict["measures"]["staleness_detectable"]["effect"] is False


def test_client_status_sequence_is_derived_and_compared_per_phase():
    """Measure 3 stores whole responses; the verdict compares the HTTP status
    triple per phase. A 500 during the outage must surface as an effect."""
    control = _record(samples=[_sample("before"), _sample("during"), _sample("after")])
    arm = _record(arm="redis_outage",
                  samples=[_sample("before"), _sample("during", 500, 500, 500), _sample("after")])
    verdict = metrics.failure_injection_verdict(control, [arm, dict(arm)])
    assert "client_status_sequence" in verdict["effects_beyond_control"]
    assert verdict["measures"]["client_status_sequence"]["effect"][1] == ("during", 500, 500, 500)


def test_every_section_9_4_measure_is_covered_by_the_verdict():
    """Guards against a measure being added to the spec and silently never
    reaching the rule that judges it."""
    assert set(metrics.CATEGORICAL_MEASURES) | set(metrics.COUNT_MEASURES) == {
        "events_lost", "recovers_without_restart", "staleness_detectable",
        "client_status_sequence",
    }


# --- run-validity guard (METRICS.md section 9.6a) --------------------------


def _posts(gaps_expected, gaps_actual):
    """Builds a post list whose intended pacing is `gaps_expected` and whose
    realised wall-clock spacing is `gaps_actual`."""
    wall, posts = 1000.0, [{"index": 0, "wall_clock": 1000.0, "expected_sleep_seconds": 0.0}]
    for i, (exp, act) in enumerate(zip(gaps_expected, gaps_actual), start=1):
        wall += act
        posts.append({"index": i, "wall_clock": wall, "expected_sleep_seconds": exp})
    return posts


def test_normal_pacing_is_not_flagged():
    fi = import_module("eval.8_2.failure_injection")
    result = fi.wall_clock_stall(_posts([2.0, 3.0, 2.5], [2.1, 3.2, 2.6]))
    assert result["wall_clock_stall_detected"] is False
    assert result["max_post_gap_overrun_seconds"] < 1.0


def test_a_suspended_arm_is_flagged():
    """The real incident: twenty hours between two posts, everything else
    normal. Nothing else in the record would have shown it."""
    fi = import_module("eval.8_2.failure_injection")
    result = fi.wall_clock_stall(_posts([2.0, 3.0, 2.5], [2.1, 73_000.0, 2.6]))
    assert result["wall_clock_stall_detected"] is True
    assert result["max_post_gap_overrun_seconds"] > 72_000


def test_the_guard_measures_overrun_not_raw_gap():
    """A long gap that the scenario INTENDED (a slow-paced arm) is not a
    stall -- only the excess over the intended pacing counts."""
    fi = import_module("eval.8_2.failure_injection")
    result = fi.wall_clock_stall(_posts([600.0], [601.0]))
    assert result["wall_clock_stall_detected"] is False


def test_a_measure_undefined_in_the_control_is_not_reported_as_an_effect():
    """`recovers_without_restart` is None in the control -- there is no
    injection to recover from -- so comparing an arm's True against it would
    report "the system recovered" as an effect OF THE FAILURE. Reported under
    measures_not_comparable_to_control instead, still stability-checked."""
    control = _record(recovers=None)
    arm = _record(arm="redis_outage", recovers=True)
    verdict = metrics.failure_injection_verdict(control, [arm, dict(arm)])
    assert verdict["measures_not_comparable_to_control"] == ["recovers_without_restart"]
    assert "recovers_without_restart" not in verdict["effects_beyond_control"]
    assert verdict["measures"]["recovers_without_restart"]["effect"] is True
    assert verdict["measures"]["recovers_without_restart"]["comparable_to_control"] is False


def test_an_arm_that_fails_to_recover_is_still_visible_in_the_measure():
    """Dropping the control comparison must not hide the finding itself."""
    control = _record(recovers=None)
    arm = _record(arm="x", recovers=False)
    verdict = metrics.failure_injection_verdict(control, [arm, dict(arm)])
    assert verdict["measures"]["recovers_without_restart"]["effect"] is False
