"""Import/wiring smoke test for eval/8_2, mirroring
eval/8_1/tests/test_run_smoke.py's first layer.

That file exists because a broken import in eval/8_1/run.py sat undetected
on main for a whole session (Decision D's git mv). These modules import
platform code across three sys.path insertions (platform/, the use case,
eval/), so the same class of break is available here; a bare import catches
it before a 10-minute live run does.

No live-run layer here: unlike eval/8_1's run_pipeline(), this harness has
no read-only subset mode -- it starts services, submits a Flink job, and
produces real Kafka traffic. Driving it is the manual verification step in
docs/platform/stage15c-eval-8_2-harness.md, not a pytest test.
"""
import sys
from importlib import import_module
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT))


def test_modules_import():
    for name in ("metrics", "scenario_gen", "store_access", "orchestration", "harness", "run"):
        import_module(f"eval.8_2.{name}")


def test_k_sweep_stays_within_the_chillout_catalog():
    """METRICS.md section 1 flags this explicitly: chillout has 15 tagged
    tracks, so a K beyond that would hit scenario_gen's loud raise mid-run."""
    run = import_module("eval.8_2.run")
    assert max(run.K_SWEEP) <= 15


def test_pivot_scenario_shape_matches_the_spec():
    run = import_module("eval.8_2.run")
    specs = run.build_specs(seed=42, speed=60.0, run_id="t")
    assert [s.genre_track_counts[0][1] for s in specs] == run.K_SWEEP
    for spec in specs:
        assert spec.genre_track_counts[0][0] == "chillout"
        assert spec.genre_track_counts[1] == ("rock", 8)
        # the pivot is the first post-pivot TRACK index, i.e. exactly K
        assert spec.pivot_track_index == spec.genre_track_counts[0][1]


def test_session_ids_are_scoped_to_the_invocation():
    """Reusing a session id across invocations makes the next run replay the
    previous one's events off the never-purged behavioral-events topic --
    observed live, see build_specs()'s docstring."""
    run = import_module("eval.8_2.run")
    first = {s.session_id for s in run.build_specs(42, 60.0, "runA")}
    second = {s.session_id for s in run.build_specs(42, 60.0, "runB")}
    assert first.isdisjoint(second)


def test_extra_scenarios_cover_metrics_4_to_7():
    """Stage 15C.2's three scenarios: one long session (metrics 4/5) and
    three pivot replicates (metrics 6/7), of which exactly one is jittered."""
    run = import_module("eval.8_2.run")
    specs = {s.name: s for s in run.build_extra_specs(seed=42, speed=60.0, run_id="t")}
    assert set(specs) == {"long_session", *run.DETERMINISM_RUNS, run.LATE_EVENT_RUN}

    long_session = specs["long_session"]
    assert long_session.genre_track_counts == run.LONG_SESSION_GENRES
    assert sum(n for _, n in long_session.genre_track_counts) == 40
    # Metric 4 runs slower than the rest of the pack, by measurement --
    # see LONG_SESSION_SPEED's rationale.
    assert long_session.speed == run.LONG_SESSION_SPEED < 60.0
    assert long_session.pivot_track_index is None

    replicates = [specs[n] for n in (*run.DETERMINISM_RUNS, run.LATE_EVENT_RUN)]
    assert [r.jitter for r in replicates] == [0.0, 0.0, run.JITTER_LEVEL]


def test_determinism_replicates_differ_in_nothing_a_seed_controls():
    """Metric 6 asks whether the pipeline reproduces, so the two replicates
    must be identical apart from the session id -- and their difference is
    also metric 7's noise floor, which the jitter effect has to beat."""
    run = import_module("eval.8_2.run")
    specs = {s.name: s for s in run.build_extra_specs(seed=42, speed=60.0, run_id="t")}
    a, b = (specs[n] for n in run.DETERMINISM_RUNS)
    assert a.session_id != b.session_id
    for field in ("genre_track_counts", "seed", "speed", "jitter", "pivot_track_index"):
        assert getattr(a, field) == getattr(b, field)


def test_late_event_arm_differs_from_the_baseline_only_in_jitter():
    run = import_module("eval.8_2.run")
    specs = {s.name: s for s in run.build_extra_specs(seed=42, speed=60.0, run_id="t")}
    baseline, jittered = specs[run.DETERMINISM_RUNS[0]], specs[run.LATE_EVENT_RUN]
    for field in ("genre_track_counts", "seed", "speed", "pivot_track_index"):
        assert getattr(baseline, field) == getattr(jittered, field)
    assert baseline.jitter == 0.0 and jittered.jitter > 0.0


def test_long_session_genres_stay_within_their_catalog_counts():
    """METRICS.md section 1's loud-raise rule: scenario_gen refuses to shrink
    a scenario silently, so an over-large n would abort mid-run."""
    run = import_module("eval.8_2.run")
    live_counts = {"rock": 102, "electronic": 73, "chillout": 15, "dance": 21}
    for genre, n in run.LONG_SESSION_GENRES:
        assert n <= live_counts[genre]


def test_compare_runs_is_reused_from_eval_8_1_without_leaking_sys_path():
    """METRICS.md section 7 asks metric 6 to reuse eval/8_1/diff_runs.py's
    methodology. That module puts eval/8_1 on sys.path at import, and
    eval/8_1 has modules whose bare names collide with this package's."""
    run = import_module("eval.8_2.run")
    before = list(sys.path)
    compare_runs = run._load_compare_runs()
    assert sys.path == before
    result = compare_runs(
        {1: [{"recommended_track_id": 5, "score": 1.0}]},
        {1: [{"recommended_track_id": 6, "score": 1.0}]},
    )
    assert result["classification_counts"]["membership_changed"] == 1


def test_raw_artifact_split_round_trips_vectors_exactly():
    """Profile vectors are split into a side file to keep the records
    readable, never rounded -- metric 6 compares for exact equality."""
    run = import_module("eval.8_2.run")
    vector = [0.1234567890123456, -0.9876543210987654]
    records = [{"session_id": "s1", "profile_samples": [
        {"computed_at": 1699999999.1234567, "n_events": 3, "vector": vector}]}]
    slim, vectors = run.split_raw_artifacts([{**r} for r in records])
    assert "vector" not in slim[0]["profile_samples"][0]
    restored = run.merge_raw_artifacts(slim, vectors)
    assert restored[0]["profile_samples"][0]["vector"] == vector
