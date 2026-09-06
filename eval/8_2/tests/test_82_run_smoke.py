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
