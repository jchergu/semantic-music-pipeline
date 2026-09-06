"""
eval/8_2 -- 8.2 (streaming, reactive) evaluation pack. Stage 15C.

An ACTIVE harness, unlike eval/8_1: it starts the services, submits the
Flink job, drives the seeded simulator's event stream, runs both consumer
groups, and measures the live system while a scenario plays out. See
METRICS.md (signed off 2026-09-03) for every definition, and README.md for
the runbook.

Stage 15C covered metrics 1 (reactivity), 2 (latency) and 3 (cold->warm
transition), all three off one pivot scenario. Stage 15C.2 adds metrics 4
(semantic coherence), 5 (catalog coverage), 6 (determinism) and 7
(late-event handling) on the same harness, with three more scenarios: a
40-track four-genre long session, and three replicates of one pivot K --
two content-identical jitter-0 runs plus one jittered run.

The two jitter-0 replicates do double duty. They ARE metric 6's
determinism comparison, and the difference between them is also the
run-to-run noise floor metric 7's jitter effect has to beat before it
counts as real (the same methodology eval/8_1's Stage 15A.2 used to
validate its reconstruction path against ANN noise).

No accuracy metric anywhere in this pack: no ground truth exists for this
dataset, so nothing here is precision@k, recall@k or NDCG.

Usage:
    platform/enrichment/.venv/bin/python -m eval.8_2.run

Writes:
    eval/8_2/reactivity.json           metric 1, per-K curves
    eval/8_2/latency.json              metric 2, NOT a deterministic artifact
    eval/8_2/results.json              metrics 3, 6, 7
    eval/8_2/coherence.json            metric 4
    eval/8_2/coverage.json             metric 5
    eval/8_2/raw_scenario_records.json  everything observed except the bulk arrays
    eval/8_2/raw_profile_vectors.json   the sampled 512-dim session profiles
    eval/8_2/raw_embeddings.json        CLAP embeddings of every track played
    eval/8_2/tables.md
    eval/8_2/figures/*.png
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent.parent / "platform"))

from simulator import events as sim_events  # noqa: E402

from . import metrics, orchestration, scenario_gen, store_access  # noqa: E402
from .harness import ScenarioSpec, run_scenario  # noqa: E402

OUT_DIR = ROOT
FIGURES_DIR = ROOT / "figures"

# METRICS.md section 2: K pre-pivot chillout tracks, then a fixed 8 rock
# tracks. chillout has 15 tagged tracks in the live catalog, so K stays
# inside that; the post-pivot length is fixed so the sweep varies one thing.
K_SWEEP = [3, 5, 8, 12]
PRE_PIVOT_GENRE = "chillout"
POST_PIVOT_GENRE = "rock"
POST_PIVOT_TRACKS = 8

# METRICS.md section 1's long session, for metrics 4 and 5: four genres, ten
# tracks each, all comfortably inside their catalog counts (rock 102,
# electronic 73, chillout 15, dance 21 -- verified live, unchanged since the
# spec was written).
LONG_SESSION_GENRES = [("rock", 10), ("electronic", 10), ("chillout", 10), ("dance", 10)]

# Metric 4's --speed, chosen by measurement as METRICS.md section 5 requires,
# not defaulted. The Flink job overwrites session:{id}:profile on every
# window fire and keeps no history, so the profile time series exists only as
# far as polling can resolve it -- and the writes are not evenly spaced. An
# event-time gap between tracks (median 231s here) advances the watermark
# past several 30s slides at once, so the job fires those windows
# milliseconds apart, in a burst that no poll rate can separate. The
# resolvable ceiling is therefore one vector per watermark-advancing event,
# not one per window fire.
#
# Measured on a 12-event rock probe (6 watermark-advancing events, 45 window
# fires analytically) at a 0.5s poll interval:
#   speed 60 -> 5 of 6 distinct writes captured (min burst gap 3.30s wall)
#   speed 30 -> 6 of 6 distinct writes captured (min burst gap 6.60s wall)
# 30 it is: full capture, at 13x the poll interval of headroom on the
# tightest gap, for 323s of wall-clock instead of 162s.
LONG_SESSION_SPEED = 30.0
PROFILE_POLL_INTERVAL_SECONDS = 0.5

# Metrics 6 and 7 both run the pivot scenario at one fixed K. K=8 because
# Stage 15C confirmed the warm-path precondition held at every K, and 8 is
# the largest that still leaves chillout headroom (15 tagged tracks).
REPLICATE_K = 8
JITTER_LEVEL = 1.0
DETERMINISM_RUNS = ("det_rep1", "det_rep2")
LATE_EVENT_RUN = "late_jitter"


def build_specs(seed: int, speed: float, run_id: str) -> list[ScenarioSpec]:
    """`run_id` scopes the session ids to this invocation, and is not
    optional. The behavioral-events topic is append-only and never purged,
    and both consumer groups start from "earliest" (the platform's own
    setting), so a session id reused across invocations makes this run
    replay the previous run's events for that session before reaching its
    own -- observed live while building this stage: a re-run of K=3 consumed
    22 stale events, exhausted its event budget, and reported 0 post-pivot
    refreshes. Scenario *content* stays fully seed-determined; only the Redis
    and Kafka keying moves."""
    return [
        ScenarioSpec(
            name=f"pivot_K{k}",
            role="pivot_sweep",
            session_id=f"eval82-pivot-K{k}-{run_id}",
            genre_track_counts=[(PRE_PIVOT_GENRE, k), (POST_PIVOT_GENRE, POST_PIVOT_TRACKS)],
            seed=seed,
            speed=speed,
            pivot_track_index=k,
            profile_poll_interval_seconds=PROFILE_POLL_INTERVAL_SECONDS,
        )
        for k in K_SWEEP
    ]


def build_extra_specs(seed: int, speed: float, run_id: str) -> list[ScenarioSpec]:
    """Stage 15C.2's three additional scenarios.

    The long session feeds metrics 4 and 5 and runs at its own, slower speed
    (see LONG_SESSION_SPEED). The three pivot replicates all share ONE spec
    apart from jitter and session id, which is the point: det_rep1 and
    det_rep2 differ in nothing a seed controls, so metric 6 can ask whether
    the pipeline reproduces, and metric 7 can use their difference as the
    noise floor its jitter effect must beat.
    """
    def replicate(name: str, jitter: float) -> ScenarioSpec:
        return ScenarioSpec(
            name=name,
            role="determinism" if name in DETERMINISM_RUNS else "late_event",
            session_id=f"eval82-{name}-{run_id}",
            genre_track_counts=[(PRE_PIVOT_GENRE, REPLICATE_K), (POST_PIVOT_GENRE, POST_PIVOT_TRACKS)],
            seed=seed,
            speed=speed,
            pivot_track_index=REPLICATE_K,
            jitter=jitter,
            profile_poll_interval_seconds=PROFILE_POLL_INTERVAL_SECONDS,
        )

    return [
        ScenarioSpec(
            name="long_session",
            role="long_session",
            session_id=f"eval82-long-{run_id}",
            genre_track_counts=list(LONG_SESSION_GENRES),
            seed=seed,
            speed=LONG_SESSION_SPEED,
            pivot_track_index=None,
            profile_poll_interval_seconds=PROFILE_POLL_INTERVAL_SECONDS,
        ),
        replicate(DETERMINISM_RUNS[0], 0.0),
        replicate(DETERMINISM_RUNS[1], 0.0),
        replicate(LATE_EVENT_RUN, JITTER_LEVEL),
    ]


def simulated_spans(specs: list[ScenarioSpec], ids_by_genre: dict, durations: dict) -> list[float]:
    """Each scenario's simulated duration, needed to lay out event-time
    anchors before any of them runs. Built against a throwaway anchor --
    the span itself doesn't depend on where the scenario starts."""
    spans = []
    for spec in specs:
        script = scenario_gen.build_genre_session_script(spec.genre_track_counts, spec.seed, ids_by_genre)
        planned = sim_events.build_session_events(script, spec.session_id, durations, metrics.EVAL_EPOCH)
        spans.append(sum(p["wall_clock_gap_seconds"] for p in planned))
    return spans


# --- metric computation over scenario records ---


def reactivity(record: dict) -> dict:
    """Metric 1. Jaccard of each post-pivot recommendation snapshot against
    the last pre-pivot one; the crossing point is where the set has turned
    over. Excluded (not reported) if the warm-path precondition failed or
    if there was no pre-pivot refresh to compare against."""
    precondition_ok = record["warm_path_precondition"].get("profile_exists_at_pivot") is True
    pre = [s for s in record["snapshots"] if not s["post_pivot"]]
    post = [s for s in record["snapshots"] if s["post_pivot"]]

    exclusions = []
    if not precondition_ok:
        exclusions.append("no session profile existed at the pivot (still on the cold-start path)")
    if not pre:
        exclusions.append("no pre-pivot refresh to compare against")

    pre_pivot_set = set(pre[-1]["track_ids"]) if pre else set()
    curve = [(s["refresh_index"], metrics.jaccard(pre_pivot_set, set(s["track_ids"]))) for s in post]
    pivot_event_index = record["pivot_event_index"]

    return {
        "k": record["genre_track_counts"][0][1],
        "session_id": record["session_id"],
        "excluded": bool(exclusions),
        "exclusion_reasons": exclusions,
        "pre_pivot_refresh_count": len(pre),
        "pre_pivot_set": sorted(pre_pivot_set),
        "post_pivot_refresh_count": len(post),
        # Both readings are reported: the refresh index (what METRICS.md
        # defines the crossing on) and the event index behind it. The two
        # aren't interchangeable -- the debounce is wall-clock, so --speed
        # changes how many events fall behind one refresh (see latency.json's
        # speed_caveat).
        "curve": [
            {
                "refresh_index": snap["refresh_index"],
                "jaccard": value,
                "event_index": snap["event_index"],
                "events_since_pivot": snap["event_index"] - pivot_event_index,
            }
            for (_, value), snap in zip(curve, post)
        ],
        "events_to_adaptation": metrics.events_to_adaptation(curve),
        "actions": [s["action"] for s in record["snapshots"]],
    }


def latency(records: list[dict]) -> dict:
    """Metric 2, pooled across every scenario run in the pack."""
    h1 = [p["h1_post_seconds"] for r in records for p in r["posts"]]
    h2 = [s["h2_profile_lag_seconds"] for r in records for s in r["profile_samples"]
          if s["h2_profile_lag_seconds"] is not None]
    h3 = [x["refresh_compute_seconds"] for r in records for x in r["refresh_results"]
          if x.get("refreshed") and x.get("refresh_compute_seconds") is not None]
    arrival_to_refresh = [x["arrival_to_refresh_seconds"] for r in records for x in r["refresh_results"]
                          if x.get("refreshed") and x.get("arrival_to_refresh_seconds") is not None]

    # The debounce floor doesn't show up in arrival_to_refresh (an event that
    # DOES trigger a refresh is served immediately -- that number is the
    # refresh's own cost). It shows up in the spacing BETWEEN consecutive
    # refreshes and in how many events accumulated behind each one: the two
    # branches of should_refresh() are "5s elapsed" and "3 events queued", so
    # the observed values clustering at exactly those two constants is the
    # evidence the floor is real rather than incidental.
    inter_refresh, events_behind = [], []
    # Split by --speed as well as pooled. H1/H2/H3 are each one operation's
    # own cost and pool cleanly across speeds, but these two are not: the
    # debounce constants are wall-clock while --speed compresses only session
    # time, so a scenario at speed 30 faces a debounce half as strict in
    # session-time terms as one at speed 60 and accumulates a different
    # number of events behind each refresh. Pooling them across the pack's
    # two speeds would average away exactly the effect speed_caveat below
    # describes.
    inter_refresh_by_speed: dict[str, list[float]] = {}
    events_behind_by_speed: dict[str, list[float]] = {}
    for r in records:
        speed_key = f"{r['speed']:g}"
        last_refresh_wall = None
        since = 0
        for x in r["refresh_results"]:
            since += 1
            if not x.get("refreshed"):
                continue
            if last_refresh_wall is not None:
                inter_refresh.append(x["wall_clock"] - last_refresh_wall)
                events_behind.append(since)
                inter_refresh_by_speed.setdefault(speed_key, []).append(x["wall_clock"] - last_refresh_wall)
                events_behind_by_speed.setdefault(speed_key, []).append(float(since))
            last_refresh_wall = x["wall_clock"]
            since = 0

    from streaming.recommendation_refresh import DEBOUNCE_MIN_EVENTS, DEBOUNCE_MIN_INTERVAL_SECONDS

    return {
        "note": (
            "Wall-clock throughout, never event_time -- --speed decouples the two, so an "
            "event_time-based latency would be meaningless. Three hops, not the roadmap's six: "
            "the other three aren't independently instrumentable without rewriting Flink internals "
            "(METRICS.md section 3)."
        ),
        "not_deterministic": (
            "Fresh wall-clock timings against the current environment. Explicitly outside any "
            "determinism claim, same stance eval/8_1/latency.json takes."
        ),
        "h1_ingest_post_seconds": metrics.percentiles(h1),
        "h2_profile_compute_lag_seconds": metrics.percentiles(h2),
        "h3_refresh_compute_seconds": metrics.percentiles(h3),
        "arrival_to_refresh_seconds": metrics.percentiles(arrival_to_refresh),
        "inter_refresh_interval_seconds": metrics.percentiles(inter_refresh),
        "events_behind_each_refresh": metrics.percentiles([float(v) for v in events_behind]),
        "inter_refresh_interval_seconds_by_speed": {
            k: metrics.percentiles(v) for k, v in sorted(inter_refresh_by_speed.items())
        },
        "events_behind_each_refresh_by_speed": {
            k: metrics.percentiles(v) for k, v in sorted(events_behind_by_speed.items())
        },
        "debounce_design_floor": {
            "min_interval_seconds": DEBOUNCE_MIN_INTERVAL_SECONDS,
            "min_events": DEBOUNCE_MIN_EVENTS,
            "note": (
                "A stated design floor, not a measured latency. Its evidence is "
                "inter_refresh_interval_seconds clustering at min_interval_seconds and "
                "events_behind_each_refresh clustering at min_events -- that clustering is the "
                "debounce working, not a bug."
            ),
        },
        "pooling_note": (
            "H1/H2/H3 pool across every scenario in the pack: each is one operation's own cost, and "
            "a second --speed is a different operating point for the same hop, not a different "
            "measurement. The two debounce-sensitive series are additionally reported per speed, "
            "because those DO change with it -- see speed_caveat."
        ),
        "speed_caveat": (
            "The debounce constants are WALL-CLOCK, but --speed compresses only the simulated "
            "session clock, so a scenario run at speed S faces a debounce S times stricter in "
            "session-time terms than production would. This changes how many events fall behind "
            "each refresh, and therefore the refresh cadence metric 1's curve is indexed on -- "
            "which is why each curve point also carries its event_index. It does not affect H1/H2/H3, "
            "each of which is a single operation's own cost."
        ),
        "raw_samples": {
            "h1": h1, "h2": h2, "h3": h3,
            "arrival_to_refresh": arrival_to_refresh,
            "inter_refresh_interval": inter_refresh,
            "events_behind_each_refresh": events_behind,
        },
    }


def cold_start_seed_track(record: dict) -> int | None:
    for entry in record["refresh_results"]:
        if entry.get("action") == "cold_start":
            return entry.get("seed_track_id")
    return None


def cold_warm(records: list[dict]) -> dict:
    per_session = {
        r["session_id"]: metrics.cold_warm_transition(r["snapshots"], r["session_start_wall"])
        for r in records
    }
    reached = [v for v in per_session.values() if v["reached_warm_path"]]
    handoffs = [v["handoff_jaccard"] for v in reached if v["handoff_jaccard"] is not None]
    seeds = [cold_start_seed_track(r) for r in records]
    distinct_seeds = len({s for s in seeds if s is not None})
    # Grouped rather than only counted: the grouping says which sessions are
    # repeats of each other, which is the thing a reader has to know before
    # averaging anything below.
    grouped: dict[str, list[str]] = {}
    for record, seed_track in zip(records, seeds):
        grouped.setdefault(str(seed_track), []).append(record["session_id"])
    largest = max((len(v) for v in grouped.values()), default=0)
    return {
        "independent_observations": distinct_seeds,
        "independence_caveat": (
            None if distinct_seeds >= len(records) else
            f"{len(records)} sessions but only {distinct_seeds} distinct cold-start seed track(s), "
            f"the largest group covering {largest} of them. Every pivot-family scenario -- the four "
            "K-sweep runs and the three K=8 replicates -- opens on the same track, because "
            "random.Random(seed).sample() returns prefix-nested draws for increasing K. Their "
            "handoff numbers are therefore ONE observation repeated, not several independent ones. "
            "Do not read the mean below as an average over independent runs; see "
            "sessions_by_cold_start_seed_track for which sessions are repeats of which."
        ),
        "sessions_by_cold_start_seed_track": grouped,
        "cold_start_seed_track_per_session": dict(zip([r["session_id"] for r in records], seeds)),
        "note": (
            "The cold->warm handoff is not gated by an event count: it is a race between two "
            "independent consumer groups on the same topic (Flink's, which writes "
            "session:{id}:profile, and the refresh loop's, which reads it). Reported as a race, "
            "per METRICS.md section 4."
        ),
        "sessions_measured": len(per_session),
        "sessions_reaching_warm_path": len(reached),
        "handoff_jaccard_mean": (sum(handoffs) / len(handoffs)) if handoffs else None,
        "per_session": per_session,
    }


def _load_compare_runs():
    """METRICS.md section 7 asks metric 6 to reuse eval/8_1/diff_runs.py's
    comparison methodology rather than growing a second differ, so this
    imports the real function instead of restating it.

    Loaded lazily and behind a sys.path restore: diff_runs.py puts eval/8_1
    on sys.path at import time (to reach its own store_access and
    diff_recommendations siblings), and eval/8_1 has modules whose bare names
    -- store_access, metrics, run -- collide with this package's. Nothing
    here imports those bare names (every intra-package import in eval/8_2 is
    relative), but leaving eval/8_1 permanently at the front of sys.path for
    a process that also runs platform code is a trap worth not setting.
    """
    import importlib

    saved = list(sys.path)
    sys.path.insert(0, str(ROOT.parent / "8_1"))
    try:
        return importlib.import_module("diff_runs").compare_runs
    finally:
        sys.path[:] = saved


def coherence(record: dict, embeddings: dict[str, list[float]]) -> dict:
    """Metric 4, for one scenario run."""
    series = metrics.coherence_series(record["profile_samples"], record["posts"], embeddings)
    sampled = len(record["profile_samples"])
    # The ceiling on how many distinct profile writes polling can resolve --
    # see metrics.watermark_advancing_events() and LONG_SESSION_SPEED for the
    # measurement this ceiling came from.
    advancing = metrics.watermark_advancing_events(record["posts"])
    return {
        "name": record["name"],
        "session_id": record["session_id"],
        "speed": record["speed"],
        "poll_interval_seconds": record["profile_poll_interval_seconds"],
        "window_seconds": metrics.COHERENCE_WINDOW_SECONDS,
        "distinct_profile_writes_observed": sampled,
        "watermark_advancing_events": advancing,
        "sampling_note": (
            "Each watermark-advancing event fires several sliding windows within milliseconds, "
            "and the job overwrites one Redis key, so at most one vector per such event is "
            "observable at ANY poll rate. distinct_profile_writes_observed is measured against "
            "watermark_advancing_events, not against the analytic window-fire count."
        ),
        "series": series,
        "summary": metrics.coherence_summary(series),
    }


def coverage(record: dict) -> dict:
    """Metric 5, for one scenario run."""
    cov = metrics.catalog_coverage(record["snapshots"])
    return {
        "name": record["name"],
        "session_id": record["session_id"],
        "genre_track_counts": record["genre_track_counts"],
        "tracks_played": len(record["track_ids_in_order"]),
        **cov,
    }


def determinism(record_a: dict, record_b: dict) -> dict:
    """Metric 6: two content-identical runs (same seed, same speed, jitter 0)
    compared refresh by refresh, against a rule pre-registered in
    metrics.determinism_verdict()."""
    compare_runs = _load_compare_runs()
    a = metrics.snapshots_as_ranked_rows(record_a["snapshots"])
    b = metrics.snapshots_as_ranked_rows(record_b["snapshots"])
    # Deliberately the UNION of refresh indices, not the intersection: a
    # refresh that only one run produced is a divergence, and intersecting
    # would quietly drop exactly the evidence of it.
    cmp = compare_runs(a, b)
    max_delta = max((v["max_delta"] for v in cmp["per_seed"].values()), default=0.0)
    verdict = metrics.determinism_verdict(
        (len(record_a["snapshots"]), len(record_b["snapshots"])), cmp, max_delta
    )
    return {
        "runs": [record_a["session_id"], record_b["session_id"]],
        "scenario": f"{PRE_PIVOT_GENRE}({REPLICATE_K}) -> {POST_PIVOT_GENRE}({POST_PIVOT_TRACKS})",
        "seed": record_a["seed"],
        "speed": record_a["speed"],
        "jitter": record_a["jitter"],
        "caveat": (
            "The two replicates cannot share a session id or an event-time anchor: the "
            "behavioral-events topic is never purged and both consumer groups read from "
            "earliest, and Flink's watermark is stream-wide so scenarios must advance through "
            "event time. Anchors are aligned to whole multiples of the 300s window size "
            "(metrics.anchor_schedule), so window boundaries fall identically relative to each "
            "run's own events -- everything a seed can control is held fixed, and what is left "
            "is exactly what this metric is asking about."
        ),
        "verdict": verdict,
        "per_refresh": {
            str(k): {"classification": v["classification"],
                     "a_track_ids": v["a_track_ids"],
                     "b_track_ids": v["b_track_ids"],
                     "max_score_delta": v["max_delta"]}
            for k, v in sorted(cmp["per_seed"].items())
        },
    }


def _late_event_arm(record: dict, embeddings: dict[str, list[float]]) -> dict:
    react = reactivity(record)
    return {
        "name": record["name"],
        "session_id": record["session_id"],
        "jitter": record["jitter"],
        "events_to_adaptation": react["events_to_adaptation"],
        "refresh_count": len(record["snapshots"]),
        "post_pivot_refresh_count": react["post_pivot_refresh_count"],
        # Keyed by POSITION within the post-pivot refresh sequence, not by
        # absolute refresh index or event index. Jitter permutes delivery
        # order, so the n-th delivered event is not the same event across
        # arms and an event-indexed comparison would be comparing different
        # events to each other. "The n-th refresh after the pivot" is the
        # same question in every arm.
        "reactivity_curve": {i: p["jaccard"] for i, p in enumerate(react["curve"], start=1)},
        "coherence": metrics.coherence_series(record["profile_samples"], record["posts"], embeddings),
        "delivery": metrics.late_delivery_count(record["posts"]),
        "pivot_delivery_shift": record["pivot_delivery_shift"],
        "warm_path_precondition": record["warm_path_precondition"],
    }


def late_event(baseline_a: dict, baseline_b: dict, jittered: dict,
               embeddings: dict[str, list[float]]) -> dict:
    """Metric 7: does the silent drop Stage 15B proved exists show up
    downstream, above this pipeline's own run-to-run noise."""
    arms = {
        "baseline_a": _late_event_arm(baseline_a, embeddings),
        "baseline_b": _late_event_arm(baseline_b, embeddings),
        "jittered": _late_event_arm(jittered, embeddings),
    }
    verdict = metrics.late_event_verdict(arms["baseline_a"], arms["baseline_b"], arms["jittered"])
    return {
        "scenario": f"{PRE_PIVOT_GENRE}({REPLICATE_K}) -> {POST_PIVOT_GENRE}({POST_PIVOT_TRACKS})",
        "jitter_level": JITTER_LEVEL,
        "mechanism_reference": (
            "docs/platform/stage15b-simulator-jitter-and-late-event-audit.md -- an event delivered "
            "more than 5s of event time behind the watermark is silently and permanently dropped "
            "from the windowed profile (no allowedLateness, no side output). That was established "
            "live there; this metric only asks whether the drop is VISIBLE downstream."
        ),
        "verdict": verdict,
        "arms": {
            name: {k: v for k, v in arm.items() if k != "coherence"}
            for name, arm in arms.items()
        },
        "coherence_by_arm": {name: arm["coherence"] for name, arm in arms.items()},
    }


# --- raw artifacts ---


def split_raw_artifacts(records: list[dict]) -> tuple[list[dict], dict]:
    """Pulls the 512-dim profile vectors out of the scenario records into a
    side file. Kept verbatim, never rounded -- metric 6 compares outputs for
    exact equality, and a "just for the file size" rounding is exactly the
    kind of quiet transform that would make such a comparison meaningless.
    Keyed by repr(computed_at), which round-trips a Python float exactly."""
    vectors: dict[str, dict[str, list[float]]] = {}
    slim = []
    for record in records:
        vectors[record["session_id"]] = {
            repr(sample["computed_at"]): sample["vector"]
            for sample in record["profile_samples"] if sample.get("vector")
        }
        trimmed = dict(record)
        trimmed["profile_samples"] = [
            {k: v for k, v in sample.items() if k != "vector"}
            for sample in record["profile_samples"]
        ]
        slim.append(trimmed)
    return slim, vectors


def merge_raw_artifacts(records: list[dict], vectors: dict) -> list[dict]:
    for record in records:
        by_computed_at = vectors.get(record["session_id"], {})
        for sample in record["profile_samples"]:
            sample["vector"] = by_computed_at.get(repr(sample["computed_at"]))
    return records


# --- outputs ---


def write_figures(reactivity_results: list[dict], latency_results: dict) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(8, 4.5))
    plotted = 0
    longest = 0
    # Distinct markers/dashes because curves overlap exactly: K=3 and K=5
    # produce identical Jaccard series here, and a solid line would hide one
    # of them entirely.
    styles = [("o", "-"), ("s", "--"), ("^", "-."), ("D", ":")]
    for res in reactivity_results:
        if res["excluded"] or not res["curve"]:
            continue
        # Plotted against position WITHIN the post-pivot sequence, not the
        # absolute refresh index: the absolute index starts wherever that K's
        # pre-pivot refreshes happened to end, which staggers the four curves
        # apart and makes them impossible to compare by eye. The crossing
        # point reported everywhere else stays the absolute index.
        xs = list(range(1, len(res["curve"]) + 1))
        ys = [p["jaccard"] for p in res["curve"]]
        marker, dashes = styles[plotted % len(styles)]
        ax.plot(xs, ys, marker=marker, linestyle=dashes, alpha=0.85,
                label=f"K={res['k']} (crossed at refresh {res['events_to_adaptation']})")
        plotted += 1
        longest = max(longest, len(xs))
    ax.axhline(metrics.ADAPTATION_JACCARD_THRESHOLD, linestyle="--", color="grey",
               label=f"adaptation threshold ({metrics.ADAPTATION_JACCARD_THRESHOLD})")
    ax.set_xlabel("refreshes since the pivot")
    ax.set_xticks(range(1, max(longest, 1) + 1))
    ax.set_ylabel("Jaccard vs. last pre-pivot recommendation set")
    ax.set_title("Metric 1: reactivity after a chillout -> rock pivot")
    ax.set_ylim(0, 1.05)
    if plotted:
        ax.legend()
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "reactivity_curves.png", dpi=150)
    plt.close(fig)

    hops = [("H1 ingest POST", "h1_ingest_post_seconds"),
            ("H2 profile lag", "h2_profile_compute_lag_seconds"),
            ("H3 refresh compute", "h3_refresh_compute_seconds")]
    labels, p50s, p95s, p99s = [], [], [], []
    for label, key in hops:
        stats = latency_results[key]
        if stats.get("n"):
            labels.append(f"{label}\n(n={stats['n']})")
            p50s.append(stats["p50"])
            p95s.append(stats["p95"])
            p99s.append(stats["p99"])
    if labels:
        fig, ax = plt.subplots(figsize=(8, 4.5))
        x = range(len(labels))
        width = 0.27
        ax.bar([i - width for i in x], p50s, width, label="p50")
        ax.bar(list(x), p95s, width, label="p95")
        ax.bar([i + width for i in x], p99s, width, label="p99")
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels)
        ax.set_ylabel("seconds")
        ax.set_yscale("log")
        ax.set_title("Metric 2: latency per hop (wall-clock, log scale)")
        ax.legend()
        fig.tight_layout()
        fig.savefig(FIGURES_DIR / "latency_breakdown.png", dpi=150)
        plt.close(fig)


def write_coherence_figure(coherence_results: list[dict], late_event_results: dict | None) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    for res in coherence_results:
        series = [s for s in res["series"] if s["cos_recent"] is not None or s["cos_early"] is not None]
        if not series:
            continue
        xs = [s["session_elapsed_seconds"] / 60.0 for s in series]
        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.plot(xs, [s["cos_recent"] for s in series], marker="o", linestyle="-",
                label="cos(profile, last 5 min of session)")
        ax.plot(xs, [s["cos_early"] for s in series], marker="s", linestyle="--",
                label="cos(profile, first 5 min of session)")
        ax.axhline(0.0, color="grey", linewidth=0.8)
        ax.set_xlabel("session time elapsed (minutes, simulated clock)")
        ax.set_ylabel("cosine similarity")
        ax.set_title(f"Metric 4: semantic coherence over session time ({res['name']})")
        ax.legend()
        fig.tight_layout()
        fig.savefig(FIGURES_DIR / f"coherence_{res['name']}.png", dpi=150)
        plt.close(fig)

    if not late_event_results:
        return
    fig, ax = plt.subplots(figsize=(8, 4.5))
    styles = {"baseline_a": ("o", "-"), "baseline_b": ("s", "--"), "jittered": ("^", "-.")}
    plotted = False
    for name, series in late_event_results["coherence_by_arm"].items():
        pts = [(s["session_elapsed_seconds"] / 60.0, s["cos_recent"])
               for s in series if s["cos_recent"] is not None]
        if not pts:
            continue
        marker, dashes = styles.get(name, ("x", ":"))
        jitter = late_event_results["arms"][name]["jitter"]
        ax.plot([x for x, _ in pts], [y for _, y in pts], marker=marker, linestyle=dashes,
                alpha=0.85, label=f"{name} (jitter {jitter})")
        plotted = True
    ax.set_xlabel("session time elapsed (minutes, simulated clock)")
    ax.set_ylabel("cos(profile, last 5 min of session)")
    ax.set_title("Metric 7: two jitter-0 replicates vs. one jittered run")
    if plotted:
        ax.legend()
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "late_event_coherence.png", dpi=150)
    plt.close(fig)


def write_coverage_figure(coverage_results: list[dict]) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    plottable = [r for r in coverage_results if r["curve"]]
    if not plottable:
        return
    fig, ax = plt.subplots(figsize=(8, 4.5))
    for res in plottable:
        xs = [c["refresh_index"] for c in res["curve"]]
        ys = [c["cumulative_unique"] for c in res["curve"]]
        ax.plot(xs, ys, marker="o", linestyle="-",
                label=f"{res['name']} ({res['unique_recommended']}/{res['catalog_size']} tracks)")
        last_new = res["last_refresh_contributing_new_tracks"]
        if res["refreshes_after_last_new_track"] > 0:
            ax.axvline(last_new, linestyle=":", color="grey")
            ax.annotate("last new track", xy=(last_new, ys[-1]), xytext=(2, -12),
                        textcoords="offset points", fontsize=8, color="grey")
    ax.set_xlabel("refresh index")
    ax.set_ylabel("cumulative unique tracks recommended")
    ax.set_title("Metric 5: catalog coverage accumulation over a 40-track session")
    ax.legend()
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "coverage_curve.png", dpi=150)
    plt.close(fig)


def write_tables(reactivity_results: list[dict], latency_results: dict, cold_warm_results: dict,
                 coherence_results: list[dict], coverage_results: list[dict],
                 determinism_results: dict | None, late_event_results: dict | None,
                 nesting_note: str | None = None) -> None:
    lines = [
        "# eval/8_2 — tables (metrics 1-7)",
        "",
        "Generated by `python -m eval.8_2.run`. Metrics 1/2/3 are stage 15C, "
        "metrics 4/5/6/7 stage 15C.2.",
        "",
        "## Metric 1 — reactivity after a chillout → rock pivot",
        "",
        "| K (pre-pivot tracks) | pre-pivot refreshes | post-pivot refreshes | events to adaptation | excluded |",
        "|---|---|---|---|---|",
    ]
    for res in reactivity_results:
        adaptation = res["events_to_adaptation"]
        lines.append(
            f"| {res['k']} | {res['pre_pivot_refresh_count']} | {res['post_pivot_refresh_count']} | "
            f"{adaptation if adaptation is not None else 'never crossed'} | "
            f"{'yes — ' + '; '.join(res['exclusion_reasons']) if res['excluded'] else 'no'} |"
        )

    lines += [
        "",
        f"Adaptation threshold: Jaccard < {metrics.ADAPTATION_JACCARD_THRESHOLD} against the last "
        "pre-pivot recommendation set (pre-registered before the first run).",
        "",
        "> " + (nesting_note or ""),
        "",
        "## Metric 2 — latency per hop (wall-clock)",
        "",
        "| Hop | n | p50 (s) | p95 (s) | p99 (s) |",
        "|---|---|---|---|---|",
    ]
    for label, key in [("H1 ingest POST", "h1_ingest_post_seconds"),
                       ("H2 profile compute lag", "h2_profile_compute_lag_seconds"),
                       ("H3 refresh compute", "h3_refresh_compute_seconds"),
                       ("Event arrival → refresh", "arrival_to_refresh_seconds"),
                       ("Interval between refreshes", "inter_refresh_interval_seconds")]:
        s = latency_results[key]
        if not s.get("n"):
            lines.append(f"| {label} | 0 | — | — | — |")
            continue
        lines.append(f"| {label} | {s['n']} | {s['p50']:.4f} | {s['p95']:.4f} | {s['p99']:.4f} |")

    floor = latency_results["debounce_design_floor"]
    behind = latency_results["events_behind_each_refresh"]
    lines += [
        "",
        f"Debounce design floor (stated, not measured): refresh at most once per "
        f"{floor['min_interval_seconds']}s **or** once per {floor['min_events']} events. "
        + (f"Observed events behind each refresh: p50 {behind['p50']:.0f}, max {behind['max']:.0f}."
           if behind.get("n") else ""),
        "",
        "| Speed | n | events behind each refresh (p50 / max) | interval between refreshes (p50 s) |",
        "|---|---|---|---|",
    ]
    for speed_key, stats in latency_results["events_behind_each_refresh_by_speed"].items():
        gap = latency_results["inter_refresh_interval_seconds_by_speed"].get(speed_key, {})
        lines.append(
            f"| {speed_key} | {stats['n']} | {stats['p50']:.0f} / {stats['max']:.0f} | "
            + (f"{gap['p50']:.2f} |" if gap.get("n") else "— |")
        )
    lines += [
        "",
        "> " + latency_results["pooling_note"],
        "",
        "> " + latency_results["speed_caveat"],
        "",
        "## Metric 3 — cold-start → warm transition",
        "",
    ]
    if cold_warm_results.get("independence_caveat"):
        lines += ["> " + cold_warm_results["independence_caveat"], ""]
    lines += [
        "| Session | reached warm path | cold-start refreshes | seconds to first warm | handoff Jaccard | same ranking |",
        "|---|---|---|---|---|---|",
    ]
    for session_id, v in cold_warm_results["per_session"].items():
        secs = v["seconds_to_first_warm"]
        hj = v["handoff_jaccard"]
        rank = v.get("handoff_rank_identical")
        lines.append(
            f"| `{session_id}` | {'yes' if v['reached_warm_path'] else 'no'} | "
            f"{v['cold_start_refresh_count']} | {f'{secs:.1f}' if secs is not None else '—'} | "
            f"{f'{hj:.3f}' if hj is not None else '—'} | "
            f"{'—' if rank is None else ('yes' if rank else 'no')} |"
        )
    lines += [
        "",
        "> The handoff Jaccard is over SETS, as METRICS.md section 4 defines it. The last column "
        "guards against misreading a 1.000: a warm refresh can return the same ten tracks in a "
        "different order, which is a real change the set comparison cannot see.",
    ]

    # --- metric 4 ---
    lines += [
        "",
        "## Metric 4 — semantic coherence over session time",
        "",
        "| Scenario | speed | profile writes observed / resolvable | mean cos(recent) | mean cos(first 5 min) | samples closer to recent |",
        "|---|---|---|---|---|---|",
    ]
    for res in coherence_results:
        sm = res["summary"]
        if not sm.get("n"):
            lines.append(f"| `{res['name']}` | {res['speed']:.0f} | "
                         f"{res['distinct_profile_writes_observed']}/{res['watermark_advancing_events']} | — | — | — |")
            continue
        lines.append(
            f"| `{res['name']}` | {res['speed']:.0f} | "
            f"{res['distinct_profile_writes_observed']}/{res['watermark_advancing_events']} | "
            f"{sm['cos_recent_mean']:.4f} | {sm['cos_early_mean']:.4f} | "
            f"{sm['samples_closer_to_recent_than_early']}/{sm['n']} "
            f"({sm['fraction_closer_to_recent']:.0%}) |"
        )
    lines += [
        "",
        "> Both windows are 5 minutes of **session** time, the same span as the job's own sliding "
        "window. Each watermark-advancing event fires several 30s-slide windows milliseconds apart "
        "over one overwritten Redis key, so one vector per such event is the ceiling at any poll "
        "rate — the second column is measured against that ceiling, not against the analytic "
        "window-fire count.",
    ]

    # --- metric 5 ---
    lines += [
        "",
        "## Metric 5 — catalog coverage and attractor collapse",
        "",
        "| Scenario | tracks played | refreshes | unique tracks recommended | coverage of 411 | last refresh adding a new track | flat tail |",
        "|---|---|---|---|---|---|---|",
    ]
    for res in coverage_results:
        if not res["curve"]:
            lines.append(f"| `{res['name']}` | {res['tracks_played']} | 0 | — | — | — | — |")
            continue
        lines.append(
            f"| `{res['name']}` | {res['tracks_played']} | {res['refresh_count']} | "
            f"{res['unique_recommended']} | {res['coverage_fraction']:.2%} | "
            f"{res['last_refresh_contributing_new_tracks']} | "
            f"{res['refreshes_after_last_new_track']} refreshes ({res['flat_tail_fraction']:.0%}) |"
        )
    lines += [
        "",
        "> \"Attractor collapse\" is the flat tail: refreshes after the last one that contributed a "
        "track never seen before. A long tail means the session kept moving but the recommender "
        "kept returning the same pool.",
    ]

    # --- metric 6 ---
    lines += ["", "## Metric 6 — determinism", ""]
    if determinism_results is None:
        lines += ["Not computed this run (both determinism replicates are required).", ""]
    else:
        v = determinism_results["verdict"]
        lines += [
            f"**VERDICT: {'PASS' if v['pass'] else 'FAIL'}** — same seed, same `--speed`, "
            f"`--jitter 0`, two runs of `{determinism_results['scenario']}`.",
            "",
            "| Check | Observed | Required | Pass |",
            "|---|---|---|---|",
            f"| Refresh count | {v['refresh_count_check']['run_a']} vs "
            f"{v['refresh_count_check']['run_b']} | equal | "
            f"{'yes' if v['refresh_count_check']['pass'] else 'no'} |",
            f"| Snapshot identity | {v['snapshot_identity_check']['classification_counts']} over "
            f"{v['snapshot_identity_check']['refreshes_compared']} refreshes | all `identical` | "
            f"{'yes' if v['snapshot_identity_check']['pass'] else 'no'} |",
            f"| Max score delta | {v['score_delta_check']['observed_max_delta']:g} | exactly 0.0 | "
            f"{'yes' if v['score_delta_check']['pass'] else 'no'} |",
            "",
            "> The rule above was pre-registered in `metrics.determinism_verdict()` before either "
            "replicate ran. Score equality is required at exactly 0.0 because `eval/8_1`'s Stage "
            "15A.2 measured this scoring path's repeated-run noise floor at exactly 0.0 across "
            "41,100 matched pairs.",
        ]
        if v["snapshot_identity_check"]["diverging_refresh_indices"]:
            lines += [
                "",
                "Diverging refresh indices: "
                + ", ".join(str(i) for i in v["snapshot_identity_check"]["diverging_refresh_indices"]),
            ]

    # --- metric 7 ---
    lines += ["", "## Metric 7 — late-event handling", ""]
    if late_event_results is None:
        lines += ["Not computed this run (all three replicate arms are required).", ""]
    else:
        lines += [
            "| Arm | jitter | events delivered late (past the 5s bound) | max lateness (s) | refreshes | events to adaptation |",
            "|---|---|---|---|---|---|",
        ]
        for name, arm in late_event_results["arms"].items():
            d = arm["delivery"]
            lines.append(
                f"| `{name}` | {arm['jitter']} | {d['events_delivered_late']}/{d['events']} | "
                f"{d['max_lateness_seconds']:.1f} | {arm['refresh_count']} | "
                f"{arm['events_to_adaptation'] if arm['events_to_adaptation'] is not None else 'never crossed'} |"
            )
        lines += [
            "",
            "| Measure | baseline vs baseline (noise) | baseline vs jittered (effect) | above noise |",
            "|---|---|---|---|",
        ]
        for measure, m in late_event_results["verdict"]["measures"].items():
            def _fmt(x):
                return "—" if x is None else (f"{x:.4f}" if isinstance(x, float) else str(x))
            distinguishable = m["distinguishable_from_noise"]
            lines.append(
                f"| {measure} | {_fmt(m['baseline_vs_baseline'])} | {_fmt(m['baseline_vs_jittered'])} | "
                f"{'—' if distinguishable is None else ('yes' if distinguishable else 'no')} |"
            )
        lines += [
            "",
            "> " + late_event_results["verdict"]["rule"],
            "",
            "> The drop mechanism itself is not re-derived here — see "
            "`docs/platform/stage15b-simulator-jitter-and-late-event-audit.md`, which proved live "
            "that an event past the watermark bound is silently and permanently dropped. This "
            "metric only asks whether that drop is visible downstream.",
        ]

    lines.append("")
    (OUT_DIR / "tables.md").write_text("\n".join(lines))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--speed", type=float, default=60.0,
                        help="Wall-clock compression factor for the simulated session clock.")
    parser.add_argument("--k", type=int, nargs="*", default=None,
                        help="Override the K sweep (default: 3 5 8 12).")
    parser.add_argument("--run-id", default=None,
                        help="Scopes session ids to this invocation (default: a UTC timestamp). "
                             "See build_specs() for why reusing one is not safe.")
    parser.add_argument("--from-records", default=None,
                        help="Recompute every metric from a saved raw_scenario_records.json instead "
                             "of running live. The raw records ARE the measurement; everything else "
                             "is derived from them, so a changed metric definition doesn't need "
                             "another 5-minute run against the stack.")
    parser.add_argument("--skip-extra-scenarios", action="store_true",
                        help="Run only the metric 1/2/3 K sweep, skipping stage 15C.2's long "
                             "session and the three pivot replicates (metrics 4-7). Cuts the run "
                             "from roughly 18 minutes to 6.")
    parser.add_argument("--no-manage-flink", action="store_true",
                        help="Reuse an operator-submitted Flink job instead of cancelling/resubmitting.")
    args = parser.parse_args(argv)

    if args.k:
        global K_SWEEP
        K_SWEEP = args.k

    if args.from_records:
        records_path = Path(args.from_records)
        records = json.loads(records_path.read_text())
        vectors_path = records_path.with_name("raw_profile_vectors.json")
        embeddings_path = records_path.with_name("raw_embeddings.json")
        if vectors_path.exists():
            records = merge_raw_artifacts(records, json.loads(vectors_path.read_text()))
        embeddings = json.loads(embeddings_path.read_text()) if embeddings_path.exists() else {}
        if not embeddings:
            print(f"note: {embeddings_path.name} not found -- metric 4/7 coherence will be empty")
        run_id = records[0]["session_id"].rsplit("-", 1)[-1] if records else "unknown"
        write_outputs(records, run_id, embeddings, recomputed_from=args.from_records)
        return

    genres = sorted({PRE_PIVOT_GENRE, POST_PIVOT_GENRE, *(g for g, _ in LONG_SESSION_GENRES)})
    pg_conn = store_access.connect_postgres()
    try:
        ids_by_genre = {
            genre: scenario_gen.fetch_genre_track_ids(pg_conn, genre) for genre in genres
        }
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id, duration_sec FROM tracks")
            durations = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        pg_conn.close()

    run_id = args.run_id or dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")
    specs = build_specs(args.seed, args.speed, run_id)
    if not args.skip_extra_scenarios:
        specs += build_extra_specs(args.seed, args.speed, run_id)
    anchors = metrics.anchor_schedule(simulated_spans(specs, ids_by_genre, durations))
    milvus_collection = store_access.connect_milvus()
    redis_client = store_access.connect_redis()

    records: list[dict] = []
    embeddings: dict = {}
    try:
        with orchestration.uvicorn_service("semantic_api.main:app", "semantic-api") as semantic_api_url, \
             orchestration.uvicorn_service("event_ingestion.main:app", "event-ingestion") as ingestion_url, \
             orchestration.flink_session_profile_job(manage=not args.no_manage_flink):
            for spec, anchor in zip(specs, anchors):
                print(f"[{spec.name}] anchor={anchor.isoformat()}")
                records.append(run_scenario(spec, anchor, ids_by_genre, durations,
                                            ingestion_url, semantic_api_url,
                                            redis_client, milvus_collection))
        # Metric 4 needs an embedding for every track any scenario played.
        # Fetched here, inside the Milvus connection's lifetime, and saved
        # alongside the records so --from-records can recompute coherence
        # with no live stores at all.
        played = sorted({int(t) for r in records for t in r["track_ids_in_order"]})
        embeddings = store_access.fetch_embeddings(milvus_collection, played)
        missing = [t for t in played if str(t) not in embeddings]
        if missing:
            print(f"warning: no Milvus embedding for track(s) {missing} -- "
                  "metric 4's reference means will be computed without them")
    finally:
        store_access.disconnect_milvus()

    write_outputs(records, run_id, embeddings)


def write_outputs(records: list[dict], run_id: str, embeddings: dict[str, list[float]],
                  recomputed_from: str | None = None) -> None:
    # Raw artifacts first, before any metric is computed. A live run costs
    # ~18 minutes and the records ARE the measurement -- everything else is
    # derived from them (that is what --from-records exists for), so a crash
    # in a metric must not be able to destroy the run that produced it.
    if recomputed_from is None:
        slim, vectors = split_raw_artifacts(records)
        (OUT_DIR / "raw_scenario_records.json").write_text(json.dumps(slim, indent=2, sort_keys=True) + "\n")
        (OUT_DIR / "raw_profile_vectors.json").write_text(json.dumps(vectors, indent=2, sort_keys=True) + "\n")
        (OUT_DIR / "raw_embeddings.json").write_text(json.dumps(embeddings, indent=2, sort_keys=True) + "\n")

    by_role: dict[str, list[dict]] = {}
    by_name = {r["name"]: r for r in records}
    for record in records:
        by_role.setdefault(record.get("role", "pivot_sweep"), []).append(record)

    sweep = by_role.get("pivot_sweep", [])
    reactivity_results = [reactivity(r) for r in sweep]
    # Latency and the cold->warm race pool across EVERY scenario in the pack,
    # per METRICS.md sections 3 and 4 -- more hops and more handoffs, and the
    # long session's slower speed is a different operating point for the same
    # hops rather than a different measurement.
    latency_results = latency(records)
    cold_warm_results = cold_warm(records)

    coherence_targets = by_role.get("long_session", [])
    coherence_results = [coherence(r, embeddings) for r in coherence_targets]
    coverage_results = [coverage(r) for r in coherence_targets]

    determinism_results = None
    if all(name in by_name for name in DETERMINISM_RUNS):
        determinism_results = determinism(by_name[DETERMINISM_RUNS[0]], by_name[DETERMINISM_RUNS[1]])

    late_event_results = None
    if all(name in by_name for name in (*DETERMINISM_RUNS, LATE_EVENT_RUN)):
        late_event_results = late_event(by_name[DETERMINISM_RUNS[0]], by_name[DETERMINISM_RUNS[1]],
                                        by_name[LATE_EVENT_RUN], embeddings)

    # Restricted to the K sweep: the long session and the replicates are not
    # part of the sweep, and keying them by their first genre's track count
    # would collide (all three replicates share K=8).
    pre_pivot_sets = {
        r["genre_track_counts"][0][1]: r["track_ids_in_order"][: r["genre_track_counts"][0][1]]
        for r in sweep
    }
    ordered_ks = sorted(pre_pivot_sets)
    nested = all(
        pre_pivot_sets[a] == pre_pivot_sets[b][: len(pre_pivot_sets[a])]
        for a, b in zip(ordered_ks, ordered_ks[1:])
    )

    nesting_note = (
            "METRICS.md's 'Resolved before handoff' section states that each K independently "
            "reseeds sample() so the pre-pivot track sets are NOT nested. Measured here, they are: "
            "random.Random(seed).sample(pool, k) draws sequentially from the same seeded stream, so "
            "a larger k extends the smaller k's draw. This is arguably the better design for the "
            "metric (only pre-pivot LENGTH varies, the opening sequence is held fixed) but the "
            "spec's stated reason for accepting the sweep was factually wrong, and the K runs are "
            "therefore not independent samples."
            if nested else
        "Pre-pivot track sets are not nested across K, as METRICS.md assumed."
    )

    (OUT_DIR / "reactivity.json").write_text(json.dumps({
        "metric": "1 -- reactivity after a hard genre pivot",
        "pre_pivot_tracks_per_k": pre_pivot_sets,
        "pre_pivot_sets_nested": nested,
        "nesting_note": nesting_note,
        "scenario": f"{PRE_PIVOT_GENRE}(K) -> {POST_PIVOT_GENRE}({POST_PIVOT_TRACKS})",
        "adaptation_threshold": metrics.ADAPTATION_JACCARD_THRESHOLD,
        "k_sweep": K_SWEEP,
        "run_id": run_id,
        "per_k": reactivity_results,
    }, indent=2, sort_keys=True) + "\n")

    (OUT_DIR / "latency.json").write_text(json.dumps(latency_results, indent=2, sort_keys=True) + "\n")

    (OUT_DIR / "coherence.json").write_text(json.dumps({
        "metric": "4 -- semantic coherence over session time",
        "window_seconds": metrics.COHERENCE_WINDOW_SECONDS,
        "speed": LONG_SESSION_SPEED,
        "speed_rationale": (
            "Chosen by measurement, per METRICS.md section 5: on a 12-event probe at a 0.5s poll "
            "interval, speed 60 resolved 5 of 6 distinct profile writes and speed 30 resolved 6 of "
            "6. The ceiling is one vector per watermark-advancing event -- each such event fires "
            "several 30s-slide windows milliseconds apart over one overwritten Redis key."
        ),
        "run_id": run_id,
        "per_scenario": coherence_results,
    }, indent=2, sort_keys=True) + "\n")

    (OUT_DIR / "coverage.json").write_text(json.dumps({
        "metric": "5 -- catalog coverage and attractor collapse",
        "catalog_size": metrics.CATALOG_SIZE,
        "scenario": " -> ".join(f"{g}({n})" for g, n in LONG_SESSION_GENRES),
        "run_id": run_id,
        "per_scenario": coverage_results,
    }, indent=2, sort_keys=True) + "\n")

    (OUT_DIR / "results.json").write_text(json.dumps({
        "stage": "15C.2",
        "run_id": run_id,
        "generated_for": "8.2 (streaming, reactive)",
        "cold_warm_transition": cold_warm_results,
        "recomputed_from_records": recomputed_from,
        "determinism_check": determinism_results or {
            "not_computed": f"requires both replicate runs {DETERMINISM_RUNS}"},
        "late_event_effect": late_event_results or {
            "not_computed": f"requires runs {(*DETERMINISM_RUNS, LATE_EVENT_RUN)}"},
    }, indent=2, sort_keys=True) + "\n")

    write_figures(reactivity_results, latency_results)
    write_coherence_figure(coherence_results, late_event_results)
    write_coverage_figure(coverage_results)
    write_tables(reactivity_results, latency_results, cold_warm_results, coherence_results,
                 coverage_results, determinism_results, late_event_results,
                 nesting_note=nesting_note)

    print("\nWrote reactivity.json, latency.json, coherence.json, coverage.json, results.json, "
          "raw_*.json, tables.md, figures/*.png")
    for res in reactivity_results:
        status = "EXCLUDED" if res["excluded"] else f"events_to_adaptation={res['events_to_adaptation']}"
        print(f"  K={res['k']}: {status} "
              f"({res['pre_pivot_refresh_count']} pre / {res['post_pivot_refresh_count']} post refreshes)")
    for res in coverage_results:
        print(f"  {res['name']}: coverage {res['coverage_fraction']:.2%} "
              f"({res['unique_recommended']}/{res['catalog_size']}), "
              f"flat tail {res['refreshes_after_last_new_track']}/{res['refresh_count']} refreshes")
    for res in coherence_results:
        sm = res["summary"]
        if sm.get("n"):
            print(f"  {res['name']}: coherence mean cos(recent)={sm['cos_recent_mean']:.4f} "
                  f"vs cos(first 5 min)={sm['cos_early_mean']:.4f} over {sm['n']} samples")
    if determinism_results:
        print(f"  determinism: {'PASS' if determinism_results['verdict']['pass'] else 'FAIL'} "
              f"({determinism_results['verdict']['snapshot_identity_check']['classification_counts']})")
    if late_event_results:
        distinguishable = late_event_results["verdict"]["measures_distinguishable"]
        print(f"  late event: measures above run-to-run noise: {distinguishable or 'none'}")

    errors = [e for r in records for e in r["errors"]]
    if errors:
        print(f"\n{len(errors)} error(s) recorded during the run:")
        for e in errors:
            print(f"  {e}")


if __name__ == "__main__":
    main()
