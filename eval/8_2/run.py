"""
eval/8_2 -- 8.2 (streaming, reactive) evaluation pack. Stage 15C.

An ACTIVE harness, unlike eval/8_1: it starts the services, submits the
Flink job, drives the seeded simulator's event stream, runs both consumer
groups, and measures the live system while a scenario plays out. See
METRICS.md (signed off 2026-09-03) for every definition, and README.md for
the runbook.

This stage covers metrics 1 (reactivity), 2 (latency) and 3 (cold->warm
transition) -- all three come off the same pivot scenario. Metrics 4/5/6/7
(coherence, coverage, determinism, late-event) are stage 15C.2.

No accuracy metric anywhere in this pack: no ground truth exists for this
dataset, so nothing here is precision@k, recall@k or NDCG.

Usage:
    platform/enrichment/.venv/bin/python -m eval.8_2.run

Writes:
    eval/8_2/reactivity.json           metric 1, per-K curves
    eval/8_2/latency.json              metric 2, NOT a deterministic artifact
    eval/8_2/results.json              metric 3 (+ markers for 15C.2's metrics)
    eval/8_2/raw_scenario_records.json  everything observed, for debugging/15C.2
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
            session_id=f"eval82-pivot-K{k}-{run_id}",
            genre_track_counts=[(PRE_PIVOT_GENRE, k), (POST_PIVOT_GENRE, POST_PIVOT_TRACKS)],
            seed=seed,
            speed=speed,
            pivot_track_index=k,
        )
        for k in K_SWEEP
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
    for r in records:
        last_refresh_wall = None
        since = 0
        for x in r["refresh_results"]:
            since += 1
            if not x.get("refreshed"):
                continue
            if last_refresh_wall is not None:
                inter_refresh.append(x["wall_clock"] - last_refresh_wall)
                events_behind.append(since)
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
    return {
        "independent_observations": distinct_seeds,
        "independence_caveat": (
            None if distinct_seeds >= len(records) else
            f"{len(records)} sessions but only {distinct_seeds} distinct cold-start seed track(s): "
            "random.Random(seed).sample() returns prefix-nested draws for increasing K, so every K "
            "in the sweep opens on the same tracks and the handoff numbers below are effectively "
            "ONE observation repeated, not several independent ones. Do not read the mean as an "
            "average over independent runs."
        ),
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


def write_tables(reactivity_results: list[dict], latency_results: dict, cold_warm_results: dict,
                 nesting_note: str | None = None) -> None:
    lines = [
        "# eval/8_2 — tables (stage 15C: metrics 1, 2, 3)",
        "",
        "Generated by `python -m eval.8_2.run`. Metrics 4/5/6/7 are stage 15C.2.",
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
        "> " + latency_results["speed_caveat"],
        "",
        "## Metric 3 — cold-start → warm transition",
        "",
    ]
    if cold_warm_results.get("independence_caveat"):
        lines += ["> " + cold_warm_results["independence_caveat"], ""]
    lines += [
        "| Session | reached warm path | cold-start refreshes | seconds to first warm | handoff Jaccard |",
        "|---|---|---|---|---|",
    ]
    for session_id, v in cold_warm_results["per_session"].items():
        secs = v["seconds_to_first_warm"]
        hj = v["handoff_jaccard"]
        lines.append(
            f"| `{session_id}` | {'yes' if v['reached_warm_path'] else 'no'} | "
            f"{v['cold_start_refresh_count']} | {f'{secs:.1f}' if secs is not None else '—'} | "
            f"{f'{hj:.3f}' if hj is not None else '—'} |"
        )
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
    parser.add_argument("--no-manage-flink", action="store_true",
                        help="Reuse an operator-submitted Flink job instead of cancelling/resubmitting.")
    args = parser.parse_args(argv)

    if args.k:
        global K_SWEEP
        K_SWEEP = args.k

    if args.from_records:
        records = json.loads(Path(args.from_records).read_text())
        run_id = records[0]["session_id"].rsplit("-", 1)[-1] if records else "unknown"
        write_outputs(records, run_id, recomputed_from=args.from_records)
        return

    pg_conn = store_access.connect_postgres()
    try:
        ids_by_genre = {
            genre: scenario_gen.fetch_genre_track_ids(pg_conn, genre)
            for genre in (PRE_PIVOT_GENRE, POST_PIVOT_GENRE)
        }
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id, duration_sec FROM tracks")
            durations = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        pg_conn.close()

    run_id = args.run_id or dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")
    specs = build_specs(args.seed, args.speed, run_id)
    anchors = metrics.anchor_schedule(simulated_spans(specs, ids_by_genre, durations))
    milvus_collection = store_access.connect_milvus()
    redis_client = store_access.connect_redis()

    records: list[dict] = []
    try:
        with orchestration.uvicorn_service("semantic_api.main:app", "semantic-api") as semantic_api_url, \
             orchestration.uvicorn_service("event_ingestion.main:app", "event-ingestion") as ingestion_url, \
             orchestration.flink_session_profile_job(manage=not args.no_manage_flink):
            for spec, anchor in zip(specs, anchors):
                print(f"[{spec.name}] anchor={anchor.isoformat()}")
                records.append(run_scenario(spec, anchor, ids_by_genre, durations,
                                            ingestion_url, semantic_api_url,
                                            redis_client, milvus_collection))
    finally:
        store_access.disconnect_milvus()

    write_outputs(records, run_id)


def write_outputs(records: list[dict], run_id: str, recomputed_from: str | None = None) -> None:
    reactivity_results = [reactivity(r) for r in records]
    latency_results = latency(records)
    cold_warm_results = cold_warm(records)

    pre_pivot_sets = {
        r["genre_track_counts"][0][1]: r["track_ids_in_order"][: r["genre_track_counts"][0][1]]
        for r in records
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

    (OUT_DIR / "results.json").write_text(json.dumps({
        "stage": "15C",
        "run_id": run_id,
        "generated_for": "8.2 (streaming, reactive)",
        "cold_warm_transition": cold_warm_results,
        "recomputed_from_records": recomputed_from,
        "determinism_check": {"pending": "15C.2 (METRICS.md section 7)"},
        "late_event_effect": {"pending": "15C.2 (METRICS.md section 8)"},
    }, indent=2, sort_keys=True) + "\n")

    if recomputed_from is None:
        (OUT_DIR / "raw_scenario_records.json").write_text(json.dumps(records, indent=2, sort_keys=True) + "\n")

    write_figures(reactivity_results, latency_results)
    write_tables(reactivity_results, latency_results, cold_warm_results, nesting_note=nesting_note)

    print("\nWrote reactivity.json, latency.json, results.json, raw_scenario_records.json, "
          "tables.md, figures/*.png")
    for res in reactivity_results:
        status = "EXCLUDED" if res["excluded"] else f"events_to_adaptation={res['events_to_adaptation']}"
        print(f"  K={res['k']}: {status} "
              f"({res['pre_pivot_refresh_count']} pre / {res['post_pivot_refresh_count']} post refreshes)")
    errors = [e for r in records for e in r["errors"]]
    if errors:
        print(f"\n{len(errors)} error(s) recorded during the run:")
        for e in errors:
            print(f"  {e}")


if __name__ == "__main__":
    main()
