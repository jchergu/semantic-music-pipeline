"""
Pure metric computations for eval/8_2 -- no I/O, no live services, so every
definition in METRICS.md is unit-testable against synthetic input rather
than only observable at the end of a 10-minute live run. Same split
eval/8_1/metrics.py uses.

Thresholds and rules here are PRE-REGISTERED: they were committed before
the first live run produced a number, and are not to be retuned to make a
curve cross. If the pivot never turns the recommendation set over,
events_to_adaptation is None and that is the finding.
"""
from __future__ import annotations

import datetime as dt
import math

# Metric 1: a post-pivot snapshot counts as "adapted" once it shares less
# than this fraction of its union with the last pre-pivot snapshot.
ADAPTATION_JACCARD_THRESHOLD = 0.3

# Anchor scheduling (see docs/platform/stage15c-eval-8_2-harness.md): the
# Flink job's watermark is stream-wide, not per session key, so scenarios
# must advance monotonically through event time. 300s is both the gap left
# between consecutive scenarios and the alignment quantum -- it equals the
# job's SlidingEventTimeWindows size (5 min) and is a whole multiple of its
# 30s slide, so shifting a scenario by a multiple of it moves every window
# boundary identically and leaves window/event alignment unchanged.
SCENARIO_GAP_SECONDS = 300
WINDOW_ALIGNMENT_SECONDS = 300

EVAL_EPOCH = dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc)


def jaccard(a: set, b: set) -> float:
    """|a ∩ b| / |a ∪ b|. Two empty sets are treated as identical (1.0)
    rather than 0/0 -- the caller only ever compares recommendation sets,
    and "both empty" is genuinely no turnover."""
    if not a and not b:
        return 1.0
    return len(a & b) / len(a | b)


def events_to_adaptation(
    curve: list[tuple[int, float]], threshold: float = ADAPTATION_JACCARD_THRESHOLD
) -> int | None:
    """curve: [(refresh_index, jaccard_vs_pre_pivot), ...] for post-pivot
    refreshes, in order. Returns the refresh_index of the first entry below
    `threshold`, or None if the scenario ended without crossing."""
    for refresh_index, value in curve:
        if value < threshold:
            return refresh_index
    return None


def percentiles(values: list[float], ps: tuple[int, ...] = (50, 95, 99)) -> dict:
    """Nearest-rank percentiles over a small sample. No interpolation:
    every reported number is an actually-observed measurement, which is the
    honest choice at the sample sizes this harness produces (tens, not
    thousands). `n` is reported alongside so the p99 of a 12-sample set is
    readable as what it is."""
    if not values:
        return {"n": 0}
    ordered = sorted(values)
    out: dict = {"n": len(ordered), "min": ordered[0], "max": ordered[-1]}
    out["mean"] = sum(ordered) / len(ordered)
    for p in ps:
        rank = max(1, math.ceil(p / 100 * len(ordered)))
        out[f"p{p}"] = ordered[rank - 1]
    return out


def cold_warm_transition(refresh_log: list[dict], session_start_wall: float) -> dict:
    """Metric 3, over one session's refresh log (oldest first). Each entry:
    {"refresh_index", "action", "wall_clock", "track_ids"}.

    The cold->warm handoff is not gated by an event count -- it is a race
    between two independent consumer groups (Flink's, which writes
    session:{id}:profile, and the refresh loop's, which reads it). So what
    is reported is the race's observed outcome: when the first warm refresh
    landed, whether a cold-start refresh immediately preceded it, and how
    much the recommendation set visibly jumped at the handoff.
    """
    real = [r for r in refresh_log if r["action"] in ("cold_start", "warm")]
    first_warm_pos = next((i for i, r in enumerate(real) if r["action"] == "warm"), None)

    if first_warm_pos is None:
        return {
            "reached_warm_path": False,
            "cold_start_refresh_count": sum(1 for r in real if r["action"] == "cold_start"),
            "seconds_to_first_warm": None,
            "preceded_by_cold_start": None,
            "handoff_jaccard": None,
        }

    first_warm = real[first_warm_pos]
    preceding = real[first_warm_pos - 1] if first_warm_pos > 0 else None
    preceded_by_cold_start = preceding is not None and preceding["action"] == "cold_start"

    handoff_jaccard = None
    handoff_rank_identical = None
    if preceded_by_cold_start:
        handoff_jaccard = jaccard(set(preceding["track_ids"]), set(first_warm["track_ids"]))
        # METRICS.md section 4 defines the handoff discontinuity as a Jaccard
        # over SETS, and that definition stands. But a Jaccard of 1.0 can
        # still hide a real change -- observed here, where the warm path
        # returned the same ten tracks and moved the top one to position six.
        # Reported as a sibling field so the 1.0 cannot be read as "the
        # handoff changed nothing", without redefining the signed-off metric.
        handoff_rank_identical = list(preceding["track_ids"]) == list(first_warm["track_ids"])

    return {
        "reached_warm_path": True,
        "cold_start_refresh_count": sum(1 for r in real if r["action"] == "cold_start"),
        "seconds_to_first_warm": first_warm["wall_clock"] - session_start_wall,
        "first_warm_refresh_index": first_warm["refresh_index"],
        "preceded_by_cold_start": preceded_by_cold_start,
        "handoff_jaccard": handoff_jaccard,
        "handoff_rank_identical": handoff_rank_identical,
    }


def align_up(seconds: float, quantum: int = WINDOW_ALIGNMENT_SECONDS) -> int:
    return int(math.ceil(seconds / quantum) * quantum)


def anchor_schedule(
    span_seconds: list[float],
    epoch: dt.datetime = EVAL_EPOCH,
    gap_seconds: int = SCENARIO_GAP_SECONDS,
    quantum: int = WINDOW_ALIGNMENT_SECONDS,
) -> list[dt.datetime]:
    """Event-time anchors for a sequence of scenarios of the given simulated
    spans, run back to back against one Flink job.

    Two properties, both required (see this module's constants):
      - strictly increasing, each anchor at least `gap_seconds` past the
        previous scenario's last event -- otherwise a scenario's events
        arrive behind a watermark its predecessor already advanced, and
        Flink drops them silently (Stage 15B's finding, self-inflicted).
      - every anchor an exact multiple of `quantum` from `epoch`, so window
        boundaries fall identically relative to each scenario's own events.
    """
    anchors: list[dt.datetime] = []
    offset = 0
    for span in span_seconds:
        anchors.append(epoch + dt.timedelta(seconds=offset))
        offset = align_up(offset + span + gap_seconds, quantum)
    return anchors


# --- Stage 15C.2: metrics 4, 5, 6, 7 ---

# Metric 4: the "last N minutes of session time" comparison window. Equal to
# the Flink job's own SlidingEventTimeWindows size (5 min) on purpose -- the
# question metric 4 asks is whether the centroid tracks its window's contents
# and lets older tracks decay out, so the reference window has to be the same
# one the job is actually computing over.
COHERENCE_WINDOW_SECONDS = 300

CATALOG_SIZE = 411  # the seed dataset, fixed since stage 2 (CLAUDE.md)


def cosine(a: list[float], b: list[float]) -> float | None:
    """None rather than 0.0 for a zero-norm vector: "these are orthogonal"
    and "one of these carries no information" are different findings, and a
    silent 0.0 would report the second as the first."""
    if len(a) != len(b):
        raise ValueError(f"dimension mismatch: {len(a)} vs {len(b)}")
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0.0 or nb == 0.0:
        return None
    return dot / (na * nb)


def mean_vector(vectors: list[list[float]]) -> list[float] | None:
    """Unweighted componentwise mean. Deliberately unweighted: the session
    profile Flink writes IS weighted and recency-decayed, so weighting this
    reference the same way would compare the decay against itself and could
    only ever agree."""
    if not vectors:
        return None
    dim = len(vectors[0])
    if any(len(v) != dim for v in vectors):
        raise ValueError("ragged vectors")
    return [sum(v[i] for v in vectors) / len(vectors) for i in range(dim)]


def _event_time_epoch(post: dict) -> float:
    return dt.datetime.fromisoformat(post["event_time"]).timestamp()


def coherence_series(
    samples: list[dict],
    posts: list[dict],
    embeddings_by_track: dict[str, list[float]],
    window_seconds: int = COHERENCE_WINDOW_SECONDS,
) -> list[dict]:
    """Metric 4. For every distinct session profile vector observed, the
    cosine against (a) the mean embedding of tracks played in the last
    `window_seconds` of SESSION time and (b) the mean embedding of tracks
    played in the session's first `window_seconds`.

    `samples`: [{"computed_at": wall-clock float, "vector": [...]}, ...].
    `posts`:   [{"wall_clock": float, "event_time": ISO str, "track_id": str}, ...].

    Session time, not wall-clock, defines both windows -- `--speed` decouples
    the two, so a wall-clock window would mean something different at every
    speed. A sample is placed in session time by the last event posted at or
    before Flink computed it: that event is the newest information the window
    could possibly have contained.

    Emits nothing for a sample that predates the first post (no session-time
    position exists for it yet), and leaves a cosine None rather than
    guessing when its side of the comparison has no embeddable track.
    """
    if not posts:
        return []
    times = [(_event_time_epoch(p), p) for p in posts]
    session_start = min(t for t, _ in times)
    early_cutoff = session_start + window_seconds
    early_ids = sorted({p["track_id"] for t, p in times if t <= early_cutoff})
    early_mean = mean_vector([embeddings_by_track[t] for t in early_ids if t in embeddings_by_track])

    out: list[dict] = []
    for sample in samples:
        computed_at = sample["computed_at"]
        sent = [(t, p) for t, p in times if p["wall_clock"] <= computed_at]
        if not sent:
            continue
        now_session = max(t for t, _ in sent)
        recent_ids = sorted({p["track_id"] for t, p in sent if t > now_session - window_seconds})
        recent_mean = mean_vector([embeddings_by_track[t] for t in recent_ids if t in embeddings_by_track])
        vector = sample["vector"]
        out.append({
            "computed_at": computed_at,
            "session_elapsed_seconds": now_session - session_start,
            "n_events_in_profile": sample.get("n_events"),
            "recent_track_ids": recent_ids,
            "early_track_ids": early_ids,
            "cos_recent": cosine(vector, recent_mean) if recent_mean else None,
            "cos_early": cosine(vector, early_mean) if early_mean else None,
        })
    return out


def coherence_summary(series: list[dict]) -> dict:
    """Reduces metric 4's two series to the claim they are evidence for:
    does the profile stay closer to what is playing NOW than to how the
    session opened. Reported as an observed rate, not a pass/fail -- there
    is no pre-registered threshold for this one because METRICS.md defines
    metric 4 as "plot both series", not as a hypothesis test."""
    pairs = [(s["cos_recent"], s["cos_early"]) for s in series
             if s["cos_recent"] is not None and s["cos_early"] is not None]
    if not pairs:
        return {"n": 0}
    recent = [r for r, _ in pairs]
    early = [e for _, e in pairs]
    closer = sum(1 for r, e in pairs if r > e)
    return {
        "n": len(pairs),
        "cos_recent_mean": sum(recent) / len(recent),
        "cos_early_mean": sum(early) / len(early),
        "cos_recent_first": recent[0],
        "cos_recent_last": recent[-1],
        "cos_early_first": early[0],
        "cos_early_last": early[-1],
        "samples_closer_to_recent_than_early": closer,
        "fraction_closer_to_recent": closer / len(pairs),
    }


def cumulative_unique_curve(snapshots: list[dict]) -> list[dict]:
    """Metric 5's curve: unique recommended track ids accumulated over the
    session's refreshes, in refresh order."""
    seen: set[str] = set()
    curve = []
    for snap in snapshots:
        before = len(seen)
        seen.update(snap["track_ids"])
        curve.append({
            "refresh_index": snap["refresh_index"],
            "cumulative_unique": len(seen),
            "new_this_refresh": len(seen) - before,
        })
    return curve


def catalog_coverage(snapshots: list[dict], catalog_size: int = CATALOG_SIZE) -> dict:
    """Metric 5. Coverage fraction plus the shape of how it got there.

    "Attractor collapse" -- the failure METRICS.md anticipates -- is a curve
    that flattens well before the session ends, i.e. the recommender keeps
    returning the same pool no matter how the session moves. Reported as the
    last refresh that contributed anything new and the share of refreshes
    after it, so the flattening is a number, not an impression from a plot.
    """
    curve = cumulative_unique_curve(snapshots)
    if not curve:
        return {"refresh_count": 0, "unique_recommended": 0, "catalog_size": catalog_size,
                "coverage_fraction": 0.0, "curve": []}
    unique = curve[-1]["cumulative_unique"]
    contributing = [c for c in curve if c["new_this_refresh"] > 0]
    last_new_pos = max(i for i, c in enumerate(curve) if c["new_this_refresh"] > 0)
    return {
        "refresh_count": len(curve),
        "unique_recommended": unique,
        "catalog_size": catalog_size,
        "coverage_fraction": unique / catalog_size,
        "refreshes_contributing_new_tracks": len(contributing),
        "last_refresh_contributing_new_tracks": curve[last_new_pos]["refresh_index"],
        "refreshes_after_last_new_track": len(curve) - last_new_pos - 1,
        "flat_tail_fraction": (len(curve) - last_new_pos - 1) / len(curve),
        "curve": curve,
    }


def snapshots_as_ranked_rows(snapshots: list[dict]) -> dict[int, list[dict]]:
    """Adapts a run's recommendation snapshots to the {key: ranked rows}
    shape eval/8_1/diff_runs.py::compare_runs() already consumes, keyed by
    refresh index. METRICS.md section 7 asks for that tool's methodology
    rather than a second differ, and its per-key identical/order_only/
    membership_changed classification is exactly the question here -- the
    only change is that a "seed" there is a refresh here."""
    return {
        snap["refresh_index"]: [
            {"recommended_track_id": int(row["track_id"]), "score": float(row["score"])}
            for row in snap["recs"]
        ]
        for snap in snapshots
    }


def determinism_verdict(refresh_counts: tuple[int, int], comparison: dict, max_score_delta: float) -> dict:
    """Metric 6's PRE-REGISTERED rule, fixed here before either replicate
    ran, so it cannot be relaxed to make a run pass.

    PASS iff, between two content-identical runs (same seed, same --speed,
    --jitter 0):
      - both produced the same number of refreshes, AND
      - every refresh index compared classifies as `identical` (same track
        ids in the same order), AND
      - the maximum score delta across matched track ids is exactly 0.0.

    Score equality is required at exactly 0.0, not within a tolerance:
    eval/8_1's Stage 15A.2 measured the repeated-run noise floor of this
    scoring path at exactly 0.0 across 41,100 matched pairs, so any nonzero
    drift here is a new effect that 8.1's evidence does not cover and must
    not be absorbed by a tolerance invented after the fact.

    A FAIL is a legitimate result to report, not a defect to hide: the
    refresh debounce is wall-clock while --speed compresses only session
    time, and the profile the warm path reads is written by a separately
    scheduled consumer group, so nothing in the design guarantees that the
    same seed selects the same refresh points.
    """
    counts_match = refresh_counts[0] == refresh_counts[1]
    classifications = comparison["classification_counts"]
    all_identical = classifications["order_only"] == 0 and classifications["membership_changed"] == 0
    delta_zero = max_score_delta == 0.0
    return {
        "pass": bool(counts_match and all_identical and delta_zero),
        "refresh_count_check": {
            "run_a": refresh_counts[0], "run_b": refresh_counts[1], "pass": counts_match,
        },
        "snapshot_identity_check": {
            "classification_counts": classifications,
            "refreshes_compared": comparison["total_seeds"],
            "diverging_refresh_indices": comparison["affected_seed_ids"],
            "pass": all_identical,
        },
        "score_delta_check": {"observed_max_delta": max_score_delta, "required": 0.0, "pass": delta_zero},
    }


def _curve_by_event_index(record_reactivity: dict) -> dict[int, float]:
    return {p["event_index"]: p["jaccard"] for p in record_reactivity["curve"]}


def _coherence_by_elapsed(series: list[dict]) -> list[tuple[float, float]]:
    return [(s["session_elapsed_seconds"], s["cos_recent"]) for s in series if s["cos_recent"] is not None]


def _max_aligned_delta(a: list[tuple[float, float]], b: list[tuple[float, float]]) -> float | None:
    """Largest |Δ| between two (session_elapsed, value) series, compared at
    the session-time positions they share. Session elapsed time is the only
    axis comparable across two runs -- sample counts and wall-clock times
    differ because window fires are not under the harness's control."""
    b_by_t = {t: v for t, v in b}
    common = [(v, b_by_t[t]) for t, v in a if t in b_by_t]
    if not common:
        return None
    return max(abs(x - y) for x, y in common)


def late_event_verdict(baseline_a: dict, baseline_b: dict, treatment: dict) -> dict:
    """Metric 7's PRE-REGISTERED rule, fixed before any of the three runs.

    Stage 15B established the *mechanism* live (a jittered event beyond the
    5s watermark bound is silently and permanently dropped from the windowed
    profile). Metric 7 asks the different question of whether that drop is
    VISIBLE downstream. A bare jitter-0 vs jitter-1.0 delta cannot answer
    it, because this pipeline is not run-to-run identical to begin with --
    so the comparison needs its own noise floor, exactly as Stage 15A.2's
    ANN validation did.

    Two content-identical jitter-0 replicates supply it. The late-event
    effect is reported DISTINGUISHABLE only where the baseline-vs-treatment
    difference strictly exceeds the baseline-vs-baseline difference on the
    same measure. Anything at or below that is reported as within run-to-run
    noise, which is a real finding too: METRICS.md section 8 explicitly
    admits a null result, since a dropped event need not have a visible
    effect at every window/K combination.

    Each argument: {"events_to_adaptation", "refresh_count",
    "reactivity_curve" (dict event_index -> jaccard), "coherence"}.
    """
    def _adaptation_delta(x: dict, y: dict) -> int | None:
        if x["events_to_adaptation"] is None or y["events_to_adaptation"] is None:
            return None
        return abs(x["events_to_adaptation"] - y["events_to_adaptation"])

    def _jaccard_delta(x: dict, y: dict) -> float | None:
        common = set(x["reactivity_curve"]) & set(y["reactivity_curve"])
        if not common:
            return None
        return max(abs(x["reactivity_curve"][i] - y["reactivity_curve"][i]) for i in common)

    measures = {}
    for name, fn in [
        ("events_to_adaptation", _adaptation_delta),
        ("refresh_count", lambda x, y: abs(x["refresh_count"] - y["refresh_count"])),
        ("max_reactivity_jaccard_delta", _jaccard_delta),
        ("max_coherence_cos_recent_delta",
         lambda x, y: _max_aligned_delta(_coherence_by_elapsed(x["coherence"]),
                                         _coherence_by_elapsed(y["coherence"]))),
    ]:
        noise = fn(baseline_a, baseline_b)
        effect = fn(baseline_a, treatment)
        distinguishable = (
            None if noise is None or effect is None else bool(effect > noise)
        )
        measures[name] = {
            "baseline_vs_baseline": noise,
            "baseline_vs_jittered": effect,
            "distinguishable_from_noise": distinguishable,
        }

    decided = [m["distinguishable_from_noise"] for m in measures.values()
               if m["distinguishable_from_noise"] is not None]
    return {
        "rule": (
            "An effect counts as real only where the jitter-0 vs jitter-1.0 difference strictly "
            "exceeds the difference between two content-identical jitter-0 replicates on the same "
            "measure. Pre-registered before any of the three runs."
        ),
        "measures": measures,
        "any_measure_distinguishable": bool(any(decided)) if decided else None,
        "measures_distinguishable": sorted(k for k, v in measures.items()
                                           if v["distinguishable_from_noise"]),
    }


# Stage 13's job builds watermarks with
# for_bounded_out_of_orderness(Duration.of_seconds(5)) -- confirmed unchanged
# by Stage 15B's read-only audit of the job.
WATERMARK_BOUND_SECONDS = 5.0


def watermark_advancing_events(posts: list[dict]) -> int:
    """How many delivered events actually carried the watermark forward --
    the ceiling on how many distinct profile vectors polling can resolve.

    The Flink job overwrites one Redis key per window fire and keeps no
    history, and a single watermark advance can release seven or eight 30s
    slides at once (a `complete` event moves session time by most of a track
    duration), which the job then fires milliseconds apart. So the observable
    resolution is one vector per advance, not one per window fire, at any
    poll rate.

    Defined on DELIVERY order, which is what `posts` records: the watermark
    tracks the highest event_time seen so far by the source, so an event
    delivered behind the high-water mark advances nothing. That also makes
    this correct for a jittered run without a special case.
    """
    high_water = None
    advancing = 0
    for post in posts:
        t = _event_time_epoch(post)
        if high_water is None or t > high_water:
            advancing += 1
            high_water = t
    return advancing


def late_delivery_count(posts: list[dict], bound_seconds: float = WATERMARK_BOUND_SECONDS) -> dict:
    """How many events this run actually delivered late enough for Flink to
    drop them -- metric 7's DOSE, measured rather than assumed from the
    --jitter setting.

    `posts` is in delivery (POST, and therefore Kafka) order. An event is
    counted late when its own event_time falls more than `bound_seconds`
    below the highest event_time delivered before it: that is exactly the
    condition under which the watermark has already advanced past it and the
    window it belongs to has already fired. Stage 15B confirmed live that
    such an event is silently and permanently dropped, not side-outputted --
    the job configures no allowedLateness and no late side output.

    --jitter is a probability over a bounded positional reorder, so the
    number of events it actually pushes past the bound is a draw, not a
    setting. A jitter run with a dose of zero would explain a null result,
    and without this number that explanation would be indistinguishable from
    "the drop had no downstream effect".
    """
    high_water = None
    late, lateness = 0, []
    for post in posts:
        t = _event_time_epoch(post)
        if high_water is not None and t < high_water - bound_seconds:
            late += 1
            lateness.append(high_water - t)
        high_water = t if high_water is None else max(high_water, t)
    return {
        "events": len(posts),
        "events_delivered_late": late,
        "watermark_bound_seconds": bound_seconds,
        "max_lateness_seconds": max(lateness) if lateness else 0.0,
        "mean_lateness_seconds": (sum(lateness) / len(lateness)) if lateness else 0.0,
    }
