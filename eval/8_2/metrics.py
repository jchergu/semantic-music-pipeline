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
    if preceded_by_cold_start:
        handoff_jaccard = jaccard(set(preceding["track_ids"]), set(first_warm["track_ids"]))

    return {
        "reached_warm_path": True,
        "cold_start_refresh_count": sum(1 for r in real if r["action"] == "cold_start"),
        "seconds_to_first_warm": first_warm["wall_clock"] - session_start_wall,
        "first_warm_refresh_index": first_warm["refresh_index"],
        "preceded_by_cold_start": preceded_by_cold_start,
        "handoff_jaccard": handoff_jaccard,
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
