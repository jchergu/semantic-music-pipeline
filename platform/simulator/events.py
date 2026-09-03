"""
Stage 12: pure event-construction logic for the behavioral-event simulator.

No network, no time.sleep -- cli.py is the only module that actually POSTs
or sleeps. This mirrors usecases/8_1_batch_reactive/recommender/ranking.py's
testability pattern (pure function, synthetic-input-testable).

event_time and position_ms are additions to platform/event_ingestion's
BehavioralEvent shape (session_id/event_type/track_id today) -- the schema
is `extra="allow"` specifically to permit this (see
platform/event_ingestion/main.py), not a silent extension of a frozen
contract. event_time carries the *simulated* session clock; the gap this
module returns per event is also on that simulated clock -- cli.py divides
it by --speed to get the real sleep duration, decoupling the two per the
stage 12 requirement (matters for Session E's future Flink event-time
windows).

Stage 15B: apply_jitter() stays pure too -- it takes an externally-owned,
already-seeded rng rather than seeding its own, so cli.py's single seeded
rng remains the sole source of randomness (see apply_jitter's docstring
for why this matters for --seed determinism).
"""
from __future__ import annotations

import datetime as dt
import random

KNOWN_EVENT_TYPES = {"play", "skip", "complete", "like"}


class ScriptValidationError(Exception):
    pass


def resolve_position_ms(event: dict, duration_sec: int) -> int:
    """Exactly one of position_ms/position_pct must be given per event --
    absolute for the early-skip case (a fixed <5000ms threshold regardless
    of track length, per Session E's own spec), relative for completion."""
    has_ms = "position_ms" in event
    has_pct = "position_pct" in event
    if has_ms == has_pct:
        raise ScriptValidationError(
            f"event must specify exactly one of position_ms/position_pct: {event!r}"
        )
    if has_ms:
        return int(event["position_ms"])
    return int(round(event["position_pct"] * duration_sec * 1000))


def build_session_events(
    script: dict,
    session_id: str,
    durations_by_track_id: dict[int, int],
    start_time: dt.datetime,
) -> list[dict]:
    """
    Returns an ordered list of {"event": {...payload...},
    "wall_clock_gap_seconds": float}. The gap is the SIMULATED elapsed time
    since the previous event (0.0 for the first); event_time in the payload
    always advances by the full simulated gap.

    A track's contribution to the session clock is the position_ms of its
    *last* event, not its full duration -- an early skip genuinely means
    the listener left early, so the next track's play event follows almost
    immediately, not after the nominal duration has elapsed.
    """
    out: list[dict] = []
    current_time = start_time
    cumulative_ms = 0
    prev_absolute_ms: int | None = None

    for track in script["tracks"]:
        track_id = track["track_id"]
        if track_id not in durations_by_track_id:
            raise ScriptValidationError(f"track_id {track_id} not found in the live catalog")
        duration_sec = durations_by_track_id[track_id]

        last_position_ms = 0
        for raw_event in track["events"]:
            if raw_event.get("type") not in KNOWN_EVENT_TYPES:
                raise ScriptValidationError(
                    f"unknown event type {raw_event.get('type')!r}, expected one of {sorted(KNOWN_EVENT_TYPES)}"
                )
            position_ms = resolve_position_ms(raw_event, duration_sec)
            absolute_ms = cumulative_ms + position_ms
            gap_seconds = 0.0 if prev_absolute_ms is None else max(0.0, (absolute_ms - prev_absolute_ms) / 1000.0)
            current_time = current_time + dt.timedelta(seconds=gap_seconds)

            event = {
                "session_id": session_id,
                "event_type": raw_event["type"],
                "track_id": str(track_id),
                "event_time": current_time.isoformat(),
                "position_ms": position_ms,
            }
            out.append({"event": event, "wall_clock_gap_seconds": gap_seconds})
            prev_absolute_ms = absolute_ms
            last_position_ms = position_ms

        cumulative_ms += last_position_ms

    return out


MAX_JITTER_DELAY = 3  # bounded displacement in *positions*, not seconds -- see apply_jitter's docstring


def apply_jitter(
    planned: list[dict], jitter: float, rng: random.Random, max_delay: int = MAX_JITTER_DELAY
) -> list[dict]:
    """Reorders a `jitter` fraction of `planned` (event_time-ordered, as
    build_session_events() returns it) to simulate late-arriving events for
    exercising Flink's bounded-out-of-orderness watermark. Each event's own
    event_time is left untouched -- it still records when the listening
    action actually happened; only its position in the *delivery* sequence
    (POST order, and therefore Kafka order -- the behavioral-events topic
    has a single partition, so delivery order == produce order exactly)
    moves later, mirroring a client buffering an event and sending it after
    already-newer ones.

    Single left-to-right pass: for each index i, with probability `jitter`
    it is swapped forward with the event at min(i + rng.randint(1,
    max_delay), n-1); the scan resumes past the swapped-to index so no
    event participates in two swaps. A permutation of `planned`, not a
    lossy transform -- every event that went in comes back out exactly
    once.

    jitter <= 0.0 returns `planned` unchanged, with zero draws from `rng`
    -- the default (--jitter omitted) is therefore byte-identical to
    pre-stage-15B behavior, including the exact sequence of rng calls
    cli.py's script-selection already makes.

    Callers MUST only invoke this from single-threaded code holding the
    sole seeded `rng` cli.py owns -- concurrent sessions calling this from
    separate threads would race on `rng` and break seed-determinism.
    """
    if jitter <= 0.0:
        return planned
    n = len(planned)
    order = list(range(n))
    i = 0
    while i < n:
        if rng.random() < jitter:
            j = min(i + rng.randint(1, max_delay), n - 1)
            order[i], order[j] = order[j], order[i]
            i = j + 1
        else:
            i += 1
    return [planned[idx] for idx in order]
