"""
Stage 13: session profile centroid -- pure weighting/decay/centroid logic,
no I/O, unit-testable against synthetic data (mirrors
usecases/8_1_batch_reactive/recommender/ranking.py's testability pattern).

This is the canonical reference implementation of the weight table and
recency decay from the roadmap. Session E is hard-timeboxed to one
session with an explicit PyFlink-or-fallback decision point -- see
CLAUDE.md's stage 13 entry for which path this session actually landed
on and why.

Event weights:
    complete             +1.0
    like                 +1.5
    skip, <5000ms        -1.0   (absolute position_ms -- matches the
                                  event simulator's early-skip scripting)
    skip, >80% duration  +0.8   (needs the track's duration_sec)
    skip, otherwise      -0.3
    play                 +0.2

Recency decay: exponential, half-life 3 EVENTS (a count, not a duration) --
the most recent event in the window gets full weight; an event k events
before it is scaled by 0.5**(k/3).
"""

COMPLETE_WEIGHT = 1.0
LIKE_WEIGHT = 1.5
SKIP_EARLY_WEIGHT = -1.0
SKIP_LATE_WEIGHT = 0.8
SKIP_OTHER_WEIGHT = -0.3
PLAY_WEIGHT = 0.2

EARLY_SKIP_MS = 5000
LATE_SKIP_DURATION_FRACTION = 0.8
DECAY_HALF_LIFE_EVENTS = 3
EMBED_DIM = 512


def base_weight(event_type: str, position_ms: int, duration_sec: int | None = None) -> float:
    if event_type == "complete":
        return COMPLETE_WEIGHT
    if event_type == "like":
        return LIKE_WEIGHT
    if event_type == "play":
        return PLAY_WEIGHT
    if event_type == "skip":
        if position_ms < EARLY_SKIP_MS:
            return SKIP_EARLY_WEIGHT
        if duration_sec and position_ms >= LATE_SKIP_DURATION_FRACTION * duration_sec * 1000:
            return SKIP_LATE_WEIGHT
        return SKIP_OTHER_WEIGHT
    return 0.0


def compute_centroid(
    events: list[dict], embeddings: dict[str, list[float]], durations: dict[str, int] | None = None
) -> tuple[list[float] | None, float]:
    """events: ordered oldest -> newest, each {"event_type", "track_id", "position_ms", ...}.
    embeddings: track_id (str) -> 512-dim vector. durations: track_id (str) -> duration_sec.

    Returns (centroid, weight_total). centroid is None when weight_total is
    0 (e.g. every event's track is missing an embedding, or the weights
    happen to cancel exactly) -- distinguishing "no signal" from a
    legitimate all-zero vector, which normalizing 0/0 would silently and
    incorrectly produce.

    Normalizes by the sum of |weight| (not the signed sum): weights can be
    negative (skips), and dividing by a signed sum that's small or
    negative would flip or blow up the result in exactly the cases this
    function is meant to handle correctly.
    """
    durations = durations or {}
    n = len(events)
    weighted_sum = [0.0] * EMBED_DIM
    weight_total = 0.0
    for idx, event in enumerate(events):
        events_ago = n - 1 - idx
        decay = 0.5 ** (events_ago / DECAY_HALF_LIFE_EVENTS)
        track_id = event.get("track_id")
        weight = base_weight(event.get("event_type", ""), event.get("position_ms", 0), durations.get(track_id)) * decay
        embedding = embeddings.get(track_id)
        if embedding is None:
            continue
        for i in range(EMBED_DIM):
            weighted_sum[i] += weight * embedding[i]
        weight_total += abs(weight)

    if weight_total == 0:
        return None, 0.0
    return [v / weight_total for v in weighted_sum], weight_total
