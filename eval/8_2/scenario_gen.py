"""
Genre-based session script generation for eval/8_2 (METRICS.md section 1).

Deliberately NOT part of platform/simulator/: this queries tracks.genre_tags
to pick tracks, and the simulator has never done a content lookup of any
kind (it fetches durations, nothing else). Keeping DB-driven scenario
construction here leaves that module's contract unchanged.

Output is the same dict shape platform/simulator/scripts_io.load_script()
returns, so it feeds platform/simulator/events.build_session_events()
directly -- no new consumer of that function, and the canned YAML scripts
and these generated ones stay interchangeable.
"""
from __future__ import annotations

import random


class ScenarioGenerationError(Exception):
    pass


def fetch_genre_track_ids(pg_conn, genre: str) -> list[int]:
    """Every track tagged with `genre`, ordered by id.

    ORDER BY id is load-bearing, not cosmetic: without it Postgres is free
    to return rows in any order, so a seeded sample() over the result would
    be reproducible only by luck. The seed fixes the *choice*; this fixes
    the *list being chosen from*.
    """
    with pg_conn.cursor() as cur:
        cur.execute("SELECT id FROM tracks WHERE %s = ANY(genre_tags) ORDER BY id", (genre,))
        return [row[0] for row in cur.fetchall()]


def build_genre_session_script(
    genre_track_counts: list[tuple[str, int]],
    seed: int,
    ids_by_genre: dict[str, list[int]],
    completion_pct: float = 0.97,
) -> dict:
    """[(genre, n_tracks), ...] in play order -> one session script.

    Each (genre, n) pair contributes n distinct tracks of that genre, drawn
    by random.Random(seed).sample() from that genre's id list. Every track
    gets a {play, position_ms: 0} + {complete, position_pct} pair -- an
    engaged listener, so the semantic signal the pivot is supposed to move
    isn't confounded by skip weighting.

    Raises if a genre can't supply n tracks: a scenario that silently
    shrinks would quietly change what every downstream metric is measuring.

    `ids_by_genre` is passed in rather than queried here so this function
    stays pure and unit-testable; run.py calls fetch_genre_track_ids().
    """
    rng = random.Random(seed)
    tracks: list[dict] = []
    used: set[int] = set()

    for genre, n in genre_track_counts:
        available = [t for t in ids_by_genre.get(genre, []) if t not in used]
        if len(available) < n:
            raise ScenarioGenerationError(
                f"genre {genre!r} can supply {len(available)} unused track(s), scenario asked for {n}"
            )
        chosen = rng.sample(available, n)
        used.update(chosen)
        for track_id in chosen:
            tracks.append(
                {
                    "track_id": track_id,
                    "genre": genre,
                    "events": [
                        {"type": "play", "position_ms": 0},
                        {"type": "complete", "position_pct": completion_pct},
                    ],
                }
            )

    name = "__".join(f"{genre}{n}" for genre, n in genre_track_counts)
    return {
        "name": f"gen_{name}_seed{seed}",
        "intent": " -> ".join(f"{n} {genre} tracks" for genre, n in genre_track_counts),
        "tracks": tracks,
    }


def pivot_event_index(script: dict, pivot_track_index: int) -> int:
    """Index, in the flat event sequence build_session_events() produces, of
    the first event belonging to script["tracks"][pivot_track_index].

    The harness needs this to split recommendation snapshots into pre- and
    post-pivot, and to know exactly when to check the warm-path
    precondition. Computed from the script rather than by matching track
    ids at post time, because a track id can legitimately recur.
    """
    return sum(len(t["events"]) for t in script["tracks"][:pivot_track_index])
