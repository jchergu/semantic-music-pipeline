"""
Stage 5 (8.1): ranking.

Pure function, no I/O, independently unit-testable against synthetic
candidate dicts. Merges the three raw candidate sources produced by
context_builder.py for one seed track into a single deduplicated,
scored, descending-sorted recommendation list.

Scoring: additive weighted sum.

    score = similarity              (Milvus cosine, ~0-1; 0 if the track
                                      wasn't in the similarity candidate pool)
          + GENRE_BOOST  if the track also appears in the seed's genre
                            siblings (a coarse signal: this 411-track /
                            77-genre dataset averages ~10 tracks per genre,
                            so genre co-membership alone is weak evidence
                            of relevance)
          + ARTIST_BOOST if the track is by the same artist as the seed
                            (a strong, high-precision signal: 411 tracks /
                            201 artists means ~2 tracks/artist on average
                            — sharing an artist is rare and meaningful,
                            even when CLAP similarity alone doesn't rank
                            the track highly)

Both boosts are kept well under similarity's own [0, 1] range so they act
as re-ranking nudges — breaking ties and pulling borderline candidates
into the top_k — rather than overriding the acoustic similarity signal
outright. Boosts stack additively: a track confirmed relevant by multiple
sources (e.g. a top acoustic match that's also by the same artist) ranks
above one confirmed by only one source.

This is a documented, tunable heuristic appropriate for a prototype at
this scale (411 tracks, no ground-truth relevance labels) — not a learned
or validated ranking model.
"""

DEFAULT_GENRE_BOOST = 0.05
DEFAULT_ARTIST_BOOST = 0.15


def score_recommendations(
    seed_track_id: int,
    similar: list[dict],
    genre_siblings: list[dict],
    same_artist: list[dict],
    *,
    genre_boost: float = DEFAULT_GENRE_BOOST,
    artist_boost: float = DEFAULT_ARTIST_BOOST,
    top_k: int = 10,
) -> list[dict]:
    candidates: dict[int, dict] = {}

    for c in similar:
        tid = c["track_id"]
        if tid == seed_track_id:
            continue
        candidates[tid] = {
            "track_id": tid,
            "title": c["title"],
            "artist_name": c["artist_name"],
            "similarity": c["score"],
            "genre_sibling": False,
            "same_artist": False,
        }

    def _touch(tid: int, title: str) -> dict:
        return candidates.setdefault(
            tid,
            {
                "track_id": tid,
                "title": title,
                "artist_name": None,
                "similarity": 0.0,
                "genre_sibling": False,
                "same_artist": False,
            },
        )

    for c in genre_siblings:
        tid = c["track_id"]
        if tid == seed_track_id:
            continue
        _touch(tid, c["title"])["genre_sibling"] = True

    for c in same_artist:
        tid = c["track_id"]
        if tid == seed_track_id:
            continue
        _touch(tid, c["title"])["same_artist"] = True

    ranked = []
    for entry in candidates.values():
        score = entry["similarity"]
        if entry["genre_sibling"]:
            score += genre_boost
        if entry["same_artist"]:
            score += artist_boost
        ranked.append({**entry, "score": score})

    # Secondary sort key (track_id) makes output deterministic on exact
    # score ties — matters for reproducible `rank` values and for tests.
    ranked.sort(key=lambda r: (-r["score"], r["track_id"]))
    return ranked[:top_k]
