"""
Stage 5 (8.1): trigger handler.

Trigger model: seed-track only, no fabricated user/session identity — a
"trigger" is a single existing track ID. This module's only job is to
resolve *which* track ID(s) a batch invocation should treat as triggers.

It is the one place in the recommender that queries Postgres directly for
`tracks.id`, rather than going through the Semantic API — the API has no
"list tracks" endpoint (deliberately kept out of stage 4's fixed surface),
and enumerating existing IDs to know what to iterate over is closer to a
batch job's own work-item bookkeeping than to a semantic read. Every read
of a given seed track's *content* (title, embedding, graph, artist) still
goes through the Semantic API via context_builder.py — this module never
reads anything but the id column.
"""


def get_seed_track_ids(conn, *, seed_track_id: int | None, limit: int | None) -> list[int]:
    """Resolve the list of seed track IDs for one batch invocation.

    - `seed_track_id` given: exactly that one ID (existence is validated
      downstream by the Semantic API's 404, not here).
    - otherwise: every track ID in Postgres, ordered by id, optionally
      capped to the first `limit` (mirrors enrich.py's --limit).
    """
    if seed_track_id is not None:
        return [seed_track_id]

    query = "SELECT id FROM tracks ORDER BY id"
    params: tuple = ()
    if limit is not None:
        query += " LIMIT %s"
        params = (limit,)

    with conn.cursor() as cur:
        cur.execute(query, params)
        return [row[0] for row in cur.fetchall()]
