# contracts/

Versioned interface artifacts shared between the platform (L1/L2/L3) and
the individual use cases (8.1, 8.2, 8.3) — not use-case-specific code, and
not platform implementation either. This is where the platform's promises
to its consumers get written down once they need to be written down at
all:

- the frozen OpenAPI export of the Semantic API
- Kafka topic schemas — media-stream and behavioral-event topics are
  separate topics with separate schemas, per the platform's Kafka design
- the shared recommendation response shape

Behavioral-event ingestion is a separate service from the Semantic API —
the API stays read-only (see `semantic-api-v1.json` below) — and that
ingestion service produces to the `behavioral-events` topic. See
CLAUDE.md's L3 architecture section (Decision A, 2026-08-30) for the
decision.

This directory is empty right now, on purpose. With a single use case
(8.1) consuming the platform, the contract between them is implicit — it
lives in the Semantic API's own code and 8.1's own tests, and duplicating
it here would just be a second copy to keep in sync for no reader. It gets
populated when a second independent consumer (8.2) exists and the
contract between "what the platform promises" and "what a specific use
case assumes" stops being something one person can hold in their head.

Do not add anything here speculatively. If you're looking at this file
wondering what belongs in contracts/, the answer is: nothing yet.

## semantic-api-v1.json

Frozen 2026-08-28, ahead of 8.2 work starting. This is the 8.1-era
Semantic API surface — the 6 read-only GET endpoints in
`platform/semantic_api/main.py` as they exist today, exported via
`app.openapi()`. 8.2 must not silently change it: a field rename,
removal, or type change on any endpoint 8.1 depends on
(`/tracks/{id}/similar`'s `title`/`artist_name`/`track_id`/`score`,
`/tracks/{id}/graph`'s `artist`/`related_by_genre`, `/artists/{name}
/tracks`'s `track_id`/`title`) breaks
`usecases/8_1_batch_reactive/recommender/context_builder.py` and/or
`ranking.py`. Diff a new `app.openapi()` export against this file before
changing any existing endpoint's response shape; update this file
deliberately, in the same change, if the break is intentional.
