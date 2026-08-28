# Stage 5 — Recommender Engine (trigger handler, context builder, ranking)

Status: **verified working**, 2026-08-07.

## What this stage does

The Layer 3 demo application this pipeline exists to feed. Batch job that,
for a given seed track, produces a ranked list of recommended tracks and
writes them to Postgres.

**Trigger model: seed-track only, no fabricated user/session identity.** A
"trigger" is a single existing track ID, standing in for "this track was
just played/selected" — matches "fans also like" / "up next" style
recommendation. This was a deliberate choice over inventing synthetic
user/session data: no user or behavioral data exists anywhere in this
pipeline yet (Kafka is provisioned but unwired; CLAUDE.md explicitly scopes
behavioral-event topics to 8.2, not 8.1), so a user-personalized design
would have meant fabricating a data model the rest of the pipeline doesn't
have.

Code: `usecases/8_1_batch_reactive/recommender/trigger_handler.py`, `context_builder.py`, `ranking.py`,
`recommend.py`. Schema: `usecases/8_1_batch_reactive/recommender/schema.sql` (new `recommendations`
table, applied automatically by `recommend.py` on startup, same pattern as
the other stages).

The recommender consumes the stage 4 Semantic API **over HTTP** for
everything relating to a track's content — it never queries Postgres,
Milvus, or Neo4j directly for track data. CLAUDE.md states the Semantic API
is "reused by: Recommender Engine [and sibling 8.2/8.3 consumers]" — a real
service boundary, not just a convenient shortcut. The one narrow exception:
`trigger_handler.py` queries Postgres directly for `tracks.id` to know
which IDs a batch run should iterate over — the API has no list-tracks
endpoint (correctly out of stage 4's frozen surface), and enumerating IDs
is job bookkeeping, not a semantic read.

## Pipeline

1. **`trigger_handler.get_seed_track_ids`** — resolves the batch's seed
   track IDs: either exactly one (`--seed-track-id`) or every track ordered
   by id, optionally capped by `--limit`.
2. **`context_builder.build_context`** — for one seed track, three HTTP
   calls to the Semantic API:
   - `GET /tracks/{id}/similar?k=candidate_k` (Milvus cosine; `candidate_k`
     defaults to 25, deliberately wider than the final `top_k=10` so a
     same-artist/genre-sibling track just outside the raw similarity
     top-10 can still be boosted into the final result)
   - `GET /tracks/{id}/graph` → `related_by_genre`
   - `GET /artists/{artist}/tracks` (using the graph response's `artist`
     field) → same-catalog tracks, seed excluded
3. **`ranking.score_recommendations`** — pure function, no I/O, merges the
   three sources into one deduplicated, scored, descending-sorted list.
4. **`recommend.py`** writes the top-K to `recommendations`, tagged with a
   shared `run_id` per invocation.

## Ranking formula

```
score = similarity              (Milvus cosine, ~0-1; 0 if the track
                                  wasn't in the similarity candidate pool)
      + GENRE_BOOST  (0.05)  if the track also appears in the seed's
                                genre siblings
      + ARTIST_BOOST (0.15)  if the track is by the same artist as the seed
```

Weights are grounded in the actual stage-3 dataset stats
(`docs/platform/stage3-enrichment.md`): 764 `HAS_GENRE` edges / 77 genres ≈ 9.9
tracks per genre on average — genre co-membership is a coarse, low-precision
signal, so it gets a small boost. 411 tracks / 201 artists ≈ 2.0 tracks per
artist — same-artist is rare and high-precision, so it gets a bigger boost.
Both stay well under similarity's own range so they act as re-ranking
nudges/tie-breakers, not overrides — a track confirmed relevant by multiple
sources (e.g. a top acoustic match that's also by the same artist) ranks
above one confirmed by only one source, since boosts stack additively.

This is a documented, tunable heuristic appropriate for a prototype at this
scale (411 tracks, no ground-truth relevance labels) — not a learned or
validated ranking model. Reciprocal Rank Fusion (RRF) was considered as a
more "principled" alternative but rejected: the genre-sibling and
same-artist lists coming out of Neo4j aren't meaningfully ordered
(arbitrary Cypher return order), so RRF's rank-based fusion wouldn't add
real rigor over a membership boost here, while being harder to justify in
a writeup than "cosine plus a documented flat bonus."

Sanity-checked against real output for seed track 1 ("Wish You Were Here"
by The.madpix.project): track 12 ("Moments", same artist) landed at rank 1
with score 0.6585 — its raw similarity was 0.5085, matching
`0.5085 + ARTIST_BOOST(0.15) = 0.6585` exactly. Track 29 ("Divergence
(Remastered VIP)") landed at rank 3 with score 0.5699, matching its raw
similarity 0.5199 plus `GENRE_BOOST(0.05)` exactly. Both boosts apply
precisely as designed.

## Schema

```sql
CREATE TABLE recommendations (
    id                      SERIAL PRIMARY KEY,
    run_id                  UUID NOT NULL,
    seed_track_id           INTEGER NOT NULL REFERENCES tracks(id),
    recommended_track_id    INTEGER NOT NULL REFERENCES tracks(id),
    rank                    INTEGER NOT NULL,
    score                   REAL NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, seed_track_id, rank),
    CHECK (seed_track_id <> recommended_track_id)
);
```

`UNIQUE (run_id, seed_track_id, rank)` + `ON CONFLICT DO NOTHING` in the
insert makes re-running the same `--run-id` idempotent. The `CHECK`
constraint is a DB-level backstop on top of `ranking.py`'s own defensive
seed-exclusion.

## Test fixture: `semantic_api_server`

`tests/conftest.py` gained a new session-scoped fixture that launches
`python -m uvicorn semantic_api.main:app` as a subprocess on a dynamically-assigned
free port (avoiding both the known-occupied `:8000` on the dev machine and
the docs' example `:8010`), polls `/health` until ready (30s timeout),
yields the base URL, and tears the process down afterward. This keeps the
test suite self-contained — no manual "start the API first" step — matching
every other fixture here (`pg_conn`, `milvus_collection`, `neo4j_driver`)
auto-connecting to the live stack rather than requiring setup.

## Run outcome

| Metric | Value |
|---|---|
| Seed tracks processed | 411 / 411 (100%, zero skips or failures) |
| Recommendation rows written | 4110 (411 × top_k=10) |
| Self-recommendations | 0 |
| Seeds with != 10 recommendations | 0 |
| Score range | 0.181 – 1.179 (mean 0.794) |
| Wall-clock for the full 411-seed batch | ~8.8s (`user 1.3s`), against a locally running API + live stack |

## Verification

`usecases/8_1_batch_reactive/tests/test_uc81_recommender.py`:

- 7 unit tests for `ranking.py` against synthetic candidate dicts (no live
  services): default similarity ordering, additive genre+artist boosts,
  candidates with no similarity score, dedup across sources, seed
  self-exclusion, `top_k` truncation, deterministic tie-breaking
- 1 integration test (`test_recommend_batch_writes_valid_rows`): runs
  `recommend.main([...])` for a real seed track against the live stack +
  the `semantic_api_server` fixture, asserts the resulting Postgres rows
  (count == top_k, no self-recommendation, ranks 1..top_k, scores
  descending), cleans up its own `run_id` rows after

All 8 pass, alongside the existing 5 stage 2 + 6 stage 3 + 8 stage 4 tests
(27/27 total across all four stages).

## Reproducing / extending

```bash
cd recommender
python3 -m venv .venv
source .venv/bin/activate  # or use .venv/bin/python directly
pip install -r requirements.txt   # no install.sh needed here

cd ..
platform/semantic_api/.venv/bin/python -m uvicorn semantic_api.main:app --app-dir platform --port 8010 &   # run the Semantic API

usecases/8_1_batch_reactive/recommender/.venv/bin/python usecases/8_1_batch_reactive/recommender/recommend.py --seed-track-id 1 --top-k 5   # single seed
usecases/8_1_batch_reactive/recommender/.venv/bin/python usecases/8_1_batch_reactive/recommender/recommend.py --limit 20                     # small batch
usecases/8_1_batch_reactive/recommender/.venv/bin/python usecases/8_1_batch_reactive/recommender/recommend.py                                 # full batch, all 411 tracks

platform/enrichment/.venv/bin/python -m pip install fastapi==0.115.0 "uvicorn[standard]==0.32.0" httpx==0.27.2  # stage 4 test deps, if not already installed
platform/enrichment/.venv/bin/python -m pytest   # verify (stages 2+3+4+5, both tests/ and usecases/) — spins up its own API instance, no manual server needed
```

Requires the full stack up (`docker compose up -d`) and stages 2-4 already
run (this stage reads through the Semantic API, which reads Postgres/Milvus/
Neo4j data that `ingest.py`/`enrich.py` populate — it doesn't populate
anything upstream itself).
