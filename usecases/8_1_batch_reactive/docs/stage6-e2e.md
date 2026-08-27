# Stage 6 — End-to-end test

Status: **verified working**, 2026-08-07. This is the final stage of the
8.1 (batch, reactive) build order — see the capstone summary at the bottom.

## What this stage does

Traces a small set of "golden" tracks through every layer of the pipeline
in one test run — L1 (Postgres/MinIO) → L2 (Milvus/Neo4j) → L3 (Semantic
API) → Recommender Engine — asserting data consistency at each hop.

This is deliberately **not** a from-scratch rebuild. Stages 2-5 each
already have their own verification suite proving their own output is
internally consistent; what none of them prove is that the *same* piece of
data stays consistent as it flows through all five stages together. Stage
6 closes that gap: it's a lineage test, not a fresh-build test.

**Scope decision (confirmed with the user):** run against the current,
already-populated live stack rather than wiping all Docker volumes and
re-ingesting/re-enriching from zero. A full clean-slate rebuild would prove
reproducibility more completely, but is destructive (loses the current
verified 411-track dataset until the rebuild finishes), slow
(re-ingestion from Jamendo took real wall-clock time with API rate
limits), and re-consumes Jamendo/AcoustID API quota for no correctness
benefit beyond what the lineage test already proves. The rebuild sequence
is documented below as a runbook instead — runnable on demand (e.g. for a
thesis defense reproducibility demo) but not exercised automatically.

Code: `tests/test_stage6_e2e.py`. No new source modules — this stage only
adds tests, reusing every fixture already built for stages 2-5
(`pg_conn`, `s3_client`, `milvus_collection`, `neo4j_driver`,
`semantic_api_server`).

## What gets checked

For 3 golden tracks (first 3 by `id`):

1. **Postgres + MinIO** — row has non-empty title/artist/jamendo_id; the
   MinIO audio object at `minio_bucket/minio_object_key` exists and is
   non-empty.
2. **Milvus** — a `track_embeddings` row exists for the track's id.
3. **Neo4j** — a `(:Artist)-[:PERFORMED]->(:Track {track_id})` edge exists,
   and the artist name matches Postgres exactly.
4. **Semantic API** — `GET /tracks/{id}` returns the same title/artist/
   genre_tags as Postgres and `enriched: true`; `GET /tracks/{id}/graph`'s
   artist matches Postgres; `GET /tracks/{id}/similar` returns `k` results,
   none of them the track itself.
5. **Recommender** — running `recommend.py` fresh for each golden track (not
   reading stale rows from an earlier batch run) produces exactly `top_k`
   rows per seed in Postgres; every `recommended_track_id` joins to a real
   `tracks` row (the join itself is the FK-integrity check — a dangling
   reference would silently drop out of an inner join rather than error,
   so a full row count is the meaningful assertion); no self-recommendation;
   scores descending per seed.

Running the recommender fresh (rather than just reading pre-existing rows
from the stage 5 batch run) makes this test self-sufficient on a clean
checkout — it doesn't depend on a human having run `recommend.py` manually
at some point before the test suite runs.

## Bug found while building this stage

`test_golden_tracks_embedded_in_milvus` failed intermittently depending on
what ran before it — specifically, it broke whenever `tests/test_stage4_api.py`
ran first in the same session. Root cause: `api/dependencies.py` opened its
Milvus connection under pymilvus's default alias, `"default"` — the exact
same alias `tests/conftest.py`'s session-scoped `milvus_collection` fixture
uses. `test_stage4_api.py`'s `client` fixture drives the FastAPI app via
`TestClient(app)`, which runs the app **in-process** (unlike
`semantic_api_server`, which runs it as a real subprocess) — so its
lifespan shutdown call to `disconnect_all()` tore down the shared
`"default"` alias out from under `milvus_collection` the moment stage 4's
test module finished, breaking Milvus for every later test in the same
pytest session, including stage 6's.

Fixed by giving the API its own dedicated alias, `"api"`
(`api/dependencies.py`), fully isolated from the test suite's own `"default"`
alias. This only mattered because of the in-process `TestClient` usage in
stage 4's tests — the subprocess-based `semantic_api_server` fixture used
by stages 5 and 6 was never actually at risk (a subprocess has its own,
separate pymilvus connection registry), but the fix keeps both call sites
consistent rather than leaving a footgun for the next in-process test.

## Full rebuild runbook (documented, not auto-run)

For a genuine from-scratch reproducibility demonstration — e.g. for a
thesis defense — the full pipeline can be rebuilt from zero. **This is
destructive**: it wipes the current dataset until the rebuild completes,
takes real wall-clock time (ingestion involves rate-limited external API
calls; enrichment re-runs CLAP inference over the whole dataset), and
re-consumes Jamendo/AcoustID API quota. Confirm before running.

```bash
docker compose down -v   # wipes Postgres/MinIO/Milvus/Neo4j volumes
docker compose up -d
docker compose ps        # wait for all services healthy

ingestion/.venv/bin/python ingestion/ingest.py --limit 500

enrichment/.venv/bin/python enrichment/enrich.py

api/.venv/bin/uvicorn api.main:app --port 8010 &

recommender/.venv/bin/python recommender/recommend.py

enrichment/.venv/bin/pytest tests/   # full suite against the rebuilt stack
```

## Run outcome

| Metric | Value |
|---|---|
| Golden tracks traced | 3 / 3, full lineage confirmed at every layer |
| Fresh recommendations generated during the test | 15 (3 seeds × top_k=5), all valid, cleaned up after |
| Bugs found | 1 (Milvus alias collision, see above), fixed |

## Verification

`tests/test_stage6_e2e.py`:

- `test_golden_tracks_exist_in_postgres_and_minio`
- `test_golden_tracks_embedded_in_milvus`
- `test_golden_tracks_in_neo4j_graph_match_postgres`
- `test_golden_tracks_via_semantic_api_match_postgres`
- `test_recommender_produces_valid_recommendations_for_golden_tracks`

All 5 pass, alongside the existing 5 stage 2 + 6 stage 3 + 8 stage 4 + 8
stage 5 tests (**32/32 total** across all six stages).

## Reproducing / extending

```bash
docker compose up -d   # if not already running
enrichment/.venv/bin/pytest tests/test_stage6_e2e.py -v
# or the whole suite:
enrichment/.venv/bin/pytest tests/ -v
```

No manual server setup needed — `semantic_api_server` (session-scoped,
`tests/conftest.py`) launches its own uvicorn subprocess on a free port
for the whole test session.

---

## 8.1 (batch, reactive) — complete

All six build-order stages are done and verified:

| Stage | What | Status |
|---|---|---|
| 1 | Docker Compose (Postgres, MinIO, Neo4j, Milvus, Kafka, Redis) | ✅ `docker-compose.yml` |
| 2 | Seed ingestion (Jamendo → Postgres/MinIO) | ✅ 411 tracks, `docs/stage2-ingestion.md` |
| 3 | L2 enrichment (CLAP → Milvus, Neo4j) | ✅ 411/411 embedded + graphed, `docs/stage3-enrichment.md` |
| 4 | Semantic API (FastAPI) | ✅ 6 endpoints, `docs/stage4-semantic-api.md` |
| 5 | Recommender Engine | ✅ 411/411 seeds, 4110 recommendations, `docs/stage5-recommender.md` |
| 6 | End-to-end test | ✅ this document |

**32/32 tests pass** across the whole suite (`enrichment/.venv/bin/pytest
tests/`). Dataset: 411 real, licensed tracks (within the 200-500 seed
range), all fully enriched and recommendable. Stack: Postgres (system of
record), MinIO (object storage), Milvus (CLAP embeddings), Neo4j (semantic
graph), FastAPI (shared L3 API), all via Docker Compose — Kafka/Redis
provisioned but correctly left unwired, reserved for 8.2/8.3.
