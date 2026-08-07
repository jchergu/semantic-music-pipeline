# Project: Semantic-Aware Multimodal Music Pipelining

Master's thesis (Bologna). Kappa-style streaming pipeline, 3 layers, with a
Recommender Engine as the Layer 3 demo. Currently implementing **8.1 (batch,
reactive)**. Do not build 8.2/8.3 infra unless explicitly asked — they are
separate modules, not shared code paths.

## Architecture (fixed — do not propose alternatives without being asked)

**L1 — Data Ingestion/Preparation**
- Object storage: MinIO (S3-compatible)
- Feature store: Parquet / Delta Lake
- Metadata DB: PostgreSQL (system of record — not Redis)
- Kafka topics: media streams and behavioral events are SEPARATE topics, separate consumers
- Fingerprinting: Chromaprint/AcousticID → MusicBrainz linkage

**L2 — Semantic Enrichment**
- CLAP embeddings → Milvus (vector index) — not ChromaDB, not FAISS
- Neo4j knowledge graph — not RDF/Jena
- Kafka/Flink for streaming enrichment
- Spark for nightly batch enrichment

**L3 — Application / Semantic API**
- One shared Semantic API (FastAPI), reused by: Recommender Engine, Similarity
  Search, Auto-tagging, Playlist Generation. These are sibling consumers, not
  separate stacks.
- Redis: session cache for 8.2/8.3 use cases ONLY. Never a system-of-record
  substitute for PostgreSQL.

## Use case taxonomy

| Use case | Mode | Type | Status |
|---|---|---|---|
| 8.1 | Batch | Reactive | **Active implementation** |
| 8.2 | Streaming | Reactive | Not started |
| 8.3 | Streaming | Proactive | Not started — auto-tagging reused as live classifier only, no live writes to Neo4j |

8.1 / 8.2 / 8.3 are independent modules at prototype scope. Do not couple them
or share state between them unless the task explicitly says so.

## Dataset constraints

- Seed dataset: 200–500 tracks (not full corpora). This is intentional, to
  keep CLAP inference feasible on local CPU. Do not suggest scaling this up.
- Starting from zero data — nothing is ingested yet.

## Confirmed build order for 8.1 (follow this sequence, one stage per session)

1. Docker Compose environment (Postgres, MinIO, Neo4j, Milvus, Kafka, Redis)
2. Seed dataset ingestion → PostgreSQL/MinIO
3. L2 enrichment via CLAP → Milvus and Neo4j
4. Semantic API in FastAPI
5. Recommender Engine modules (trigger handler, context builder, ranking)
6. End-to-end test

Do not skip ahead to a later stage until the current one is verified working.

## Stack (all open-source, self-hostable)

PostgreSQL, Neo4j, Milvus, MinIO, Kafka, Flink, Redis, Docker Compose, CLAP,
Chromaprint/AcousticID, Librosa, Essentia, FastAPI.

## Commands

- `docker compose up -d` — bring up the stack (Postgres, MinIO, Neo4j, Milvus
  + its etcd/MinIO deps, Kafka + Zookeeper, Redis)
- `docker compose ps` — verify all services healthy before moving to next stage
- `docker compose down` — stop the stack (add `-v` to also wipe volumes)
- (test/lint commands go here once they exist)

Stage 1 (Docker Compose environment) verified working as of 2026-08-06 — all
9 containers report healthy.

Stage 2 (Seed dataset ingestion) verified working as of 2026-08-06 — 411
tracks ingested from Jamendo into PostgreSQL/MinIO (target was 500; see
`docs/stage2-ingestion.md` for why 411 is within spec and considered done).
All 5 tests in `tests/test_stage2_ingestion.py` pass. Metrics/plots in
`reports/stage2/`.

Stage 3 (L2 enrichment: CLAP → Milvus, Neo4j) verified working as of
2026-08-07 — all 411 tracks embedded (laion_clap, 512-dim) and indexed in
Milvus, graph written to Neo4j (411 Track / 201 Artist / 77 Genre nodes).
See `docs/stage3-enrichment.md` for the run outcome and a packaging note on
`enrichment/install.sh` (laion-clap's declared deps don't install cleanly
via a plain `pip install -r requirements.txt` on Python 3.12). All 6 tests
in `tests/test_stage3_enrichment.py` pass. Re-verified 2026-08-07 with a
full `--force` re-enrichment run; caught and fixed a Milvus test flakiness
bug in the process (see doc).

Stage 4 (Semantic API, FastAPI) verified working as of 2026-08-07 — one
shared read-only service over Postgres/Milvus/Neo4j: `/health`,
`/tracks/{id}`, `/tracks/{id}/similar`, `/tracks/{id}/graph`,
`/artists/{name}/tracks`, `/genres/{name}/tracks`. See
`docs/stage4-semantic-api.md`. All 7 tests in `tests/test_stage4_api.py`
pass (19/19 total across stages 2+3+4).

Stage 5 (Recommender Engine: trigger handler, context builder, ranking)
verified working as of 2026-08-07 — seed-track-only trigger model (no
fabricated user/session identity), consumes the Semantic API over HTTP,
combines CLAP similarity + genre-sibling + same-artist signals into a
scored ranked list, writes to a new Postgres `recommendations` table. Full
batch run: 411/411 seeds, 4110 rows, zero skips, ~8.8s wall-clock. See
`docs/stage5-recommender.md` for the ranking formula and its dataset-
grounded weight justification. All 8 tests in
`tests/test_stage5_recommender.py` pass (27/27 total across stages
2+3+4+5).

Stage 6 (end-to-end test) verified working as of 2026-08-07 — lineage test
tracing 3 golden tracks through every layer (Postgres/MinIO → Milvus/Neo4j
→ Semantic API → Recommender), run against the live already-populated
stack (not a destructive from-scratch rebuild; that sequence is documented
as a runbook in `docs/stage6-e2e.md` for on-demand use, e.g. a thesis
defense demo, but not exercised automatically). Found and fixed a real bug
in the process: `api/dependencies.py` shared pymilvus's default connection
alias (`"default"`) with `tests/conftest.py`'s `milvus_collection` fixture,
so `test_stage4_api.py`'s in-process `TestClient` teardown silently broke
Milvus for every test running after it in the same session — fixed by
giving the API its own alias (`"api"`). All 5 tests in
`tests/test_stage6_e2e.py` pass (32/32 total across all six stages).

**8.1 (batch, reactive) build order is now complete** — see the capstone
summary at the bottom of `docs/stage6-e2e.md`.

## Working agreement

- Propose a plan before editing more than one file. Wait for confirmation.
- One build-order stage per session. Verify it works before moving on.
- If something in this file conflicts with what I ask you to do in a message,
  my message wins — but flag the conflict first, don't silently override.
