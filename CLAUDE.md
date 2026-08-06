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

## Working agreement

- Propose a plan before editing more than one file. Wait for confirmation.
- One build-order stage per session. Verify it works before moving on.
- If something in this file conflicts with what I ask you to do in a message,
  my message wins — but flag the conflict first, don't silently override.
