# Semantic-Aware Multimodal Music Pipelining

A Kappa-style, three-layer streaming/batch pipeline for music semantic
enrichment and recommendation, built as part of a Master's thesis
(University of Bologna). This repository implements **8.1 — the batch,
reactive** track: ingestion, semantic enrichment, a shared semantic API,
and a recommender engine, verified end to end on a real dataset.

**Status: 8.1 build order complete.** 411 tracks ingested, embedded, and
graphed; 4,110 recommendations generated; 32/32 automated tests passing.
See [`reports/results.html`](reports/results.html) for the full results
writeup (architecture, embedding visualization, worked recommendation
example) — open it directly in a browser, no server required.

## Architecture

Three layers, each independently testable:

- **L1 — Data Ingestion.** Raw audio → object storage (MinIO); track
  metadata → PostgreSQL (system of record); Chromaprint/AcoustID
  fingerprinting with best-effort MusicBrainz linkage.
- **L2 — Semantic Enrichment.** CLAP audio embeddings → Milvus (vector
  index); a Track/Artist/Genre knowledge graph → Neo4j.
- **L3 — Application.** A single shared FastAPI Semantic API over all
  three stores, consumed by the Recommender Engine (trigger handler →
  context builder → ranking) — and designed to be reused by future
  sibling consumers (similarity search, auto-tagging, playlist
  generation) without duplicating store access.

The Recommender Engine only talks to the Semantic API over HTTP — it
never queries Postgres, Milvus, or Neo4j directly for track content. See
`reports/results.html` for a diagram.

Kafka and Redis are provisioned in the Docker stack but intentionally
left unwired in this track — they're reserved for the streaming (8.2) and
proactive-streaming (8.3) tracks, which are separate, out-of-scope
modules.

## Build order

| # | Stage | Code | Docs |
|---|---|---|---|
| 1 | Docker Compose environment | `docker-compose.yml` | — |
| 2 | Seed dataset ingestion (Jamendo → Postgres/MinIO) | `ingestion/` | [`docs/stage2-ingestion.md`](docs/stage2-ingestion.md) |
| 3 | L2 enrichment (CLAP → Milvus, Neo4j) | `enrichment/` | [`docs/stage3-enrichment.md`](docs/stage3-enrichment.md) |
| 4 | Semantic API (FastAPI) | `api/` | [`docs/stage4-semantic-api.md`](docs/stage4-semantic-api.md) |
| 5 | Recommender Engine | `recommender/` | [`docs/stage5-recommender.md`](docs/stage5-recommender.md) |
| 6 | End-to-end test | `tests/test_stage6_e2e.py` | [`docs/stage6-e2e.md`](docs/stage6-e2e.md) |

## Dataset

411 real, Creative-Commons-licensed tracks pulled from the Jamendo API —
deliberately kept to a 200–500 track seed (not a full corpus) to keep CLAP
inference feasible on a local CPU. See `docs/stage2-ingestion.md` for why
Jamendo was chosen over alternatives and how the dataset was assembled.

## Quickstart

Requires Docker, Python 3.12, and `fpcalc` (`sudo apt install
libchromaprint-tools`) on `PATH`.

```bash
cp .env.example .env
# fill in JAMENDO_CLIENT_ID / ACOUSTID_API_KEY (free signup at
# devportal.jamendo.com and acoustid.org) and set real passwords

docker compose up -d
docker compose ps    # wait for all services healthy
```

Each stage has its own virtualenv and can be run independently once the
stack is up and the previous stage's data exists:

```bash
# Stage 2 — ingest the seed dataset
cd ingestion && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cd .. && ingestion/.venv/bin/python ingestion/ingest.py --limit 500

# Stage 3 — CLAP embeddings → Milvus + Neo4j
cd enrichment && python3 -m venv .venv && bash install.sh
cd .. && enrichment/.venv/bin/python enrichment/enrich.py

# Stage 4 — Semantic API
cd api && python3 -m venv .venv && bash install.sh
cd .. && api/.venv/bin/uvicorn api.main:app --port 8010

# Stage 5 — Recommender Engine (with the API running)
cd recommender && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cd .. && recommender/.venv/bin/python recommender/recommend.py
```

`enrichment/install.sh` and `api/install.sh` exist (rather than a plain
`pip install -r requirements.txt`) because of real dependency-resolution
issues on Python 3.12 — see each stage's doc for the specifics.

## Tests

```bash
enrichment/.venv/bin/pip install -r recommender/requirements.txt \
    fastapi==0.115.0 "uvicorn[standard]==0.32.0" httpx==0.27.2
enrichment/.venv/bin/pytest tests/ -v
```

Runs all 32 tests across stages 2–6 against the live stack. The Semantic
API test fixture launches its own subprocess on a free port, so nothing
needs to be started manually first.

## Stack

PostgreSQL, MinIO, Milvus, Neo4j, FastAPI, LAION-CLAP, Docker Compose.
Kafka and Redis are present but unused in this track (see Architecture).
