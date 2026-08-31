# Semantic-Aware Multimodal Music Pipelining

A Kappa-style, three-layer streaming/batch pipeline for music semantic
enrichment and recommendation, built as part of a Master's thesis
(University of Bologna). The repository is a shared **platform** (L1
ingestion, L2 enrichment, L3 Semantic API) plus independent **use cases**
that each consume it without sharing runtime state or code paths with one
another:

| Use case | Mode | Type | Status |
|---|---|---|---|
| 8.1 | Batch | Reactive | **Complete** — see below |
| 8.2 | Streaming | Reactive | Not started |
| 8.3 | Streaming | Proactive | Not started |

**8.1 status: build order complete.** 411 tracks ingested, embedded, and
graphed; 4,110 recommendations generated; 32/32 automated tests passing.
See [`reports/results.html`](reports/results.html) for the full results
writeup (architecture, embedding visualization, worked recommendation
example) — open it directly in a browser, no server required — and
[`docs/results-evaluation.md`](docs/results-evaluation.md) for the
quantitative/qualitative evaluation of recommendation quality that sits on
top of it.

## Architecture

Three platform layers, each independently testable, shared by every use
case:

- **L1 — Data Ingestion.** Raw audio → object storage (MinIO); track
  metadata → PostgreSQL (system of record); Chromaprint/AcoustID
  fingerprinting with best-effort MusicBrainz linkage.
- **L2 — Semantic Enrichment.** CLAP audio embeddings → Milvus (vector
  index); a Track/Artist/Genre knowledge graph → Neo4j.
- **L3 — Application.** A single shared FastAPI Semantic API over all
  three stores — designed to be reused by every use case's own consumers
  (8.1's Recommender Engine now; similarity search, auto-tagging, playlist
  generation later) without duplicating store access.

8.1's Recommender Engine only talks to the Semantic API over HTTP — it
never queries Postgres, Milvus, or Neo4j directly for track content. See
`reports/results.html` for a diagram.

Kafka and Redis are provisioned in the Docker stack but intentionally left
unwired by the platform and by 8.1 — they're reserved for 8.2 (streaming,
reactive) and 8.3 (streaming, proactive), which are independent modules:
they share the platform (L1/L2/L3) but not each other, and not 8.1.

`contracts/` holds versioned interface artifacts shared between the
platform and its use cases (OpenAPI export, Kafka topic schemas, the
recommendation response shape) — empty until a second independent
consumer (8.2) exists and that contract stops being implicit. See
[`contracts/README.md`](contracts/README.md).

## Build order

**Platform (stages 1-4, shared by every use case):**

| # | Stage | Code | Docs |
|---|---|---|---|
| 1 | Docker Compose environment | `docker-compose.yml` | — |
| 2 | Seed dataset ingestion (Jamendo → Postgres/MinIO) | `platform/ingestion/` | [`docs/platform/stage2-ingestion.md`](docs/platform/stage2-ingestion.md) |
| 3 | L2 enrichment (CLAP → Milvus, Neo4j) | `platform/enrichment/` | [`docs/platform/stage3-enrichment.md`](docs/platform/stage3-enrichment.md) |
| 4 | Semantic API (FastAPI) | `platform/semantic_api/` | [`docs/platform/stage4-semantic-api.md`](docs/platform/stage4-semantic-api.md) |

**8.1 — batch, reactive (stages 5-6, this use case only):**

| # | Stage | Code | Docs |
|---|---|---|---|
| 5 | Recommender Engine | `usecases/8_1_batch_reactive/recommender/` | [`usecases/8_1_batch_reactive/docs/uc81-recommender.md`](usecases/8_1_batch_reactive/docs/uc81-recommender.md) |
| 6 | End-to-end test | `usecases/8_1_batch_reactive/tests/test_uc81_e2e.py` | [`usecases/8_1_batch_reactive/docs/uc81-e2e.md`](usecases/8_1_batch_reactive/docs/uc81-e2e.md) |

8.2 and 8.3 will each get their own stages 5-6 (or however many they need)
under `usecases/8_2_streaming_reactive/` and
`usecases/8_3_streaming_proactive/` respectively, reusing platform stages
1-4 unchanged.

## Dataset

411 real, Creative-Commons-licensed tracks pulled from the Jamendo API —
deliberately kept to a 200–500 track seed (not a full corpus) to keep CLAP
inference feasible on a local CPU. See `docs/platform/stage2-ingestion.md` for why
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
cd platform/ingestion && python3 -m venv .venv && .venv/bin/python -m pip install -r requirements.txt
cd ../.. && platform/ingestion/.venv/bin/python platform/ingestion/ingest.py --limit 500

# Stage 3 — CLAP embeddings → Milvus + Neo4j
cd platform/enrichment && python3 -m venv .venv && bash install.sh
cd ../.. && platform/enrichment/.venv/bin/python platform/enrichment/enrich.py

# Stage 4 — Semantic API
cd platform/semantic_api && python3 -m venv .venv && bash install.sh
cd ../.. && platform/semantic_api/.venv/bin/python -m uvicorn semantic_api.main:app \
    --app-dir platform --port 8010

# Stage 5 (8.1) — Recommender Engine (with the API running)
cd usecases/8_1_batch_reactive/recommender && python3 -m venv .venv && .venv/bin/python -m pip install -r requirements.txt
cd ../../.. && usecases/8_1_batch_reactive/recommender/.venv/bin/python usecases/8_1_batch_reactive/recommender/recommend.py
```

`platform/enrichment/install.sh` and `platform/semantic_api/install.sh` exist
(rather than a plain `pip install -r requirements.txt`) because of real
dependency-resolution issues on Python 3.12 — see each stage's doc for the
specifics.

Note the `-m pip` / `-m uvicorn` / (below) `-m pytest` forms rather than the
`.venv/bin/pip` / `.venv/bin/uvicorn` / `.venv/bin/pytest` console-script
wrappers: those wrappers hardcode an absolute shebang path back to the venv's
original location at creation time, which breaks if the venv directory is
ever moved (as happened in the platform/usecases restructuring). Invoking the
interpreter directly with `-m` sidesteps that.

## Tests

```bash
platform/enrichment/.venv/bin/python -m pip install -r usecases/8_1_batch_reactive/recommender/requirements.txt \
    fastapi==0.115.0 "uvicorn[standard]==0.32.0" httpx==0.27.2
platform/enrichment/.venv/bin/python -m pytest -v
```

Runs all 32 tests — platform stages 2-4 plus 8.1's stages 5-6 — against
the live stack. The Semantic API test fixture launches its own subprocess
on a free port, so nothing needs to be started manually first. To run only
the platform tests (no use case): `pytest tests/ -v`. To run only 8.1's
own tests: `pytest usecases/8_1_batch_reactive/tests/ -v`.

## Reports

Standalone scripts in `reports/`, each writing its own subdirectory (or,
for `results_evaluation.py`, using the shared `reports/_db.py` connection
helper) plus feeding data into `reports/results.html`:

- `stage2_metrics.py` → `reports/stage2/` — ingestion metrics/plots
- `stage3_embedding_projection.py` → `reports/stage3/` — CLAP embedding
  t-SNE projection
- `uc81_results.py` → `reports/uc81/` — 8.1 recommendation score
  distribution + one worked ranking example
- `results_evaluation.py` → `reports/results_evaluation/` — recommendation
  quality evaluation backing `docs/results-evaluation.md` (same-artist /
  genre-overlap rates, catalog-skew check, 8-seed qualitative spot-check)

## Stack

PostgreSQL, MinIO, Milvus, Neo4j, FastAPI, LAION-CLAP, Docker Compose.
Kafka, Flink, and Redis are present but unused by the platform and by 8.1
(see Architecture) — reserved for 8.2/8.3.
