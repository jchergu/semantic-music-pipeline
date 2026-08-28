# Project: Semantic-Aware Multimodal Music Pipelining

Master's thesis (Bologna). Kappa-style streaming pipeline, 3 layers, with a
Recommender Engine as the Layer 3 demo for the first use case. **8.1
(batch, reactive) is complete** — see Build order below. 8.2 is the
logical next step but has not been started; do not build 8.2/8.3 infra
unless explicitly asked — they are separate modules, not shared code
paths with each other or with 8.1.

This file is tracked in git (as of the commit that rescoped it to the
whole project) and is in scope for code review like any other file in the
repo — it is not exempt just because it's Claude Code's own working
notes.

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

### Platform contracts

- **Milvus connection alias**: every independent consumer of the Semantic
  API's Milvus collection must use its own `connections.connect(alias=...)`
  value — never share pymilvus's default `"default"` alias across
  processes/fixtures that manage their own connection lifecycle. The
  Semantic API uses alias `"api"` (`platform/semantic_api/dependencies.py`);
  `tests/conftest.py`'s `milvus_collection` fixture uses `"default"`. This
  is load-bearing, not stylistic: sharing an alias caused a real bug where
  one test file's connection teardown silently broke Milvus for every test
  running after it in the same session (see
  `usecases/8_1_batch_reactive/docs/uc81-e2e.md`). Any new 8.2/8.3
  consumer that opens its own Milvus connection needs its own alias.
- `contracts/` holds the versioned interface artifacts shared between the
  platform and its use cases once a second independent consumer exists
  (frozen OpenAPI export, Kafka topic schemas, the recommendation response
  shape). Empty today — see `contracts/README.md` for why, and don't
  populate it speculatively.

## Use case taxonomy

| Use case | Mode | Type | Status |
|---|---|---|---|
| 8.1 | Batch | Reactive | **Complete** — build order done, results evaluated |
| 8.2 | Streaming | Reactive | Not started |
| 8.3 | Streaming | Proactive | Not started — auto-tagging reused as live classifier only, no live writes to Neo4j |

8.1 / 8.2 / 8.3 are independent modules: no shared runtime state, no shared
code paths between them. They are independent OF EACH OTHER, not of the
platform (L1, L2, Semantic API), which is shared by design.

## Dataset constraints

- Seed dataset: 200–500 tracks (not full corpora). This is intentional, to
  keep CLAP inference feasible on local CPU. Do not suggest scaling this up.
- 8.1 used this dataset already (411 tracks, Jamendo). A future 8.2/8.3 use
  case reuses the same ingested/enriched platform data — do not re-ingest.

## Build order

The build order has two tiers: the platform (shared, built once) and each
use case's own stages on top of it (independent per use case).

### Platform build order (stages 1-4 — shared by every use case, done)

1. Docker Compose environment (Postgres, MinIO, Neo4j, Milvus, Kafka, Redis)
2. Seed dataset ingestion → PostgreSQL/MinIO
3. L2 enrichment via CLAP → Milvus and Neo4j
4. Semantic API in FastAPI

Do not skip ahead to a later stage until the current one is verified
working. This still applies to 8.2/8.3's own stages once they start —
they build on top of the platform stages above, which do not need
re-verifying per use case.

**Stage 1** (Docker Compose environment) verified working as of
2026-08-06 — all 9 containers report healthy.

**Stage 2** (Seed dataset ingestion) verified working as of 2026-08-06 —
411 tracks ingested from Jamendo into PostgreSQL/MinIO (target was 500;
see `docs/platform/stage2-ingestion.md` for why 411 is within spec and
considered done). All 5 tests in `tests/test_stage2_ingestion.py` pass.
Metrics/plots in `reports/stage2/`.

**Stage 3** (L2 enrichment: CLAP → Milvus, Neo4j) verified working as of
2026-08-07 — all 411 tracks embedded (laion_clap, 512-dim) and indexed in
Milvus, graph written to Neo4j (411 Track / 201 Artist / 77 Genre nodes).
See `docs/platform/stage3-enrichment.md` for the run outcome and a
packaging note on `platform/enrichment/install.sh` (laion-clap's declared
deps don't install cleanly via a plain `pip install -r requirements.txt`
on Python 3.12). All 6 tests in `tests/test_stage3_enrichment.py` pass.
Re-verified 2026-08-07 with a full `--force` re-enrichment run; caught and
fixed a Milvus test flakiness bug in the process (see doc).

**Stage 4** (Semantic API, FastAPI) verified working as of 2026-08-07 —
one shared read-only service over Postgres/Milvus/Neo4j: `/health`,
`/tracks/{id}`, `/tracks/{id}/similar`, `/tracks/{id}/graph`,
`/artists/{name}/tracks`, `/genres/{name}/tracks`. See
`docs/platform/stage4-semantic-api.md`. All 8 tests in
`tests/test_stage4_api.py` pass (19/19 total across stages 2+3+4).

### 8.1 (batch, reactive) build order (stages 5-6 — this use case only)

5. Recommender Engine modules (trigger handler, context builder, ranking)
6. End-to-end test

**Stage 5** (Recommender Engine: trigger handler, context builder, ranking)
verified working as of 2026-08-07 — seed-track-only trigger model (no
fabricated user/session identity), consumes the Semantic API over HTTP,
combines CLAP similarity + genre-sibling + same-artist signals into a
scored ranked list, writes to a new Postgres `recommendations` table. Full
batch run: 411/411 seeds, 4110 rows, zero skips, ~8.8s wall-clock. See
`usecases/8_1_batch_reactive/docs/uc81-recommender.md` for the ranking
formula and its dataset-grounded weight justification. All 8 tests in
`usecases/8_1_batch_reactive/tests/test_uc81_recommender.py` pass (27/27
total across stages 2+3+4+5).

**Stage 6** (end-to-end test) verified working as of 2026-08-07 — lineage
test tracing 3 golden tracks through every layer, run against the live
already-populated stack (not a destructive from-scratch rebuild; that
sequence is documented as a runbook in
`usecases/8_1_batch_reactive/docs/uc81-e2e.md` for on-demand use, e.g. a
thesis defense demo, but not exercised automatically). Found and fixed a
real bug in the process: `platform/semantic_api/dependencies.py` shared
pymilvus's default connection alias (`"default"`) with `tests/conftest.py`'s
`milvus_collection` fixture, so `test_stage4_api.py`'s in-process
`TestClient` teardown silently broke Milvus for every test running after
it in the same session — fixed by giving the API its own alias (`"api"`,
see Platform contracts above). All 5 tests in
`usecases/8_1_batch_reactive/tests/test_uc81_e2e.py` pass (32/32 total
across all six stages).

**8.1 (batch, reactive) build order is complete** — see the capstone
summary at the bottom of `usecases/8_1_batch_reactive/docs/uc81-e2e.md`.

### Post-build-order work (8.1)

Not a new stage — a results/evaluation write-up and a monorepo
restructuring done after the build order closed:

- **2026-08-27**: repository restructured into `platform/` (shared L1/L2/L3
  code) and `usecases/<name>/` (per-use-case code), a move-only refactor
  followed by an import/build-path fix-up commit.
- **2026-08-28**: `docs/results-evaluation.md` + `reports/results_evaluation.py`
  added — goes beyond "the pipeline runs end to end" (stage 6's claim) to
  ask "are the recommendations any good": same-artist/genre-overlap rates,
  a catalog-size skew check on the ranking's artist boost, and an 8-seed
  qualitative spot-check against the live 411-seed/4110-row
  `recommendations` table. Writes `reports/results_evaluation/results.json`
  and feeds a new section of `reports/results.html`.
  `reports/_db.py` was added alongside it as the shared Postgres
  connection + `.env`-loading helper for scripts in `reports/`
  (`results_evaluation.py` and `reports/uc81_results.py` both use it —
  don't hand-roll a new `psycopg2.connect()` in a new `reports/` script,
  import from `_db` instead).
- Same day: a code-review pass on that work found and fixed 2 latent crash
  bugs in `results_evaluation.py` (an unguarded `float(None)` and an
  unguarded dict lookup, both reachable only on a future re-run against a
  different dataset shape, not on the current data) plus several stale
  post-move paths and reuse cleanups.
- Same day: this file itself was found to be gitignored and untracked
  since 2026-08-07 (see Working agreement) — it had drifted out of sync
  with the monorepo move (wrong paths, a wrong test count) because no
  review process ever saw it. Re-tracked, corrected, and rescoped from
  "8.1 project" framing to whole-project framing in the process.

## Stack (all open-source, self-hostable)

PostgreSQL, Neo4j, Milvus, MinIO, Kafka, Flink, Redis, Docker Compose, CLAP,
Chromaprint/AcousticID, Librosa, Essentia, FastAPI.

## Commands

Bring the stack up/down:

```bash
docker compose up -d   # Postgres, MinIO, Neo4j, Milvus + etcd/MinIO deps, Kafka + Zookeeper, Redis
docker compose ps      # verify all services healthy before moving to next stage
docker compose down    # stop the stack (add -v to also wipe volumes)
```

Run the full test suite (32 tests, platform stages 2-4 + 8.1 stages 5-6)
against the live stack — uses `platform/enrichment/.venv` because it
already carries psycopg2/httpx/matplotlib/pytest; the extra installs pull
in what stage 5/6's own tests need that that venv doesn't have by default:

```bash
platform/enrichment/.venv/bin/python -m pip install -r usecases/8_1_batch_reactive/recommender/requirements.txt \
    fastapi==0.115.0 "uvicorn[standard]==0.32.0" httpx==0.27.2
platform/enrichment/.venv/bin/python -m pytest -v
```

Run only the platform tests (no use case): `platform/enrichment/.venv/bin/python -m pytest tests/ -v`

Run only 8.1's own tests: `platform/enrichment/.venv/bin/python -m pytest usecases/8_1_batch_reactive/tests/ -v`

Regenerate the reports (each needs the stack up; `uc81_results.py` also
needs the Semantic API running — see Quickstart in README.md):

```bash
platform/enrichment/.venv/bin/python reports/stage2_metrics.py
platform/enrichment/.venv/bin/python reports/stage3_embedding_projection.py
platform/enrichment/.venv/bin/python reports/uc81_results.py
platform/enrichment/.venv/bin/python reports/results_evaluation.py
```

No lint command exists in this repo yet — don't invent one; add it here
once one is actually configured.

## Working agreement

- Propose a plan before editing more than one file. Wait for confirmation.
- One build-order stage per session. Verify it works before moving on.
- If something in this file conflicts with what I ask you to do in a message,
  my message wins — but flag the conflict first, don't silently override.
- This file is tracked and reviewable (see header). Keep it in sync with
  reality as part of any change that moves/renames something it
  references — don't let it drift again.
