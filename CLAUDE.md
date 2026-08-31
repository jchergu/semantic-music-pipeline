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
- Behavioral-event ingestion is a SEPARATE SERVICE, not part of the Semantic
  API. The Semantic API stays read-only — that property is frozen in
  `contracts/semantic-api-v1.json` and must not be reversed without an
  explicit decision. The ingestion service produces to the
  `behavioral-events` Kafka topic, separate from the `media-stream` topic
  per the Kafka topics bullet above. **Decision A, 2026-08-30.**
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
  shape). `contracts/semantic-api-v1.json` — the frozen OpenAPI export —
  was added 2026-08-28 ahead of 8.2 work starting; see
  `contracts/README.md` for what's in scope today and don't populate the
  rest speculatively.

## Use case taxonomy

| Use case | Mode | Type | Status |
|---|---|---|---|
| 8.1 | Batch | Reactive | **Complete** — build order done, results evaluated |
| 8.2 | Streaming | Reactive | Not started |
| 8.3 | Streaming | Proactive | Not started — auto-tagging reused as live classifier only, no live writes to Neo4j |

8.1 / 8.2 / 8.3 are independent modules: no shared runtime state, no shared
code paths between them. They are independent OF EACH OTHER, not of the
platform (L1, L2, Semantic API), which is shared by design.

The Kafka consumer that reads behavioral events and maintains session
state in Redis is PLATFORM-owned (build order stages 9-10 below), not
8.2-owned — 8.3 is also streaming and will reuse the same consumer. This
does not violate the independence rule above: 8.1/8.2/8.3 are independent
of each other, not of the platform. A future session should not refuse to
extract shared streaming components on independence grounds. **Decision
B, 2026-08-30.**

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
- **2026-08-28**: a read-only recon pass (post-rename health check, ahead
  of starting 8.2) confirmed the platform has no streaming-reactive
  capability yet — see Platform build order (stages 7-11) below.

### Platform build order (stages 7-11 — required before 8.2, all done)

8.2 (streaming, reactive) needed platform capabilities beyond stages 1-4.
These were platform stages, not 8.2-specific work — Kafka wiring, Flink,
Redis, an events schema, and an ingestion path are shared infra any
streaming use case would need, the same way stages 1-4 are shared by
every use case. The 2026-08-28 recon confirmed none of it existed yet;
stages 7-11 have since all been built and verified (see below). This
closes the platform build order — every platform prerequisite 8.2 needs
now exists. **8.2 itself remains out of scope and not started** — per
this file's header, do not build 8.2/8.3 infra unless explicitly asked;
completing the platform's own prerequisites is not the same thing as
starting 8.2.

7. Kafka topics + producer/consumer wiring (`media-stream` and
   `behavioral-events` topics are separate topics, per the L1
   architecture section above — none of that exists in code today)
8. Flink provisioning (add it to `docker-compose.yml` and stand up the
   service — it isn't there at all right now)
9. Redis session cache (wire up a client and a real cache path —
   currently boots and nothing touches it; platform-owned per Decision B
   above, not 8.2-owned, since 8.3 reuses the same consumer)
10. Postgres sessions/events schema (no sessions or events tables exist
    today — only `tracks`, from `platform/ingestion/schema.sql`, and
    8.1's own `recommendations` table; read by the platform-owned
    consumer from Decision B above, not by 8.2 code directly)
11. Event ingestion path — a separate service per Decision A above, not
    an addition to the Semantic API's endpoint list. The Semantic API
    stays read-only; a behavioral event has nowhere to land today
    because that ingestion service doesn't exist yet, not because the
    API needs a write path.

**Stage 7** (Kafka topics + producer/consumer wiring) verified working as
of 2026-08-30 — `media-stream` and `behavioral-events` topics created
idempotently on the existing compose broker (`platform/streaming/topics.py`),
plus a minimal synchronous producer/consumer
(`platform/streaming/producer.py`, `platform/streaming/consumer.py`)
proving real round-trip delivery over the live broker, not mocked. Uses
`confluent-kafka==2.5.3` — verified to resolve a prebuilt wheel for this
environment, so no `install.sh` was needed (unlike enrichment/semantic_api).
See `docs/platform/stage7-kafka.md`. All 3 tests in
`tests/test_stage7_kafka.py` pass (35/35 total across stages 2-4+7 and
8.1's stages 5-6). No session state, Redis, ingestion service, or 8.2
code was touched at this point in the build order — see stages 8-11 below
for Flink provisioning, the Redis session cache, Postgres durability, and
the event ingestion service, done separately.

**Stage 8** (Flink provisioning) verified working as of 2026-08-31 — a
JobManager and TaskManager (`flink:1.19.1-scala_2.12-java11`) added to
`docker-compose.yml`, both healthy/registered on the live compose network:
the JobManager's REST API answers `/config` and the TaskManager shows up
in `/taskmanagers` with 2 free slots. Provisioning only, per the stage 7-8
build-order guardrail above — no Flink job was written or deployed, and
nothing yet reads from or writes to this cluster. See
`docs/platform/stage8-flink.md`. Both tests in `tests/test_stage8_flink.py`
pass (37/37 total across stages 2-4+7-8 and 8.1's stages 5-6). No Redis,
Postgres schema, ingestion service, or 8.2 code was touched at this point
in the build order — see stages 9-11 below for Redis, Postgres, and the
event ingestion service, done separately.

**Stage 9** (Redis session cache) verified working as of 2026-08-31 — the
Redis half of Decision B's platform-owned consumer: a session-state module
(`platform/streaming/session_state.py`, `record_event`/`get_session_events`
with a sliding 30-min TTL) and a Kafka → Redis consumer
(`platform/streaming/session_consumer.py::consume_and_cache_one`) that
reads `behavioral-events` and caches each event under its `session_id`.
Uses `redis==5.0.8` (sync client, matches this package's plain-synchronous
style). Decision B explicitly says this consumer spans stages 9-10 — this
stage is Redis only; stage 10 later adds Postgres durability on top of the
same consumer. Event shape is ad hoc (a JSON object with a `session_id`
key), not a frozen contract, same stance stage 7 took with its plain
string payloads. See `docs/platform/stage9-redis.md`. All 3 tests in
`tests/test_stage9_redis.py` pass (40/40 total across stages 2-4+7-9 and
8.1's stages 5-6). No Postgres schema, ingestion service, or 8.2 code was
touched at this point in the build order — see stages 10-11 below for
Postgres durability and the event ingestion service, done separately.

**Stage 10** (Postgres sessions/events schema) verified working as of
2026-08-31 — the Postgres durability half of Decision B's platform-owned
consumer: a `sessions`/`events` schema
(`platform/streaming/schema.sql`), applied idempotently by
`platform/streaming/session_store.py::apply_schema`, and the *same*
Kafka → Redis consumer from stage 9
(`session_consumer.py::consume_and_cache_one`) extended with an optional
`pg_conn` parameter that also calls `session_store.persist_event` — not a
new consumer. Uses `psycopg2-binary==2.9.9`, matching every other
Postgres-touching module in this repo. Decision B's "consumer spans
stages 9-10" is now complete: one poll updates Redis (session state) and
Postgres (durability) together. See
`docs/platform/stage10-postgres-events.md`. All 3 tests in
`tests/test_stage10_postgres_events.py` pass (43/43 total across stages
2-4+7-10 and 8.1's stages 5-6). No ingestion service or 8.2 code was
touched at this point in the build order — see stage 11 below for the
event ingestion service, done separately.

**Stage 11** (Event ingestion path) verified working as of 2026-08-31 — a
new FastAPI service, separate from the Semantic API per Decision A
(`platform/event_ingestion/main.py`): `POST /events` validates a
behavioral event and produces it onto `behavioral-events` via stage 7's
`streaming.producer.produce()`, closing the loop stages 7-10 opened with
manually-produced test messages. Event shape stays ad hoc
(`session_id`/`event_type`/`track_id` plus arbitrary extra fields), not a
frozen contract — `contracts/` stays empty until 8.2 is a second
independent producer/consumer. See
`docs/platform/stage11-event-ingestion.md`. All 3 tests in
`tests/test_stage11_event_ingestion.py` pass, including an end-to-end
test driving the full HTTP → Kafka → Redis + Postgres chain through the
platform-owned consumer from stages 9-10 (46/46 total across stages
2-4+7-11 and 8.1's stages 5-6). No 8.2 code was touched. **This closes
the platform build order (stages 1-11)** — every prerequisite 8.2 needs
now exists; 8.2 itself is a separate, not-yet-started piece of work.

## Stack (all open-source, self-hostable)

**Provisioned and used** — running in `docker-compose.yml`, with real code
paths reading/writing them today: PostgreSQL — `tracks` (stage 2) and
8.1's own `recommendations` table, plus `sessions`/`events`
(`platform/streaming/schema.sql`, stage 10) for Decision B's
platform-owned consumer. Neo4j, Milvus, MinIO. Kafka (+ Zookeeper) — as
of stage 7, `platform/streaming/` creates its two topics and can
produce/consume round-trip; as of stage 11,
`platform/event_ingestion/main.py` is a real producer onto
`behavioral-events` over HTTP. Redis — as of stage 9,
`platform/streaming/session_state.py` and `session_consumer.py` cache
behavioral events per session with a sliding TTL; as of stage 10 the same
consumer also persists durably to Postgres. Decision B's "Kafka consumer
that reads behavioral events and maintains session state in Redis,"
spanning stages 9-10, is complete on both stores, and stage 11 gives the
whole chain (Kafka → Redis + Postgres) its first real producer. All of
this is still platform-owned plumbing with no 8.2/8.3 consumer yet — see
the Build order note above.

**Provisioned but unused** — running in `docker-compose.yml`, boots
healthy, but no code anywhere in the repo produces to, consumes from, or
connects to it. Reserved for 8.2/8.3, planned for streaming enrichment:
- Flink — a JobManager + TaskManager run in `docker-compose.yml` as of
  stage 8 and are confirmed healthy/registered with each other, but no
  code anywhere submits a job to them yet

Also in the stack, used by the platform build order: CLAP,
Chromaprint/AcousticID, Librosa, FastAPI, Docker Compose. Essentia has no
footprint anywhere in the repo (not in any requirements.txt, not
imported) — dropped from this list rather than repeated here inaccurately.

Docker Compose's project name is pinned to `8-1-batch-reactive` via a
top-level `name:` key in `docker-compose.yml`, even though the repo was
renamed to `semantic-music-pipeline`. Compose derives its volume-name
prefix from the project name, and the existing 411-track dataset lives in
`8-1-batch-reactive_*` volumes (created before the rename). Renaming the
project would silently start the stack on fresh, empty volumes instead of
the real data — not worth it for a cosmetic match, so the pin stays.

## Commands

Bring the stack up/down:

```bash
docker compose up -d   # Postgres, MinIO, Neo4j, Milvus + etcd/MinIO deps, Kafka + Zookeeper, Redis, Flink
docker compose ps      # verify all services healthy before moving to next stage
docker compose down    # stop the stack (add -v to also wipe volumes)
```

Run the full test suite (46 tests, platform stages 2-4+7-11 + 8.1 stages
5-6) against the live stack — uses `platform/enrichment/.venv` because it
already carries psycopg2/httpx/matplotlib/pytest (stage 8's Flink and
stage 10's Postgres tests need nothing beyond that venv's defaults — the
venv already has `psycopg2-binary==2.9.9`, same pin
`platform/streaming/requirements.txt` declares); the extra installs pull
in what stage 5/6's, stage 7's, and stage 9's own tests need that that
venv doesn't have by default (`-r platform/streaming/requirements.txt`
also brings in `redis==5.0.8` for stage 9). Stage 11's own
`platform/event_ingestion/requirements.txt` needs nothing beyond
fastapi/uvicorn/confluent-kafka/python-dotenv, all already covered by the
installs below:

```bash
platform/enrichment/.venv/bin/python -m pip install -r usecases/8_1_batch_reactive/recommender/requirements.txt \
    -r platform/streaming/requirements.txt \
    fastapi==0.115.0 "uvicorn[standard]==0.32.0" httpx==0.27.2
platform/enrichment/.venv/bin/python -m pytest -v
```

Run only the platform tests (no use case): `platform/enrichment/.venv/bin/python -m pytest tests/ -v`

Run only 8.1's own tests: `platform/enrichment/.venv/bin/python -m pytest usecases/8_1_batch_reactive/tests/ -v`

Run the stage 11 event ingestion service standalone (same
`--app-dir platform` pattern as the stage 4 Semantic API):

```bash
platform/enrichment/.venv/bin/python -m uvicorn event_ingestion.main:app \
    --app-dir platform --port 8020
```

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
- `thesis/` is prose (the thesis draft, split into `thesis/NN-*.md` chapter
  files, built via `thesis/Makefile`). It is off-limits during code
  sessions: don't touch it as part of a refactor, formatting pass, or any
  code-focused change. Only edit it when the task is explicitly about the
  thesis text itself.
