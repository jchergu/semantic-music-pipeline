# Project: Semantic-Aware Multimodal Music Pipelining

Master's thesis (Bologna). Kappa-style streaming pipeline, 3 layers, with a
Recommender Engine as the Layer 3 demo for the first use case. **8.1
(batch, reactive) is complete** — see Build order below. **8.2 (streaming,
reactive) is in progress** — stages 13-14 and 15A/15B/15C/15C.2 are
done (session profile centroid, recommendation refresh loop, bug closure,
simulator late-event support, and the `eval/8_2` harness with **all seven
metrics** of its signed-off spec), **stage 16** (both refresh daemons
+ `session_api`, Decision E) and **15D** (failure injection + the TTL fix
stage 16 deferred) are done; **15E** (write chapter 6's 8.2 half) is all
that remains. Do not build further 8.2/8.3 work unless explicitly asked — they
are separate modules, not shared code paths with each other or with 8.1.

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
- Recommendation *delivery* for the streaming use cases is likewise a
  SEPARATE SERVICE (`platform/session_api/`, stage 16), not endpoints added
  to the Semantic API — the mirror image of Decision A, and platform-owned
  for the same reason Decision B gives (8.3 reads session state the same
  way). Three grounds: `contracts/semantic-api-v1.json` is frozen and
  adding endpoints would force a contract revision that buys nothing; the
  Semantic API has no Redis dependency today and session recommendations
  are per-session derived state, not semantic content (see the Redis bullet
  below); and events already flow IN through their own small service, so
  they should flow OUT through one. Reads `session:{id}:recs` and
  `session:{id}:profile`; it does not compute recommendations — that stays
  `streaming/recommendation_refresh.py`'s job, run continuously by
  `streaming/refresh_daemon.py` (the persistent-consumer gap flagged since
  stage 12 and again in stage 15C).

  **Delivery is request/response, NOT WebSocket**, despite what
  `thesis/06-streaming-use-cases.md` §6.1 currently says — that line is to
  be rewritten in a thesis session, not honored in code. 8.2 vs 8.3 is
  *explicit query vs system-inferred suggestion*, not pull vs push; a
  pushed recommendation is behaviourally proactive, so building push for
  8.2 would blur the exact distinction the use-case taxonomy rests on.
  Push belongs to 8.3 or future work. **Decision E, 2026-09-06.**
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
| 8.2 | Streaming | Reactive | In progress — stages 13-14 + 15A/15B/15C/15C.2 + 16 + 15D done (`eval/8_2` complete, metrics 1-8; daemons + `session_api` delivering; derived-state TTLs closed); only 15E (thesis §6.1) remains |
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

A second consumer of `behavioral-events` is coming — a future Flink job
(stage 13) computing the session semantic profile centroid — so the
ownership boundary between it and the existing stages-9-10 consumer is
fixed now, before that job exists: the existing consumer owns RAW state
(the Postgres `events` log, `platform/streaming/session_store.py`; raw
session events in Redis under `session:{id}:events`,
`platform/streaming/session_state.py`); the future Flink job will own
DERIVED state (the weighted centroid vector, Redis `session:{id}:profile`
— reserved via `session_state.py::profile_key()`, not read or written by
anything yet). The two consumers use separate Kafka consumer groups; the
existing consumer's group id is no longer left to whatever string a
caller happens to make up — `streaming/config.py::SESSION_CONSUMER_GROUP_ID`
is the canonical, documented value a real (non-test) deployment should
pass explicitly (still a required parameter, not a default, on
`consume_and_cache_one`/`_many` — tests keep using their own throwaway
group ids for isolation). `session_consumer.py::rebuild_session_state()`
replays the durable Postgres log back into Redis — recovery after a Redis
flush/restart, proven by
`tests/test_stage10_postgres_events.py::test_rebuild_session_state_recovers_from_postgres_after_redis_flush`.
**Decision C, 2026-08-31.**

Stage 14 (8.2's recommendation refresh loop) needs to reuse
`ranking.score_recommendations()` — the pure scoring function 8.1's stage
5 built — without reimplementing it. It lived inside
`usecases/8_1_batch_reactive/recommender/`, a use-case-owned directory;
CLAUDE.md's independence rule is between use cases, not between a use
case and the platform, so this is the same move Decision B already made
for the streaming consumer. Extracted verbatim to
`platform/scoring/ranking.py` (`git mv`, history preserved; named
`scoring`, not `recommender`, to avoid colliding with
`usecases/8_1_batch_reactive/recommender/`'s own top-level package name —
stage 14 needs both directories on `sys.path` in the same process). Only
two import sites needed fixing
(`usecases/8_1_batch_reactive/recommender/recommend.py`,
`usecases/8_1_batch_reactive/tests/test_uc81_recommender.py`); all 13 of
8.1's own tests pass unchanged after the move. **Decision D, 2026-08-31.**

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
- **2026-08-31**: `eval/8_1/` — a broader, harder-nosed
  **systems and behavior** evaluation pack, distinct from
  `reports/results_evaluation.py`'s "are the recommendations good" framing
  above. There is no ground truth and no real users for this dataset, so
  it computes **no accuracy metric** (no precision@k, recall@k, NDCG) —
  that would require relevance labels that don't exist. Read-only against
  the existing 411-seed/4110-row `recommendations` table and the existing
  stores (Postgres/Milvus/Neo4j); `recommend.py` is never re-run. Six
  metrics: (1) signal contribution — which of similarity/genre-sibling/
  same-artist explains each recommendation, reconstructed from the stores
  since the `recommendations` table itself only persists the final score,
  not per-signal flags; (2) catalog coverage and its Gini coefficient;
  (3) intra-list diversity (mean pairwise CLAP cosine distance per top-10);
  (4) latency per stage (Milvus ANN / Neo4j / ranking), measured fresh
  against the live stack since no per-stage timing was ever recorded
  during the original batch run — reported separately in
  `eval/8_1/latency.json`, not folded into the deterministic
  `eval/8_1/results.json`; (5) KG connectivity (Track/Artist/Genre degree
  distribution, zero-genre-sibling seed count); (6) failure/edge cases
  (seeds with fewer than 10 recommendations). Outputs:
  `eval/8_1/results.json`, `eval/8_1/latency.json`, `eval/8_1/tables.md`,
  `eval/8_1/figures/*.png`. Entrypoint: `python -m eval.8_1.run` (via
  `platform/enrichment/.venv`, which already has every package this needs
  — pymilvus/neo4j/psycopg2/matplotlib/numpy — confirmed by direct import
  check, no new installs required). Own Milvus connection alias
  (`"eval"`), per the Platform contracts rule below. Verified working —
  18 new unit tests (all pure functions, no live services) pass, plus the
  live run against the full 411-seed/4110-row dataset (64/64 total tests
  now, `pytest.ini`'s `testpaths` extended to include `eval`).
  `results.json` confirmed byte-identical across two consecutive runs.
  Real numbers: 97.08% catalog coverage (Gini 0.3942), mean intra-list
  diversity 0.2562, zero seeds with fewer than 10 recommendations, 59
  seeds with zero genre siblings. One genuine finding surfaced building
  this, not just a metric readout: reconstructing "genre_sibling" per row
  first via the complete `tracks.genre_tags` overlap produced a ~39%
  self-inconsistency rate against the stored `score` — traced to
  `/tracks/{id}/graph`'s `related_by_genre` being capped at 10 candidates
  with **no `ORDER BY`** (`platform/semantic_api/main.py`), so large
  genres (mean ~9.9 tracks/genre, but skewed — median is only 3) lose most
  of their true siblings to an arbitrary cut, not a principled one.
  Reconstructing the exact capped candidate set instead
  (`kg_connectivity.capped_genre_sibling_ids`, same Cypher, batched)
  brought inconsistency to exactly 0/4110 and turned the finding into a
  quantified one: of 2,200 rows that genuinely share a genre tag with
  their seed, 1,600 (72.73%) never received `GENRE_BOOST` — see
  `eval/8_1/README.md`.

### Platform build order (stages 7-12 — required before 8.2, all done)

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
frozen contract — the rest of `contracts/` (Kafka topic schemas, the
shared recommendation response shape) stays unpopulated until 8.2 is a
second independent producer/consumer (`contracts/semantic-api-v1.json`
itself was already frozen 2026-08-28, ahead of this stage — see
`contracts/README.md`). See
`docs/platform/stage11-event-ingestion.md`. All 3 tests in
`tests/test_stage11_event_ingestion.py` pass, including an end-to-end
test driving the full HTTP → Kafka → Redis + Postgres chain through the
platform-owned consumer from stages 9-10 (46/46 total across stages
2-4+7-11 and 8.1's stages 5-6). No 8.2 code was touched. This closed the
platform build order as it stood on 2026-08-31 — see stage 12 below,
added the same day once Session B's `eval/8_1` work surfaced the need for
a reproducible event stream ahead of any 8.2/8.3 measurement.

**Stage 12** (Event simulator) verified working as of 2026-08-31 — a
deterministic behavioral-event simulator (`platform/simulator/`),
explicitly requested (not a build-order guess): a CLI that replays
scripted listening sessions against the live 411-track catalog by POSTing
to `platform/event_ingestion`, with `--seed`/`--speed`/`--sessions` for
full reproducibility and simulated/wall-clock time decoupling (needed for
Session E's future Flink event-time windows). Adds two fields to the
event payload beyond `platform/event_ingestion/main.py`'s current
`session_id`/`event_type`/`track_id` — `event_time` (simulated clock,
anchored to a fixed reference instant, not wall-clock `now()`) and
`position_ms` (needed to classify skip timing later: `<5000ms` = early,
`>80%` duration = late). The schema is `extra="allow"` (ad hoc, not
frozen) specifically to permit this; both fields are called out here
explicitly rather than added quietly. Three canned YAML scripts against
real tracks: a coherent same-genre session (rock, ids 2/3/14/15), three
consecutive early skips (dance, ids 1/4/7 then 12), and a context switch
partway through (chillout ids 83/97 then hiphop ids 73/89). See
`docs/platform/stage12-event-simulator.md` and
`platform/simulator/README.md`.

Verifying the exit criterion surfaced two real bugs, not assumed away:
`session_consumer.consume_and_cache_one()` can't drain more than one event
per session (it never commits Kafka offsets, so a repeated call just
re-reads the same "earliest" match again — no existing stage 9-11 test had
ever needed more than one event per test, so this never surfaced before);
fixed by adding `consume_and_cache_many()` alongside it (existing function
untouched). And `event_time` was originally wall-clock `datetime.now()`-
anchored, which can't be byte-identical across separate invocations —
fixed by anchoring to a fixed reference instant instead. Still no
persistent Kafka consumer daemon (out of this stage's scope, arguably
stage 9-10's own gap) — verification drives `consume_and_cache_many`
directly. 15 new tests in `tests/test_stage12_event_simulator.py` (13
pure + 2 live against the real stack) pass, plus a manual run of the
literal exit-criterion invocation (`--seed 42 --speed 10`, run twice)
confirmed in Postgres: 18 events landed under the same deterministic
`sim-42-0` session_id, every one of the 9 distinct events appearing
exactly twice (79/79 total tests across the whole repo).

### 8.2 (streaming, reactive) build order (stages 13-15, in progress)

Unlike stages 7-12 above (platform prerequisites *for* 8.2, not 8.2
itself — see the header of this file), stage 13 is 8.2's own first piece
of real business logic. Hard-timeboxed to one session (Session E of the
roadmap) with an explicit PyFlink-or-fallback decision point going in —
**the PyFlink path succeeded**, no fallback was needed.

**Stage 13** (Session profile centroid) verified working as of
2026-08-31 — `platform/streaming/flink_session_profile_job.py`, a real
PyFlink job on `flink:2.2.1-scala_2.12-java17` (upgraded from stage 8's
`1.19.1`, built via a new `platform/streaming/Dockerfile.flink`):
`KafkaSource` on `behavioral-events` → event-time watermarks (5s bounded
out-of-orderness) → `keyBy(session_id)` →
`SlidingEventTimeWindows(5 min, 30s slide)` → a `ProcessWindowFunction`
computing a weighted, recency-decayed (half-life 3 events) centroid of
session track embeddings, written to `session:{id}:profile` in Redis —
the namespace Decision C reserved for exactly this. Weight table and
centroid math live in `platform/streaming/session_profile.py` (pure,
15 unit tests, `tests/test_stage13_session_profile.py`) as the tested
reference; the Flink job's Python UDF workers duplicate the same logic
inline rather than importing it (no `platform/` mounted into the
container), verified to stay in sync by a key-format cross-check test.
`platform/streaming/config.py`'s hosts became env-var-overridable
(`KAFKA_HOST`/`REDIS_HOST`/`POSTGRES_HOST`, `localhost` default
preserved) — the first code running *inside* the docker-compose network
rather than against host-mapped ports.
`platform/streaming/preload_embeddings_to_redis.py` is a one-off
host-side script copying every track's embedding + `duration_sec` into
Redis, since the Flink workers have neither `pymilvus` nor host DB access
by design. See `docs/platform/stage13-flink-session-job.md` for the full
design, the two real bugs found submitting the job (a missing `python`
binary symlink, and Docker's `ADD <url>` silently landing the Kafka
connector JAR as unreadable by the non-root `flink` user — neither was a
PyFlink API problem), and both the analytical and live verification of
the exit criterion: replaying the three-early-skips script pushes the
session centroid to **cosine ≈ −0.98 against the high-energy region**
(individual tracks in that region score +0.75 to +0.86), live-confirmed
by pulling the actual vector the running job wrote to Redis, not just
computed offline. Job submission and live verification were manual (no
automated test drives the Flink job's own lifecycle — judged out of
scope for the timebox); the exact commands and their real captured
output are in the stage doc.

**Stage 14** (Recommendation refresh loop) verified working as of
2026-08-31 — `platform/streaming/recommendation_refresh.py`, an
independent Kafka consumer group (`RECS_REFRESH_GROUP_ID`) on
`behavioral-events`: on each event, a debounced (5s or 3 events,
whichever first — `should_refresh()`) decision to refresh
`session:{id}:recs` in Redis — 8.2's first actual recommendation output.
Cold start (fewer than 2 raw events, or no profile yet) falls back to
exactly 8.1's own batch path (`context_builder.build_context()` +
`scoring.ranking.score_recommendations()`, Decision D), seeded by the
session's first track. The warm path is genuinely new: direct Milvus
search over the profile vector (own alias `recs-refresh` — the Semantic
API has no search-by-vector endpoint), excluding every track already
played in the session, re-ranked against the session's active context
(`context_builder`'s genre-sibling/same-artist calls, reused, filtered
again against played tracks since Neo4j doesn't know about session play
history) — then the same `ranking.score_recommendations()`. Catalog
exhaustion (fewer than 10 novel candidates — real on a 411-track catalog)
is logged explicitly, not padded or hidden. See
`docs/platform/stage14-recommendation-refresh.md` for the full design
and a real bug found testing it: the first version of
`process_one_event()` created a fresh Kafka `Consumer` per call, which
(offsets are never committed, same as `session_consumer.py`) always
rescans from "earliest" and returns the same first message again — the
same bug class stage 12 already fixed for the raw-state consumer, except
this time it would have broken any real repeated use, not just the test.
Fixed with `new_consumer()` + an optional reusable `consumer` parameter.
13 new tests in `tests/test_stage14_recommendation_refresh.py` (11 pure +
2 live) pass: the exit criterion (recs appear after a skip, stay
byte-identical through a debounced no-op, and the count-based debounce
branch independently re-arms a real refresh) and a dedicated warm-path
test (seeds a fake profile directly rather than waiting on stage 13's
real window timing, confirms already-played tracks never reappear).
109/109 tests pass repo-wide.

### Stage 15A (bug closure, ahead of 8.2's evaluation harness)

Verified working as of 2026-09-01 — not a new build-order stage, a
prerequisite bug-closure pass before 8.2 gets its own `eval/8_2/` harness
(later stages), since 8.2's cold-start fallback in
`recommendation_refresh.py` reuses `context_builder.py` +
`platform/scoring/ranking.py` exactly as 8.1's `recommend.py` does
(Decision D) — any latent bug in that shared path would poison 8.2's
measurements too.

Closed the `related_by_genre` finding from the 8.1 eval pack (see the
2026-08-31 entry above): `platform/semantic_api/main.py`'s
`/tracks/{id}/graph` Cypher had `LIMIT 10` with no `ORDER BY`, so on
skewed/large genres the 10 siblings returned were an arbitrary cut, not
a principled one. Fixed by ordering candidates by shared-genre-count
descending, `track_id` ascending as a deterministic tiebreak. Fixes the
*live* endpoint only — `eval/8_1/kg_connectivity.py::
capped_genre_sibling_ids` deliberately keeps reproducing the *original*
query, since it exists to reconcile against the already-generated,
frozen 4110-row `recommendations` table (2026-08-07), not to mirror
current API behavior; its docstring now says so explicitly. A new
regression test, `tests/test_stage4_api.py::
test_track_graph_related_by_genre_ordered_by_shared_genre_count`,
confirmed to fail against the old query and pass against the fix.
Re-running `eval/8_1` (`python -m eval.8_1.run`) after the fix produced
a byte-identical `results.json` — confirming the fix is forward-looking
(benefits 8.2's shared code path) and doesn't retroactively change any
8.1 metric, so the 8.1 results write-up needs no revision. Only
`latency.json`/`tables.md`'s live-measured per-stage timings shifted,
expected run-to-run noise already treated separately from the
deterministic metrics.

Re-running `eval/8_1` also surfaced a second, unrelated latent bug:
`eval/8_1/run.py` still imported `from recommender import ranking`, the
pre-Decision-D path — Decision D's `git mv` to
`platform/scoring/ranking.py` (2026-08-31) fixed the two import sites it
checked (`usecases/8_1_batch_reactive/recommender/recommend.py` and its
own test) but missed this one, so `eval/8_1/run.py` had been broken on
`main` since that commit. Fixed by adding `platform/` to its `sys.path`
and importing `from scoring import ranking`.

### Stage 15A.2 (canonical results consolidation)

Verified working as of 2026-09-01 — closes the duality Stage 15A left
open (a stale `results.json` + a current `regenerated_results.json`,
neither declared canonical) by first validating that
`eval/8_1/regenerate_recommendations.py`'s reconstruction path (calls
`context_builder.build_context()`/`ranking.score_recommendations()`
directly) actually agrees with the real recommender
(`usecases/8_1_batch_reactive/recommender/recommend.py`), since a naive
diff can't tell "the paths differ" apart from "Milvus ANN is
non-deterministic."

**ANN noise floor**: backed up the frozen `recommendations` table
(`pg_dump`, restore-tested into a throwaway database) before writing
anything, then ran the real `recommend.py` five times end-to-end under
fresh `run_id`s. All 10 pairwise comparisons (`eval/8_1/diff_runs.py`)
came back at **exactly zero** — zero membership/order changes, zero
score drift across 41,100 matched track-id pairs; the noise floor is
0.0, not the nonzero number expected going in. New run_ids deleted after
diffing (row-count-verified before and after) — the frozen
`run_id 3a7ffa23-...` was never touched. The Milvus collection's index
is **IVF_FLAT** (nlist=128, queried at nprobe=16 —
`platform/semantic_api/main.py`), not FLAT/exhaustive: it's genuinely an
approximate index (a true nearest neighbor can be missed if it falls in
an unprobed cluster), so "0.0" is *run-to-run repeatability against a
static, unrebuilt index*, not a claim that Milvus search is
unconditionally deterministic or that IVF_FLAT achieves exact recall —
neither was tested. See `eval/8_1/noise_floor.json`'s
`milvus_index_info`/`milvus_index_note` fields.

This result also retroactively corrects an explanation from the
regeneration diff two sessions ago (`eval/8_1/regeneration_diff.json`,
Stage 15A.1): 11 seeds classified `identical` still showed a small score
delta, at the time attributed to "Milvus ANN's approximate-search
non-determinism." That attribution never made it into this file, only
into chat, but it was wrong regardless — the 0.0 noise floor rules it
out. The `score::float8` fix below was also tested against it and made
zero difference (SCORE_TOLERANCE=1e-4 already swamps that ~1e-7-scale
artifact by three orders of magnitude, so it was never a candidate
cause). The real cause, found by direct inspection: every one of the 15
rows differs by exactly ±0.05 (`GENRE_BOOST`) — candidates whose
genre-sibling status flipped between the pre- and post-Stage-15A query
without changing rank. A real, if small, additional effect of the
ordering fix that the track_id-based classification (277/411 "affected"
seeds) was never designed to count. See
`eval/8_1/diff_recommendations.py`'s module docstring and
`regeneration_diff.json`'s `identical_but_row_diffs_note`.

**Path validation**: diffing one real run against
`regenerate_recommendations.py`'s output under a pre-registered rule
(written before this number existed: PASS iff membership-changed count
and max score delta don't exceed the noise floor) first came back FAIL
— max delta 5.8e-8. Investigated per the prompt's instruction rather
than loosened the rule: traced to `float4send()` showing the *actual*
stored bytes were bit-identical to the regenerated value's float32 cast,
but a plain `SELECT score FROM recommendations` doesn't round-trip a
Postgres `real` column exactly (`extra_float_digits=0`'s default 6-digit
text output loses precision that `psycopg2` then parses back into a
slightly-off float64). Fixed by casting `score::float8` server-side in
both `eval/8_1/diff_runs.py` and `eval/8_1/diff_recommendations.py` — an
exact, lossless widening, no reliance on client-side GUC settings.
Re-ran: **bit-exact match, VERDICT: PASS.** The reconstruction script and
the real recommender produce identical output.

**Canonicalization**: with validation passing, promoted the current
(post-Stage-15A ordering fix) pack to `eval/8_1/results.json` /
`tables.md` / `figures/*.png`; the previous stale pack (frozen table +
pre-fix reconstruction) renamed to `frozen_legacy_results.json` /
`frozen_legacy_tables.md` / `figures/frozen_legacy/*.png`, each marked
with an explicit provenance note (generated under the original
unordered `LIMIT 10` query, retained for provenance only, not to be
cited). Same treatment for `eval/8_1/kg_connectivity.py`: added
`capped_genre_sibling_ids_legacy()` reproducing the original query,
docstring-marked historical; `capped_genre_sibling_ids()` (already
un-frozen in Stage 15A) stays the sole canonical function. Also fixed
the "no ORDER BY" note text that Stage 15A's fix had made stale in
`eval/8_1/metrics.py` (x2), `eval/8_1/run.py`, and `eval/8_1/README.md`
— now describes current behavior (ordered by shared-genre-count) while
still stating the *unaddressed* limitation explicitly (the cap of 10
itself, not the ordering, still causes most of the genre-boost coverage
gap — 72.73%→72.48%, barely moved by the ordering fix alone).
`eval/8_1/run.py` refactored (Stage 15A.1) into a shared `run_pipeline()`
is unaffected by this stage beyond the note-text fix.

New tools, reusable beyond this session: `eval/8_1/diff_runs.py` (the
noise-floor/path-validation methodology, callable against any set of
`run_id`s and a candidate table — this is also the N≥5 repeated-run
measurement 8.2's determinism metric will need, produced as a byproduct
here, not extra work).

Exactly one `results.json` now exists in `eval/8_1/` and it reflects
current code. All 112 pre-existing tests plus this session's work still
pass — no new tests were added (the smoke test from Stage 15A.1 already
covers the `run.py` import/wiring path this session's refactor didn't
touch further).

### Stage 15B (simulator capability audit and late-event support)

Verified working as of 2026-09-01 — the first piece of 8.2's own stage 15
work, ahead of `eval/8_2/` (stage 15C): the headline justification for
using Flink over a scheduled job is event-time semantics with watermarks,
but the stage 12 simulator only ever emitted events in strict
`event_time` order, so that machinery had never been exercised. Audited
`platform/streaming/flink_session_profile_job.py` (read-only, unchanged
this stage): the watermark bound is confirmed exactly
`for_bounded_out_of_orderness(Duration.of_seconds(5))`, and the
`SlidingEventTimeWindows(5 min, 30s slide)` has no `allowedLateness` and
no side-output configured. Added `--jitter` to the stage 12 simulator
(`platform/simulator/cli.py`/`events.py::apply_jitter()`) — a pure,
seeded-`rng`-driven bounded reorder of an already-planned session's
delivery order (event_time values themselves untouched, only position
moves, by up to `MAX_JITTER_DELAY` = 3 positions); `--jitter 0.0`
(default) is a byte-identical no-op. Planning
(`build_session_events`/`apply_jitter`) moved out of the per-session
thread into `main()`'s single-threaded section, since concurrent
sessions racing on one seeded `rng` from separate threads would have
broken `--seed` determinism. See
`docs/platform/stage15b-simulator-jitter-and-late-event-audit.md`.

Manually submitted the unmodified Stage 13 job and replayed
`coherent_session.yaml` twice (same absolute event-time span, different
session_ids) — once plain, once with `--jitter 1.0` — and diffed every
window the job printed. Live-confirmed, not just reasoned about: the
jittered session's opening `play` event never appeared in any of the 7
early windows the baseline showed it in — not delayed, not
side-outputted, silently and permanently dropped (a window later in both
sessions' output that *does* fire for both differs in `weight_total`
despite matching event count, and the two sessions' final windows are
identical, confirming the dropped event never rejoins). This job is not
modified this stage — adding `allowedLateness`/side-output handling is
left as a decision for 15C/15D. `tests/test_stage15b_jitter.py`: 6 new
tests (5 pure on `apply_jitter`, 1 live proving genuine Kafka
delivery-order divergence from event_time order beyond the 5s bound).
118/118 tests pass repo-wide.

### Stage 15C (eval/8_2 harness core + metrics 1/2/3)

Verified working as of 2026-09-06 — `eval/8_2/`, built to the metric spec
signed off 2026-09-03 (`eval/8_2/METRICS.md`). Scoped by explicit decision
to the harness plus **metrics 1 (reactivity), 2 (latency per hop) and 3
(cold-start → warm transition)**, all three off one pivot scenario;
metrics 4/5/6/7 (coherence, coverage, determinism, late-event) are **Stage
15C.2** and reuse this infrastructure. `results.json` carries explicit
`pending` markers for them rather than omitting the keys.

Unlike `eval/8_1` (passive: queries the frozen 4110-row table, never
re-runs anything), this is an **active harness** — per scenario it runs a
paced event poster, the real stages 9-10 raw-state consumer, a refresh
driver loop over `process_one_event()`, and a profile-meta poller
concurrently, and starts/stops the Semantic API, the event ingestion
service and the Flink job itself (`eval/8_2/orchestration.py`), so
`python -m eval.8_2.run` is one command. The refresh driver loop is
harness-owned, test-shaped code — explicitly NOT a step toward a
persistent refresh daemon in `platform/streaming/`, which remains the
open gap stage 12 first flagged.

Two additive platform edits, both sanctioned by the spec:
`flink_session_profile_job.py` gained one `hset` writing
`session:{id}:profile_meta` (`computed_at`/`n_events`) — a *sibling* key,
never fields inside `session:{id}:profile`, which
`recommendation_refresh.py` `json.loads`es as a flat vector;
`session_state.py::profile_meta_key()` is its canonical definition, held
to the same cross-check test as `profile_key()`. And
`process_one_event()` now returns `refresh_compute_seconds`, timed
*inside* the function around the `should_refresh()` →
`refresh_recommendations()` block, since timing it from outside would fold
in the up-to-10s Kafka poll wait. The debounced no-op branch is untouched.

Three things the spec got wrong or omitted, all corrected in `METRICS.md`
itself rather than silently diverged from: its section 0 process list was
missing the **Semantic API** (both refresh paths call `context_builder`
over HTTP) and the **stages 9-10 raw-state consumer** (without it
`session:{id}:events` stays empty and *no refresh ever fires* — stage 14's
live test hides this by calling `record_event()` directly); and its
"Resolved before handoff" claim that the K sweep isn't nested is false —
`random.Random(seed).sample()` draws sequentially, so K=3 ⊂ K=5 ⊂ K=8 ⊂
K=12. The nesting is arguably better for metric 1 (only pre-pivot length
varies) but it means the K runs aren't independent samples, and metric 3's
four sessions share one cold-start seed track, so its four handoff numbers
are **one observation repeated** — reported in the data as
`reactivity.json::pre_pivot_sets_nested` and
`results.json::cold_warm_transition.independent_observations`.

Because Flink's watermark is stream-wide rather than per key, scenarios
run back to back need scheduled event-time anchors
(`metrics.anchor_schedule()`): strictly increasing with a ≥5 min gap, and
every anchor a whole multiple of 300s from a fixed epoch so
`SlidingEventTimeWindows` boundaries fall identically across runs (300s is
the window size and a multiple of the 30s slide). The harness also cancels
any pre-existing job and submits a fresh one per invocation.

Two real bugs found by running it. Session ids must be scoped per
invocation (`--run-id`): `behavioral-events` is never purged and both
consumer groups start from `earliest`, so a re-used session id made the
sweep replay the pre-flight's 22 stale events and report 0 post-pivot
refreshes for K=3. And reconciling the test count (expected 148, measured
146) exposed a silent pytest collision — `eval/8_2/tests/test_metrics.py`
and `test_run_smoke.py` resolved to the same modules as `eval/8_1`'s
same-named files (the package dirs `8_1`/`8_2` aren't valid identifiers),
so the suite collected eval/8_1's tests twice and none of eval/8_2's;
fixed by the `test_82_*` prefix, and any future `eval/8_3/tests/` needs
the same care.

Results (speed 60, seed 42, warm-path precondition satisfied at every K —
nothing excluded): **adaptation is immediate** — every K crosses the
pre-registered Jaccard < 0.3 threshold at its *first* post-pivot refresh,
1-2 events after the pivot, and stays at ~0 after;
`events_to_adaptation` (4/5/7/10 for K=3/5/8/12) rises with K only because
more pre-pivot refreshes precede the crossing. Latency: H1 ingest POST
p50 8.4 ms, H2 profile compute lag p50 2.56 s, H3 refresh compute p50
38.9 ms — H2 dominates by two orders of magnitude, the only hop waiting on
a windowed job. `events_behind_each_refresh` came back p50 = p95 = max =
3, so the debounce fired on its *count* branch essentially every time at
this speed, not its 5s interval branch. All four sessions reached the warm
path after one cold-start refresh, ~5.3-5.5 s in. See
`docs/platform/stage15c-eval-8_2-harness.md` and `eval/8_2/README.md`; a
`--from-records` flag recomputes every metric from saved raw records with
no live run. 148/148 tests passed repo-wide at the time.

The numbers above are this stage's own run. Stage 15C.2 re-ran the whole
pack and **the artifacts checked in under `eval/8_2/` are now that later
run's** — the deterministic metrics reproduced exactly (`events_to_adaptation`
4/5/7/10, unchanged), while the wall-clock latencies moved as expected for
a quantity both eval packs explicitly exclude from any reproduction claim
(H1 7.2 ms, H2 2.12 s, H3 24.4 ms). `events_behind_each_refresh` is also
now reported per `--speed`: the p50 = p95 = max = 3 recorded above still
holds at speed 60, but the long session's speed 30 sits at p50 = 2, where
the debounce's 5s interval branch fires before its 3-event branch.

### Stage 15C.2 (eval/8_2 metrics 4/5/6/7 — `eval/8_2` complete)

Verified working as of 2026-09-06 — the four metrics Stage 15C deferred
with explicit `pending` markers, on the same harness and the same anchor
schedule, no new orchestration. Three scenarios added, bringing the pack
to eight: a 40-track four-genre `long_session`
(`rock`/`electronic`/`chillout`/`dance`, 10 each) feeding metrics 4 and 5,
plus three replicates of the pivot at K=8 — `det_rep1`, `det_rep2`
(both `--jitter 0`) and `late_jitter` (`--jitter 1.0`). A full run is
~25 minutes; `--skip-extra-scenarios` runs only the metric 1/2/3 sweep.
Metrics 1/2/3 reproduced Stage 15C's published numbers exactly.

**The design decision that shapes metrics 6 and 7**: the two jitter-0
replicates do double duty. They are metric 6's determinism comparison,
*and* their difference is the run-to-run noise floor metric 7's jitter
effect has to beat before it counts as real. `METRICS.md` §8 asked only
for a bare jitter-0 vs jitter-1.0 delta, which is uninterpretable here —
the debounce is wall-clock while `--speed` compresses only session time,
and the warm path reads a profile written by a separately scheduled
consumer group, so any two runs differ somewhat. Same methodology Stage
15A.2 used against the ANN noise floor, at the cost of one extra scenario
rather than a separate experiment. Both verdict rules are pre-registered
in `metrics.py` (`determinism_verdict`, `late_event_verdict`), committed
before any of the three runs. Metric 7 also measures its own **dose**
(`late_delivery_count`) — `--jitter` is a probability, so how many events
it actually pushes past the 5s watermark bound is a draw, and without that
number a null result would be indistinguishable from "nothing was
dropped".

**Metric 4's `--speed` was picked by measurement**, as `METRICS.md` §5
demanded rather than defaulted, and the probe exposed a constraint the
spec did not anticipate: profile writes arrive in *bursts*. A `complete`
event advances session time by most of a track duration (median 231s),
advancing the watermark past seven or eight 30s slides at once, which the
job fires milliseconds apart over one overwritten Redis key. **The
resolvable ceiling is one vector per watermark-advancing event, not one
per window fire**, at any poll rate. Measured on a 12-event probe at a
0.5s poll: speed 60 captured 5 of 6 distinct writes, speed 30 captured
6 of 6 — hence `LONG_SESSION_SPEED = 30`. `coherence.json` reports capture
against that ceiling, not against the analytic window-fire count, which
would report complete sampling as ~13%.

Real numbers. **Metric 4**: 39 of 41 resolvable writes captured (95%);
mean `cos(profile, last 5 min of session)` **0.8707** against mean
`cos(profile, first 5 min)` **0.6186**, with 35 of 39 samples (90%)
closer to the recent window — the first evidence for the recency decay
`session_profile.py` has claimed in a comment since stage 13. **Metric
5**: **168 distinct tracks, 40.88% of the catalog**, over 40 refreshes,
with 35 of those 40 contributing something never recommended before and
the last new track arriving at refresh 38 — **no attractor collapse**, a
flat tail of 2 refreshes (5%). **Metric 6: PASS**, and more strongly than
expected — 11 refreshes each, all 11 `identical`, max score delta exactly
0.0; a FAIL was a legitimate possible outcome and the claim stays bounded
to two runs at one K, one speed, on an idle machine. **Metric 7**: the
dose was heavy (20 of 32 events delivered late, up to 648.9s past the
bound) and three of four measures cleared the zero noise floor —
`refresh_count` 11→12, max reactivity Jaccard delta 0.2500, max coherence
`cos_recent` delta **0.5574** — but **`events_to_adaptation` is 7 in all
three arms**. The profile is measurably corrupted and the recommendation
sets genuinely differ, yet losing most of the pre-pivot profile does not
change *when* the recommender turns over, only *what* it turns over to.

Two by-products. The debounce statistics are now split per `--speed` as
well as pooled, and the split earned its keep: speed 60 sits at p50 = 3
events behind each refresh (the debounce's *count* branch) while speed 30
sits at p50 = 2 (the 5s *interval* branch fires first) — pooling the
pack's two speeds would have averaged away exactly the effect
`latency.json`'s `speed_caveat` describes. And metric 3's handoff Jaccard
was found to hide a rank change: the long session reports 1.000 because
the first warm refresh returned the same ten tracks as the cold start, in
a different order (the top track dropped to position six).
`METRICS.md` §4 defines it over *sets* and that stands, so
`cold_warm_transition()` reports `handoff_rank_identical` alongside it
rather than redefining the metric — it is `false` for every session in the
pack, including the four at Jaccard 0.818.

One real bug found and fixed: `run_scenario()` located the pivot as an
index into the event-time-ordered plan and then applied that index to the
post-jitter list. `apply_jitter()` permutes *delivery* order and leaves
`event_time` untouched, so on `late_jitter` that would have labelled the
wrong refreshes post-pivot — metric 7 comparing a mislabelled curve
against a correct one, with no error anywhere. Fixed by locating the pivot
by identity before the permutation and finding its new position after;
`pivot_event_time_index`, `pivot_event_index` and `pivot_delivery_shift`
are all recorded so the displacement is visible in the data.

Raw artifacts are now split (`raw_scenario_records.json` +
`raw_profile_vectors.json` + `raw_embeddings.json`, ~2.9 MB total, nothing
rounded because metric 6 compares for exact equality) and written
**before** any metric is computed — a live run costs ~25 minutes and the
raw records *are* the measurement. `--from-records` reads all three and
was used to regenerate the published outputs after three definitions were
refined post-run, with no second live run. See
`docs/platform/stage15c2-eval-8_2-metrics-4-7.md` and `eval/8_2/README.md`.
61 tests in `eval/8_2/tests/` (up from 29), all pure; 180/180 pass
repo-wide.

### Stage 16 (refresh daemons + `session_api`, Decision E)

Verified working as of 2026-09-06 — 8.2's delivery path, and the closure
of the persistent-consumer gap stage 12 first flagged. Two components:

- **`platform/session_api/`** — a separate FastAPI service per Decision E,
  read-only, request/response (never WebSocket), whose **only dependency
  is Redis**. It computes nothing: `GET /sessions/{id}/recommendations`,
  `/profile` and a `/sessions/{id}` status endpoint serve what the refresh
  daemon and stage 13's Flink job already wrote. No Postgres/Milvus/Neo4j,
  because the rows `recommendation_refresh` stores are already
  self-contained. 404 deliberately distinguishes "unknown session" from
  "session exists but has no recommendations yet" — to a client those are
  completely different situations, and one 404 for both would make the
  second look like a bug.
- **Two daemons, not one.** Decision E names only
  `platform/streaming/refresh_daemon.py`, but running just that produces
  nothing: `refresh_recommendations()` returns `skip_insufficient_data`
  below two events in `session:{id}:events`, and only the stages 9-10
  raw-state consumer writes that key — which had no daemon either. So
  `platform/streaming/session_consumer_daemon.py` ships alongside it
  (separate module, separate consumer group, per Decision C's ownership
  split), sharing only signal handling and logging via
  `daemon_runtime.py`. **Explicitly approved as an addition to Decision
  E's literal wording, 2026-09-06.**

**These daemons commit Kafka offsets; nothing before them did.** Every
pre-stage-16 consumer runs `enable.auto.commit: False` and never commits —
right for a bounded, throwaway-group-id read where "start from earliest"
is wanted. A daemon is the opposite: it runs under the canonical group id
from `config.py` and restarts, and `behavioral-events` is never purged, so
without commits every restart would re-push all cached events into Redis
and duplicate rows into the Postgres log. Both daemons commit manually and
synchronously *after* the write (at-least-once). No existing consumer's
configuration changed — commits are per group and every older caller uses
a throwaway id. Measured: first start on the 1,820-message topic drained
1,780 cacheable events in 7.5s (raw) and 571 refreshes in ~11s (refresh,
16ms per cold-start refresh), zero errors; both groups then sat at lag 0,
and a real restart replayed **0 events**.

Two real bugs, both found by running a daemon for the first time.
`refresh_recommendations()` indexed Postgres integer track ids with a bare
`int()`, but a behavioral event's `track_id` is a free-form string on an
`extra="allow"` schema — every consumer before this was scoped to one
session or one test's payloads, whereas a daemon reads *every* session, and
the first non-numeric id it met (`"xyz789"`, left on the never-purged topic
by stage 11's own test) raised `ValueError` and killed the loop. Fixed with
`recommendation_refresh.as_track_id()` at all three call sites, plus a new
`skip_unseedable_cold_start` action for a session whose opening track can't
seed the fallback. That new action exposed a second-order bug:
`process_one_event()` decided "real work happened" via
`action != "skip_insufficient_data"`, so any *new* skip would have counted
as a refresh and spent the debounce budget — now checked by `skip_` prefix.
Separately, both daemon loops now contain per-event failures (log, count,
commit past the message, continue) rather than exiting, since a daemon that
dies on one poison message is not a daemon.

**Deferred finding, handed to 15D**: `session:{id}:events` has a 30-minute
sliding TTL (stage 9) but `:profile`, `:profile_meta`, `:recs` and
`:refresh_meta` have **none** — stage 13's `redis.set` and stage 14's
`_write_recs` set no TTL. So `session_api` can serve recommendations for a
session whose raw state expired hours ago, and the derived keyspace grows
without bound. Surfaced rather than patched by explicit decision
(2026-09-06): fixing it changes stages 13 and 14, and 15D's failure
injection is the right place to decide what a client should see when
session state disappears underneath it. `GET /sessions/{id}` reports it as
`raw_state_expired` with an explanatory note.

`contracts/` deliberately untouched: `contracts/README.md` says the shared
recommendation response shape lands there once a second independent
consumer exists, and 8.3 doesn't exist yet. Revisit when 8.3 starts.
`_session_key()` in `session_state.py` became public `events_key()` in the
process — `session_api` needs it, and `tests/test_stage10_postgres_events.py`
was already importing the private name.

17 new tests (8 daemon, 9 API) against the live stack, plus a manual run of
the real deployment shape — five processes under the canonical group ids,
simulator-driven, with and without the Flink job. See
`docs/platform/stage16-session-api.md`. 197/197 pass repo-wide.

### Stage 15D (failure injection + the TTL fix stage 16 deferred)

Verified working as of 2026-09-08. Two halves, sequenced deliberately —
**measure first, then fix** — so the fix's effect is observed rather than
asserted. Sequenced after stage 16 on purpose: once `session_api` existed
there was finally a client, so this stage could ask what a client sees when
a store dies mid-session.

**Metric 8**, added to `eval/8_2/METRICS.md` as §9 and committed **before
any arm ran** so the pre-registration is verifiable. Four arms — a
no-injection `control` plus `redis_outage`, `semantic_api_outage` and
`ttl_expiry` — two replicates each, on the K=8 pivot scenario metrics 6/7
already use. Scope fixed by explicit decision: Postgres/Milvus/Kafka
outages are **not** tested (§9.6 records what reading the code predicts for
them, marked as predictions), and the `allowedLateness` decision 15B/15C.2
left open **stays open**, since metric 7 exists to quantify the consequence
of not having it.

Run by `python -m eval.8_2.failure_run`, a **separate entry point from
`run.py`**: metrics 1-7 are measured against harness-owned threads, while
metric 8 drives the real stage 16 deployment — five processes (Semantic API,
event ingestion, `session_api`, both daemons under the **canonical group
ids**) plus the Flink job. Committed offsets and restart behaviour are part
of what is measured. `orchestration.py` gained `RestartableService` (same
port across restarts, so killing the Semantic API is not indistinguishable
from a permanent outage) and `daemon_process`; `uvicorn_service` was
refactored onto the former, so there is one copy of that lifecycle.

Real numbers (both replicates agreed on every measure; no arm stalled).
**`redis_outage`**: 6 events accepted by the ingestion service never reached
the Postgres log — both daemons caught `redis.ConnectionError`, counted an
error and **committed the offset past the message**. That handler is right
for a poison message and wrong for a dependency outage, and the two are
indistinguishable to it; there is no retry, and the advanced offset means a
restart cannot recover them. Both daemons did recover with nothing
restarted (redis-py is pool-backed), and a client saw 500s *during* the
outage only. The outage also killed the Flink job permanently — its window
function writes to Redis and the compose cluster runs it with no
checkpointing, so the restart strategy is "none"; the harness resubmits
between arms, without which every later arm would have run with no profile
at all. **`semantic_api_outage`: no measure distinguishes it from the
control**, and that is the finding. No events are lost (the raw daemon does
not depend on it) and every client request returns 200 throughout, while
every refresh attempted during the outage fails and `session:{id}:recs`
silently stops moving — served as current, because `:recs` carries no
timestamp of any kind, unlike `:profile` and its sibling `:profile_meta`.
**`ttl_expiry`**: visible, via the `raw_state_expired` flag stage 16 added.

**The TTL fix.** `streaming/config.py` now defines `DERIVED_TTL_SECONDS`
(= `SESSION_TTL_SECONDS`), applied to all four derived keys: TTLs in
`recommendation_refresh._write_recs()` and on the `hincrby` that *creates*
`:refresh_meta` (a session that never refreshes would otherwise leave an
immortal key), and in `flink_session_profile_job.py` (constant duplicated
inline for the same reason the key strings are, and cross-checked by test).
`session_state.py` gained `refresh_meta_key()` and `expire_derived()` —
`:refresh_meta` was the one session key with no canonical definition here,
which is exactly how it became the one derived key nobody noticed had no
TTL. **The asymmetry becomes a bound, not a reversal** — worth stating
precisely, since the obvious claim ("derived now expires first") is false:
raw events refresh their TTL on every event while derived state refreshes
only on a refresh or a window close, and a window can close either side of
the last event. Measured on the post-fix pack across 32 derived keys in 8
sessions, derived-minus-raw TTL lands in **[-21s, +23s]** (`:recs` and
`:refresh_meta` always at or before raw; `:profile`/`:profile_meta` either
side). Derived state now expires within about half a minute of its session
instead of never, and `GET /sessions/{id}` still reports the transient. `session_api._missing()` now decides "known session"
from **any** session key: keying it off `:events` alone made that function
collapse the two cases it exists to separate once the raw list expired
(regression test confirmed failing against the old code).
`track:{id}:embedding`/`:duration_sec` stay TTL-free by design — catalog
state, not session state.

Two things the running of it found. A **suspended machine produced a
perfect-looking, invalid arm**: the first pre-fix pack was frozen twenty
hours mid-arm by an overnight suspend, and the arm completed, lost no
events and agreed with its replicate — while actually being a different
experiment, since the debounce is wall-clock and `:events` has a 30-minute
TTL. `failure_injection.wall_clock_stall()` now compares realised against
intended pacing per post and flags any overrun beyond 60s; **a flagged arm
is excluded and re-run, never reweighted**, and that pack was discarded.
And **a measure the control cannot have is not control-differenceable**:
`recovers_without_restart` is `None` in the control, so the rule as written
reported every arm's `True` as an *effect of the failure*; corrected to
report such measures under `measures_not_comparable_to_control` — a
post-hoc correction that **removes a vacuous effect rather than creating
one**, recorded as such in §9.5.

**Metrics 1-7 are unchanged by the fix**: `eval.8_2.run --from-records`
reproduced `results.json`, `reactivity.json`, `coherence.json` and
`coverage.json` byte-identically (md5-verified). The live 25-minute harness
re-run was deliberately skipped in favour of this recomputation, which
answers exactly that question at zero live cost — recorded so the
substitution is visible. See
`docs/platform/stage15d-failure-injection.md`. 215/215 pass repo-wide.

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
whole chain (Kafka → Redis + Postgres) its first real producer. Stage 12
(`platform/simulator/`) is the first *scripted, reproducible* producer —
deterministic sessions instead of one-off manual test payloads. Flink —
as of stage 13, `platform/streaming/flink_session_profile_job.py` is a
real PyFlink job consuming `behavioral-events` and writing to Redis (the
JobManager/TaskManager pair now runs a rebuilt `flink:2.2.1` image, up
from stage 8's plain `1.19.1`, via `platform/streaming/Dockerfile.flink`).
This is 8.2's own first consumer of the platform — everything above it in
this paragraph remains platform-owned plumbing.

As of stage 16, two persistent daemons keep that state current
(`platform/streaming/session_consumer_daemon.py` for raw state,
`refresh_daemon.py` for recommendations — the first Kafka consumers in this
repo to commit offsets, since they are the first that restart), and
`platform/session_api/` is a third FastAPI service serving
`session:{id}:recs` and `session:{id}:profile` read-only over Redis alone
(Decision E). Like the Semantic API and the event ingestion service, all
three run on the host via uvicorn/python rather than in `docker-compose.yml`.

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

Run the full test suite (**215 tests**, measured: 47 across platform stages
2-4+7-11 + 8.1 stages 5-6 (includes Stage 15A's `related_by_genre`
ordering regression test), 20 for `eval/8_1` (18 pure metric functions + 2
smoke), 15 for stage 12's event simulator, 2 for Decision C's consumer
ownership boundary — see `tests/test_stage10_postgres_events.py` — 16 for
stage 13's session profile centroid (Stage 15C added the
`profile_meta_key` cross-check), 13 for stage 14's recommendation refresh
loop, 6 for stage 15B's simulator jitter/late-event audit, 73 for
`eval/8_2` (29 from stage 15C's harness plus 32 added by Stage 15C.2 for
metrics 4-7, plus 12 added by Stage 15D for metric 8), 17 for stage 16's
daemons and session API, and 6 for Stage 15D's derived-key TTLs —
`pytest.ini`'s `testpaths` includes `eval` alongside `tests`/`usecases`.
The enumeration sums to exactly 215 as of Stage 15D
(47+20+15+2+16+13+6+73+17+6):
the older 2-test drift noted here since Stage 15A.2 was the
`eval/8_1` smoke tests going uncounted, reconciled 2026-09-06. Note that
test files under `eval/` must be uniquely named across packages — `8_1`
and `8_2` aren't valid Python identifiers, so same-named files silently
collide, which cost 2 tests until Stage 15C caught it)
against the live stack — uses `platform/enrichment/.venv`
because it already carries psycopg2/httpx/matplotlib/pytest (stage 8's
Flink and stage 10's Postgres tests need nothing beyond that venv's
defaults — the venv already has `psycopg2-binary==2.9.9`, same pin
`platform/streaming/requirements.txt` declares); the extra installs pull
in what stage 5/6's, stage 7's, and stage 9's own tests need that that
venv doesn't have by default (`-r platform/streaming/requirements.txt`
also brings in `redis==5.0.8` for stage 9). Stage 11's own
`platform/event_ingestion/requirements.txt` needs nothing beyond
fastapi/uvicorn/confluent-kafka/python-dotenv, all already covered by the
installs below. `eval/8_1` needs nothing beyond this venv's defaults
either — pymilvus/neo4j/matplotlib/numpy are already present, confirmed by
direct import check (see CLAUDE.md's `eval/` section). Stage 12's
`platform/simulator/requirements.txt` needs nothing beyond this venv's
defaults either — pyyaml/httpx confirmed present:

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

Run the stage 16 delivery path — the two consumer daemons and the session
API. The daemons use the canonical consumer group ids from
`streaming/config.py` by default; the refresh daemon needs the Semantic API
up (both refresh paths call `context_builder` over HTTP), and both need the
event ingestion service to have something to consume. A daemon's *first*
start on a topic it has never committed against replays the whole retained
backlog once (measured: ~7.5s for 1,780 events on the raw side); every
restart after that resumes from its committed offset:

```bash
platform/enrichment/.venv/bin/python -m uvicorn session_api.main:app \
    --app-dir platform --port 8030
PYTHONPATH=platform platform/enrichment/.venv/bin/python \
    -m streaming.session_consumer_daemon
PYTHONPATH=platform platform/enrichment/.venv/bin/python \
    -m streaming.refresh_daemon --semantic-api-url http://127.0.0.1:8000
# then, for any session the daemons have seen:
curl -s localhost:8030/sessions/<session_id>
curl -s localhost:8030/sessions/<session_id>/recommendations
curl -s localhost:8030/sessions/<session_id>/profile
```

Both daemons are needed: the refresh daemon reads `session:{id}:events` to
decide what to refresh, and only the raw-state daemon writes that key.

Run the stage 12 event simulator (needs the event ingestion service above
running):

```bash
PYTHONPATH=platform platform/enrichment/.venv/bin/python -m simulator.cli \
    --seed 42 --speed 10 --sessions 3
```

Build and submit the stage 13 PyFlink session profile job (needs the
stack up and, to actually see output, the event ingestion service +
simulator above; see `docs/platform/stage13-flink-session-job.md` for the
full sequence including the embeddings preload step):

```bash
docker compose build flink-jobmanager flink-taskmanager
docker compose up -d flink-jobmanager flink-taskmanager
PYTHONPATH=platform platform/enrichment/.venv/bin/python platform/streaming/preload_embeddings_to_redis.py
docker cp platform/streaming/flink_session_profile_job.py 8-1-flink-jobmanager:/opt/flink/session_profile_job.py
docker exec 8-1-flink-jobmanager flink run -d -py /opt/flink/session_profile_job.py
```

Regenerate the reports (each needs the stack up; `uc81_results.py` also
needs the Semantic API running — see Quickstart in README.md):

```bash
platform/enrichment/.venv/bin/python reports/stage2_metrics.py
platform/enrichment/.venv/bin/python reports/stage3_embedding_projection.py
platform/enrichment/.venv/bin/python reports/uc81_results.py
platform/enrichment/.venv/bin/python reports/results_evaluation.py
```

Regenerate `eval/8_1`'s evaluation pack (needs the stack up; reads
Postgres/Milvus/Neo4j directly, no Semantic API needed):

```bash
platform/enrichment/.venv/bin/python -m eval.8_1.run
```

Run `eval/8_2`'s harness (stages 15C + 15C.2, all seven metrics). Needs
the stack up and the track embeddings preloaded into Redis; it starts the
Semantic API, the event ingestion service, the Flink job and both consumer
loops itself, and cancels/tears them down afterward. **~25 minutes** for
all eight scenarios; `--skip-extra-scenarios` runs only the metric 1/2/3 K
sweep in ~6:

```bash
PYTHONPATH=platform platform/enrichment/.venv/bin/python platform/streaming/preload_embeddings_to_redis.py
platform/enrichment/.venv/bin/python -m eval.8_2.run
# only the metric 1/2/3 K sweep (~6 min instead of ~25):
platform/enrichment/.venv/bin/python -m eval.8_2.run --skip-extra-scenarios
# recompute every metric from a previous run's records, no live run --
# reads raw_profile_vectors.json and raw_embeddings.json alongside it:
platform/enrichment/.venv/bin/python -m eval.8_2.run --from-records eval/8_2/raw_scenario_records.json
```

Run `eval/8_2`'s metric 8 (Stage 15D failure injection). A **separate entry
point**: unlike `run.py`, which drives harness-owned threads, this starts the
real stage 16 deployment — Semantic API, event ingestion, `session_api` and
both daemons under the canonical group ids — plus the Flink job, and injects
failures into it. **It stops and starts the `8-1-redis` container**, so don't
run it against a stack anyone else is using. ~15 minutes for all eight arms:

```bash
platform/enrichment/.venv/bin/python -m eval.8_2.failure_run
# recompute every verdict from a previous run's records, no live run:
platform/enrichment/.venv/bin/python -m eval.8_2.failure_run \
    --from-records eval/8_2/raw_failure_records.json
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
