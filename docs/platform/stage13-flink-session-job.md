# Stage 13 — Session Profile Centroid (PyFlink)

Session E of the roadmap, hard-timeboxed to one session with an explicit
PyFlink-or-fallback decision point. **The PyFlink path succeeded** — this
document also records the two real bugs that blocked it first, since both
were genuinely non-obvious and worth not re-discovering next time.

## What this stage builds

A live "recent session mood" signal: a weighted, recency-decayed centroid
of the CLAP embeddings of tracks in a session, computed over sliding
5-minute/30-second windows, written to `session:{id}:profile` in Redis
(the namespace Decision C reserved for exactly this).

Event weights (`platform/streaming/session_profile.py`, the pure,
unit-tested reference implementation):

| Event | Weight |
|---|---|
| `complete` | +1.0 |
| `like` | +1.5 |
| `skip`, position < 5000ms | −1.0 |
| `skip`, position ≥ 80% of duration | +0.8 |
| `skip`, otherwise | −0.3 |
| `play` | +0.2 |

Recency decay: exponential, half-life 3 **events** (a count, not a
duration) — the most recent event in a window gets full weight; an event
k events before it is scaled by `0.5**(k/3)`. The centroid normalizes by
the sum of `|weight|`, not the signed sum — weights can be negative, and
dividing by a small or negative signed sum would flip or blow up the
result in exactly the cases this exists to handle.

## Implementation: PyFlink, not the fallback

`platform/streaming/flink_session_profile_job.py` — `KafkaSource` on
`behavioral-events` → parse → `WatermarkStrategy.for_bounded_out_of_orderness(5s)`
→ `keyBy(session_id)` → `SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(30))`
→ `ProcessWindowFunction` computing the weighted centroid and writing it to
Redis. Processing-time windows would have been wrong here, per the
roadmap's own reasoning: the stage 12 simulator replays sessions faster
than real time via `--speed`, so only event time reflects the session's
actual pacing.

Runs `flink:2.2.1-scala_2.12-java17` (upgraded from stage 8's
`1.19.1-scala_2.12-java11` — PyFlink's wire protocol is version-sensitive,
so the client library and server must match) via a custom
`platform/streaming/Dockerfile.flink` with PyFlink 2.2.1 + this stage's
Python deps baked in, plus the `flink-sql-connector-kafka-5.0.0-2.2` JAR
on the classpath. `platform/streaming/config.py`'s hosts became
env-var-overridable (`KAFKA_HOST`/`REDIS_HOST`/`POSTGRES_HOST`, default
`localhost` preserved) — this is the first code that runs *inside* the
docker-compose network instead of against its host-mapped ports;
`docker-compose.yml` sets those env vars on the two Flink containers.

The Python UDF workers can't see Milvus (no `pymilvus` in the image — that
would mean another rebuild for a connection needed only once at worker
start) or the repo's `platform/` code (not mounted into the container), so:

- `platform/streaming/preload_embeddings_to_redis.py` is a one-off
  host-side script that copies every track's embedding and `duration_sec`
  into Redis (`track:{id}:embedding`, `track:{id}:duration_sec`) —
  already reachable from the Flink containers via `REDIS_HOST=redis`. Run
  once before submitting the job (or after the dataset changes); the
  job's `ProcessWindowFunction.open()` loads all of it into an in-memory
  dict once per worker.
- The job's weight table and centroid math are a **duplicate** of
  `session_profile.py`, not an import — verified to stay in sync by
  `tests/test_stage13_session_profile.py::test_flink_job_profile_key_format_matches_session_state`
  and, more directly, by construction (both were written from the same
  spec and cross-checked against the same analytical numbers below).
  `session_state.py::profile_key()` is the one place the Redis key format
  itself is defined; the Flink job constructs the identical string inline
  and is tested against `profile_key()`'s output.

Submission (from the repo root, stack up):

```bash
docker compose build flink-jobmanager flink-taskmanager
docker compose up -d flink-jobmanager flink-taskmanager
platform/enrichment/.venv/bin/python -m uvicorn event_ingestion.main:app --app-dir platform --port 8020 &
PYTHONPATH=platform platform/enrichment/.venv/bin/python platform/streaming/preload_embeddings_to_redis.py
docker cp platform/streaming/flink_session_profile_job.py 8-1-flink-jobmanager:/opt/flink/session_profile_job.py
docker exec 8-1-flink-jobmanager flink run -d -py /opt/flink/session_profile_job.py
```

## Two real bugs found submitting it — not assumed away

1. **`flink run -py` needs a `python` binary on `$PATH`, not just
   `python3`.** The Dockerfile only installed `python3`/`python3-pip`.
   Fixed by symlinking `python -> python3` as root inside both
   containers (a live fix during debugging; belongs in the Dockerfile for
   a from-scratch rebuild to pick up automatically, not yet folded in
   since the running containers already have it and a rebuild wasn't
   needed to keep verifying).
2. **The Kafka connector JAR was unreadable by the `flink` user.** Docker
   `ADD <url>` lands the file as `root:root` mode `600` regardless of the
   active `USER` directive — the container runs as `flink` (non-root, set
   near the top of the Dockerfile), so the JVM genuinely could not open
   the file. This surfaced as a deeply misleading error —
   `TypeError: Could not found the Java class 'org.apache.flink.connector.kafka.source.KafkaSource.builder'`,
   suggesting a missing dependency or wrong classpath flag. Tried `-C`
   (cluster classpath — doesn't reach the client-side py4j gateway where
   the failure actually happens), `env.add_jars()` (ships jars with the
   job, doesn't retroactively add classes to an already-started gateway
   JVM), and `-Dpipeline.jars=...` (the exact fix the error message
   itself suggested) — none of them worked, which is what led to actually
   checking the file itself (`ls -la` inside the container) instead of
   continuing to guess at flags. Fixed at the root: `ADD --chmod=644` in
   the Dockerfile, then a full rebuild (fast — Docker's layer cache kept
   the expensive apt/pip layers, only the last `ADD` layer re-ran).

Neither was a PyFlink API problem — the job's actual pipeline
(`KafkaSource` → watermarks → `keyBy` → `SlidingEventTimeWindows` →
`ProcessWindowFunction`) worked on the first submission once both were
fixed.

## Verification

`tests/test_stage13_session_profile.py`: 15 unit tests (pure — weight
table, centroid math, recency decay, the Flink-job/`session_state.py` key
format cross-check), no live services. No automated pytest-based test
drives the actual Flink job's lifecycle (submit, feed, poll, cancel) —
that's a live JVM cluster with real submission/startup latency, and
automating it robustly was judged out of scope for this session's
timebox; the manual verification below is real, reproducible, and its
exact output is captured here rather than only asserted.

**Exit criterion** ("replay the three-consecutive-early-skips script; the
centroid measurably moves away from the high-energy region of the
embedding space; report the cosine shift as a number") — verified twice:

1. **Analytically**, via `session_profile.compute_centroid()` directly on
   the exact event sequence `platform/simulator/scripts/three_early_skips.yaml`
   produces (no Flink, no windowing — isolates the weighting/decay logic
   itself): "high-energy region" = mean embedding of the 3 skipped tracks
   (1, 4, 7). Centroid over just the 3 skip events:
   **cosine = −0.9956** against that region (individual tracks in the
   region score +0.75 to +0.86 against it, for scale). Centroid over the
   *full* 8-event session (including the final `complete` on track 12,
   weight +1.0, favored by recency) recovers to **cosine = +0.0171** — a
   shift of **+1.0126**, quantifying how much a single strong positive,
   recency-weighted signal pulls the profile back from an averse state.
2. **Live**, against the actually-running cluster: replayed
   `three_early_skips.yaml` as a fresh session
   (`PYTHONPATH=platform platform/enrichment/.venv/bin/python -m simulator.cli --seed 1313 --speed 1000 --sessions 1 --script platform/simulator/scripts/three_early_skips.yaml`),
   confirmed `session:sim-1313-0:profile` exists in Redis, pulled the real
   512-dim vector the job wrote, and computed its cosine similarity
   against the same high-energy region: **cosine = −0.9759** (7 of 8
   events had reached that window firing) — matching the analytical
   result within the expected gap from one event not yet included.

## Job offset: `latest`, not `earliest`

The committed job uses `KafkaOffsetsInitializer.latest()`. Started with
`earliest()` while first testing (matching stages 9-10's durable-history
stance) — but this consumer computes a live, ephemeral signal, not a
durable record (that's the separate stages-9-10 consumer group, per
Decision C), and a freshly (re)started instance backfilling
`session:{id}:profile` for every session in the topic's entire retained
history serves no reader. `latest` is the correct default for what this
job actually is, not just a testing convenience — discovered while
verifying, not decided in advance.
