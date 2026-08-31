# Stage 8 — Flink Provisioning

Status: **verified working**, 2026-08-31.

## What this stage does

The second of the not-started streaming platform stages (7-11, see
CLAUDE.md's Build order) stands up Apache Flink itself: a JobManager and a
TaskManager, both healthy on the docker-compose network, with the
JobManager's REST API confirmed reachable and the TaskManager confirmed
registered against it. This is deliberately narrow — **no Flink job is
deployed or written**, no code anywhere reads from or writes to Flink, and
nothing here touches Kafka topics, Redis, or Postgres. Those are stage 9
(Redis session cache), stage 10 (Postgres sessions/events schema), and
whatever future 8.2 stage actually submits a streaming enrichment job to
this cluster.

Code: two new services in `docker-compose.yml`
(`flink-jobmanager`, `flink-taskmanager`).

## Services

| Service | Container | Role | Port |
|---|---|---|---|
| `flink-jobmanager` | `8-1-flink-jobmanager` | cluster coordinator, REST/web UI | `${FLINK_JOBMANAGER_WEBUI_PORT:-8081}` → 8081 |
| `flink-taskmanager` | `8-1-flink-taskmanager` | executes tasks, registers with the JobManager over RPC | not exposed to host |

Both use `flink:1.19.1-scala_2.12-java11` — pinned to an exact version,
matching every other image in `docker-compose.yml` (e.g.
`confluentinc/cp-kafka:7.6.1`, `milvusdb/milvus:v2.4.5`).

`FLINK_PROPERTIES` sets `jobmanager.rpc.address: flink-jobmanager` on both
containers (the standard way the official image is configured — an
embedded YAML block via env var) so the TaskManager can find the
JobManager by Docker service name. The TaskManager additionally sets
`taskmanager.numberOfTaskSlots: 2`.

The JobManager has a healthcheck (`curl -f http://localhost:8081/config`)
matching every other service in the compose file. The TaskManager has
none — like `zookeeper`, it doesn't expose a port to check and is only
depended on by name; `flink-taskmanager` declares
`depends_on: flink-jobmanager (condition: service_healthy)` so it never
starts racing an unready JobManager.

No new named volume was added: with no jobs deployed, there's nothing
durable to persist yet (no checkpoints, no job JARs). A volume for
checkpoint/savepoint storage is deferred to whichever stage first submits
a real job.

## Verification

Manual, before writing the automated test:
1. `docker compose up -d flink-jobmanager flink-taskmanager`
2. `docker compose ps` — both containers up, jobmanager reports `healthy`.
3. `curl -sf http://localhost:8081/config` — 200, returns Flink version
   `1.19.1`.
4. `curl -s http://localhost:8081/taskmanagers` — after a few seconds,
   reports one registered task manager with 2 free slots.

## Test results

`tests/test_stage8_flink.py`, against the live cluster, no mocking:
- `test_jobmanager_rest_reachable` — GETs `/config`, asserts 200 and a
  `flink-version` key in the response.
- `test_taskmanager_registered` — polls `/taskmanagers` for up to 30s
  (TaskManager registration lags the JobManager's own healthcheck by a
  couple of seconds after a cold start) and asserts at least one entry.

Both pass, alongside the existing 35 (stages 2-4+7, and 8.1's stages 5-6)
— **37/37 total**, zero regressions.

## Reproducing / extending

Requires `docker compose up -d` with `flink-jobmanager` and
`flink-taskmanager` healthy/running.

```bash
docker compose up -d flink-jobmanager flink-taskmanager
platform/enrichment/.venv/bin/python -m pytest tests/test_stage8_flink.py -v
```

Explicitly deferred to later stages (do not build against this doc as if
they're done): any actual Flink job — streaming enrichment, session-state
computation, or otherwise — Redis session state (stage 9), the Postgres
sessions/events schema (stage 10), the ingestion service (stage 11), and
8.2 as the first real consumer of this cluster.
