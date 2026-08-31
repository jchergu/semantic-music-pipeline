# Stage 11 — Event Ingestion Path

Status: **verified working**, 2026-08-31.

## What this stage does

The last item on the platform build order (stages 7-11, see CLAUDE.md's
Build order) gives behavioral events somewhere to actually land: a new
FastAPI service, separate from the Semantic API, per Decision A. It
accepts one HTTP request per event and produces it onto the
`behavioral-events` Kafka topic — from there, the platform-owned consumer
built across stages 9-10 (`platform/streaming/session_consumer.py`) picks
it up and caches it in Redis and Postgres exactly as before. This closes
the loop stages 7-10 opened: those stages proved the plumbing worked with
manually-produced test messages; this stage is the first real caller.

Per Decision A, this is **not** an addition to
`platform/semantic_api/main.py`'s endpoint list — the Semantic API stays
read-only. This is deliberately narrow — **no 8.2/8.3 code** was touched,
and the event shape stays ad hoc (a JSON object with `session_id` and
`event_type`, extra fields allowed), not a frozen contract in
`contracts/`, which stays empty until 8.2 is a second independent
producer/consumer.

Code: `platform/event_ingestion/main.py` (the service),
`platform/event_ingestion/requirements.txt`.

## Design

`POST /events` — body validated by a `BehavioralEvent` pydantic model
(`session_id: str`, `event_type: str`, `track_id: str | None = None`,
`model_config = ConfigDict(extra="allow")` to keep the shape extensible
rather than frozen). Calls stage 7's
`streaming.producer.produce(TOPIC_BEHAVIORAL_EVENTS,
json.dumps(event.model_dump()), key=event.session_id)` — no new Kafka
code, just a new caller. Returns `202` + `{"status": "accepted"}`.

`GET /health` — `{"status": "ok"}`. Unlike the Semantic API's three-store
health check, this service holds no persistent store connections of its
own (Kafka producers here are per-call, matching `producer.py`'s existing
design), so there's nothing else to probe.

Run the same way as the Semantic API: `uvicorn event_ingestion.main:app
--app-dir platform`.

## Verification

Manual, before writing the automated test:
```bash
platform/enrichment/.venv/bin/python -m pip install -r platform/event_ingestion/requirements.txt
cd platform && enrichment/.venv/bin/python -m uvicorn event_ingestion.main:app --port 8020 &
curl -sf http://localhost:8020/health
curl -sf -X POST http://localhost:8020/events \
  -H 'Content-Type: application/json' \
  -d '{"session_id": "smoke-test", "event_type": "play", "track_id": "abc123"}'
```
Both returned as expected (`{"status":"ok"}`, `{"status":"accepted"}`).

## Test results

`tests/test_stage11_event_ingestion.py`, against the live stack (real
Kafka, Redis, Postgres — no mocking), using a new `event_ingestion_server`
fixture in `tests/conftest.py` (same subprocess-uvicorn-on-a-free-port
shape as the existing `semantic_api_server` fixture):
- `test_health` — `GET /health` returns 200.
- `test_post_event_produces_to_kafka` — `POST /events`, then confirms the
  exact event landed on `behavioral-events` via
  `session_consumer.consume_and_cache_one`.
- `test_end_to_end_http_to_redis_and_postgres` — the capstone: `POST
  /events` through the live service, drives the platform-owned consumer
  one step (`consume_and_cache_one(..., pg_conn=...)`), then confirms the
  event is readable back out of both Redis (stage 9) and Postgres (stage
  10) — the full HTTP → Kafka → Redis + Postgres chain, in the same
  spirit as 8.1's own stage-6 end-to-end test.

All 3 pass, alongside the existing 43 (stages 2-4+7-10 and 8.1's stages
5-6) — **46/46 total**, zero regressions.

## Reproducing / extending

Requires `docker compose up -d` with `kafka`, `zookeeper`, `redis`, and
`postgres` healthy (all provisioned since stages 1/7/9/10; nothing new to
start for this stage).

```bash
platform/enrichment/.venv/bin/python -m pip install -r platform/event_ingestion/requirements.txt
platform/enrichment/.venv/bin/python -m pytest tests/test_stage11_event_ingestion.py -v
```

This closes the platform build order (stages 1-11). What's left is 8.2
(streaming, reactive) itself — building its own use-case stages on top of
this now-complete platform, per CLAUDE.md's "do not build 8.2/8.3 infra
unless explicitly asked."
