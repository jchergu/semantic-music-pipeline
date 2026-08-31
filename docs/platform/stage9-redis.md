# Stage 9 — Redis Session Cache

Status: **verified working**, 2026-08-31.

## What this stage does

The third of the not-started streaming platform stages (7-11, see
CLAUDE.md's Build order) gives Redis its first real callers: a session
cache module (`platform/streaming/session_state.py`) and a Kafka
consumer that reads `behavioral-events` and writes into it
(`platform/streaming/session_consumer.py`). Together these are the Redis
half of "the Kafka consumer that reads behavioral events and maintains
session state in Redis" from CLAUDE.md's Decision B — that decision
explicitly says the consumer spans **stages 9-10**: this stage gives it
session state in Redis, stage 10 later adds Postgres durability on top of
the same consumer. This is deliberately narrow — **no Postgres write path
exists yet**, there's **no real ingestion service** producing behavioral
events in a live flow (stage 11), and the event shape used here (a JSON
object with a `session_id` key) is ad hoc, not a frozen contract, the same
stance stage 7 already took with its plain string payloads (see
`contracts/README.md`).

Code: `platform/streaming/session_state.py` (Redis read/write),
`platform/streaming/session_consumer.py` (Kafka → Redis wiring),
`platform/streaming/config.py` (Redis host/port/TTL constants, added
alongside stage 7's Kafka config).

## Design

`session_state.py` exposes two functions against a module-level
`redis.Redis` client:
- `record_event(session_id, event)` — `RPUSH`es the JSON-encoded event
  onto `session:{session_id}:events`, then `EXPIRE`s the key to
  `SESSION_TTL_SECONDS` (1800s). The TTL reset on every call makes it a
  sliding window: a session survives 30 minutes past its *last* event, not
  its first.
- `get_session_events(session_id)` — `LRANGE`s the list back,
  JSON-decoded.

`session_consumer.py::consume_and_cache_one` is a bounded poll loop
against `TOPIC_BEHAVIORAL_EVENTS` (same deadline/0.5s-slice shape as
stage 7's `consumer.py::consume_one`) that JSON-decodes each message and
calls `record_event`. Two defensive skips were needed once tested against
the live (non-empty) topic:
- Non-JSON messages are skipped — `behavioral-events` already carries
  stage 7's own plain-string test payloads from earlier runs, which
  aren't behavioral events at all and would otherwise crash `json.loads`.
- `expected_session_id`, when given, skips any message whose
  `session_id` doesn't match — the same stale-message problem stage 7
  found and fixed with `expected_value` (topics aren't purged between
  runs, so a fresh "earliest" consumer group can surface a leftover
  message from a previous run instead of the one this call just
  produced).

## Verification

Manual, before writing the automated test:
```bash
platform/enrichment/.venv/bin/python -m pip install redis==5.0.8
cd platform && enrichment/.venv/bin/python -c "
from streaming.session_state import record_event, get_session_events
record_event('smoke-test', {'event_type': 'play'})
print(get_session_events('smoke-test'))
"
```
Printed `[{'event_type': 'play'}]` — direct Redis round trip confirmed
before wiring the Kafka side.

## Test results

`tests/test_stage9_redis.py`, against the live stack, no mocking:
- `test_record_and_get_session_events` — direct Redis round trip.
- `test_session_ttl_is_set` — TTL is set and within `SESSION_TTL_SECONDS`
  after a write.
- `test_consume_and_cache_from_behavioral_events` — produces a JSON event
  onto `behavioral-events` via stage 7's `producer.produce()`, consumes
  and caches it via `consume_and_cache_one`, asserts it landed in Redis —
  the real Kafka → Redis path, not just Redis in isolation.

All 3 pass, alongside the existing 37 (stages 2-4+7-8 and 8.1's stages
5-6) — **40/40 total**, zero regressions.

## Reproducing / extending

Requires `docker compose up -d` with `kafka`, `zookeeper`, and `redis`
healthy (all provisioned since stage 1; nothing new to start for this
stage).

```bash
platform/enrichment/.venv/bin/python -m pip install -r platform/streaming/requirements.txt
platform/enrichment/.venv/bin/python -m pytest tests/test_stage9_redis.py -v
```

Explicitly deferred to later stages (do not build against this doc as if
they're done): Postgres durability for this same consumer (stage 10), a
frozen event schema in `contracts/` (stays empty until 8.2 is a second
real consumer), a real ingestion service actually producing behavioral
events in a live flow (stage 11, Decision A), and 8.2/8.3 as real
consumers of session state.
