# Stage 10 — Postgres Sessions/Events Schema

Status: **verified working**, 2026-08-31.

## What this stage does

The fourth of the not-started streaming platform stages (7-11, see
CLAUDE.md's Build order) gives the platform-owned consumer from Decision B
its Postgres durability half. Decision B explicitly says this consumer —
"the Kafka consumer that reads behavioral events and maintains session
state in Redis" — spans stages 9-10: stage 9 gave it Redis session state
(`platform/streaming/session_state.py`); this stage adds a `sessions`/
`events` schema and wires the *same* consumer
(`platform/streaming/session_consumer.py`) to persist to it, not a new
consumer. This is deliberately narrow — **no real ingestion service**
produces behavioral events in a live flow yet (stage 11), and the event
shape (a JSON object with a `session_id` key) is still ad hoc, not a
frozen contract, the same stance stage 7 and stage 9 already took.

Code: `platform/streaming/schema.sql` (new tables),
`platform/streaming/session_store.py` (Postgres read/write),
`platform/streaming/session_consumer.py` (extended with an optional
`pg_conn` parameter), `platform/streaming/config.py` (`PG_DSN`, added
alongside the existing Kafka/Redis config).

## Schema

```sql
CREATE TABLE IF NOT EXISTS sessions (
    session_id      TEXT PRIMARY KEY,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
    id              SERIAL PRIMARY KEY,
    session_id      TEXT NOT NULL REFERENCES sessions(session_id),
    event_type      TEXT,
    payload         JSONB NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

No FK to `tracks` — nothing here assumes the ad hoc event payload carries
a valid `track_id`. Applied idempotently by `session_store.apply_schema()`,
same pattern as `usecases/8_1_batch_reactive/recommender/schema.sql` being
applied inline by `recommend.py`.

## Design

`session_store.py`:
- `apply_schema(conn)` — executes `schema.sql`, idempotent.
- `persist_event(conn, session_id, event)` — upserts the `sessions` row
  (`ON CONFLICT (session_id) DO UPDATE SET last_seen_at = now()`), then
  inserts one `events` row (`event_type` pulled out as its own column,
  full event kept as `payload` JSONB), both in one transaction.
- `get_events(conn, session_id)` — reads events back out for
  verification.

`session_consumer.py::consume_and_cache_one` gained an optional
`pg_conn=None` parameter: when given, it calls `persist_event` right
after the existing Redis `record_event` call, so one poll now updates
both stores. Default `None` keeps stage 9's Redis-only call sites and
tests working unchanged.

## Verification

Manual, before writing the automated test:
```bash
platform/enrichment/.venv/bin/python -m pip install -r platform/streaming/requirements.txt
cd platform && enrichment/.venv/bin/python -c "
import psycopg2
from streaming.config import PG_DSN
from streaming.session_store import apply_schema, persist_event, get_events
conn = psycopg2.connect(PG_DSN)
apply_schema(conn)
persist_event(conn, 'smoke-test', {'event_type': 'play'})
print(get_events(conn, 'smoke-test'))
"
```
Printed `[{'event_type': 'play'}]` — direct Postgres round trip confirmed
before wiring the consumer.

## Test results

`tests/test_stage10_postgres_events.py`, against the live stack, no
mocking:
- `test_persist_and_get_events` — direct Postgres round trip.
- `test_persist_event_upserts_session` — two `persist_event` calls with
  the same `session_id` produce exactly one `sessions` row (not two) and
  two `events` rows.
- `test_consume_and_cache_persists_to_postgres` — produces a JSON event
  onto `behavioral-events`, consumes it with `pg_conn` set, asserts it
  landed in *both* Redis (stage 9) and Postgres (this stage) — the same
  consumer, now durable in both places, per Decision B.

All 3 pass, alongside the existing 40 (stages 2-4+7-9 and 8.1's stages
5-6) — **43/43 total**, zero regressions.

## Reproducing / extending

Requires `docker compose up -d` with `postgres`, `kafka`, `zookeeper`, and
`redis` healthy (all provisioned since stage 1/7/9; nothing new to start
for this stage).

```bash
platform/enrichment/.venv/bin/python -m pip install -r platform/streaming/requirements.txt
platform/enrichment/.venv/bin/python -m pytest tests/test_stage10_postgres_events.py -v
```

Explicitly deferred to later stages (do not build against this doc as if
they're done): a real ingestion service actually producing behavioral
events in a live flow (stage 11, Decision A), a frozen event schema in
`contracts/` (stays empty until 8.2 is a second real consumer), and
8.2/8.3 as real consumers of this data.
