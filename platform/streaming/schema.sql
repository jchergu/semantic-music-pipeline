-- Stage 10: sessions/events durability for the platform-owned consumer
-- from Decision B (CLAUDE.md). Applied idempotently by
-- session_store.apply_schema() at call time, same pattern as
-- usecases/8_1_batch_reactive/recommender/schema.sql applying its own
-- schema.sql. No FK to `tracks` -- the event payload shape is still ad
-- hoc (see docs/platform/stage9-redis.md), not a frozen contract.
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

CREATE INDEX IF NOT EXISTS idx_events_session_id ON events (session_id);
