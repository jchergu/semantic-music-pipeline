-- Stage 5 (8.1): Recommender Engine output.
-- One row per (run_id, seed_track_id, rank): a ranked recommendation
-- produced by one batch invocation of recommend.py for one seed track.
-- Applied idempotently by recommend.py at startup, same pattern as
-- ingest.py/enrich.py applying ingestion/schema.sql. Assumes `tracks`
-- already exists (stages 2-4 already ran) — this file only owns the new
-- table, it does not re-apply ingestion/schema.sql.
CREATE TABLE IF NOT EXISTS recommendations (
    id                      SERIAL PRIMARY KEY,
    run_id                  UUID NOT NULL,
    seed_track_id           INTEGER NOT NULL REFERENCES tracks(id),
    recommended_track_id    INTEGER NOT NULL REFERENCES tracks(id),
    rank                    INTEGER NOT NULL,
    score                   REAL NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_recommendations_run_seed_rank UNIQUE (run_id, seed_track_id, rank),
    CONSTRAINT chk_recommendations_not_self CHECK (seed_track_id <> recommended_track_id)
);

CREATE INDEX IF NOT EXISTS idx_recommendations_run_id ON recommendations (run_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_seed_track_id ON recommendations (seed_track_id);
