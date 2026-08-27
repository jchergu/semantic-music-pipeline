-- L1 metadata system of record for the 8.1 seed dataset.
CREATE TABLE IF NOT EXISTS tracks (
    id                          SERIAL PRIMARY KEY,
    jamendo_id                  TEXT NOT NULL UNIQUE,
    title                       TEXT NOT NULL,
    artist_name                 TEXT NOT NULL,
    duration_sec                INTEGER,
    genre_tags                  TEXT[],
    license_ccurl               TEXT,
    minio_bucket                TEXT NOT NULL,
    minio_object_key            TEXT NOT NULL,
    chromaprint_fingerprint     TEXT,
    musicbrainz_recording_id    TEXT,
    musicbrainz_match_score     REAL,
    ingested_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    enriched_at                 TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_tracks_musicbrainz_recording_id
    ON tracks (musicbrainz_recording_id);

-- Added in stage 3 (L2 enrichment); ALTER needed since `tracks` already
-- exists from stage 2 and CREATE TABLE IF NOT EXISTS is a no-op on it.
ALTER TABLE tracks ADD COLUMN IF NOT EXISTS enriched_at TIMESTAMPTZ;
