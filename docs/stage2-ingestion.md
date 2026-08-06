# Stage 2 — Seed dataset ingestion

Status: **verified working**, 2026-08-06.

## What this stage does

L1 ingestion for the 8.1 batch/reactive pipeline: pulls a seed dataset of
real, licensed audio from the Jamendo API, uploads the raw audio to MinIO
(object storage), computes a Chromaprint fingerprint per track, attempts a
best-effort AcoustID → MusicBrainz linkage, and writes the metadata to
PostgreSQL (system of record, per the fixed architecture).

Code: `ingestion/ingest.py`, schema in `ingestion/schema.sql`.

## Why Jamendo, not FMA

Disk budget was the deciding constraint: the dev machine had only 12GB free
at the time. FMA (Free Music Archive) is distributed as one large archive
per split — even the smallest split (`fma_small`) is a ~7.2GB download
before any subsetting is possible. Jamendo's API serves tracks individually
via a free API key, so the download footprint scales with how many tracks
you actually keep (~2.3GB for the 411 tracks ingested here) instead of
requiring the whole corpus up front.

MusicBrainz itself was never a candidate audio source — it's a metadata-only
database. The architecture's "Chromaprint/AcoustID → MusicBrainz linkage"
step assumes audio comes from elsewhere first; MusicBrainz is the linkage
target, not the source.

## Dataset outcome

| Metric | Value |
|---|---|
| Tracks ingested | 411 (target was 500; see "Known limitation" below) |
| Total audio size in MinIO | 2.31 GB |
| Duration range | 76s – 953s, mean 231s |
| Distinct genre tags | 77 |
| AcoustID → MusicBrainz match rate | 49.4% (203/411) |

Full metrics: `reports/stage2/metrics.json`. Plots: `reports/stage2/*.png`
(genre distribution, duration histogram, MusicBrainz match rate) — generated
by `reports/stage2_metrics.py`, safe to re-run any time the dataset changes.

The ~49% MusicBrainz match rate is expected, not a bug: a large share of
Jamendo's catalog is independent/self-released music that was never
submitted to MusicBrainz. This is itself a legitimate observation for the
thesis — real-world fingerprint linkage against MusicBrainz is partial by
nature for non-mainstream corpora, and the pipeline needs to tolerate
`musicbrainz_recording_id IS NULL` as a normal case downstream (L2/L3
must not assume every track has an MBID).

## Known limitation: Jamendo pagination instability

Target was 500 tracks. First fetch (offset 0-200, `order=popularity_total`)
got 200 tracks; a second fetch (offset 200-500) was needed to reach 500, but
~90 of those 300 turned out to be duplicates of the first batch (deduped via
Postgres `ON CONFLICT (jamendo_id) DO NOTHING`). Root cause: Jamendo's
`order=popularity_total` sort is not perfectly stable across separate HTTP
requests — likely ties on the popularity score get broken differently
between calls. Net result: 411 unique tracks, which is still within the
project's specified 200-500 seed range, so no further fetching was done.

Separately, an earlier attempt to paginate using `boost=popularity_total`
(relevance re-ranking) rather than `order=popularity_total` (actual sort)
silently returned 0 results for any offset >= 200 — this looks like a
documented-but-easy-to-miss quirk of the Jamendo API where `boost` doesn't
support deep pagination. `ingest.py` uses `order`, not `boost`, for this
reason (see the inline comment in `fetch_jamendo_tracks`).

The fetch loop also retries transient empty pages (observed once, cause
unclear — possibly rate limiting or eventual-consistency on Jamendo's side)
up to 4 times with backoff before concluding the catalog is exhausted.

## Verification

`tests/test_stage2_ingestion.py` (run via `ingestion/.venv/bin/pytest tests/`):

- `test_dataset_within_seed_range` — row count is 200-500
- `test_no_duplicate_jamendo_ids` — no duplicate source IDs
- `test_every_track_has_a_fingerprint` — Chromaprint ran for every row
- `test_postgres_minio_parity` — every Postgres row has a matching MinIO
  object and vice versa (catches orphaned uploads or missing files)
- `test_durations_are_plausible` — sanity bounds on duration, catches
  corrupt downloads

All 5 pass as of this writing.

## Reproducing / extending

```bash
cd ingestion
source .venv/bin/activate  # or use .venv/bin/python directly
python ingest.py --limit 500                    # fresh run
python ingest.py --limit 300 --offset 200        # resume from a given offset

cd ..
ingestion/.venv/bin/pytest tests/                # verify
ingestion/.venv/bin/python reports/stage2_metrics.py  # regenerate plots
```

Requires `fpcalc` on PATH (`sudo apt install libchromaprint-tools`) and a
`.env` with `JAMENDO_CLIENT_ID` / `ACOUSTID_API_KEY` (free signup at
devportal.jamendo.com and acoustid.org).
