# Stage 4 — Semantic API (FastAPI)

Status: **verified working**, 2026-08-07.

## What this stage does

One shared, read-only FastAPI service over the three L1/L2 stores built in
stages 2-3: Postgres (track metadata), Milvus (CLAP embeddings), Neo4j
(Track/Artist/Genre graph). Per the fixed architecture, this is meant to be
reused by every L3 consumer — the stage 5 Recommender Engine now, and
eventually Similarity Search / Auto-tagging / Playlist Generation for
8.2/8.3 — so it exposes generic reads, not anything recommender-specific.

Code: `platform/semantic_api/main.py` (app + routes), `platform/semantic_api/dependencies.py` (connection
lifecycle).

## Endpoints

| Endpoint | Source | Notes |
|---|---|---|
| `GET /health` | all three | per-service boolean status; 503 if any is down |
| `GET /tracks/{id}` | Postgres | metadata row; 404 if missing |
| `GET /tracks/{id}/similar?k=10` | Milvus + Postgres | CLAP cosine similarity search, excludes the query track itself, joined back to Postgres for title/artist |
| `GET /tracks/{id}/graph` | Neo4j | artist, genres, up to 10 tracks sharing a genre |
| `GET /artists/{name}/tracks` | Neo4j | tracks performed by an artist |
| `GET /genres/{name}/tracks` | Neo4j | tracks tagged with a genre |

`/tracks/{id}/similar` and `/tracks/{id}/graph` both 404 through the same
`_fetch_track_row` check used by `/tracks/{id}`, so an unknown track ID
fails the same way everywhere instead of leaking a Milvus/Neo4j-specific
empty-result response.

## Connection lifecycle

A FastAPI `lifespan` context (`platform/semantic_api/dependencies.py: connect_all` /
`disconnect_all`) opens, once per process:

- a `psycopg2.pool.ThreadedConnectionPool` (1-10 connections, autocommit —
  every query here is a read, no transaction needs to stay open across a
  request)
- one Milvus `connections.connect(alias="api")` + a loaded `Collection`
  handle, reused across requests. Uses a dedicated alias rather than
  pymilvus's default `"default"` — see `usecases/8_1_batch_reactive/docs/stage6-e2e.md` for why (a
  cross-test-file bug found while building stage 6, fixed here).
- one `neo4j.GraphDatabase.driver`, with a fresh `session()` per request
  (the driver-level connection pooling happens underneath, per the neo4j
  Python driver's own recommended usage)

All three are exposed via `Depends(...)` so route handlers don't touch
connection setup directly.

## Verification

Ran the server locally (`uvicorn semantic_api.main:app --port 8010`) and hit every
endpoint by hand against the live 411-track dataset before writing
automated tests:

- `/health` → `{"status":"ok","services":{"postgres":true,"milvus":true,"neo4j":true}}`
- `/tracks/1` → real metadata for track 1 (`"Wish You Were Here"` by
  `The.madpix.project`)
- `/tracks/1/similar?k=3` → 3 other tracks, sorted by descending cosine
  score, track 1 itself absent
- `/tracks/1/graph` → artist + 3 genres + 10 genre-sibling tracks
- `/tracks/999999999` → `404`
- `/artists/The.madpix.project/tracks`, `/genres/house/tracks` → correct
  listings

`tests/test_stage4_api.py` (FastAPI `TestClient`, module-scoped fixture so
the lifespan connects once per test run, not once per test):

- `test_health` — all three services report up
- `test_get_track` — response matches the first Postgres row by field
- `test_get_track_not_found` — 404 for a nonexistent ID
- `test_similar_tracks_excludes_self_and_respects_k` — exactly `k` results,
  none of them the query track, scores in descending order
- `test_similar_tracks_not_found` — 404 for a nonexistent ID
- `test_track_graph_matches_postgres` — returned artist/genres match the
  Postgres row for the same track
- `test_artist_tracks_lookup`, `test_genre_tracks_lookup` — nonempty, real
  results

All 7 pass, alongside the existing 5 stage 2 + 6 stage 3 tests (19/19
total across all three stages).

## Packaging note

`platform/semantic_api/install.sh` exists instead of a plain `pip install -r
requirements.txt` for the same reason as `platform/enrichment/install.sh`:
`pymilvus==2.4.9` needs `setuptools<81` (pkg_resources was removed in
setuptools 81+) and a newer `environs` than it declares (its
`environs<=9.5.0` pin resolves to a version bundled with a `marshmallow`
that's incompatible with the one actually resolved here). No laion-clap
involved this time, so it's a much shorter script — install everything
with `setuptools<81` pinned, then upgrade `environs` afterward. The
resulting "pip's dependency resolver..." conflict warning at the end is
cosmetic; imports and all tests pass.

## Reproducing / extending

```bash
cd api
python3 -m venv .venv
source .venv/bin/activate  # or use .venv/bin/python directly
bash install.sh

cd ..
platform/semantic_api/.venv/bin/python -m uvicorn semantic_api.main:app --app-dir platform --reload --port 8010   # run the server

platform/enrichment/.venv/bin/python -m pip install fastapi==0.115.0 "uvicorn[standard]==0.32.0" httpx==0.27.2
platform/enrichment/.venv/bin/python -m pytest tests/                          # verify (stages 2+3+4)
```

Requires the full stack up (`docker compose up -d`) and stage 3 already run
(the API reads Milvus/Neo4j data that `platform/enrichment/enrich.py` writes — it
doesn't populate anything itself).
