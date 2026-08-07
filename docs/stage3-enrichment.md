# Stage 3 — L2 semantic enrichment (CLAP → Milvus, Neo4j)

Status: **verified working**, 2026-08-07.

## What this stage does

Batch job over the stage 2 seed dataset: computes a CLAP audio embedding for
every track, indexes it in Milvus for similarity search, and writes a
Track/Artist/Genre subgraph into Neo4j from the existing Postgres metadata.
Idempotent — only processes rows where `tracks.enriched_at IS NULL`, so a
partial or interrupted run can simply be re-invoked.

Code: `enrichment/enrich.py`. Schema change: `enriched_at TIMESTAMPTZ` added
to `tracks` (`ingestion/schema.sql`, applied automatically by `enrich.py`
on startup, same pattern as `ingest.py`).

## Embedding model

[laion_clap](https://github.com/LAION-AI/CLAP) 1.1.6, `enable_fusion=False`,
default pretrained checkpoint (`630k-audioset-best.pt`, non-fusion,
630k+AudioSet training data). Produces a 512-dim float embedding per track.
Runs on CPU — no GPU used or required, consistent with the seed-dataset
scale constraint.

Tracks are embedded in batches of 8 (`--batch-size`, default 8) to amortize
model call overhead; each batch downloads its audio from MinIO to a temp
file, runs `get_audio_embedding_from_filelist`, and cleans up.

## Milvus collection

- Name: `track_embeddings`
- Schema: `track_id` (INT64, primary key, matches Postgres `tracks.id`),
  `embedding` (FLOAT_VECTOR, dim=512)
- Index: IVF_FLAT, COSINE metric, `nlist=128`

## Neo4j graph schema

```
(:Artist {name})-[:PERFORMED]->(:Track {track_id, jamendo_id, title})
(:Track)-[:HAS_GENRE]->(:Genre {name})
```

`track_id` mirrors the Postgres `tracks.id` primary key so the three stores
(Postgres, Milvus, Neo4j) stay joinable on the same integer ID.

## Run outcome

| Metric | Value |
|---|---|
| Tracks enriched | 411 / 411 (100%, zero skips or failures) |
| Milvus vectors | 411, dim 512 |
| Neo4j `:Track` nodes | 411 |
| Neo4j `:Artist` nodes | 201 |
| Neo4j `:Genre` nodes | 77 (matches distinct genre tag count from stage 2) |
| Neo4j `:PERFORMED` relationships | 411 |
| Neo4j `:HAS_GENRE` relationships | 764 |
| Wall-clock for the 406-track batch run | ~4 min (~0.6s/track), CPU-only, `batch_size=8` |

(5 tracks were enriched in an initial `--limit 5` dry run to validate the
pipeline end-to-end before committing to the full batch; the remaining 406
picked up automatically via the `enriched_at IS NULL` idempotency check.)

## Packaging note: why `enrichment/install.sh` instead of `pip install -r requirements.txt`

`laion-clap==1.1.6`'s own package metadata pins `numpy==1.23.5` exactly.
That version has no Python 3.12 wheel and fails to build from source under
modern setuptools (`pkgutil.ImpImporter` was removed in 3.12). It also
doesn't declare `torch` or `torchvision` as dependencies at all, despite
needing both, and pulls in an unpinned `transformers`, which resolves to a
version far too new for its 2023-era `from transformers import BertModel,
RobertaModel, BartModel` style imports.

`install.sh` works around this by installing `laion-clap` with `--no-deps`
and pinning its transitive dependencies to versions verified to work
together on this Python 3.12 / CPU-only setup (see the script for the exact
sequence and each override's rationale). `requirements.txt` is kept as a
version record of what ends up installed, not as something to feed directly
to `pip install -r`.

Two more runtime overrides worth calling out because they look like errors
but aren't: `pymilvus==2.4.9` needs `setuptools<81` (pkg_resources was
removed in setuptools 81) and a newer `environs` than it declares (its
`environs<=9.5.0` pin ships with a `marshmallow` incompatible with the one
actually resolved here). Both are pinned in `install.sh`; the resulting
"pip's dependency resolver..." conflict warnings are cosmetic — imports and
the E2E test pass.

## Verification

`tests/test_stage3_enrichment.py`:

- `test_every_track_is_enriched` — every Postgres row has `enriched_at` set
- `test_milvus_vector_count_matches_tracks` — Milvus entity count == Postgres row count
- `test_milvus_embedding_dim` — collection's vector field is 512-dim
- `test_milvus_ids_match_postgres_ids` — Milvus `track_id`s == Postgres `id`s (no orphans either direction)
- `test_neo4j_track_node_count_matches_postgres` — Neo4j `:Track` count == Postgres row count
- `test_neo4j_every_track_has_an_artist` — every `:Track` has an inbound `:PERFORMED` edge

All 6 pass, alongside the existing 5 stage 2 tests (11/11 total).

**Milvus gotcha found while re-testing:** right after `enrich.py --force`
re-enriches all 411 tracks (upserting on the existing `track_id` primary
keys), `Collection.num_entities` transiently read 822 (411×2) — Milvus
counts tombstoned pre-upsert segments until a background compaction
reconciles them, even though a query already returns exactly 411 correct,
deduplicated rows. The tests query for the true row count/ID set instead of
trusting `num_entities` right after a write, so this doesn't cause a false
failure — but it's worth knowing about if you see `num_entities` look wrong
immediately after a bulk upsert elsewhere in this project.

## Reproducing / extending

```bash
cd enrichment
python3 -m venv .venv
source .venv/bin/activate  # or use .venv/bin/python directly
bash install.sh

cd ..
enrichment/.venv/bin/python enrichment/enrich.py              # fresh/resume run
enrichment/.venv/bin/python enrichment/enrich.py --limit 5    # small dry run
enrichment/.venv/bin/python enrichment/enrich.py --force      # re-enrich everything

enrichment/.venv/bin/pip install -r requirements-dev.txt      # adds pytest, for running tests from this venv
enrichment/.venv/bin/pytest tests/                             # verify (stage 2 + stage 3)
```

Requires the full stack up (`docker compose up -d`) — Postgres, MinIO,
Milvus (+ etcd/MinIO deps), Neo4j — and the same `.env` used by stage 2.
First run downloads the ~1.8GB CLAP checkpoint into
`enrichment/.venv/lib/python3.12/site-packages/laion_clap/`; cached after
that (`load_ckpt()` skips the download if the file already exists).
