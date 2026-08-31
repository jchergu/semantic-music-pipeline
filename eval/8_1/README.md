# eval/8_1 — 8.1 Evaluation Data Pack

Systems and behavior evaluation of the existing 411-seed / 4,110-row
`recommendations` table produced by
`usecases/8_1_batch_reactive/recommender/recommend.py` (stage 5). Read-only
against that table and the existing Postgres/Milvus/Neo4j stores —
`recommend.py` is never re-run, no new recommendation rows are written
anywhere.

**No accuracy metric.** There is no ground truth and no real users for this
dataset, so this pack computes no precision@k, recall@k, or NDCG — see
CLAUDE.md's `eval/` section for why.

## Regenerating

Requires the docker compose stack up (`docker compose up -d` from the repo
root) — Postgres, Milvus, and Neo4j must be healthy. No other service is
needed (this reads stores directly, not through the Semantic API).

```bash
platform/enrichment/.venv/bin/python -m eval.8_1.run
```

All dependencies (`pymilvus`, `neo4j`, `psycopg2`, `matplotlib`, `numpy`)
are already present in `platform/enrichment/.venv`; `requirements.txt` here
documents them for reference, matching every other component's convention.

Takes under a minute — the slow part is the per-seed latency measurement
(411 live Milvus + Neo4j calls).

## Outputs

- `results.json` — metrics 1 (signal contribution), 2 (catalog coverage), 3
  (intra-list diversity), 5 (KG connectivity), 6 (failure/edge cases). Fully
  deterministic: two runs produce a byte-identical file.
- `latency.json` — metric 4 (per-stage latency). **Not** deterministic —
  these are fresh wall-clock timings against the current environment, not a
  decomposition of the original stage-5 batch run's historical "~8.8s / 411
  seeds" aggregate (that number was never broken down by stage). Kept in its
  own file, separate from `results.json`, specifically so the determinism
  claim above holds literally rather than being quietly relaxed.
- `tables.md` — Markdown tables assembled from both files above, ready to
  paste into the thesis.
- `figures/*.png` — coverage long tail, diversity histogram, latency
  breakdown.

## Notable finding: the genre-boost coverage gap

`signal_contribution.genre_boost_coverage_gap` in `results.json` quantifies
something worth calling out explicitly: the Semantic API's
`/tracks/{id}/graph` endpoint caps `related_by_genre` at 10 candidates with
**no ordering** (`LIMIT 10`, no `ORDER BY` — see
`platform/semantic_api/main.py`). For genres with more than 10 member
tracks (the distribution is skewed: mean ~9.9 tracks/genre but median only
3), most of a seed's true genre-sibling tracks never receive `GENRE_BOOST`
at all, purely because of which 10 happened to come back from an unordered
Cypher query. This was confirmed empirically, not assumed — see the git
history for the diagnostic that traced specific rows back to the exact
Cypher the batch run actually issued.

## How signal contribution is reconstructed

`recommendations` only persists `(seed_track_id, recommended_track_id,
rank, score)` — `ranking.py`'s per-candidate `similarity`/`genre_sibling`/
`same_artist` booleans were never written to Postgres. `metrics.py`'s
`reconstruct_signals()` rebuilds them:

- `same_artist`: exact (`tracks.artist_name` match).
- `genre_sibling`: exact — reproduces the same capped, unordered Cypher
  query `context_builder.py` actually called at generation time
  (`kg_connectivity.capped_genre_sibling_ids`), not a broader tag-overlap
  guess.
- similarity: recovered as `score - boosts` (what `ranking.py` actually
  used), cross-checked against the true CLAP cosine (from Milvus) to detect
  whether the candidate was in the original top-25 similarity pool.

With the exact capped genre-sibling ground truth, the reconstruction is
100% internally consistent (`reconstruction_inconsistent_count: 0` in the
current results.json) — the field exists to catch any future drift, not
because it's expected to fire.
