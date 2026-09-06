# eval/8_2 — 8.2 (streaming, reactive) Evaluation Pack

**This is an active harness, not a passive one.** `eval/8_1` queries a frozen
4,110-row Postgres table that already existed and never re-runs the
recommender. There is no equivalent table here: 8.2's output is a Redis key
(`session:{id}:recs`) rewritten live by two independently-scheduled consumers
racing on one Kafka topic. So this pack *drives* the system — it starts the
services, submits the Flink job, replays a seeded event stream, runs both
consumer groups, and measures what happens while it happens.

Open this expecting orchestration and threads, not queries.

**No accuracy metric.** No ground truth and no real users exist for this
dataset, so nothing here is precision@k, recall@k or NDCG. Every metric is
latency, reactivity, coverage or semantic coherence — a systems claim.

## Status

Stage 15C covers **metrics 1, 2 and 3** of `METRICS.md` (signed off
2026-09-03): reactivity, latency per hop, and the cold-start → warm
transition. All three come off the same pivot scenario. Metrics 4/5/6/7
(coherence, coverage, determinism, late-event handling) are **stage 15C.2**
and reuse the harness built here; `results.json` carries explicit `pending`
markers for them rather than leaving the keys out.

## Running it

Needs the docker compose stack up (`docker compose up -d`) with Postgres,
Milvus, Neo4j, Kafka, Redis and both Flink containers healthy, and the track
embeddings preloaded into Redis:

```bash
PYTHONPATH=platform platform/enrichment/.venv/bin/python platform/streaming/preload_embeddings_to_redis.py
platform/enrichment/.venv/bin/python -m eval.8_2.run
```

Everything else — the Semantic API, the event ingestion service, the Flink
job, and both Kafka consumer loops — is started and torn down by the harness
itself. A run of the default K sweep takes roughly 5 minutes wall-clock.

Useful flags:

- `--speed` (default 60): wall-clock compression of the simulated session
  clock. **The debounce constants in `recommendation_refresh.py` are
  wall-clock and are not compressed**, so a higher speed means a debounce
  that is proportionally stricter in session-time terms. See
  `latency.json`'s `speed_caveat`.
- `--k 3 5` : override the K sweep.
- `--from-records eval/8_2/raw_scenario_records.json` : recompute every
  metric from a previous run's raw records, with no live run at all. The raw
  records *are* the measurement; the metric files are derived, so changing a
  definition doesn't cost another live run.
- `--no-manage-flink` : reuse an operator-submitted job instead of
  cancelling and resubmitting (debugging only).

## What must be running, and why five things not three

`METRICS.md` section 0 lists three. Building this stage found two more, both
hard blockers:

| Process | Why |
|---|---|
| Event ingestion service | The only way an event reaches Kafka (Decision A: the Semantic API is read-only). |
| **Semantic API** | Both refresh paths call `context_builder.build_context(http_client, …)`, which is pure HTTP against it. |
| Stage 13 Flink job | Writes `session:{id}:profile`; without it every refresh stays on the cold-start path forever. |
| **Stages 9-10 raw-state consumer** | Populates `session:{id}:events`. `refresh_recommendations()` returns `skip_insufficient_data` below 2 events, so with nothing running it **no refresh ever happens**. Stage 14's live test hides this by calling `record_event()` directly; this harness runs the real consumer. |
| Refresh driver loop | Repeated `process_one_event()` over one reused `Consumer`. |

The refresh driver loop is harness-owned, test-shaped code. It is **not** a
step toward a persistent refresh daemon in `platform/streaming/` — that gap
is real and stays open, the same one stage 12 flagged for the raw-state
consumer.

## Outputs

- `reactivity.json` — metric 1, per-K Jaccard curves and the crossing point.
- `latency.json` — metric 2. **Not** a deterministic artifact (fresh
  wall-clock timings), same stance `eval/8_1/latency.json` takes.
- `results.json` — metric 3, plus `pending` markers for 15C.2's metrics.
- `raw_scenario_records.json` — every post, refresh result, snapshot and
  profile sample. The measurement of record; feeds `--from-records`.
- `tables.md`, `figures/*.png`.

## Two caveats that are in the data, not just here

1. **The K sweep is nested.** `random.Random(seed).sample(pool, k)` draws
   sequentially from one seeded stream, so K=3 ⊂ K=5 ⊂ K=8 ⊂ K=12 — every K
   opens on the same tracks. `METRICS.md`'s "Resolved before handoff" section
   states the opposite; it was wrong, and is corrected there now. The nesting
   is arguably *better* for metric 1 (only pre-pivot length varies) but it
   means the K runs are not independent samples.
2. **Metric 3's four sessions are one observation.** Following directly from
   (1): all four share a cold-start seed track, so all four report the same
   handoff Jaccard. `results.json` reports
   `cold_warm_transition.independent_observations` as 1 and refuses to let
   the mean be read as an average over independent runs.

## Test filenames here carry an `82_` prefix

Not style. pytest derives module names from file paths, and `8_1`/`8_2`
aren't valid Python identifiers, so same-named test files across the two eval
packages resolve to the same module: before the rename a full-suite run
collected `eval/8_1/tests/test_metrics.py` twice and ran none of this pack's
metric or smoke tests. Keep any future `eval/8_3/tests/` files uniquely named.

## Session ids are scoped per invocation

Session ids carry a `--run-id` (a UTC timestamp by default). This is not
cosmetic: `behavioral-events` is append-only and never purged, and both
consumer groups start from `earliest`, so reusing a session id makes the next
run replay the previous run's events for that session. Observed live while
building this stage — a re-run of K=3 consumed 22 stale events, exhausted its
event budget and reported 0 post-pivot refreshes. Scenario *content* stays
fully seed-determined; only the keying moves.
