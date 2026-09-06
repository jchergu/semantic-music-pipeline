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

**All seven metrics of `METRICS.md` (signed off 2026-09-03) are built.**
Stage 15C landed metrics 1, 2 and 3 — reactivity, latency per hop, and the
cold-start → warm transition — off one pivot scenario. Stage 15C.2 added
metrics 4 (semantic coherence), 5 (catalog coverage), 6 (determinism) and 7
(late-event handling) on the same harness, with no new orchestration.

## The eight scenarios

| Scenario | Feeds | Speed | Jitter |
|---|---|---|---|
| `pivot_K3` / `K5` / `K8` / `K12` | metrics 1, 2, 3 | 60 | 0 |
| `long_session` — `rock(10)→electronic(10)→chillout(10)→dance(10)` | metrics 4, 5 | **30** | 0 |
| `det_rep1` | metric 6 replicate A, **and** metric 7's baseline | 60 | 0 |
| `det_rep2` | metric 6 replicate B, **and** metric 7's noise floor | 60 | 0 |
| `late_jitter` | metric 7 treatment | 60 | **1.0** |

Metrics 2 and 3 pool across every scenario; metric 1 uses the K sweep only.

## Headline results (2026-09-06, seed 42, `--run-id 15c2a`)

The warm-path precondition held in all seven pivot-family sessions —
nothing was excluded from any curve.

- **Reactivity (1)**: adaptation is immediate. Every K crosses the
  pre-registered Jaccard < 0.3 threshold at its *first* post-pivot refresh;
  `events_to_adaptation` is 4/5/7/10 for K=3/5/8/12, rising with K only
  because more pre-pivot refreshes precede the crossing.
- **Latency (2)**: H1 ingest POST p50 7.2 ms, H2 profile compute lag p50
  2.12 s, H3 refresh compute p50 24.4 ms. H2 dominates by two orders of
  magnitude — the only hop waiting on a windowed job.
- **Cold → warm (3)**: a race, not a threshold. Every session reached the
  warm path after one cold-start refresh, 5.3–15.0 s in.
- **Coherence (4)**: recency decay confirmed. Mean `cos(profile, last 5 min
  of session)` **0.8707** vs mean `cos(profile, first 5 min)` **0.6186**,
  with 35 of 39 samples (90%) closer to the recent window. The first
  evidence for a claim `session_profile.py` has carried in a comment since
  stage 13.
- **Coverage (5)**: **no attractor collapse.** 168 distinct tracks —
  **40.88% of the 411-track catalog** — over 40 refreshes, with 35 of those
  40 contributing a track never recommended before and the last new one
  arriving at refresh 38.
- **Determinism (6)**: **PASS.** Two content-identical runs, 11 refreshes
  each, all 11 `identical`, max score delta exactly 0.0. A FAIL was a
  legitimate possible outcome; the claim stays bounded to two runs at one
  K, one speed, on an idle machine.
- **Late events (7)**: the drop is visible, the headline survives it. 20 of
  32 events were delivered past the 5s watermark bound (up to 648.9 s
  late), and three of four measures cleared the zero noise floor —
  `refresh_count` 11→12, max reactivity Jaccard delta 0.2500, max coherence
  `cos_recent` delta **0.5574**. But **`events_to_adaptation` is 7 in all
  three arms**: losing most of the pre-pivot profile changes *what* the
  recommender turns over to, not *when*.

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
itself. A full run of all eight scenarios takes roughly **18 minutes**
wall-clock; `--skip-extra-scenarios` cuts it to about 6 by running only the
metric 1/2/3 K sweep.

Useful flags:

- `--speed` (default 60): wall-clock compression of the simulated session
  clock. **The debounce constants in `recommendation_refresh.py` are
  wall-clock and are not compressed**, so a higher speed means a debounce
  that is proportionally stricter in session-time terms. See
  `latency.json`'s `speed_caveat`.
- `--k 3 5` : override the K sweep.
- `--skip-extra-scenarios` : run only the K sweep (metrics 1/2/3), skipping
  the long session and the three pivot replicates.
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
- `coherence.json` — metric 4, the two cosine series over session time.
- `coverage.json` — metric 5, coverage fraction and the cumulative-unique curve.
- `results.json` — metrics 3, 6 and 7.
- `raw_scenario_records.json` / `raw_profile_vectors.json` /
  `raw_embeddings.json` — every post, refresh result, snapshot, sampled
  profile vector and the CLAP embeddings metric 4 compares against. The
  measurement of record; together they feed `--from-records`. The vectors and
  embeddings live in side files only to keep the records readable — nothing
  is rounded, because metric 6 compares outputs for exact equality.
- `tables.md`, `figures/*.png`.

`write_outputs()` writes the three raw files **before** computing any metric:
a live run costs ~18 minutes and the raw records *are* the measurement, so a
crash in a metric must not be able to destroy the run that produced it.

## Metrics 6 and 7 share their replicates on purpose

`det_rep1` and `det_rep2` differ in nothing a seed controls. That makes them
metric 6's determinism comparison — and it makes their difference the
**run-to-run noise floor** metric 7's jitter effect has to beat before it
counts as real. A bare jitter-0 vs jitter-1.0 delta would be uninterpretable
here: the refresh debounce is wall-clock while `--speed` compresses only
session time, and the profile the warm path reads is written by a separately
scheduled consumer group, so any two runs differ somewhat. Both rules are
pre-registered in `metrics.py` (`determinism_verdict`, `late_event_verdict`),
committed before any of the three runs — the same methodology `eval/8_1`'s
Stage 15A.2 used against the ANN noise floor.

Metric 7 also measures its own **dose**: `--jitter` is a probability, so
`metrics.late_delivery_count()` counts how many events the draw actually
pushed past the 5s watermark bound. Without that, a null result would be
indistinguishable from "nothing was dropped."

## Metric 4 runs slower than the rest of the pack, by measurement

The Flink job overwrites `session:{id}:profile` on every window fire and keeps
no history, so the profile time series exists only as far as polling can
resolve it — and the writes are not evenly spaced. A `complete` event advances
session time by most of a track duration (median 231s here), advancing the
watermark past seven or eight 30s slides at once; the job fires those windows
milliseconds apart over one key. **The ceiling is one vector per
watermark-advancing event, not one per window fire.**

Measured on a 12-event probe at a 0.5s poll interval: speed 60 captured 5 of 6
distinct writes, speed 30 captured 6 of 6. Hence `LONG_SESSION_SPEED = 30`.
`coherence.json` reports the capture against that ceiling, not against the
analytic window-fire count — which would report complete sampling as ~13%.

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
