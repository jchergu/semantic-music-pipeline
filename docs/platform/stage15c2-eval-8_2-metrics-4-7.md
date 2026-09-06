# Stage 15C.2 — `eval/8_2` Metrics 4, 5, 6 and 7

Stage 15C built the active harness and landed metrics 1/2/3 off one pivot
scenario. This stage adds the four `METRICS.md` metrics that were deferred
with explicit `pending` markers: **4 (semantic coherence over session time),
5 (catalog coverage and attractor collapse), 6 (determinism) and 7
(late-event handling)**. No new orchestration — the same five processes,
the same anchor schedule, the same threads.

Three scenarios are added to the pack, bringing it to eight:

| Scenario | Role | Feeds | Speed | Jitter |
|---|---|---|---|---|
| `long_session` | `long_session` | metrics 4, 5 | **30** | 0 |
| `det_rep1` | `determinism` | metric 6 replicate A, **and** metric 7's baseline | 60 | 0 |
| `det_rep2` | `determinism` | metric 6 replicate B, **and** metric 7's noise floor | 60 | 0 |
| `late_jitter` | `late_event` | metric 7 treatment | 60 | **1.0** |

## The design decision that shapes metrics 6 and 7: two baselines, not one

`METRICS.md` §8 asks metric 7 to report the delta between a jitter-0 and a
jitter-1.0 run. A bare delta cannot answer the question, because this
pipeline is not run-to-run identical to begin with — the refresh debounce is
wall-clock while `--speed` compresses only session time, and the profile the
warm path reads is written by a separately scheduled consumer group. Any two
runs differ somewhat; without knowing by how much, a jitter delta is
uninterpretable.

So metric 6's two content-identical replicates do double duty. Their
difference **is** the run-to-run noise floor, and metric 7's rule
(pre-registered in `metrics.late_event_verdict()` before any of the three
runs) is:

> An effect counts as real only where the jitter-0 vs jitter-1.0 difference
> **strictly exceeds** the difference between the two jitter-0 replicates,
> on the same measure.

This is the same methodology `eval/8_1`'s Stage 15A.2 used to validate its
reconstruction path against the ANN noise floor, reused rather than
reinvented — and it costs one extra scenario, not a separate experiment.

Metric 7 also measures its own **dose**. `--jitter` is a probability over a
bounded positional reorder, so how many events it actually pushes past the
5s watermark bound is a draw, not a setting.
`metrics.late_delivery_count()` counts, from the delivery order actually
posted, every event whose `event_time` fell more than 5s below the running
high-water mark — exactly the condition under which Stage 15B proved the
event is silently and permanently dropped. Without this number, a null
result would be indistinguishable from "the jitter draw happened to drop
nothing."

## Metric 4's `--speed` was chosen by measurement, as the spec required

`METRICS.md` §5 explicitly refuses to let the build session default to the
repo's usual fast speed: it asks for a value picked "by checking how often
the profile key actually changes at a couple of candidate speeds."

Doing that check first turned up the constraint that actually governs the
sampling, which the spec did not anticipate. The profile writes are **not
evenly spaced**. A track's `complete` event advances session time by the
better part of a track duration (median 231s in these scripts), which
advances the watermark past seven or eight 30s slides at once — so the job
fires those windows milliseconds apart, each overwriting the same Redis key.
**The resolvable ceiling is one vector per watermark-advancing event, not
one per window fire**, and no poll rate can raise it.

Measured on a 12-event `rock` probe (6 watermark-advancing events; 45 window
fires analytically), at a 0.5s poll interval:

| Speed | Min inter-burst gap (wall) | Distinct writes captured |
|---|---|---|
| 60 | 3.30 s | 5 of 6 |
| **30** | **6.60 s** | **6 of 6** |

Speed 30: full capture, 13× the poll interval of headroom on the tightest
gap, for 323s of wall-clock instead of 162s. `coherence.json` carries this
rationale and reports `distinct_profile_writes_observed` against
`watermark_advancing_events` — the real ceiling — rather than against the
analytic window-fire count, which would understate the sampling as ~13%
when it is in fact complete.

## Harness changes (all additive)

- The profile poller now samples `session:{id}:profile` (the vector) as well
  as `session:{id}:profile_meta`, **in one Redis pipeline** so the pair comes
  from a single consistent view. The job writes the vector first and the
  metadata second, so a sampled pair is never a new `computed_at` against a
  stale vector.
- Posts record `event_time` alongside `wall_clock`. Metric 4's two windows
  are defined in *session* time, which `--speed` decouples from wall-clock;
  a wall-clock window would put every event in "the last 5 minutes" and the
  metric would be flat by construction.
- Snapshots keep the full ranked rows, not just track ids — metric 6
  compares scores too. Ten identical tracks with different scores is not a
  reproduction, and an id-only snapshot could not tell.
- `ScenarioSpec` gains `role` (which metric family a scenario feeds) and
  `profile_poll_interval_seconds` (per scenario, since the resolvable rate
  depends on that scenario's own speed).

### One real bug this stage had to fix: the pivot index under jitter

`apply_jitter()` permutes **delivery** order and deliberately leaves each
event's `event_time` untouched. The harness located the pivot as an index
into the event-time-ordered plan and then applied that index to the
post-jitter list — correct for every scenario built so far, because none of
them was jittered. On `late_jitter` it would have labelled the wrong
refreshes post-pivot, and metric 7 would have compared a mislabelled
reactivity curve against a correct one, with no error anywhere.

Fixed by locating the pivot event by **identity** before jitter and finding
its new position afterwards (`apply_jitter` is a permutation of the same
objects, not a rebuild). Both positions are recorded —
`pivot_event_time_index`, `pivot_event_index`, and the
`pivot_delivery_shift` between them — so the displacement is visible in the
data rather than only implied.

## Raw artifacts are split, and written before any metric is computed

The sampled 512-dim profile vectors would dominate
`raw_scenario_records.json`, so they move to `raw_profile_vectors.json`,
keyed by `repr(computed_at)` (which round-trips a Python float exactly), and
the CLAP embeddings metric 4 compares against move to
`raw_embeddings.json`. Nothing is rounded: metric 6 compares outputs for
exact equality, and a "just for the file size" rounding is exactly the kind
of quiet transform that would make such a comparison meaningless.
`--from-records` reads all three, so every metric here can be recomputed
with no live stores at all.

`write_outputs()` now writes those three files **first**, before computing a
single metric. A live run costs ~18 minutes and the raw records *are* the
measurement; a crash in a metric must not be able to destroy the run that
produced it.

## Results

Run 2026-09-06, `--run-id 15c2a`, seed 42, speed 60 (long session 30),
eight scenarios, ~25 minutes wall-clock. **The warm-path precondition was
satisfied in all seven pivot-family sessions — nothing was excluded.**
Metrics 1/2/3 reproduced Stage 15C's published numbers exactly
(`events_to_adaptation` 4/5/7/10 for K=3/5/8/12).

### Metric 4 — recency decay is real, and now has evidence

`session_profile.py` has claimed a recency-decayed centroid since stage 13,
in a comment. This is the first measurement of it.

39 of 41 resolvable profile writes captured (95%) at speed 30. Across those
39 samples the profile stayed close to what was playing **now**
(`cos_recent` mean **0.8707**, ending 0.9076) while drifting away from how
the session opened (`cos_early` mean **0.6186**, falling from 0.9436 to
0.5934). **35 of 39 samples (90%) sat closer to the recent window than to
the opening one.**

The two series necessarily start on top of each other — before five minutes
of session time have elapsed, "the last five minutes" and "the first five
minutes" are the same events — and separate durably from roughly the
40-minute mark. That shape is the finding: the profile is not accumulating
the whole session, it is tracking a window and letting the opening decay
out. See `figures/coherence_long_session.png`.

### Metric 5 — no attractor collapse

Over a 40-track, four-genre session the recommender produced **168 distinct
tracks, 40.88% of the 411-track catalog**, across 40 refreshes. The
anticipated failure did not occur: **35 of the 40 refreshes contributed at
least one track never recommended before**, and the last new track arrived
at refresh 38 of 40 — a flat tail of 2 refreshes (5%). The curve in
`figures/coverage_curve.png` rises steadily rather than saturating early.

The tail that does exist is honest to state: the final two refreshes added
nothing new, which is what exhaustion would look like if the session
continued. On a 411-track catalog with played tracks excluded from every
candidate set, that is expected, not alarming.

### Metric 6 — PASS, and more strongly than expected

Two content-identical runs produced **11 refreshes each, all 11
classifying as `identical`, with a maximum score delta of exactly 0.0**.
Byte-for-byte reproduction, against a rule pre-registered before either
run.

This was not the expected outcome. The refresh debounce is wall-clock while
`--speed` compresses only session time, and the warm path reads a profile
written by a separately scheduled consumer group — nothing in the design
*guarantees* reproduction, and a FAIL was a legitimate possible result. It
reproduced anyway, which says the wall-clock debounce lands on the same
events run to run at this speed, and that the anchor alignment
(`metrics.anchor_schedule`) really does put window boundaries in the same
place relative to each run's own events.

The claim is bounded: this is **two runs, at one K, at one speed, on an
otherwise idle machine**, and it is repeatability, not a proof that the
schedule cannot diverge. `events_behind_each_refresh` (below) shows how
little headroom there is.

### Metric 7 — the silent drop is visible downstream, but not in the headline

The dose was heavy: **20 of 32 events were delivered past the 5s watermark
bound**, up to **648.9s late** — so the jittered run's profile is missing a
great deal of real information that the baselines had.

Because metric 6 came back at exactly 0.0 on every measure, the noise floor
is zero and any difference at all is above it:

| Measure | noise (baseline↔baseline) | effect (baseline↔jittered) | above noise |
|---|---|---|---|
| `events_to_adaptation` | 0 | **0** | **no** |
| `refresh_count` | 0 | 1 (11 → 12) | yes |
| max reactivity Jaccard delta | 0.0 | 0.2500 | yes |
| max coherence `cos_recent` delta | 0.0 | **0.5574** | yes |

The interesting result is the row that did **not** move. The session
profile is measurably corrupted — a cosine gap of 0.56 against the same
reference window is large — and the recommendation sets genuinely differ,
yet **`events_to_adaptation` is 7 in all three arms**. The pivot is a hard
enough semantic signal that losing most of the pre-pivot profile does not
change *when* the recommender turns over, only *what* it turns over to.

That is exactly the nuance `METRICS.md` §8 asked to be reported honestly
either way: the drop is real and visible, and the headline reactivity claim
survives it.

### Two by-products worth recording

**The per-speed split of the debounce statistics earned its keep.** Pooled,
`events_behind_each_refresh` has p50 = 3. Split, speed 60 sits at p50 = 3
(the debounce fires on its *count* branch) while speed 30 sits at p50 = 2
(the 5s *interval* branch fires first, because events arrive further apart
in wall-clock). Pooling across the pack's two speeds would have averaged
away precisely the effect `latency.json`'s `speed_caveat` describes.

**Metric 3's handoff Jaccard can hide a rank change.** The long session
reports a handoff Jaccard of 1.000 — the first warm refresh returned the
same ten tracks as the cold-start refresh. It did not return them in the
same order: the cold start's top track dropped to position six.
`METRICS.md` §4 defines the handoff over *sets* and that definition stands,
but a bare 1.000 reads as "the handoff changed nothing", so
`cold_warm_transition()` now reports `handoff_rank_identical` alongside it.
It is `false` for **every** session in the pack, including the four at
Jaccard 0.818 — the handoff always reorders, even when it barely
re-selects.

## Verification

- **61 tests in `eval/8_2/tests/`** (up from 29), all pure — no live
  services. 26 new in `test_82_metrics_15c2.py` covering every metric 4-7
  definition, and 6 more in `test_82_run_smoke.py` covering the new
  scenario specs, the `eval/8_1` import isolation, and the raw-artifact
  round trip. **180 tests pass repo-wide.**
- **The live run**: `python -m eval.8_2.run --run-id 15c2a`, eight
  scenarios against the running stack, no errors recorded in any scenario
  record.
- **`--from-records` reproduces every metric** from the saved raw
  artifacts with no live stores, and was used to regenerate the published
  outputs after three metric definitions were refined post-run
  (`watermark_advancing_events`, the per-speed latency split,
  `handoff_rank_identical`) — which is exactly what that flag exists for.
- **The metric 4 speed probe** was a separate live run against the real
  stack, not a calculation: two scenarios at speeds 60 and 30, distinct
  profile writes counted from what the poller actually observed.

## Known gaps, deliberately left open

- **Still no persistent refresh daemon.** The refresh driver loop remains
  harness-owned, test-shaped code, exactly as stage 15C left it. Stage 16
  builds `platform/streaming/refresh_daemon.py`; this stage does not
  anticipate it.
- **The Flink job is still unmodified with respect to lateness.** No
  `allowedLateness`, no side output. Stage 15B left that as a decision for
  15C/15D and metric 7 measures the consequence rather than removing it —
  changing the job here would have destroyed the very effect the metric
  exists to quantify.
- **Metric 4 compares against an unweighted reference mean.** The profile
  Flink writes is weighted and recency-decayed; weighting the reference the
  same way would compare the decay against itself and could only ever agree.
