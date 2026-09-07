# eval/8_2 Metric Spec — SIGNED OFF

Status: reviewed and signed off by the thesis author (Jacopo) on
2026-09-03. **Extended 2026-09-07** for Stage 15D with section 9 (metric 8,
failure injection) — an addition beyond the signed-off seven, not an
amendment to any of them, pre-registered under the same discipline and
scoped by explicit decision that day. **Amended 2026-09-06** during Stage
15C.2's build, in three further places, each marked inline: section 5
records the `--speed` the spec told the build session to pick by
measurement, and the sampling
ceiling that measurement exposed; section 7 records that metric 6 is run
as two replicates whose difference is reused as a noise floor; section 8
records that metric 7 is judged against that noise floor rather than as a
bare delta, and measures its own dose. **Amended 2026-09-06** during
Stage 15C's build, in three places, each marked inline: section 0's process list was missing two hard
blockers (the Semantic API and the raw-state consumer); the "Resolved
before handoff" claim that the K sweep isn't nested was wrong; and metrics
1/2/3 are now built, with 4/5/6/7 deferred to Stage 15C.2 (**built
2026-09-06**). Amendments record what was found, they do not relax any
definition. All open items from the draft (pivot genre pairing, K-sweep
values, Metric 2's H3 latency instrumentation) are resolved — see
"Resolved before handoff" below. Ready for Stage 15C's build session.
Every definition here is something the author defends in the viva — Code
implements exactly what's written, it doesn't get to fill in gaps.

**Hard framing constraint, unchanged from the roadmap**: no precision@k,
recall@k, or NDCG anywhere in this pack. There is no ground truth for this
dataset. Every metric below is latency, coverage, reactivity, or semantic
coherence — a systems/behavior claim, not an accuracy claim.

## 0. Harness architecture (read before any metric below)

`eval/8_1` is passive: it reads a frozen, already-generated Postgres table
and never re-runs `recommend.py`. `eval/8_2` cannot work that way — there
is no frozen table, only two independently-scheduled live consumers
(Flink's sliding-window job, and the stage 14 refresh loop) racing on the
same Kafka topic. `eval/8_2` is therefore an **active harness**: for every
scenario, it drives the seeded simulator, keeps its own consumer loops
running for the duration, and polls Redis/Postgres while the scenario
plays out. This is a different kind of code from `eval/8_1` and the
README should say so up front, so nobody opens this package expecting
"just queries Postgres."

**Correction, found while building Stage 15C (2026-09-06)**: the list below
had three entries and needs five. Two processes were missing, both hard
blockers rather than omissions of detail — the spec as signed off would not
have produced a single refresh. They are numbered 1a and 3a below, marked so
the diff against the signed-off version stays visible.

Concretely, for every scenario run the harness must have running,
simultaneously:

1. `platform/event_ingestion` (HTTP, already exists — start once per
   harness invocation, same as the stage 12/13 manual runbooks).

1a. **The Semantic API** (`platform/semantic_api`). Missing from the
   signed-off list. Both branches of `refresh_recommendations()` call
   `context_builder.build_context(http_client, ...)`, which is pure HTTP
   against the stage 4 API — the cold-start path and the warm path's
   genre-sibling/same-artist re-rank both go through it.
2. The unmodified Stage 13 Flink job, submitted once per harness
   invocation (same submission sequence as `stage13-flink-session-job.md`
   / the Stage 15B manual verification).
3a. **The stages 9-10 raw-state consumer**
   (`session_consumer.consume_and_cache_many()`). Missing from the
   signed-off list, and the more serious of the two omissions:
   `refresh_recommendations()` reads `get_session_events()` and returns
   `skip_insufficient_data` below `MIN_RAW_EVENTS_FOR_WARM_PATH` (2), so
   with nothing populating `session:{id}:events` **no refresh ever fires at
   all**. Stage 14's own live test papers over this by calling
   `record_event()` directly rather than running the consumer; the harness
   runs the real consumer in a thread, which is also what a real deployment
   would do.

3. **A local driver loop over `recommendation_refresh.process_one_event()`**
   — this does not run as a persistent daemon anywhere in the platform
   today (stage 14's own tests drive it the same way: create one
   `Consumer` with `new_consumer()`, pass it back into repeated
   `process_one_event(consumer=...)` calls in a loop). `eval/8_2`'s
   harness owns this loop for the duration of each scenario; it is new
   code but it is *test-shaped* code, not a new platform capability —
   worth being explicit that this loop is harness-owned, not something
   Stage 15C should try to promote into `platform/streaming/` as a
   "real" daemon (that's out of scope; flag it as a known gap the same
   way stage 12 flagged the raw-state consumer's own lack of a daemon).

## 1. Scenario generation (shared infra for metrics 1, 5, 7)

New module, `eval/8_2/scenario_gen.py` — not part of `platform/simulator/`,
since it needs to query `tracks.genre_tags` (Postgres) to pick tracks,
which the simulator itself has never done (it only fetches durations).
Keeping DB-querying scenario construction out of `platform/simulator/`
keeps that module's existing contract (durations only) unchanged.

```python
def build_genre_session_script(
    genre_track_counts: list[tuple[str, int]],  # [(genre, n_tracks), ...] in play order
    seed: int,
    completion_pct: float = 0.97,
) -> dict:
```

Deterministic given `seed`: for each `(genre, n)` pair, seeded-shuffles
the catalog's tracks tagged with that genre (`SELECT id FROM tracks WHERE
%s = ANY(genre_tags) ORDER BY id`, then `random.Random(seed).sample(...)`
— `ORDER BY id` first so the pre-shuffle order itself is deterministic,
not Postgres's arbitrary row order) and takes the first `n` — raises if
`n` exceeds that genre's catalog size, loud, not padded. Builds one
`{type: play, position_ms: 0}` + `{type: complete, position_pct:
completion_pct}` pair per track, in the same script-dict shape
`scripts_io.load_script()` returns, so it feeds directly into
`events.build_session_events()` — no new consumer of `build_session_events`
needed.

Two default genre pairs (both real, both checked against the live
catalog's actual genre distribution — `rock`=102 tracks, `electronic`=73,
`chillout`=15, `dance`=21 tracks currently tagged):

- **Pivot** (metrics 1, 7): `chillout` → `rock` — "chill to high-energy,"
  matching the roadmap's own framing. `chillout` has only 15 tagged
  tracks, so K sweeps must stay within that (see metric 1's K values
  below) — flagging this now so nobody sweeps K=20 and hits the loud
  catalog-exhaustion error above.
- **Long session** (metric 5): a 4-genre concatenation —
  `rock`(10) → `electronic`(10) → `chillout`(10) → `dance`(10) — 40
  tracks, comfortably within each genre's catalog size.

## 2. Metric 1 — Reactivity (headline metric)

**Objective**: how many refresh events after a hard genre pivot until the
recommendation set turns over.

**Scenario**: `build_genre_session_script([("chillout", K), ("rock", 8)],
seed)` — K pre-pivot tracks, a fixed 8 post-pivot (enough to observe
several post-pivot refreshes without also sweeping the post-pivot length).

**K values to sweep**: `[3, 5, 8, 12]`. **Precondition, checked and
enforced by the harness, not assumed**: before the pivot's first event is
sent, assert `session:{id}:profile` already exists in Redis (i.e. Flink
has already fired at least one window for this session). If it doesn't
for a given K, that K's run is **excluded from the reported curve, with
the exclusion logged explicitly** — a K measured while still on the
cold-start fallback path is measuring "did the single-seed-track fallback
change," not "did the centroid adapt," and reporting it uncorrected would
misrepresent what the metric claims.

**Procedure**: harness's driver loop processes every event in order.
After each processed event where `refreshed: True`, snapshot the current
`session:{id}:recs` (top-10 `track_id`s, as a set) tagged with its
refresh index (1st refresh, 2nd refresh, ...) and whether it occurred
before or after the pivot's first event.

**Computation**: `pre_pivot_set` = the last snapshot before the pivot.
For each post-pivot snapshot *i*: `jaccard_i = |pre_pivot_set ∩
snapshot_i| / |pre_pivot_set ∪ snapshot_i|`. `events_to_adaptation` = the
refresh index of the first post-pivot snapshot with `jaccard_i < 0.3`, or
`null` if the scenario ends without crossing.

**Output**: `eval/8_2/reactivity.json` — per K: the full `(refresh_index,
jaccard)` curve (report the curve, not just the crossing point, per the
roadmap) plus `events_to_adaptation`; a top-level note on any K excluded
by the warm-path precondition.

## 3. Metric 2 — Latency, decomposed per hop

**Revision from the roadmap's 6-hop wishlist**: three of those six hops
(Kafka→Flink-computed, centroid→Redis-write, Redis→debounce-release)
aren't independently instrumentable without either editing Flink's
internals in ways that go beyond a timestamp, or measuring wall-clock
against `event_time`, which is meaningless once `--speed` decouples the
two (per Stage 12's own design). Collapsing to what's genuinely
measurable, with **wall-clock timestamps only, never `event_time`**:

- **H1 — ingest**: simulator's own `httpx` POST round-trip to
  `/events` (client-side timing the harness already has for free —
  `run_session()` already calls `client.post(...)`; the eval harness
  times around its own equivalent call, doesn't touch `cli.py`).
- **H2 — profile compute lag**: `(Flink's computed_at) − (wall-clock time
  the harness posted that event)`. Requires Flink to write a wall-clock
  timestamp. **Confirmed design**: a *sibling* Redis key,
  `session:{id}:profile_meta` (hash: `computed_at`, `n_events`) —
  **not** a field inside `session:{id}:profile` itself, since
  `recommendation_refresh.py:176` does `json.loads(profile_raw)` and
  assumes a flat vector list; embedding extra fields there would break
  that parse. This is the one line changed in
  `flink_session_profile_job.py` this stage — an additive `redis.hset`
  call alongside the existing `redis.set`, nothing else in the job
  touched.
- **H3 — refresh compute**: `(time.time() after refresh_recommendations()
  returns) − (time.time() when should_refresh() was evaluated True)`.
  **Cannot be measured from outside `process_one_event()`**:
  `should_refresh()` is only evaluated after a `consumer.poll(0.5)` loop
  that can block for up to `timeout` (10s default) waiting for a Kafka
  message to arrive (`recommendation_refresh.py:221-287`) — timing the
  external call alone would fold that poll-wait time into H3, measuring
  "wait for Kafka plus refresh compute," not refresh compute. **Second
  additive edit this stage**, same shape as H2's `profile_meta` key:
  `process_one_event()` gains a `refresh_compute_seconds` field in its
  returned dict, timed internally around the existing `should_refresh()`
  → `refresh_recommendations()` block (lines 270-282) — no other
  behavior of the function changes.
- **Debounce floor**: not a measured latency — report the two constants
  (`DEBOUNCE_MIN_INTERVAL_SECONDS=5.0`, `DEBOUNCE_MIN_EVENTS=3`) as a
  **stated, deliberate design floor**, alongside the observed
  distribution of "wall-clock time between an event's arrival and the
  refresh it eventually triggers" for events that *did* trigger one —
  which will visibly cluster at the two constants above, that clustering
  *is* the evidence the floor is real, not a bug.

**Output**: `eval/8_2/latency.json` — p50/p95/p99 for H1/H2/H3 across
however many scenario runs feed it (reuse the pivot/long-session runs
already happening for metrics 1/5, don't run a dedicated latency-only
scenario). **Not covered by metric 6's determinism claim** — same stance
`eval/8_1/README.md` already takes for its own `latency.json`.

## 4. Metric 3 — Cold-start → warm transition

**Reframing from the roadmap's "event count at handoff"**: the handoff is
not gated by an event count. It's gated by whether Flink's *independent*
consumer group has already written `session:{id}:profile` by the time a
given refresh fires (`recommendation_refresh.py:163`,
`profile_raw is None`) — a race between two separately-scheduled consumer
groups on the same topic, not a threshold. Measuring it as a race is a
**stronger, more honest systems finding** than an event-count number
would be, and it's the same precondition metric 1 already has to check —
this metric reports what that precondition observes, across every run in
the pack, not just the pivot scenario.

**Computation**, aggregated across every scenario run in the pack: for
each session, record (a) the wall-clock gap between session start and the
first refresh whose `action == "warm"`, (b) whether the immediately
preceding refresh (if any) had `action == "cold_start"`, and if so (c)
the Jaccard overlap between that last cold-start `recs` set and the first
warm `recs` set — the **visible discontinuity at the handoff**, reported
as a finding either way (a large discontinuity is a legitimate result,
not a bug to hide, per the roadmap).

**Output**: folded into `eval/8_2/results.json` as `cold_warm_transition`
— not its own file, since it's a byproduct measurement over the same runs
metrics 1/5 already produce, not a separate scenario.

## 5. Metric 4 — Semantic coherence over session time

**Objective**: does recency decay behave as designed (currently only a
claim in `session_profile.py`'s comments).

**Procedure**: during the long-session scenario (metric 5's script, reused
rather than a third scenario), the harness polls `session:{id}:profile`
on a fixed wall-clock interval and stores every distinct vector it
observes with its poll wall-clock time (Flink overwrites this key on
every window fire, so this is the only way to reconstruct a time series —
no history is stored by the job itself). **Poll interval must be tuned to
`--speed`, not fixed at a value that assumes real-time pacing** — at
`--speed=1000` (used elsewhere in this repo for fast tests) events arrive
faster than a reasonable poll interval could resolve, undersampling the
profile's actual evolution. **This scenario runs at a slower `--speed`
than the rest of the pack** (a value the build session should pick by
checking how often the profile key actually changes at a couple of
candidate speeds and choosing one where the poll interval — every 1-2s
wall-clock — reliably catches distinct writes; document whatever value
is chosen and why, don't silently default to 1000 like every other
scenario in this repo does).

**Computation**: at each poll, `cosine(profile_vector, mean_embedding(
tracks played in the last 5 minutes of session-time))` vs
`cosine(profile_vector, mean_embedding(tracks played in the session's
first 5 minutes))`. Plot both series against poll time.

**Output**: `eval/8_2/coherence.json` + a figure — the two cosine series
over session time.

**Resolved during Stage 15C.2's build (2026-09-06)**, by doing the check
this section asks for rather than defaulting: **`--speed 30`, poll
interval 0.5s**. The measurement also exposed a constraint this section
did not anticipate, and which changes what "undersampling" even means
here. The profile writes are not evenly spaced. A `complete` event
advances session time by most of a track duration (median 231s in these
scripts), which advances the watermark past seven or eight 30s slides at
once — so the job fires those windows *milliseconds* apart, each
overwriting the same Redis key. **The resolvable ceiling is one vector per
watermark-advancing event, not one per window fire**, and no poll rate can
raise it. Measured on a 12-event `rock` probe (6 watermark-advancing
events; 45 window fires analytically): speed 60 captured 5 of 6 distinct
writes (min inter-burst gap 3.30s wall), speed 30 captured 6 of 6 (6.60s).
`coherence.json` therefore reports `distinct_profile_writes_observed`
against `watermark_advancing_events`, not against the analytic window-fire
count — the latter would report complete sampling as ~13%.

## 6. Metric 5 — Catalog coverage and attractor collapse

**Scenario**: the 40-track, 4-genre long session from §1.

**Procedure**: reuses metric 1's `recs` snapshotting infrastructure
(already polling/recording every refresh's top-10 across the run) —
across the whole session, take the union of every `track_id` that ever
appeared in any snapshot.

**Computation**: `unique_recommended / 411` (catalog coverage fraction);
a cumulative-unique-tracks-vs-refresh-index curve, to show convergence or
collapse visually, not just as a final ratio. A curve that flattens well
before the session ends is the "attractor collapse" finding the roadmap
anticipates — report it plainly if it happens.

**Output**: `eval/8_2/coverage.json` + a figure (cumulative unique tracks
over refresh index).

## 7. Metric 6 — Determinism

Same seed, same `--speed`, `--jitter 0` (default) → identical output.
Run the pivot scenario (§2) twice end-to-end through the full harness;
assert the `reactivity.json` curves and every `recs` snapshot are
byte-identical between the two runs. **`latency.json` is explicitly
exempt from this claim** (wall-clock timings are never expected to
repeat exactly), matching `eval/8_1`'s own precedent of keeping
`latency.json` separate from the deterministic `results.json` for exactly
this reason.

**Output**: folded into `eval/8_2/results.json` as a pass/fail
(`determinism_check`), reusing `eval/8_1/diff_runs.py`'s comparison
methodology rather than writing a second diffing tool.

**Amended during Stage 15C.2's build (2026-09-06)**, adding detail this
section left open rather than changing what it claims:

- The PASS/FAIL rule is **pre-registered** in
  `metrics.determinism_verdict()`, committed before either replicate ran.
  PASS requires all three of: equal refresh counts, every compared refresh
  classifying as `identical`, and a maximum score delta of **exactly
  0.0** — no tolerance, because Stage 15A.2 measured this scoring path's
  repeated-run noise floor at exactly 0.0 across 41,100 matched pairs, so
  any drift here is a new effect that 8.1's evidence does not cover.
- The comparison takes the **union** of refresh indices, not the
  intersection: a refresh only one run produced is a divergence, and
  intersecting would drop exactly the evidence of it.
- The two replicates cannot share a session id (the topic is never purged
  and both groups read from `earliest`) or an event-time anchor (the
  watermark is stream-wide). Anchors are aligned to whole multiples of the
  300s window size, so window boundaries fall identically relative to each
  run's own events. Everything a seed can control is held fixed; what is
  left is exactly what this metric asks about.
- The two replicates are also **reused as metric 7's noise floor** — see
  section 8. That is why they are two dedicated runs rather than a repeat
  of the sweep's K=8.

## 8. Metric 7 — Late-event handling

**Scenario**: the pivot scenario (§2) at a fixed K already confirmed to
satisfy metric 1's warm-path precondition, run twice — once at
`--jitter 0` (baseline) and once at `--jitter 1.0` — via `scenario_gen`'s
script fed through the now-jitter-capable simulator (Stage 15B).

**Computation**: re-run metric 1's reactivity computation and metric 4's
coherence computation for both runs; report the deltas
(`events_to_adaptation` shift, coherence-curve divergence). Given Stage
15B's live-confirmed finding — a jittered event can be **silently and
permanently** dropped from the windowed profile, not just delayed — the
expected result is a measurable difference in the coherence curve (the
profile is missing real information the baseline had) and possibly in
`events_to_adaptation` (a different starting profile going into the
pivot). Report whatever is actually observed, including a null result if
the dropped event turns out not to matter for the specific K chosen —
that would itself be worth stating (a dropped event doesn't automatically
imply a *visible* downstream effect at every window/K combination).

**Output**: folded into `eval/8_2/results.json` as `late_event_effect`,
cross-referencing `eval/8_1`-style — actually cross-referencing Stage
15B's doc for the underlying drop mechanism, not re-deriving it here.

**Amended during Stage 15C.2's build (2026-09-06)**, strengthening this
section in two ways it did not specify:

- **The delta is judged against a noise floor, not reported bare.** This
  section asks for "the deltas" between a jitter-0 and a jitter-1.0 run,
  but the pipeline is not run-to-run identical to begin with (the debounce
  is wall-clock while `--speed` compresses only session time, and the
  profile the warm path reads comes from a separately scheduled consumer
  group), so a bare delta is uninterpretable. Metric 6's two
  content-identical jitter-0 replicates supply the floor: an effect is
  reported as real only where the baseline-vs-jittered difference
  **strictly exceeds** the baseline-vs-baseline difference on the same
  measure. Pre-registered in `metrics.late_event_verdict()` before any of
  the three runs. Same methodology as Stage 15A.2's ANN noise floor.
- **The dose is measured, not assumed from the setting.** `--jitter` is a
  probability over a bounded positional reorder, so how many events it
  actually pushes past the 5s watermark bound is a draw.
  `metrics.late_delivery_count()` counts them from the delivery order
  actually posted. Without it, a null result would be indistinguishable
  from "the draw happened to drop nothing" — which this section's own
  admission of a null result makes it important to separate.
- Curves are aligned across arms on **position within the post-pivot
  refresh sequence** (metric 1) and on **session-elapsed time** (metric 4),
  never on event index or sample index: jitter permutes delivery order, so
  the n-th delivered event is not the same event across arms, and window
  fires are not under the harness's control, so sample counts differ.

## 9. Metric 8 — Failure injection

**Added 2026-09-07 for Stage 15D.** Not part of the 2026-09-03 sign-off:
metrics 1-7 above measure the pipeline when every component is up, and
Stage 15D was sequenced deliberately after Stage 16 so it could ask the
question none of them can — *what does a client see when a store dies
underneath a live session?* This section is pre-registered under the same
discipline as the rest of the file: the arms, the measures and the verdict
rule below were committed before the first arm ran, and
`metrics.failure_injection_verdict()` implements exactly what is written
here.

**Scope, fixed by explicit decision (2026-09-07)**: three injections —
**Redis outage**, **Semantic API outage**, and **raw-state TTL expiry** —
against the **real Stage 16 deployment shape**. Postgres, Milvus, Kafka and
Flink outages are out of scope for this stage; §9.6 records what reading the
code predicts for them, as stated hypotheses that this stage does not test.

The `allowedLateness`/side-output decision that Stage 15B and 15C.2 left to
"15C/15D" is **also out of scope and stays open**. §8's metric 7 exists to
quantify the consequence of the job having no lateness handling; adding it
here would destroy the effect that metric measures. It is future work, and
the thesis should say so.

### 9.1 What this runs against

Unlike metrics 1-7, this metric does **not** use `harness.run_scenario()`.
That harness owns test-shaped threads — its own raw-consumer loop and its own
driver loop over `process_one_event()` — and §0 is explicit that those are
not a deployment. A failure-injection result measured against harness threads
would be a claim about the harness, not about the system, and the client-visible
half would have no client at all.

So this metric drives the real thing, as five processes:

1. `platform/semantic_api` (uvicorn)
2. `platform/event_ingestion` (uvicorn)
3. `platform/session_api` (uvicorn) — **the client's view**, the component
   metrics 1-7 never touch
4. `platform/streaming/session_consumer_daemon.py` (subprocess)
5. `platform/streaming/refresh_daemon.py` (subprocess)

plus the Stage 13 Flink job, submitted the same way `orchestration.py` already
submits it. The daemons run under the **canonical group ids** from
`streaming/config.py`, not throwaway ones: committed offsets and restart
behaviour are part of what is being measured, and a throwaway group id would
replay the never-purged topic from earliest and hide exactly that.

### 9.2 Scenario and injection timing

One fixed scenario across every arm: the §2 pivot scenario at **K=8**, speed
60 — already confirmed by metrics 6 and 7 to satisfy the warm-path
precondition, so an arm that produces no warm refresh is a finding rather
than a mis-chosen scenario.

The injection is scheduled by **event index, not wall-clock**: it is applied
after the *n*-th event has been posted, held for a fixed number of subsequent
posts, then reverted. Both boundaries are recorded in the raw record. Indexing
by event keeps the outage window covering the same events in every arm and
every replicate, which is what makes `events_lost` comparable at all; a
wall-clock window would cover a different number of events each run because
posting is paced by `gap/speed` against a live stack.

The injection window is placed **after the pivot** and after at least one warm
refresh, so each arm has a healthy pre-injection baseline of its own inside
the same run.

### 9.3 Arms

| Arm | Injection | Reverted by |
|---|---|---|
| `control` | none | — |
| `redis_outage` | `docker stop 8-1-redis` | `docker start 8-1-redis` |
| `semantic_api_outage` | terminate the Semantic API's uvicorn process | restart it on the same port |
| `ttl_expiry` | `EXPIRE session:{id}:events 0` | not reverted — expiry is not an outage |

`ttl_expiry` reproduces the Stage 16 deferred finding deterministically rather
than waiting 30 minutes for the real sliding TTL to fire. It is the same idiom
`tests/test_stage16_session_api.py` already uses to construct that state, and
it makes the arm a decision procedure rather than a stopwatch.

**Each arm runs twice.** See §9.5 for why two and not five.

### 9.4 Measures

Four per arm, recorded per replicate.

1. **`events_lost`** — simulator posts accepted by the ingestion service
   (HTTP 2xx, so the event genuinely reached Kafka) that never appear in the
   Postgres `events` log for that session. Postgres is the ground truth here,
   not Redis: it is the system of record, it has no TTL, and the raw-state
   daemon writes both stores on the same poll, so a post that reached Kafka
   and is absent from Postgres was consumed and discarded.
2. **`recovers_without_restart`** — after the injection is reverted, does the
   pipeline produce a further successful refresh for that session with no
   process restarted? Recorded with the wall-clock of the first post-recovery
   refresh, or `false` with the daemon's error count if none arrives.
3. **`client_visible_signal`** — the full `GET /sessions/{id}` body plus the
   HTTP status of `/sessions/{id}/recommendations` and `/sessions/{id}/profile`,
   sampled at four points: before injection, during, immediately after
   reverting, and at end of scenario. This is the half of the metric that only
   exists because Stage 16 shipped first.
4. **`staleness_detectable`** — whether a client could tell, **from those API
   responses alone**, that what it is being served is stale or incomplete.
   Derived from measure 3, with the specific field (or its absence) recorded as
   evidence. A `false` here is the most consequential possible result: it means
   the service serves degraded output indistinguishable from healthy output.

### 9.5 Verdict rule (pre-registered)

Implemented in `metrics.failure_injection_verdict()`, committed before any arm
ran.

- **An effect counts only if it is absent from the control arm.** The control
  runs the identical scenario with no injection; any measure that already
  differs from nominal there is a property of the pipeline, not of the failure.
  This is the same shape as §7/§8's noise-floor rule, reduced to its
  categorical form.
- **An effect counts only if both replicates of an arm agree on it.** Where the
  two replicates disagree on a categorical measure, that measure is reported as
  `unstable` and **no claim is made** about it.
- A `pass` for an arm means: the failure was detected, bounded, and its
  client-visible consequence characterised. It explicitly does **not** mean the
  system behaved well. An arm where `events_lost > 0` and
  `staleness_detectable` is `false` still passes as a measurement while being a
  bad result for the system — and that distinction is the point of separating
  the verdict from the finding.

**Why two replicates and not the n≥5 this project uses elsewhere.** Stage
15A.2's ANN noise floor and §7's determinism replicates pool repeated runs
because they estimate a *ceiling on a continuous quantity* — a max score delta,
a percentile — and a single draw from a distribution is not its ceiling. The
measures here are **categorical and deterministic**: an event is in the
Postgres log or it is not; the daemon resumes or it does not; the API exposes a
staleness field or there is no such field to expose. Repetition here buys
reproducibility, not a percentile, so the second replicate is there to catch a
non-deterministic outcome (which is reported as `unstable` rather than
averaged), and a third would add nothing. `events_lost` is the one count that
could in principle vary; it is bounded by the injection window, which §9.2
fixes by event index precisely so that it cannot drift. This is a deliberate,
stated deviation from the n≥5 rule, not an oversight.

### 9.6 Predicted but untested (stated, not measured)

Reading the daemons' code predicts the following. Stage 15D does **not** test
them, and the thesis must not report them as results:

- Both daemons contain a per-event failure with a blanket `except Exception`
  that counts the error and **commits the offset past the message**. That is the
  correct policy for a poison message; applied to a dependency outage it
  discards every event arriving during the outage, unrecoverably. The
  `redis_outage` and `semantic_api_outage` arms measure this consequence
  directly, so for those two it is tested.
- The daemons open Postgres and Milvus connections once at startup and never
  re-establish them. psycopg2 does not self-heal, so a Postgres outage is
  predicted to break a daemon permanently rather than transiently — the one
  place where "recovers without restart" is predicted to be `false` for reasons
  unrelated to lost events. **Untested here** (Postgres is out of scope).
- A Kafka outage is predicted to be *invisible*: `poll()` returning nothing is
  indistinguishable from idle traffic, so the daemon reports itself healthy
  while consuming nothing. **Untested here.**

### 9.7 Output

`eval/8_2/failure_injection.json`, alongside the metric 1-7 packs and not
folded into `results.json` — this metric is measured against a different
process topology (§9.1) and merging it would blur what `results.json`'s
determinism claim covers. Raw per-arm records are written **before** any
verdict is computed, and a `--from-records` path recomputes the verdicts
without a live run, for the same reason §0 gives.

## Output file summary

- `eval/8_2/results.json` — deterministic per §6: `cold_warm_transition`
  (§4), `determinism_check` (§6 itself), `late_event_effect` (§7).
- `eval/8_2/reactivity.json` — §2, per-K curves.
- `eval/8_2/latency.json` — §3, **not** covered by the determinism claim.
- `eval/8_2/coherence.json` — §4/5.
- `eval/8_2/coverage.json` — §4/6.
- `eval/8_2/figures/*.png` — reactivity curve, coherence series, coverage
  curve, latency breakdown.
- `eval/8_2/tables.md` — thesis-ready tables assembled from the above.
- `eval/8_2/failure_injection.json` — §9, deliberately NOT folded into
  `results.json`: it is measured against a different process topology (the
  real daemons and `session_api`, not the harness's threads), and merging it
  would blur what `results.json`'s determinism claim covers.

## Resolved before handoff

- **Pivot genre pairing** (`chillout`→`rock`): checked against the live
  catalog's genre counts and the roadmap's "chill → high-energy" framing
  — no issue, kept as-is.
- **K-sweep values** `[3, 5, 8, 12]`: kept as-is. ~~Each K independently
  reseeds `sample()`, so pre-pivot track sets aren't nested across K
  values (K=5's tracks aren't K=3's plus two more)~~ — **this was wrong,
  corrected 2026-09-06 during Stage 15C's build.** Measured against the
  live catalog, the sets *are* nested: K=3 ⊂ K=5 ⊂ K=8 ⊂ K=12 (all four
  open on tracks 420, 97, 83), because `random.Random(seed).sample(pool,
  k)` draws sequentially from one seeded stream, so a larger `k` extends
  the smaller `k`'s draw rather than replacing it. The values still stand
  — nesting is arguably the *better* design here, since only pre-pivot
  length varies while the opening sequence is held fixed — but the stated
  reason for accepting them was factually wrong, and the consequence is
  real: the K runs are **not independent samples**, and metric 3's
  per-session handoff numbers (which all share one cold-start seed track)
  are one observation repeated, not four. `reactivity.json` reports
  `pre_pivot_sets_nested` and `results.json` reports
  `cold_warm_transition.independent_observations` so this is visible in the
  data, not only here.

  The warm-path precondition, the other thing this entry said to
  reconsider after a real run, excluded nothing: it was satisfied at every
  K.
- **Metric 2 H3 instrumentation**: the original draft claimed H3 needed
  no edit to `recommendation_refresh.py`. Wrong — see §3's H3 bullet,
  now corrected to specify the `refresh_compute_seconds` additive edit.
