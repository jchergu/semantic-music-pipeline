# eval/8_2 Metric Spec — SIGNED OFF

Status: reviewed and signed off by the thesis author (Jacopo) on
2026-09-03. All open items from the draft (pivot genre pairing, K-sweep
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

Concretely, for every scenario run the harness must have running,
simultaneously:

1. `platform/event_ingestion` (HTTP, already exists — start once per
   harness invocation, same as the stage 12/13 manual runbooks).
2. The unmodified Stage 13 Flink job, submitted once per harness
   invocation (same submission sequence as `stage13-flink-session-job.md`
   / the Stage 15B manual verification).
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

## Resolved before handoff

- **Pivot genre pairing** (`chillout`→`rock`): checked against the live
  catalog's genre counts and the roadmap's "chill → high-energy" framing
  — no issue, kept as-is.
- **K-sweep values** `[3, 5, 8, 12]`: kept as-is. Each K independently
  reseeds `sample()`, so pre-pivot track sets aren't nested across K
  values (K=5's tracks aren't K=3's plus two more) — reviewed and judged
  not to matter for what the metric measures (adaptation speed after a
  pivot of length K, each K run standing alone). Still reconsider if the
  warm-path precondition ends up excluding more of these than expected
  once actually run.
- **Metric 2 H3 instrumentation**: the original draft claimed H3 needed
  no edit to `recommendation_refresh.py`. Wrong — see §3's H3 bullet,
  now corrected to specify the `refresh_compute_seconds` additive edit.
