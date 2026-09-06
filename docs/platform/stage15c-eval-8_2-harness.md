# Stage 15C — `eval/8_2` Harness Core and Metrics 1/2/3

8.2's machinery has worked since stage 14 (session centroid in Flink, debounced
recommendation refresh) but had never been measured. `eval/8_2/METRICS.md` was
signed off 2026-09-03 with seven metrics; this stage builds the harness and
lands the first three. Metrics 4/5/6/7 are stage 15C.2, scoped that way
deliberately so the orchestration was proven before committing to the long
slow-speed runs.

## Why this pack is shaped nothing like `eval/8_1`

`eval/8_1` reads a frozen table. 8.2 has no frozen table — its output is a
Redis key rewritten live by two independently-scheduled consumers racing on
one Kafka topic. So `eval/8_2` drives the system and measures it in flight:
per scenario it runs a poster (main thread), the real stages 9-10 raw-state
consumer, a refresh driver loop, and a profile-meta poller, all at once.

## Corrections to METRICS.md's section 0, found by reading the code

The signed-off spec lists three processes that must be running. It needs five:

- **The Semantic API was missing.** Both branches of
  `refresh_recommendations()` call `context_builder.build_context(http_client,
  …)` — pure HTTP against the stage 4 API.
- **The stages 9-10 raw-state consumer was missing.**
  `refresh_recommendations()` reads `get_session_events()` and returns
  `skip_insufficient_data` below two events, so with nothing populating
  `session:{id}:events` **no refresh ever fires**. Stage 14's live test papers
  over this by calling `record_event()` directly; the harness runs the real
  `session_consumer.consume_and_cache_many()` in a thread instead, which is
  also what a real deployment would do.

Both are fixed in `METRICS.md` itself, marked as found during the build.

## Flink watermarks are global, so event-time anchors are scheduled

`env.set_parallelism(1)` plus `for_bounded_out_of_orderness(5s)` means the
watermark is stream-wide, not per session key. Scenarios run sequentially
against one job, so `metrics.anchor_schedule()` lays out each scenario's
event-time anchor with two properties:

- **strictly increasing**, at least 5 minutes past the previous scenario's
  last event — otherwise a scenario's events arrive behind a watermark its
  predecessor already advanced and Flink drops them silently, which is Stage
  15B's finding, self-inflicted;
- **a whole multiple of 300s from `EVAL_EPOCH` (2030-01-01T00:00:00Z)** — 300s
  is the job's window size and a multiple of its 30s slide, so shifting a
  scenario by a multiple of it moves every window boundary identically and
  leaves window/event alignment unchanged across runs. Anything else would
  change which events land in which window for reasons unrelated to the code,
  and would break 15C.2's determinism metric in a way that looks like a bug.

The harness also cancels any pre-existing Flink job and submits a fresh one
per invocation, so each run starts from a clean watermark.

## The two additive platform edits (both sanctioned by METRICS.md)

- `flink_session_profile_job.py`: one `hset` beside the existing `set`,
  writing `session:{id}:profile_meta` (`computed_at`, `n_events`). A *sibling*
  key, never fields inside `session:{id}:profile` — `recommendation_refresh.py`
  `json.loads`es that value as a flat vector and would break.
  `session_state.py::profile_meta_key()` is the canonical definition, held to
  the same cross-check test as `profile_key()`.
- `recommendation_refresh.py`: `process_one_event()` returns
  `refresh_compute_seconds`, timed *inside* the function around the
  `should_refresh()` → `refresh_recommendations()` block. Timing it from
  outside would fold in the up-to-10s `consumer.poll(0.5)` wait — that is H3's
  whole reason for existing as a spec'd edit.

The debounced no-op branch is untouched and carries no such field, which a
test asserts.

## The bug this stage found: session ids must be scoped per invocation

The first full sweep reported K=3 as `0 post-pivot refreshes` while K=5/8/12
looked normal. Cause: the pre-flight run had already used the session id
`eval82-pivot-K3`. `behavioral-events` is append-only and never purged, and
both consumer groups start from `earliest`, so the sweep's K=3 run replayed
the pre-flight's 22 stale events, hit its event budget, and never reached its
own. Fixed by scoping session ids with a `--run-id` (UTC timestamp by
default); scenario *content* stays fully seed-determined, only the Redis and
Kafka keying moves. A count guard now also records an explicit
`event/result count mismatch` error rather than silently mis-splitting
pre/post-pivot.

## A second bug, in the test suite itself

Reconciling the repo-wide test count (expected 148, measured 146) turned up a
silent collection collision: `eval/8_2/tests/test_metrics.py` and
`test_run_smoke.py` shared basenames with `eval/8_1/tests/`'s files, and
because the package directories (`8_1`, `8_2`) aren't valid Python
identifiers, pytest resolved both paths to the same module. A full-suite run
was collecting **eval/8_1's tests twice** — once under each path — and running
none of eval/8_2's metric or smoke tests at all. They passed when the
directory was targeted directly, which is why the pre-flight looked fine.

Fixed by renaming this pack's test files with an `82_` prefix
(`test_82_metrics.py`, `test_82_run_smoke.py`, `test_82_scenario_gen.py`).
Any future `eval/8_3/tests/` needs uniquely-named files for the same reason;
the note is in `test_82_metrics.py`'s module docstring.

## Results (2026-09-06, speed 60, seed 42)

Warm-path precondition satisfied at every K — no run excluded.

| K | pre-pivot refreshes | post-pivot refreshes | first post-pivot Jaccard | events to adaptation |
|---|---|---|---|---|
| 3 | 3 | 5 | 0.176 | 4 |
| 5 | 4 | 5 | 0.176 | 5 |
| 8 | 6 | 5 | 0.053 | 7 |
| 12 | 9 | 5 | 0.053 | 10 |

**The headline result is that adaptation is immediate.** Every K crosses the
pre-registered 0.3 threshold at its *first* post-pivot refresh — 1 to 2 events
after the pivot — and stays at or near 0 afterward. `events_to_adaptation`
rises with K only because more pre-pivot refreshes precede the crossing; the
plotted curves (`figures/reactivity_curves.png`, x-axis re-based to refreshes
*since* the pivot) show the four runs behaving identically.

Latency, pooled across all four runs:

| Hop | n | p50 | p95 |
|---|---|---|---|
| H1 ingest POST | 120 | 8.4 ms | 10.3 ms |
| H2 profile compute lag | 58 | 2.56 s | 3.09 s |
| H3 refresh compute | 42 | 38.9 ms | 59.0 ms |
| Interval between refreshes | 38 | 4.61 s | 10.86 s |

H2 dominates by two orders of magnitude, which is the expected shape: it is
the only hop that waits on a windowed streaming job rather than doing a single
operation. `events_behind_each_refresh` came back p50 = p95 = max = **3**,
meaning the debounce fired on its *count* branch (`DEBOUNCE_MIN_EVENTS`)
essentially every time at this speed, not its 5s interval branch — the design
floor is visible in the data rather than only asserted.

Cold → warm: all four sessions reached the warm path after exactly one
cold-start refresh, ~5.3-5.5s in, with a handoff Jaccard of 0.818.

## Two caveats the outputs encode, not just the prose

1. **The K sweep is nested.** `random.Random(seed).sample(pool, k)` draws
   sequentially from one seeded stream, so K=3 ⊂ K=5 ⊂ K=8 ⊂ K=12.
   `METRICS.md`'s "Resolved before handoff" section asserted the opposite as
   its reason for keeping the sweep values; that assertion was wrong and is
   corrected there. The nesting is arguably better for metric 1 (only
   pre-pivot length varies, the opening sequence is held fixed), but the four
   K runs are not independent samples. `reactivity.json` reports
   `pre_pivot_sets_nested` and the per-K track lists so a reader can check.
2. **Metric 3's four numbers are one observation.** Following from (1), all
   four sessions share a cold-start seed track (id 420) and therefore report
   an identical handoff Jaccard. `results.json` reports
   `cold_warm_transition.independent_observations: 1` with an explicit caveat
   string; the mean must not be read as an average over independent runs.

A third, milder caveat is recorded in `latency.json` as `speed_caveat`: the
debounce constants are wall-clock while `--speed` compresses only the
simulated session clock, so at speed 60 the debounce is 60× stricter in
session-time terms than production. It does not affect H1/H2/H3 (each a single
operation's own cost) but it does set the refresh cadence metric 1's curve is
indexed on, which is why every curve point also carries its `event_index`.

## Verification

- 29 new pure tests (`eval/8_2/tests/`) plus a new `profile_meta_key`
  cross-check in `tests/test_stage13_session_profile.py`, and new
  `refresh_compute_seconds` assertions inside stage 14's existing live test.
  **148/148 pass repo-wide**, up from 118 — and that arithmetic is what
  surfaced the collection collision above.
- Three live runs: a K=3 pre-flight, a first full sweep (which surfaced the
  stale-session-id bug), and the reported sweep after the fix. No errors
  recorded in `raw_scenario_records.json`.
- Re-invocability confirmed: the pack was regenerated repeatedly from saved
  records via `--from-records`, and the live sweep ran twice from a dirty
  Redis without contamination.

## Known gaps, deliberately left open

- No automated test drives the harness end to end — it starts services,
  submits a Flink job and produces real Kafka traffic. Same judgment stages 13
  and 15B made about the job's own lifecycle; the live runbook above is the
  verification.
- Still no persistent refresh daemon in `platform/streaming/`. The driver loop
  here is harness-owned and must not be mistaken for one.
- `allowedLateness`/side-output handling on the Flink job remains unadded
  (Stage 15B's finding). Metric 7 in 15C.2 is where that decision gets made.
