# Stage 15B — Simulator Capability Audit and Late-Event Support

Prerequisite for `eval/8_2/` (stage 15C): the headline justification for using
Flink instead of a scheduled batch job is event-time semantics with
watermarks. Before this stage, the simulator only ever emitted events in
strict `event_time` order, so that machinery had never actually been
exercised — "why Flink, not cron" had no evidence behind it. This stage
audits what the Stage 13 job actually does with a late-arriving event, and
gives the simulator a way to produce one, deterministically, under the
existing `--seed`.

## What this stage adds

**Audit findings** (read-only, `platform/streaming/flink_session_profile_job.py`
unchanged by this stage):

- The `behavioral-events` topic is single-partition (`NUM_PARTITIONS = 1`,
  `platform/streaming/config.py:35`), so Kafka delivery order equals
  produce order exactly — no separate "did Kafka reorder it" question.
- The Flink job watermarks on the JSON payload's `event_time` field, not
  Kafka's own message timestamp: `EventTimeAssigner.extract_timestamp`
  returns `value[3]` (parsed from `event["event_time"]`) and ignores
  `record_timestamp` entirely; the producer never sets a Kafka message
  timestamp either. So what matters for exercising lateness is *delivery
  order diverging from event_time order*, not wall-clock delay of the
  HTTP POST.
- The watermark bound is confirmed exactly as documented:
  `WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))`.
- `.window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(30)))`
  has **no `.allowedLateness(...)` and no `.sideOutputLateData(...)`**
  anywhere in the file (grepped, absent). Flink's default allowed
  lateness is `Duration.ZERO`. **Live-confirmed this session** (see
  below): an event that misses its windows' watermark is silently
  dropped — no error, no side output, the window simply never fires for
  the content it would have carried.

**Simulator capability**: `platform/simulator/events.py::apply_jitter()`, a
new pure function, plus a `--jitter` flag on `platform/simulator/cli.py`.

## Design

`apply_jitter(planned, jitter, rng, max_delay=MAX_JITTER_DELAY)` is a
single left-to-right pass over an already `event_time`-ordered `planned`
list: at each index `i`, with probability `jitter` the event swaps forward
with the event at `min(i + rng.randint(1, max_delay), n-1)` (`max_delay`
defaults to `3`, bounding the displacement in *positions*, not seconds);
the scan resumes past the swapped-to index so no event participates in two
swaps. Each event's own `event_time` value is untouched by the swap — only
its position in the delivery sequence moves, modeling a client buffering
an event and sending it after already-newer ones, exactly what
`for_bounded_out_of_orderness` exists to tolerate/reject. Position-based
bounding is sufficient because canned-script track gaps run tens of
seconds to minutes, so even a 1-position swap across a track boundary
trivially exceeds the 5s watermark bound.

`jitter <= 0.0` returns `planned` unchanged with **zero** `rng` draws —
`--jitter` omitted (default `0.0`) is byte-identical to pre-stage-15B
behavior, including the exact sequence of `rng` calls `cli.py` already
makes for canned-script selection.

**Structural change for determinism**: planning (`build_session_events` +
`apply_jitter`) moved out of the per-session worker thread and into
`main()`'s existing single-threaded section, ahead of spawning session
threads. Previously `run_session()` called `build_session_events()` inside
each session's thread; if jitter's `rng` draws also happened there,
concurrent session threads would race on the one seeded `rng`, making
`--jitter` output depend on OS thread scheduling and breaking `--seed`
determinism. `run_session()` is now a pure I/O driver over a pre-built
`planned` list. No test called `run_session()` directly before this
change (confirmed by repo-wide grep), so the signature change was safe.

## Scope boundary

This stage does **not** modify `flink_session_profile_job.py` — no
`allowedLateness`/side-output handling is added; whether/how to add one is
a design decision for a later stage (15C/15D), not this one. Confirming
Flink's actual drop behavior is done **manually** against the already-running
Stage 13 job (see below), mirroring that stage's own precedent: automating
the job's submit/poll/cancel lifecycle inside a pytest test was explicitly
judged out of scope for a single session's timebox there, and no such
automation exists anywhere in the repo to build on. Automated tests instead
prove the piece that *is* cleanly testable — that the simulator, driven
through the real Kafka/Redis/Postgres path, genuinely delivers events out
of `event_time` order by more than the 5s bound (the necessary
precondition for the manual Flink check, not a substitute for it).

## Manual verification (real, captured output — not a template)

Submitted the unmodified Stage 13 job against the live stack:

```
docker cp platform/streaming/flink_session_profile_job.py 8-1-flink-jobmanager:/opt/flink/session_profile_job.py
docker exec 8-1-flink-jobmanager flink run -d -py /opt/flink/session_profile_job.py
# Job has been submitted with JobID aa1480bb152b25bcbf01b0a57c2d2fb7 (offsets: latest)
```

Ran two single-session replays of `coherent_session.yaml` (4 tracks, 9
events) back to back — a baseline with no jitter and a jittered run.
Because `--sessions 1` always anchors a single session's `start_time` at
`SIMULATION_EPOCH` regardless of `--seed` (the session index, not the
seed, drives the per-session hour offset), both runs share the exact same
absolute `event_time` span, so their windows are directly comparable
despite having different `session_id`s:

```
PYTHONPATH=platform platform/enrichment/.venv/bin/python -m simulator.cli \
    --seed 20001 --speed 1000 --sessions 1 \
    --script platform/simulator/scripts/coherent_session.yaml
# sim-20001-0: 9 events

PYTHONPATH=platform platform/enrichment/.venv/bin/python -m simulator.cli \
    --seed 20002 --speed 1000 --sessions 1 \
    --script platform/simulator/scripts/coherent_session.yaml --jitter 1.0
# sim-20002-0: 9 events
```

Captured every `result.print()` line for both sessions from the
taskmanager's stdout (`docker logs 8-1-flink-taskmanager -t`) once output
stabilized:

- **`sim-20001-0` (baseline, no jitter): 31 window firings.** The first
  seven fire with `1 events in window, weight_total=0.200` — the
  session's opening `play` event on track 2 (weight `0.2`), alone in each
  of the earliest sliding windows before later events join it. The
  sequence then grows through `3`, `2`, `4`, `2`, `3`-event windows
  (`weight_total` 1.120 → 2.289) as more of the session's events enter
  the 5-minute window, and eventually settles into a tail of `5`, `3`,
  `2`-event windows as the earliest events age out of the trailing
  windows again.
- **`sim-20002-0` (jitter=1.0): 8 window firings — exactly the tail of
  the baseline's sequence, and nothing else.** The seven `1 events,
  weight_total=0.200` windows that opened the baseline's output **never
  appear at all** for the jittered session — not delayed, not
  side-outputted, simply absent. `apply_jitter` moved the session's
  opening `play` event (originally index 0) later in delivery order;
  by the time it was actually POSTed, on-time events with much later
  `event_time`s had already advanced the watermark past every window
  that event belonged to. Flink's default zero allowed-lateness silently
  dropped it from all of them, and — because the windows in question had
  no other member — nothing was printed for those windows at all: no
  error, no side output, no trace.
- **Even a window that *did* fire for both sessions shows corruption, not
  just omission**: both sessions eventually produce a `5 events in
  window` line, but `weight_total=2.435` for the baseline versus
  `weight_total=2.518` for the jittered run — matching counts, different
  membership, because which five events had beaten the watermark by
  firing time differed between the two runs.
- The two runs' **final** windows are identical (`2 events in window,
  weight_total=0.994` for both) — the dropped opening event never
  rejoins later in the session either; this is a permanent loss from the
  windowed view, not a delay.

`session:sim-20001-0:profile` and `session:sim-20002-0:profile` both exist
in Redis (`EXISTS` returns `2`), each holding whichever window fired last
for that key — consistent with the log evidence above.

**One-sentence answer**: an event arriving more than 5 seconds behind the
watermark, once its windows have already fired for lack of anything else
to trigger them, is silently and permanently dropped from every window it
would have belonged to — no error, no side output, no record that it ever
existed in the windowed view (though it still lands durably in Postgres
via the separate stages-9-10 consumer, per Decision C — this job's
windowed centroid is the only thing that loses it).

## Verification

`tests/test_stage15b_jitter.py`: 6 new tests.

- **Pure (5)**, `events.apply_jitter()`, no live services: no-op and zero
  `rng` draws at `jitter=0.0`; determinism under a fixed seed; output is a
  true permutation of the input (nothing dropped/duplicated by the
  reorder function itself — Flink-level dropping, demonstrated above, is
  a separate concern from the reorder mechanism); genuine reordering
  occurs at `jitter=1.0`; displacement never exceeds `max_delay`.
- **Live (1)**: `test_simulator_jitter_delivers_events_out_of_event_time_order_beyond_watermark_bound`
  runs the real simulator with `--jitter 1.0` against the live event
  ingestion → Kafka → Redis/Postgres stack (no Flink involved), asserts
  nothing is lost or duplicated in transit (`drained == posted`), and
  that delivery order contains an adjacent inversion whose `event_time`
  gap exceeds the Flink job's 5s watermark bound — tried over a small
  fixed seed list (`9000`-`9004`) to stay reproducible without being
  flaky about which seed happens to produce a qualifying inversion.

118/118 tests pass repo-wide (112 before this stage + 6 new).

`platform/simulator/cli.py`'s and `events.py`'s pre-existing tests
(`tests/test_stage12_event_simulator.py`, 15 tests) and Stage 14's tests
that import `events.build_session_events()` directly
(`tests/test_stage14_recommendation_refresh.py`) pass unchanged — the
`run_session()` signature change is internal to `cli.py` and nothing
outside it called that function.
