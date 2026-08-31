# Stage 12 — Event Simulator

## What this stage adds

`platform/simulator/` — a deterministic behavioral-event simulator. Nothing
in the repo could produce a scripted, reproducible stream of behavioral
events before this: the only events that had ever hit `behavioral-events`
were one-off manual test payloads (stages 7-11's own tests). Every
downstream 8.2/8.3 session in the project roadmap (a future Flink job's
event-time windows, an 8.2 evaluation harness computing overlap between
consecutive recommendation lists, cross-session similarity) depends on
being able to replay a known session and get identical behavior out.
Explicitly requested (Session C of the roadmap), not a build-order guess —
same footing as Decision B's platform-owned streaming consumer, which 8.2
and 8.3 both reuse (CLAUDE.md).

## Design

A CLI (`platform/simulator/cli.py`) that loads a YAML session script
(`platform/simulator/scripts_io.py`), resolves it into an ordered list of
events with simulated timestamps and wall-clock gaps
(`platform/simulator/events.py`, pure — no network, no `time.sleep`), then
POSTs each event to the live `platform/event_ingestion` service, sleeping
`simulated_gap_seconds / --speed` between posts.

Two fields were added to the event payload beyond
`platform/event_ingestion/main.py`'s current `session_id`/`event_type`/
`track_id` — not a silent extension of a frozen schema (the model is
`extra="allow"` specifically to permit this, same stance stages 7/9/10
already documented in `contracts/README.md`), but called out explicitly
here per this stage's own requirement to report rather than quietly add:

- `event_time` — the simulated session clock, decoupled from wall-clock
  time.
- `position_ms` — needed for Session E's future skip-timing weights
  (`<5000ms` = early skip, `>80%` duration = late skip), uncomputable
  without it.

`event_time` is anchored to a fixed reference instant
(`cli.SIMULATION_EPOCH = 2000-01-01T00:00:00Z`, offset per session index),
not wall-clock `now()` — "same seed, byte-identical event stream" requires
`event_time` itself to reproduce across separate invocations, not just the
gaps between events.

## Two bugs found while verifying the exit criterion, not assumed away

1. **`session_consumer.consume_and_cache_one()` cannot drain more than one
   event per session.** It never commits Kafka offsets (correct, by its own
   docstring, for exactly one message per call), so a second call — even
   with a fresh consumer group — just re-reads the same "earliest" matching
   message again instead of advancing. Every existing stage 9-11 test only
   ever needed one event per test, so this never surfaced before. Fixed by
   adding `consume_and_cache_many(group_id, count, ...)` alongside the
   existing function (not modifying it) — one `Consumer` for the whole
   batch instead of recreating one per message.
2. **`event_time` was originally anchored to `datetime.now()`** inside
   `run_session()`, silently breaking the "same seed, byte-identical event
   stream" requirement for any two separate CLI invocations. Fixed by the
   `SIMULATION_EPOCH` anchor above.

Both were caught by the live end-to-end test actually failing, not by
inspection — see `tests/test_stage12_event_simulator.py`.

## Verification

15 tests in `tests/test_stage12_event_simulator.py` (mirrors every other
stage's test-file convention — pure unit tests and the live end-to-end test
in one file, in root `tests/`, not a nested `platform/simulator/tests/`):
13 pure (event construction, position resolution, script validation,
including loading and validating all three shipped canned scripts) plus 2
live, against the running stack:

- `test_simulator_e2e_lands_in_redis_and_postgres` — runs the CLI against a
  live `event_ingestion_server` fixture, drains via
  `consume_and_cache_many`, asserts the drained events match exactly what
  was posted, and that they land in both Redis
  (`session_state.get_session_events`) and Postgres
  (`session_store.get_events`).
- `test_simulator_seed_determinism_across_two_runs` — runs the literal
  exit criterion: the same `--seed`/script run twice produces the same
  deterministic `session_id`, byte-identical event content across both
  runs, and — after draining both runs together — every event from run 1
  appears exactly twice in Postgres (once per run), i.e. "identical event
  sequences in Postgres, modulo event_id."

Also run manually against the live stack with the exact CLI invocation
from the roadmap's exit criterion (`--seed 42 --speed 10`, run twice) —
see the commit for the captured output.

79/79 tests pass across the whole repo (64 before this stage + 15 new).

## No persistent consumer daemon

Still doesn't exist (see `platform/simulator/README.md`) — out of this
stage's scope, arguably stage 9-10's own gap. Verification drives
`consume_and_cache_many` directly rather than a long-running service.
