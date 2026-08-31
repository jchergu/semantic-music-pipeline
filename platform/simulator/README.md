# platform/simulator — Stage 12: Event Simulator

A deterministic behavioral-event simulator: replays scripted listening
sessions against the live 411-track catalog by POSTing to
`platform/event_ingestion`'s `/events` endpoint.

## Running

Requires the docker compose stack up and the event ingestion service
running:

```bash
docker compose up -d
platform/enrichment/.venv/bin/python -m uvicorn event_ingestion.main:app --app-dir platform --port 8020
```

Then, from the repo root:

```bash
PYTHONPATH=platform platform/enrichment/.venv/bin/python -m simulator.cli \
    --seed 42 --speed 10 --sessions 3
```

- `--seed`: reproducibility seed. Same seed → identical `session_id`s
  (`f"sim-{seed}-{i}"`, no randomness involved) and identical event content,
  including `event_time` — the simulated clock is anchored to a fixed
  reference instant (`cli.SIMULATION_EPOCH`), not wall-clock `now()`,
  specifically so this holds across separate invocations at different real
  times.
- `--speed`: wall-clock compression. Events are posted with real
  `time.sleep(simulated_gap_seconds / speed)` between them, while
  `event_time` in the payload always advances by the full simulated gap —
  the two clocks are computed independently.
- `--sessions N`: N concurrent sessions (one thread each; events within a
  single session stay strictly ordered).
- `--script PATH`: use one specific YAML script for every session; omit to
  pick (seeded) among the three canned scripts in `scripts/` per session.

## Script format

```yaml
name: coherent_session
intent: "stays within one genre for the whole session"
tracks:
  - track_id: 2
    events:
      - {type: play, position_ms: 0}
      - {type: complete, position_pct: 0.97}
```

Each event needs exactly one of `position_ms` (absolute — used for the
early-skip case, a fixed `<5000ms` threshold regardless of track length) or
`position_pct` (fraction of the track's real `duration_sec`, looked up from
Postgres at runtime — used for completion, which is naturally
duration-relative). `type` must be one of `play`/`skip`/`complete`/`like`
(`simulator.events.KNOWN_EVENT_TYPES`) — matching Session E's future
skip-timing/weighting spec.

Three canned scripts ship in `scripts/`, all against real tracks in the
live dataset:

- `coherent_session.yaml` — 4 rock tracks, played through.
- `three_early_skips.yaml` — 3 tracks each skipped under 5s, then one
  played to completion.
- `context_switch.yaml` — 2 chillout tracks, then 2 hiphop tracks.

## Event shape

`platform/event_ingestion`'s `BehavioralEvent` model is `extra="allow"` —
ad hoc, not a frozen contract (see `contracts/README.md`). This simulator
adds two fields beyond the existing `session_id`/`event_type`/`track_id`:

- `event_time` — the simulated session clock (ISO 8601), decoupled from
  wall-clock time per the `--speed` requirement above.
- `position_ms` — elapsed milliseconds into the track when the event
  fired. Needed for Session E's future skip-timing weights
  (`<5000ms` = early skip, `>80%` duration = late skip), which are
  uncomputable from the event stream without it.

## No persistent consumer daemon

`platform/streaming/session_consumer.py` only had a single-message
`consume_and_cache_one()` before this stage — correct for every existing
stage 9-11 test (exactly one event per test), but it can't drain a whole
multi-event session: it never commits Kafka offsets, so a second call, even
with a fresh consumer group, just re-reads the same "earliest" matching
message again rather than advancing. This stage adds
`consume_and_cache_many(group_id, count, ...)`, which uses one `Consumer`
for the whole batch instead of recreating one per message. There is still
no long-running daemon that continuously drains the topic — verification
uses `consume_and_cache_many` directly, same as every other stage's tests
use `consume_and_cache_one`.

## Regenerating / verifying

```bash
platform/enrichment/.venv/bin/python -m pytest tests/test_stage12_event_simulator.py -v
```
