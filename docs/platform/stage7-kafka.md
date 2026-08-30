# Stage 7 — Kafka Topics + Producer/Consumer Wiring

Status: **verified working**, 2026-08-30.

## What this stage does

The first of the not-started streaming platform stages (7-11, see
CLAUDE.md's Build order) gets its Kafka piece: the two topics the L1
architecture calls for exist on the docker-compose broker, and a minimal
synchronous producer/consumer proves round-trip delivery against it. This
is deliberately narrow — it does not touch `contracts/` (per
`contracts/README.md`, topic schemas don't get frozen until a second real
consumer exists), and it does not wire up session state, Redis, a real
ingestion service, or an 8.2 consumer. Those are stages 8-11.

Code: `platform/streaming/config.py` (bootstrap servers, topic names,
partition/replication constants), `platform/streaming/topics.py` (topic
creation), `platform/streaming/producer.py`, `platform/streaming/consumer.py`.

## Topics

| Topic | Partitions | Replication factor | Future purpose |
|---|---|---|---|
| `media-stream` | 1 | 1 | ingested track/audio events (per L1 architecture) |
| `behavioral-events` | 1 | 1 | listening/interaction events, produced by the separate ingestion service from Decision A (see CLAUDE.md) |

Replication factor 1 matches the single-broker compose setup
(`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1` in `docker-compose.yml`).
`platform/streaming/topics.py::create_topics()` is idempotent — it
swallows `TOPIC_ALREADY_EXISTS` from `AdminClient.create_topics()` and
re-raises anything else, so it's safe to call on every test run or by
hand.

## Producer / Consumer

`produce(topic: str, value: str, key: str | None = None) -> None` — a
per-call `confluent_kafka.Producer`, `flush(10)`, raises if any message
wasn't delivered within the timeout.

`consume_one(topic: str, group_id: str, timeout: float = 10.0,
expected_value: str | None = None) -> str | None` — bounded poll loop
(0.5s slices against a wall-clock deadline, never blocks indefinitely),
returns `None` on timeout rather than raising. Uses
`auto.offset.reset="earliest"`, which is load-bearing: every test/demo
call here uses a brand-new consumer group created *after* the message was
already produced, so the default `"latest"` would make the consumer start
reading past the message it's supposed to see and spuriously time out.

`expected_value` exists because the topics aren't purged between runs: a
fresh consumer group with `"earliest"` reads from the very start of the
topic, so without filtering it can return a stale message left over from
a previous test run instead of the one this call just produced (found
during verification — the first full-suite run passed by accident because
the topics were still empty; the second run, against topics that already
had history, failed until this filter was added). When `expected_value`
is given, non-matching messages are skipped rather than returned.

## Verification

Manual, before writing the automated test:
1. `platform/enrichment/.venv/bin/python -m pip install -r platform/streaming/requirements.txt`
2. `python -m streaming.topics` (from `platform/`) — printed `topics
   ready: ['media-stream', 'behavioral-events']`.
3. `docker exec 8-1-kafka kafka-topics --bootstrap-server localhost:9092
   --list` — confirmed both topics present.
4. Re-ran `python -m streaming.topics` — confirmed no error on
   already-existing topics (idempotency).

## Test results

`tests/test_stage7_kafka.py`, against the live broker, no mocking:
- `test_round_trip_media_stream` / `test_round_trip_behavioral_events` —
  produce a UUID payload, consume it back with a fresh consumer group,
  assert equality.
- `test_topic_isolation` — a message produced to `media-stream` is not
  visible to a fresh consumer on `behavioral-events`.

All 3 pass, alongside the existing 32 (stages 2-4 + 8.1's own tests) —
**35/35 total**, zero regressions.

## Packaging note

`confluent-kafka==2.5.3` resolves to a prebuilt manylinux wheel for
Python 3.12/x86_64 (verified via `pip download confluent-kafka==2.5.3
--no-deps --python-version 312 --only-binary=:all:` — librdkafka is
bundled, no compile step). Plain `pip install -r
platform/streaming/requirements.txt` is sufficient; no `install.sh`
needed here, unlike `platform/enrichment` (laion-clap) and
`platform/semantic_api` (pymilvus).

Rejected `kafka-python` (unmaintained; known to break against modern
broker API-version probing — cp-kafka 7.6.1 is Kafka 3.6-line — without
manually pinning `api_version`) and `aiokafka` (async-only, doesn't match
this repo's plain-synchronous-function style used everywhere else in
`platform/`).

## Reproducing / extending

Requires `docker compose up -d` with kafka + zookeeper healthy.

```bash
platform/enrichment/.venv/bin/python -m pip install -r platform/streaming/requirements.txt
cd platform && ../enrichment/.venv/bin/python -m streaming.topics && cd ..
platform/enrichment/.venv/bin/python -m pytest tests/test_stage7_kafka.py -v
```

Explicitly deferred to later stages (do not build against this doc as if
they're done): session state and Redis (stage 9), the Postgres
sessions/events schema (stage 10), a real ingestion service actually
calling `produce()` in a live pipeline flow (stage 11, Decision A), and
8.2 as a second real consumer of any of this.
