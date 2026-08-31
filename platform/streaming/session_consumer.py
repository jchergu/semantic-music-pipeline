"""
Kafka -> Redis session-state consumer for platform stage 9.

This is the platform-owned consumer from Decision B (CLAUDE.md): it reads
behavioral events and maintains session state in Redis. Stage 9 gives it
its Redis half only; stage 10 later extends it to also persist to
Postgres. The event shape here (a JSON object with a "session_id" key) is
ad hoc, not a frozen contract -- same stance stage 7 already took with its
plain string payloads (see contracts/README.md).
"""
import json
import time

from confluent_kafka import Consumer

from streaming.config import BOOTSTRAP_SERVERS, TOPIC_BEHAVIORAL_EVENTS
from streaming.session_state import record_event


def consume_and_cache_one(group_id: str, timeout: float = 10.0, expected_session_id: str | None = None) -> dict | None:
    """Reads one behavioral event and caches it in Redis via record_event.

    `expected_session_id`, when given, skips any message that doesn't
    match instead of caching the first thing polled -- the topic isn't
    purged between runs, so a fresh "earliest" group can otherwise surface
    a stale message left over from a previous run rather than the one this
    call just produced (the same issue consumer.py::consume_one hit and
    fixed in stage 7, via expected_value). Non-JSON messages are skipped
    outright -- this topic also carries stage 7's own plain-string test
    payloads from earlier runs, which aren't behavioral events at all.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC_BEHAVIORAL_EVENTS])
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict) or "session_id" not in event:
                continue
            if expected_session_id is not None and event["session_id"] != expected_session_id:
                continue
            record_event(event["session_id"], event)
            return event
        return None
    finally:
        consumer.close()
