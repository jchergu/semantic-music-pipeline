"""
Kafka -> Redis/Postgres session-state consumer for platform stages 9-10.

This is the platform-owned consumer from Decision B (CLAUDE.md): it reads
behavioral events and maintains session state in Redis, and (stage 10)
optionally persists them durably to Postgres. The event shape here (a
JSON object with a "session_id" key) is ad hoc, not a frozen contract --
same stance stage 7 already took with its plain string payloads (see
contracts/README.md).
"""
import json
import time

from confluent_kafka import Consumer

from streaming.config import BOOTSTRAP_SERVERS, TOPIC_BEHAVIORAL_EVENTS
from streaming.session_state import record_event
from streaming.session_store import get_events, persist_event


def consume_and_cache_one(
    group_id: str, timeout: float = 10.0, expected_session_id: str | None = None, pg_conn=None
) -> dict | None:
    """Reads one behavioral event, caches it in Redis, and returns it.

    `expected_session_id`, when given, skips any message that doesn't
    match instead of caching the first thing polled -- the topic isn't
    purged between runs, so a fresh "earliest" group can otherwise surface
    a stale message left over from a previous run rather than the one this
    call just produced (the same issue consumer.py::consume_one hit and
    fixed in stage 7, via expected_value). Non-JSON messages are skipped
    outright -- this topic also carries stage 7's own plain-string test
    payloads from earlier runs, which aren't behavioral events at all.

    `pg_conn`, when given, also durably persists the event to Postgres
    (stage 10) via session_store.persist_event -- optional so stage 9's
    Redis-only callers are unaffected.
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
            if pg_conn is not None:
                persist_event(pg_conn, event["session_id"], event)
            return event
        return None
    finally:
        consumer.close()


def new_consumer(group_id: str) -> Consumer:
    """A subscribed Consumer for this topic, for a caller that wants to
    reuse one across repeated calls.

    Offsets are not auto-committed (the platform's setting everywhere), so
    a *fresh* Consumer always rescans from "earliest" -- correct for the
    bounded, throwaway-group-id callers in stages 9-12 and in tests, and
    wrong for anything that reads repeatedly. Mirrors
    recommendation_refresh.py::new_consumer(), added by stage 14 for the
    same reason; stage 16's session_consumer_daemon.py is this module's
    first repeated-read caller.
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
    return consumer


def consume_and_cache_many(
    group_id: str, count: int, timeout: float = 10.0, expected_session_id: str | None = None,
    pg_conn=None, consumer: Consumer | None = None
) -> list[dict]:
    """Reads up to `count` distinct matching events, caching each into
    Redis (and optionally Postgres), same filtering/skip rules as
    consume_and_cache_one(). Added for stage 12: consume_and_cache_one()
    never commits offsets (see its docstring -- correct for exactly one
    message per call), so a second call, even with a fresh group_id, just
    re-reads the same "earliest" matching message again rather than
    advancing -- fine for every existing single-event test, but stage 12's
    simulator needs to drain a whole multi-event session. This uses one
    Consumer for the whole batch instead of recreating one per message.

    `consumer`, when given, is reused and left open for the caller to
    close -- a repeated caller (stage 16's session_consumer_daemon.py)
    must pass the same one back in every time, or each call rescans from
    "earliest" and re-delivers the events the previous call already
    handled. Same parameter, for the same reason, as
    recommendation_refresh.process_one_event()'s.
    """
    owns_consumer = consumer is None
    if consumer is None:
        consumer = new_consumer(group_id)
    results: list[dict] = []
    try:
        deadline = time.monotonic() + timeout
        while len(results) < count and time.monotonic() < deadline:
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
            if pg_conn is not None:
                persist_event(pg_conn, event["session_id"], event)
            results.append(event)
        return results
    finally:
        if owns_consumer:
            consumer.close()


def rebuild_session_state(session_id: str, pg_conn) -> list[dict]:
    """Recovery path for RAW state (Decision C, 2026-08-31): replays a
    session's durable Postgres event log back into Redis, e.g. after a
    Redis flush or restart. Additive -- existing Redis state for the
    session, if any, is not cleared first (record_event only appends), so
    call this only when Redis is known to be empty/stale for this session,
    or duplicate entries will result."""
    events = get_events(pg_conn, session_id)
    for event in events:
        record_event(session_id, event)
    return events
