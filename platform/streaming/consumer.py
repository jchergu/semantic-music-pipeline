"""Minimal synchronous consumer for platform stage 7's round-trip proof."""
import time

from confluent_kafka import Consumer

from streaming.config import BOOTSTRAP_SERVERS


def consume_one(topic: str, group_id: str, timeout: float = 10.0, expected_value: str | None = None) -> str | None:
    """Reads a message from `topic` using a fresh `group_id`.

    `auto.offset.reset="earliest"` is load-bearing: every caller here uses
    a brand-new consumer group created *after* the message was already
    produced, so the default "latest" would make it start reading past the
    message it's supposed to see and spuriously time out.

    `expected_value`, when given, skips any message that doesn't match
    instead of returning the first thing polled — the topic isn't purged
    between runs, so "earliest" on a fresh group can otherwise surface a
    stale message left over from a previous run rather than the one this
    call just produced.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                continue
            value = msg.value().decode("utf-8")
            if expected_value is None or value == expected_value:
                return value
        return None
    finally:
        consumer.close()
