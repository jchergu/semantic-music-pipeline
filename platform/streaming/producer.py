"""Minimal synchronous producer for platform stage 7's round-trip proof."""
from confluent_kafka import Producer

from streaming.config import BOOTSTRAP_SERVERS


def produce(topic: str, value: str, key: str | None = None) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    producer.produce(
        topic,
        value=value.encode("utf-8"),
        key=key.encode("utf-8") if key is not None else None,
    )
    remaining = producer.flush(10)
    if remaining:
        raise RuntimeError(f"{remaining} message(s) not delivered to {topic!r} within 10s")
