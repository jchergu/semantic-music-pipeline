"""
Idempotent creation of the platform's two Kafka topics.

Run standalone (from repo root, with platform/ on the path):
    platform/enrichment/.venv/bin/python -m streaming.topics
"""
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from streaming.config import BOOTSTRAP_SERVERS, NUM_PARTITIONS, REPLICATION_FACTOR, TOPICS


def create_topics() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    new_topics = [
        NewTopic(topic, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)
        for topic in TOPICS
    ]
    futures = admin.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result(timeout=10)
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


if __name__ == "__main__":
    create_topics()
    print(f"topics ready: {TOPICS}")
