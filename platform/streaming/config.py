"""
Kafka bootstrap/topic configuration for platform stage 7.

Same pattern as platform/semantic_api/dependencies.py: hosts default to
localhost (every stage 7-11 script/test runs on the host machine, against
the local docker-compose stack over its mapped ports). Stage 13's Flink
job is the first code that runs *inside* the docker-compose network
instead, so each host is now an env-var override with the localhost
default preserved -- docker-compose gives the Flink containers
KAFKA_HOST=kafka/REDIS_HOST=redis/POSTGRES_HOST=postgres so this same
module resolves correctly in both places without a networking-specific
fork.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT.parent / ".env")

KAFKA_HOST = os.environ.get("KAFKA_HOST", "localhost")
# KAFKA_PORT is the host-mapped listener (localhost:9092); KAFKA_BOOTSTRAP_PORT
# lets in-cluster code (Flink) point at the internal listener (kafka:29092)
# instead -- see docker-compose.yml's KAFKA_ADVERTISED_LISTENERS.
KAFKA_BOOTSTRAP_PORT = os.environ.get("KAFKA_BOOTSTRAP_PORT", os.environ.get("KAFKA_PORT", "9092"))
BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_BOOTSTRAP_PORT}"

TOPIC_MEDIA_STREAM = "media-stream"
TOPIC_BEHAVIORAL_EVENTS = "behavioral-events"
TOPICS = [TOPIC_MEDIA_STREAM, TOPIC_BEHAVIORAL_EVENTS]

# Single-broker docker-compose cluster (KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
# in docker-compose.yml) — replication factor must match.
NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")
SESSION_TTL_SECONDS = 1800  # 30 min sliding session window

# The canonical Kafka consumer group id for the platform-owned Decision B
# consumer (session_consumer.py's consume_and_cache_one/_many). Explicit,
# not defaulted into those functions' signatures -- group_id stays a
# required parameter there, so a caller must deliberately choose to pass
# this constant. Tests intentionally use their own throwaway group ids for
# isolation instead (a fresh id per test avoids cross-test interference on
# the shared, never-purged behavioral-events topic) -- only a real,
# continuously-running deployment of this consumer (which doesn't exist
# yet -- see docs/platform/stage12-event-simulator.md's "No persistent
# consumer daemon" section) should use this one. Decision C, 2026-08-31.
SESSION_CONSUMER_GROUP_ID = "platform-session-consumer"

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
PG_DSN = (
    f"host={POSTGRES_HOST} port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)
