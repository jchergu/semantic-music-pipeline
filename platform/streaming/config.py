"""
Kafka bootstrap/topic configuration for platform stage 7.

Same pattern as platform/semantic_api/dependencies.py: host is hardcoded
to localhost (this only ever runs against the local docker-compose
stack), only the port comes from the environment.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT.parent / ".env")

BOOTSTRAP_SERVERS = f"localhost:{os.environ.get('KAFKA_PORT', '9092')}"

TOPIC_MEDIA_STREAM = "media-stream"
TOPIC_BEHAVIORAL_EVENTS = "behavioral-events"
TOPICS = [TOPIC_MEDIA_STREAM, TOPIC_BEHAVIORAL_EVENTS]

# Single-broker docker-compose cluster (KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
# in docker-compose.yml) — replication factor must match.
NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1

REDIS_HOST = "localhost"
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

PG_DSN = (
    f"host=localhost port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)
