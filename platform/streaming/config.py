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
