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

# Stage 15D. The same 30 minutes, applied to the DERIVED session keys
# (:profile, :profile_meta, :recs, :refresh_meta) that carried no expiry at
# all until this stage -- the finding stage 16 surfaced and deferred here.
#
# One constant rather than two, deliberately: derived state is meaningless
# without the session it derives from, Redis is a session cache and never a
# system-of-record substitute (CLAUDE.md), and the Postgres events log stays
# the durable record, so nothing irreplaceable expires here.
#
# The asymmetry does not become a guarantee in the other direction, and the
# docs must not claim it does. Raw events refresh their TTL on every event;
# derived state refreshes its own only when a refresh fires or a Flink window
# closes, and a window can close either side of the session's last event. So
# the derived keys now expire *near* the raw state rather than before it:
# measured across 32 derived keys in 8 sessions (Stage 15D's post-fix pack),
# derived-minus-raw TTL lands in [-21s, +23s] -- :recs and :refresh_meta
# always at or before raw, :profile/:profile_meta either side. Bounded
# seconds, set by the window/debounce cadence, instead of the unbounded
# "never" this replaces. session_api can no longer serve recommendations for
# a session it has forgotten by more than that margin, and GET /sessions/{id}
# still reports the transient via raw_state_expired.
DERIVED_TTL_SECONDS = SESSION_TTL_SECONDS

# The canonical Kafka consumer group id for the platform-owned Decision B
# consumer (session_consumer.py's consume_and_cache_one/_many). Explicit,
# not defaulted into those functions' signatures -- group_id stays a
# required parameter there, so a caller must deliberately choose to pass
# this constant. Tests intentionally use their own throwaway group ids for
# isolation instead (a fresh id per test avoids cross-test interference on
# the shared, never-purged behavioral-events topic) -- only a real,
# continuously-running deployment of this consumer should use this one.
# Since stage 16 that deployment exists: session_consumer_daemon.py passes
# this by default, and is the first consumer in the repo to commit offsets
# because it is the first that restarts. Decision C, 2026-08-31.
SESSION_CONSUMER_GROUP_ID = "platform-session-consumer"

# Stage 14's recommendation-refresh consumer: a separate, independent
# consumer group on the same behavioral-events topic (Kafka's normal
# fan-out model -- multiple groups each get their own full copy of the
# stream). It never writes session:{id}:events (that stays
# SESSION_CONSUMER_GROUP_ID's job per Decision C); it only reads raw
# state to decide what to refresh.
RECS_REFRESH_GROUP_ID = "platform-recs-refresh"

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
PG_DSN = (
    f"host={POSTGRES_HOST} port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)
