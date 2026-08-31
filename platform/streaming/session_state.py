"""
Redis session cache for platform stage 9.

The Redis half of the "Kafka consumer that reads behavioral events and
maintains session state in Redis" from CLAUDE.md's Decision B. Stage 10
later adds the Postgres durability half on top of the same consumer
(see session_consumer.py) — this module only knows about Redis.
"""
import json

import redis

from streaming.config import REDIS_HOST, REDIS_PORT, SESSION_TTL_SECONDS

_client = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)


def _session_key(session_id: str) -> str:
    return f"session:{session_id}:events"


def profile_key(session_id: str) -> str:
    """The weighted session centroid vector, DERIVED state -- owned by the
    stage 13 PyFlink job (platform/streaming/flink_session_profile_job.py),
    not this platform-owned consumer. Decision C, 2026-08-31: this module
    owns RAW state (session:{id}:events, above); nothing in this file
    reads or writes session:{id}:profile itself. The Flink job's Python
    UDF workers can't import this module directly (no platform/ on their
    path inside the container), so it constructs the identical key string
    inline -- this function is the canonical definition that string is
    tested against, not a shared import."""
    return f"session:{session_id}:profile"


def record_event(session_id: str, event: dict) -> None:
    """Appends `event` to the session's event list and refreshes its TTL.

    The TTL is reset on every call (sliding window): a session stays alive
    in Redis for SESSION_TTL_SECONDS after its *last* event, not its first.
    """
    key = _session_key(session_id)
    _client.rpush(key, json.dumps(event))
    _client.expire(key, SESSION_TTL_SECONDS)


def get_session_events(session_id: str) -> list[dict]:
    key = _session_key(session_id)
    return [json.loads(raw) for raw in _client.lrange(key, 0, -1)]
