"""
Redis session cache for platform stage 9.

The Redis half of the "Kafka consumer that reads behavioral events and
maintains session state in Redis" from CLAUDE.md's Decision B. Stage 10
later adds the Postgres durability half on top of the same consumer
(see session_consumer.py) — this module only knows about Redis.
"""
import json

import redis

from streaming.config import DERIVED_TTL_SECONDS, REDIS_HOST, REDIS_PORT, SESSION_TTL_SECONDS

_client = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)


def events_key(session_id: str) -> str:
    """The session's RAW behavioral event list, owned by this module and
    its consumer (Decision C). Public alongside profile_key()/recs_key()/
    profile_meta_key() since stage 16: platform/session_api/ reads this key
    to tell "no such session" apart from "a session exists but nothing has
    been recommended for it yet", and it should get the name from the same
    canonical place every other reader does rather than rebuilding the
    string or importing a private one."""
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


def profile_meta_key(session_id: str) -> str:
    """Sibling of profile_key(): the wall-clock provenance of the vector that
    profile_key() holds (hash fields: computed_at, n_events), written by the
    same stage 13 Flink job. Deliberately a SEPARATE key rather than extra
    fields inside session:{id}:profile -- recommendation_refresh.py
    json.loads()es that value and assumes a flat list of floats, so anything
    added there would break the warm path's parse. Added for stage 15C
    (eval/8_2 metric 2, H2: profile compute lag); same
    canonical-definition-here, duplicated-string-in-the-job arrangement as
    profile_key()."""
    return f"session:{session_id}:profile_meta"


def recs_key(session_id: str) -> str:
    """The session's current top-10 recommendations (JSON list), DERIVED
    state written by stage 14's recommendation_refresh.py, read by the
    stage 15 API endpoint the roadmap describes next. Kept here for the
    same reason as profile_key() -- one canonical definition, even though
    (unlike profile_key) the writer runs in the same process and could
    import it directly; keeping both key-name definitions in one place
    avoids a reader needing to know which module actually owns the
    string."""
    return f"session:{session_id}:recs"


def refresh_meta_key(session_id: str) -> str:
    """The refresh debounce's bookkeeping (hash fields: last_refresh_ts,
    events_since_refresh), DERIVED state written by stage 14's
    recommendation_refresh.py.

    Added in stage 15D. It was the one session key with no canonical
    definition here -- recommendation_refresh.py built the string inline --
    which is exactly how it came to be the one derived key nobody noticed
    was missing a TTL. Defined alongside its siblings now so the next
    reader sees all four derived keys in one place."""
    return f"session:{session_id}:refresh_meta"


def expire_derived(client, session_id: str, *keys: str) -> None:
    """Applies DERIVED_TTL_SECONDS to derived session keys.

    Takes the caller's own client rather than using this module's, because
    the writers of derived state each hold their own connection (stage 14's
    refresh passes one in; stage 13's Flink job cannot import this module at
    all and duplicates the call inline, the same arrangement profile_key()
    documents).

    Sliding, like record_event's: the TTL is reset on every write, so a
    session that keeps producing refreshes keeps its derived state alive."""
    for key in keys:
        client.expire(key, DERIVED_TTL_SECONDS)


def record_event(session_id: str, event: dict) -> None:
    """Appends `event` to the session's event list and refreshes its TTL.

    The TTL is reset on every call (sliding window): a session stays alive
    in Redis for SESSION_TTL_SECONDS after its *last* event, not its first.
    """
    key = events_key(session_id)
    _client.rpush(key, json.dumps(event))
    _client.expire(key, SESSION_TTL_SECONDS)


def get_session_events(session_id: str) -> list[dict]:
    key = events_key(session_id)
    return [json.loads(raw) for raw in _client.lrange(key, 0, -1)]
