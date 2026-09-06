"""
Stage 14: recommendation refresh loop.

On each behavioral event, decides (debounced) whether to refresh a
session's recommendations. When it does: Milvus ANN over the session
profile vector (stage 13) -> exclude tracks already played in this
session -> Neo4j re-rank against the session's active context -> top-10
-> session:{id}:recs in Redis (session_state.py::recs_key()).

Own Kafka consumer group (RECS_REFRESH_GROUP_ID, config.py) -- an
independent fan-out consumer of behavioral-events alongside
SESSION_CONSUMER_GROUP_ID's raw-state consumer (Decision C) and stage
13's Flink job. This module never writes session:{id}:events; it only
reads raw state (session_state.get_session_events()) to decide what to
refresh. There's no ordering guarantee that the raw-state consumer has
already cached the *current* event by the time this module processes the
same Kafka message, so the current event is appended locally rather than
assumed present in what Redis returns.

Reuses platform/scoring/ranking.py (Decision D) and
usecases/8_1_batch_reactive/recommender/context_builder.py exactly as
8.1's own recommend.py does -- this module adds a Milvus
search-by-vector path (the Semantic API has no such endpoint;
/tracks/{id}/similar only takes a track id) and per-session debounce
state, not a new scoring model.
"""
import json
import logging
import sys
import time
from pathlib import Path

import psycopg2
from confluent_kafka import Consumer
from pymilvus import Collection, connections

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "platform"))
sys.path.insert(0, str(ROOT / "usecases" / "8_1_batch_reactive"))

from recommender import context_builder  # noqa: E402
from scoring import ranking  # noqa: E402
from streaming.config import BOOTSTRAP_SERVERS, PG_DSN, RECS_REFRESH_GROUP_ID, TOPIC_BEHAVIORAL_EVENTS  # noqa: E402
from streaming.session_state import get_session_events, profile_key, recs_key  # noqa: E402

log = logging.getLogger("recommendation_refresh")

MILVUS_ALIAS = "recs-refresh"  # own alias, per CLAUDE.md's Platform contracts rule
MILVUS_COLLECTION = "track_embeddings"

DEBOUNCE_MIN_INTERVAL_SECONDS = 5.0
DEBOUNCE_MIN_EVENTS = 3
DEFAULT_TOP_K = 10  # matches recommend.py::DEFAULT_TOP_K
DEFAULT_CANDIDATE_K = 25  # matches recommend.py::DEFAULT_CANDIDATE_K
MIN_RAW_EVENTS_FOR_WARM_PATH = 2


# --- pure logic, unit-testable without live services ---


def should_refresh(
    last_refresh_ts: float | None,
    events_since_refresh: int,
    now: float,
    min_interval_seconds: float = DEBOUNCE_MIN_INTERVAL_SECONDS,
    min_events: int = DEBOUNCE_MIN_EVENTS,
) -> bool:
    """Debounce: refresh at most once per min_interval_seconds OR once per
    min_events, whichever comes first. Never refreshed yet -> always due."""
    if last_refresh_ts is None:
        return True
    return (now - last_refresh_ts) >= min_interval_seconds or events_since_refresh >= min_events


def determine_context(events: list[dict]) -> tuple[str | None, set[str]]:
    """events: ordered oldest -> newest. Returns (anchor_track_id, played_track_ids).
    anchor is the most recent event's track -- the "active context" for
    genre-sibling/same-artist re-ranking. played is every distinct track
    the session has touched, for exclusion from new candidates. Returns
    (None, set()) for an empty session (the cold-start caller checks
    event count separately, before this matters)."""
    if not events:
        return None, set()
    played = {e["track_id"] for e in events if e.get("track_id")}
    anchor = events[-1].get("track_id")
    return anchor, played


def merge_current_event(events: list[dict], current_event: dict | None) -> list[dict]:
    """Appends current_event if it isn't already the last element of
    events -- covers the race where this consumer processes a Kafka
    message before the independent raw-state consumer has cached it."""
    if current_event is None:
        return events
    if events and events[-1] == current_event:
        return events
    return events + [current_event]


# --- live I/O ---


def connect_milvus() -> Collection:
    connections.connect(alias=MILVUS_ALIAS, host="localhost", port="19530")
    collection = Collection(MILVUS_COLLECTION, using=MILVUS_ALIAS)
    collection.load()
    return collection


def disconnect_milvus() -> None:
    connections.disconnect(alias=MILVUS_ALIAS)


def connect_postgres():
    return psycopg2.connect(PG_DSN)


def milvus_search_by_vector(collection: Collection, query_vector: list, exclude_ids: set[int], candidate_k: int) -> list[tuple[int, float]]:
    kwargs = dict(
        data=[query_vector],
        anns_field="embedding",
        param={"metric_type": "COSINE", "params": {"nprobe": 16}},
        limit=candidate_k,
        output_fields=["track_id"],
    )
    if exclude_ids:
        kwargs["expr"] = f"track_id not in [{','.join(str(i) for i in sorted(exclude_ids))}]"
    result = collection.search(**kwargs)
    return [(hit.entity.get("track_id"), hit.distance) for hit in result[0]]


def fetch_track_meta(conn, track_ids: list[int]) -> dict:
    if not track_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, title, artist_name FROM tracks WHERE id = ANY(%s)", (track_ids,))
        return {row[0]: {"title": row[1], "artist_name": row[2]} for row in cur.fetchall()}


def _write_recs(redis_client, session_id: str, ranked: list[dict]) -> None:
    redis_client.set(recs_key(session_id), json.dumps(ranked))


def refresh_recommendations(
    session_id: str,
    redis_client,
    pg_conn,
    milvus_collection: Collection,
    http_client,
    current_event: dict | None = None,
) -> dict:
    """Runs the actual refresh (caller has already decided it's due) and
    writes session:{id}:recs. Returns a small result dict for logging/tests."""
    events = merge_current_event(get_session_events(session_id), current_event)

    if len(events) < MIN_RAW_EVENTS_FOR_WARM_PATH:
        return {"action": "skip_insufficient_data", "event_count": len(events)}

    profile_raw = redis_client.get(profile_key(session_id))
    anchor_id, played_ids = determine_context(events)
    played_int_ids = {int(t) for t in played_ids if t}

    if profile_raw is None:
        # Cold start: stage 13's Flink job hasn't produced a profile for
        # this session yet (its 5-min/30s-slide windows need more than a
        # couple of events to fire meaningfully). Falls back to exactly
        # 8.1's own batch path, seeded by the session's first track --
        # logged distinctly, not silently taken.
        first_track_id = int(events[0]["track_id"])
        ctx = context_builder.build_context(http_client, first_track_id, candidate_k=DEFAULT_CANDIDATE_K)
        ranked = ranking.score_recommendations(first_track_id, ctx.similar, ctx.genre_siblings, ctx.same_artist, top_k=DEFAULT_TOP_K)
        _write_recs(redis_client, session_id, ranked)
        log.info("session=%s fallback=cold_start seed=%s count=%d", session_id, first_track_id, len(ranked))
        return {"action": "cold_start", "count": len(ranked), "seed_track_id": first_track_id}

    profile_vector = json.loads(profile_raw)
    similar_raw = milvus_search_by_vector(milvus_collection, profile_vector, played_int_ids, DEFAULT_CANDIDATE_K)
    meta = fetch_track_meta(pg_conn, [tid for tid, _ in similar_raw])
    similar = [
        {"track_id": tid, "title": meta[tid]["title"], "artist_name": meta[tid]["artist_name"], "score": score}
        for tid, score in similar_raw
        if tid in meta
    ]

    anchor_int = int(anchor_id) if anchor_id else None
    genre_siblings: list[dict] = []
    same_artist: list[dict] = []
    if anchor_int is not None:
        ctx = context_builder.build_context(http_client, anchor_int, candidate_k=DEFAULT_CANDIDATE_K)
        # context_builder's Neo4j lookups don't know about this session's
        # play history -- filter played tracks out here too, or they could
        # re-enter the final list via the genre/artist boost path even
        # though the Milvus search above already excluded them.
        genre_siblings = [c for c in ctx.genre_siblings if int(c["track_id"]) not in played_int_ids]
        same_artist = [c for c in ctx.same_artist if int(c["track_id"]) not in played_int_ids]

    ranked = ranking.score_recommendations(anchor_int if anchor_int is not None else -1, similar, genre_siblings, same_artist, top_k=DEFAULT_TOP_K)
    _write_recs(redis_client, session_id, ranked)

    if len(ranked) < DEFAULT_TOP_K:
        # 411 tracks is a small catalog -- a long session can genuinely
        # exhaust novel candidates. Logged, not padded or hidden.
        log.warning("session=%s recs_short=true count=%d played=%d", session_id, len(ranked), len(played_int_ids))

    return {"action": "warm", "count": len(ranked), "anchor_track_id": anchor_int}


def new_consumer(group_id: str) -> Consumer:
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


def process_one_event(
    group_id: str,
    redis_client,
    pg_conn,
    milvus_collection: Collection,
    http_client,
    timeout: float = 10.0,
    expected_session_id: str | None = None,
    now_fn=time.time,
    consumer: Consumer | None = None,
) -> dict | None:
    """Reads one behavioral event and applies the debounced refresh
    decision. Offsets are never committed (enable.auto.commit: False, same
    as session_consumer.py), so a *fresh* Consumer always rescans from
    "earliest" and returns the same first matching message again --
    correct for a single isolated call, but a real driver calling this
    repeatedly must pass the *same* `consumer` object back in each time
    (create it once with new_consumer(), reuse it in a loop), or it will
    never advance past the first event, exactly the bug stage 12/Decision
    C's consume_and_cache_many() exists to avoid for the raw-state
    consumer. When `consumer` isn't given, one is created and closed
    within this call, for simple one-off usage."""
    owns_consumer = consumer is None
    if consumer is None:
        consumer = new_consumer(group_id)
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
            session_id = event["session_id"]
            if expected_session_id is not None and session_id != expected_session_id:
                continue

            meta_key = f"session:{session_id}:refresh_meta"
            last_ts_raw = redis_client.hget(meta_key, "last_refresh_ts")
            last_ts = float(last_ts_raw) if last_ts_raw is not None else None
            events_since = int(redis_client.hincrby(meta_key, "events_since_refresh", 1))
            now = now_fn()

            if should_refresh(last_ts, events_since, now):
                # Timed here, INSIDE this function, not by the caller: the
                # consumer.poll(0.5) loop above can block for up to `timeout`
                # waiting for a message, so timing this call from outside
                # would report "Kafka wait + refresh compute" as if it were
                # refresh compute. Added for eval/8_2's H3 latency hop
                # (stage 15C); nothing else about this branch changes.
                refresh_started = time.monotonic()
                result = refresh_recommendations(session_id, redis_client, pg_conn, milvus_collection, http_client, current_event=event)
                refresh_compute_seconds = time.monotonic() - refresh_started
                did_real_work = result["action"] != "skip_insufficient_data"
                if did_real_work:
                    # Debounce budget is spent protecting the expensive
                    # paths (Milvus search, context_builder HTTP calls)
                    # from being hammered -- the insufficient-data guard
                    # does neither, so it doesn't reset the counters; the
                    # next event re-evaluates should_refresh() against the
                    # same accumulated state instead of a falsely-reset one.
                    redis_client.hset(meta_key, mapping={"last_refresh_ts": now, "events_since_refresh": 0})
                result.update(
                    {
                        "session_id": session_id,
                        "refreshed": did_real_work,
                        "refresh_compute_seconds": refresh_compute_seconds,
                    }
                )
                return result
            return {"session_id": session_id, "refreshed": False, "events_since_refresh": events_since}
        return None
    finally:
        if owns_consumer:
            consumer.close()
