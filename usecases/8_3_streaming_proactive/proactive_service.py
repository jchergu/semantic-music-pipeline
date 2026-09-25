"""
8.3: proactive suggestion service -- design-and-feasibility per
thesis §6.2, given a minimal real vertical slice for the thesis demo.

Consumes `media-stream` (its first real producer/consumer pair anywhere in
this repo -- see media_stream_producer.py) instead of `behavioral-events`:
each message is a "now playing" event, `{session_id, track_id, event_time}`,
not a completed/skipped/liked interaction. For each one:

  1. Live classification -- CLAP zero-shot: cosine similarity between the
     track's already-computed AUDIO embedding (Redis `track:{id}:embedding`,
     stage 13's preload) and the 77 genre-label TEXT embeddings
     (precompute_genre_labels.py). No live write to Neo4j anywhere in this
     module -- the classifier only reads Redis and returns labels.
  2. Proactive suggestion -- reuses `context_builder.build_context()` +
     `platform/scoring/ranking.py::score_recommendations()` EXACTLY as
     8.1's `recommend.py` and 8.2's `recommendation_refresh.py` cold-start
     path do (Decision D): the currently-playing track stands in for the
     "seed", and the top-ranked candidate is the suggestion. This is the
     same three-signal scoring (similarity + genre-sibling + same-artist)
     8.1/8.2 already use -- 8.3 changes *when* it runs and *how the result
     is delivered*, not what it computes.
  3. Push, not pull (Decision E): the result is written to
     session:{id}:now_playing / :live_tags / :proactive_suggestion
     (platform/streaming/session_state.py) AND fanned out over WebSocket to
     any client connected to /ws/{session_id} -- the one place in this repo
     that pushes rather than serves on request, which is exactly the
     reactive/proactive distinction the use-case taxonomy rests on.

Own Kafka consumer group (UC83_PROACTIVE_GROUP_ID), independent of
SESSION_CONSUMER_GROUP_ID and RECS_REFRESH_GROUP_ID -- those read
behavioral-events; this reads media-stream, a different topic entirely.
Commits offsets like the stage 16 daemons (this is also a restartable,
canonical-group-id consumer, not a bounded test read) -- see
platform/streaming/daemon_runtime.py's docstring for why that's the
platform's convention for a consumer in this position.

Run (needs the Semantic API up, media-stream to have a producer -- see
media_stream_producer.py -- and preload_embeddings_to_redis.py already run
for stage 13):
    platform/enrichment/.venv/bin/python -m uvicorn proactive_service:app \
        --app-dir usecases/8_3_streaming_proactive --port 8040
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import redis
from confluent_kafka import Consumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "platform"))
sys.path.insert(0, str(ROOT / "usecases" / "8_1_batch_reactive"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from recommender import context_builder  # noqa: E402
from scoring import ranking  # noqa: E402
from streaming.config import BOOTSTRAP_SERVERS, DERIVED_TTL_SECONDS, REDIS_HOST, REDIS_PORT, TOPIC_MEDIA_STREAM  # noqa: E402
from streaming.daemon_runtime import configure_logging  # noqa: E402
from streaming.producer import produce  # noqa: E402
from streaming.session_state import (  # noqa: E402
    live_tags_key, now_playing_key, proactive_suggestion_key,
)

from live_classifier import classify  # noqa: E402

log = logging.getLogger("proactive_service")

UC83_PROACTIVE_GROUP_ID = "uc83-proactive-consumer"
DEFAULT_SEMANTIC_API_URL = "http://127.0.0.1:8000"
LABEL_EMBEDDINGS_PATH = Path(__file__).resolve().parent / "genre_label_embeddings.json"
TOP_K_TAGS = 3


def load_label_embeddings() -> dict[str, list[float]]:
    return json.loads(LABEL_EMBEDDINGS_PATH.read_text())


def track_embedding_key(track_id: int) -> str:
    """Matches preload_embeddings_to_redis.py's key convention exactly --
    that script is the sole writer, this the first reader outside stage 13's
    Flink job."""
    return f"track:{track_id}:embedding"


def process_now_playing(
    event: dict,
    redis_client: redis.Redis,
    http_client: httpx.Client,
    label_embeddings: dict[str, list[float]],
) -> dict | None:
    """Pure-ish orchestration (the only I/O is the two calls it's handed):
    classify the currently-playing track and compute its proactive
    suggestion. Returns the payload written to Redis and pushed over the
    socket, or None if the track has no cached embedding (not in the
    preloaded catalog -- reported, not guessed at)."""
    session_id = event["session_id"]
    track_id = int(event["track_id"])

    embedding_raw = redis_client.get(track_embedding_key(track_id))
    if embedding_raw is None:
        log.warning("session=%s track_id=%s has no cached embedding, skipping", session_id, track_id)
        return None
    embedding = json.loads(embedding_raw)

    tags = classify(embedding, label_embeddings, top_k=TOP_K_TAGS)

    ctx = context_builder.build_context(http_client, track_id)
    ranked = ranking.score_recommendations(track_id, ctx.similar, ctx.genre_siblings, ctx.same_artist, top_k=1)
    suggestion = ranked[0] if ranked else None

    now_playing = {
        "track_id": track_id,
        "title": ctx.seed_title,
        "artist_name": ctx.seed_artist_name,
        "event_time": event.get("event_time"),
    }

    redis_client.set(now_playing_key(session_id), json.dumps(now_playing), ex=DERIVED_TTL_SECONDS)
    redis_client.set(live_tags_key(session_id), json.dumps(tags), ex=DERIVED_TTL_SECONDS)
    if suggestion is not None:
        redis_client.set(proactive_suggestion_key(session_id), json.dumps(suggestion), ex=DERIVED_TTL_SECONDS)

    return {"session_id": session_id, "now_playing": now_playing, "live_tags": tags, "proactive_suggestion": suggestion}


def new_media_stream_consumer(group_id: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC_MEDIA_STREAM])
    return consumer


def consume_loop(
    stop: threading.Event,
    loop: asyncio.AbstractEventLoop,
    broadcast,
    semantic_api_url: str = DEFAULT_SEMANTIC_API_URL,
    group_id: str = UC83_PROACTIVE_GROUP_ID,
) -> None:
    """Runs on a background thread (FastAPI's event loop stays free for
    WebSocket I/O). Commits offsets after each processed message -- same
    at-least-once trade the stage 16 daemons make: a crash between the
    Redis write and the commit reprocesses one event harmlessly (the keys
    are overwritten, not accumulated)."""
    redis_client = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)
    label_embeddings = load_label_embeddings()
    consumer = new_media_stream_consumer(group_id)
    log.info("consume_loop started group_id=%s topic=%s", group_id, TOPIC_MEDIA_STREAM)
    try:
        with httpx.Client(base_url=semantic_api_url, timeout=30.0) as http_client:
            while not stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    continue
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError:
                    consumer.commit(asynchronous=False)
                    continue
                if not isinstance(event, dict) or "session_id" not in event or "track_id" not in event:
                    consumer.commit(asynchronous=False)
                    continue
                try:
                    result = process_now_playing(event, redis_client, http_client, label_embeddings)
                except Exception:  # noqa: BLE001
                    log.exception("failed to process now-playing event %r; skipping it", event)
                    consumer.commit(asynchronous=False)
                    continue
                consumer.commit(asynchronous=False)
                if result is not None:
                    asyncio.run_coroutine_threadsafe(broadcast(result["session_id"], result), loop)
    finally:
        consumer.close()


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: dict[str, set[WebSocket]] = {}
        self._lock = threading.Lock()

    async def connect(self, session_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        with self._lock:
            self._connections.setdefault(session_id, set()).add(websocket)

    def disconnect(self, session_id: str, websocket: WebSocket) -> None:
        with self._lock:
            sockets = self._connections.get(session_id)
            if sockets is not None:
                sockets.discard(websocket)

    async def broadcast(self, session_id: str, payload: dict) -> None:
        with self._lock:
            sockets = list(self._connections.get(session_id, ()))
        for ws in sockets:
            try:
                await ws.send_json(payload)
            except Exception:  # noqa: BLE001
                # A dead socket here shouldn't take the broadcast loop down;
                # disconnect() will clean it up on its own receive loop.
                log.warning("failed to push to a websocket for session=%s", session_id)


manager = ConnectionManager()
_consumer_thread: threading.Thread | None = None
_stop_event = threading.Event()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _consumer_thread
    configure_logging()
    loop = asyncio.get_running_loop()
    _stop_event.clear()
    _consumer_thread = threading.Thread(
        target=consume_loop, args=(_stop_event, loop, manager.broadcast), daemon=True
    )
    _consumer_thread.start()
    yield
    _stop_event.set()
    if _consumer_thread is not None:
        _consumer_thread.join(timeout=5.0)


app = FastAPI(title="8.3 Proactive Suggestion Service", lifespan=lifespan)
# Demo-only, same rationale as usecases/8_1_batch_reactive/demo_api.py and
# session_api's CORS addition -- lets demo/uc83.html call this over both
# fetch() and WebSocket from a local static file.
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "POST"], allow_headers=["*"])
_redis_client = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/sessions/{session_id}/now-playing")
def get_now_playing(session_id: str) -> dict:
    """Polling fallback for a client that hasn't opened the WebSocket yet
    (or can't) -- the same three keys the socket pushes, read back."""
    now_playing_raw = _redis_client.get(now_playing_key(session_id))
    live_tags_raw = _redis_client.get(live_tags_key(session_id))
    suggestion_raw = _redis_client.get(proactive_suggestion_key(session_id))
    return {
        "session_id": session_id,
        "now_playing": json.loads(now_playing_raw) if now_playing_raw else None,
        "live_tags": json.loads(live_tags_raw) if live_tags_raw else None,
        "proactive_suggestion": json.loads(suggestion_raw) if suggestion_raw else None,
    }


@app.post("/simulate/{session_id}/{track_id}")
def simulate_now_playing(session_id: str, track_id: int) -> dict:
    """Demo-only convenience: publishes one now-playing event directly onto
    media-stream, so demo/uc83.html's "Play track" button can drive the
    real pipeline without a separate CLI invocation. Identical in effect
    to running media_stream_producer.py for a single track -- this is not
    a second ingestion path, it calls the same streaming.producer.produce()
    that script does."""
    event = {"session_id": session_id, "track_id": str(track_id), "event_time": time.time()}
    produce(TOPIC_MEDIA_STREAM, json.dumps(event), key=session_id)
    return {"status": "published", "event": event}


@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str) -> None:
    await manager.connect(session_id, websocket)
    try:
        while True:
            await websocket.receive_text()  # clients don't send anything; this just detects disconnect
    except WebSocketDisconnect:
        manager.disconnect(session_id, websocket)
