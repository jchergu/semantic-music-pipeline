"""
Stage 16: Session API — recommendation delivery for the streaming use cases.

A separate service from the Semantic API, per Decision E (CLAUDE.md), and
the mirror image of Decision A: behavioral events flow IN through their own
small service, so recommendations flow OUT through one. Three grounds, all
from that decision — `contracts/semantic-api-v1.json` is frozen and adding
endpoints would force a contract revision that buys nothing; the Semantic
API has no Redis dependency and session recommendations are per-session
derived state, not semantic content; and this is platform-owned rather than
8.2-owned because 8.3 will read session state the same way (Decision B's
reasoning, applied to delivery).

**Read-only, and it computes nothing.** Producing recommendations is
`streaming/recommendation_refresh.py`'s job, run continuously by
`streaming/refresh_daemon.py`. This service only serves what that daemon
has already written to `session:{id}:recs`, plus the session profile stage
13's Flink job writes to `session:{id}:profile`. If a session has no
recommendations, the honest answer is to say so — never to compute one on
the read path, which would put a Milvus search and two HTTP calls behind a
GET and duplicate the debounce the daemon exists to enforce.

**Redis is its only dependency.** No Postgres, Milvus or Neo4j: the rows
`recommendation_refresh` stores are already self-contained (`track_id`,
`title`, `artist_name`, `similarity`, `genre_sibling`, `same_artist`,
`score`), so nothing needs enriching at read time.

**Request/response, not WebSocket**, per Decision E — deliberately, and
against what `thesis/06-streaming-use-cases.md` §6.1 currently says (that
line is to be rewritten in a thesis session, not honoured here). 8.2 vs 8.3
is *explicit query vs system-inferred suggestion*, not pull vs push; a
pushed recommendation is behaviourally proactive, so building push for 8.2
would blur the exact distinction the use-case taxonomy rests on.

Run: uvicorn session_api.main:app --app-dir platform --reload
     (from the repo root)
"""
import json
from contextlib import asynccontextmanager
from typing import Optional

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from streaming.config import REDIS_HOST, REDIS_PORT
from streaming.session_state import (
    events_key, profile_key, profile_meta_key, recs_key, refresh_meta_key,
)

_client: redis.Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client
    # redis-py is connection-pool backed, so one client serves every
    # request; no equivalent of semantic_api's ThreadedConnectionPool is
    # needed, and there is only one store to hold open.
    _client = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)
    yield
    _client.close()
    _client = None


app = FastAPI(title="Session API", lifespan=lifespan)


class Recommendation(BaseModel):
    track_id: int
    title: str
    # None is reachable: ranking.py's _touch() creates a candidate from a
    # genre-sibling or same-artist hit that the Milvus search never
    # returned, and those carry no artist name.
    artist_name: Optional[str] = None
    similarity: float
    genre_sibling: bool
    same_artist: bool
    score: float


class Recommendations(BaseModel):
    session_id: str
    count: int
    recommendations: list[Recommendation]


class SessionProfile(BaseModel):
    session_id: str
    dimension: int
    vector: list[float]
    computed_at: Optional[float] = None
    n_events: Optional[int] = None


class RawEventState(BaseModel):
    present: bool
    count: int
    ttl_seconds: Optional[int] = None


class ProfileState(BaseModel):
    present: bool
    dimension: Optional[int] = None
    computed_at: Optional[float] = None
    n_events: Optional[int] = None


class RecommendationState(BaseModel):
    present: bool
    count: int


class SessionStatus(BaseModel):
    session_id: str
    raw_events: RawEventState
    profile: ProfileState
    recommendations: RecommendationState
    raw_state_expired: bool
    notes: list[str]


def _read_profile_meta(session_id: str) -> dict:
    meta = _client.hgetall(profile_meta_key(session_id))
    return {
        "computed_at": float(meta["computed_at"]) if meta.get("computed_at") else None,
        "n_events": int(meta["n_events"]) if meta.get("n_events") else None,
    }


def _missing(session_id: str, what: str) -> HTTPException:
    """404, distinguishing "no such session" from "this session exists but
    that has not been produced for it yet".

    Worth the extra Redis read: to a client those two are completely
    different situations — a wrong session id versus a session whose first
    events have not yet cleared the refresh debounce — and collapsing them
    into one 404 would make the second look like a bug.

    Keyed off ANY of the session's keys, not the raw event list alone
    (fixed in stage 15D). Deciding it from session:{id}:events made this
    function collapse the two cases in exactly the situation it exists to
    separate: once the raw list expired, a session that still had live
    recommendations or a live profile was reported as `unknown session`.
    """
    known = _client.exists(
        events_key(session_id), profile_key(session_id),
        profile_meta_key(session_id), recs_key(session_id),
        refresh_meta_key(session_id),
    ) > 0
    detail = (
        f"session {session_id!r} exists but has no {what} yet"
        if known else f"unknown session {session_id!r}"
    )
    return HTTPException(status_code=404, detail=detail)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/sessions/{session_id}", response_model=SessionStatus)
def get_session(session_id: str) -> SessionStatus:
    """What this service can currently see for a session, across all three
    keys. Exists so a client (and stage 15D's failure injection) can tell
    *why* a recommendation is missing rather than only that it is."""
    raw_count = _client.llen(events_key(session_id))
    raw_ttl = _client.ttl(events_key(session_id))
    profile_raw = _client.get(profile_key(session_id))
    recs_raw = _client.get(recs_key(session_id))

    if raw_count == 0 and profile_raw is None and recs_raw is None:
        raise HTTPException(status_code=404, detail=f"unknown session {session_id!r}")

    profile_vector = json.loads(profile_raw) if profile_raw else None
    recs = json.loads(recs_raw) if recs_raw else []
    meta = _read_profile_meta(session_id)

    # session:{id}:events carries a 30-minute sliding TTL (stage 9), but
    # nothing ever expires session:{id}:profile, :profile_meta or :recs --
    # stage 13's redis.set and stage 14's _write_recs set no TTL. So this
    # service can serve recommendations for a session whose raw state
    # expired hours ago, and the derived keyspace grows without bound.
    # Surfaced here rather than patched: fixing it changes stages 13 and 14,
    # and stage 15D's failure injection is the place to decide what a client
    # should see when session state disappears underneath it.
    derived_present = profile_raw is not None or recs_raw is not None
    raw_state_expired = derived_present and raw_count == 0

    notes = []
    if raw_state_expired:
        notes.append(
            "Derived state (profile/recs) is present but the raw event list is gone: "
            "session:{id}:events has a 30-minute sliding TTL while the derived keys have none. "
            "Known stage 16 finding, deferred to stage 15D."
        )
    if recs_raw is None and raw_count > 0:
        notes.append(
            "No recommendations yet. Expected for a session's first event or two "
            "(refresh_recommendations needs at least 2 raw events, then the debounce applies); "
            "persistent across many sessions it would instead mean refresh_daemon.py is not running."
        )
    if profile_raw is None and raw_count > 0:
        notes.append(
            "No session profile yet: stage 13's Flink job has not fired a window for this session, "
            "so any recommendations above came from the cold-start fallback path."
        )

    return SessionStatus(
        session_id=session_id,
        raw_events=RawEventState(
            present=raw_count > 0,
            count=raw_count,
            # redis TTL: -2 = no such key, -1 = key exists with no expiry.
            # Both are reported as None rather than leaked as magic numbers.
            ttl_seconds=raw_ttl if raw_ttl >= 0 else None,
        ),
        profile=ProfileState(
            present=profile_raw is not None,
            dimension=len(profile_vector) if profile_vector else None,
            **meta,
        ),
        recommendations=RecommendationState(present=recs_raw is not None, count=len(recs)),
        raw_state_expired=raw_state_expired,
        notes=notes,
    )


@app.get("/sessions/{session_id}/recommendations", response_model=Recommendations)
def get_recommendations(session_id: str) -> Recommendations:
    """The session's current ranked recommendations, exactly as
    refresh_daemon.py last wrote them. Nothing is recomputed here."""
    raw = _client.get(recs_key(session_id))
    if raw is None:
        raise _missing(session_id, "recommendations")
    rows = json.loads(raw)
    return Recommendations(
        session_id=session_id,
        count=len(rows),
        recommendations=[Recommendation(**row) for row in rows],
    )


@app.get("/sessions/{session_id}/profile", response_model=SessionProfile)
def get_profile(session_id: str) -> SessionProfile:
    """The session's weighted, recency-decayed CLAP centroid, as stage 13's
    Flink job last computed it, with the provenance from its sibling
    profile_meta key."""
    raw = _client.get(profile_key(session_id))
    if raw is None:
        raise _missing(session_id, "profile")
    vector = json.loads(raw)
    return SessionProfile(
        session_id=session_id, dimension=len(vector), vector=vector, **_read_profile_meta(session_id)
    )
