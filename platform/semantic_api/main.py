"""
Stage 4 (8.1): Semantic API.

One shared FastAPI service over the L1/L2 stores (Postgres metadata, Milvus
CLAP embeddings, Neo4j Track/Artist/Genre graph). Reused later by the
Recommender Engine (stage 5) and, eventually, sibling 8.2/8.3 consumers
(Similarity Search, Auto-tagging, Playlist Generation) — this module has no
knowledge of any of those, it just exposes the shared reads they'll all need.

Run: uvicorn semantic_api.main:app --app-dir platform --reload
     (from the repo root, after `bash platform/semantic_api/install.sh`)
"""
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pymilvus import Collection

from semantic_api import dependencies as deps


@asynccontextmanager
async def lifespan(app: FastAPI):
    deps.connect_all()
    yield
    deps.disconnect_all()


app = FastAPI(title="Semantic API", lifespan=lifespan)


class Track(BaseModel):
    id: int
    jamendo_id: str
    title: str
    artist_name: str
    duration_sec: Optional[int]
    genre_tags: list[str]
    musicbrainz_recording_id: Optional[str]
    enriched: bool


class SimilarTrack(BaseModel):
    track_id: int
    title: str
    artist_name: str
    score: float


class RelatedTrack(BaseModel):
    track_id: int
    title: str


class TrackGraph(BaseModel):
    track_id: int
    artist: Optional[str]
    genres: list[str]
    related_by_genre: list[RelatedTrack]


class ArtistOrGenreTrack(BaseModel):
    track_id: int
    title: str


def _fetch_track_row(conn, track_id: int) -> Track:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, jamendo_id, title, artist_name, duration_sec, genre_tags,
                   musicbrainz_recording_id, enriched_at
            FROM tracks WHERE id = %s
            """,
            (track_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"track {track_id} not found")
    return Track(
        id=row[0],
        jamendo_id=row[1],
        title=row[2],
        artist_name=row[3],
        duration_sec=row[4],
        genre_tags=row[5] or [],
        musicbrainz_recording_id=row[6],
        enriched=row[7] is not None,
    )


@app.get("/health")
def health() -> JSONResponse:
    status = {"postgres": False, "milvus": False, "neo4j": False}

    try:
        conn = deps._pg_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            status["postgres"] = True
        finally:
            deps._pg_pool.putconn(conn)
    except Exception:
        pass

    try:
        deps._milvus_collection.num_entities
        status["milvus"] = True
    except Exception:
        pass

    try:
        with deps._neo4j_driver.session() as session:
            session.run("RETURN 1").consume()
        status["neo4j"] = True
    except Exception:
        pass

    ok = all(status.values())
    return JSONResponse(
        status_code=200 if ok else 503,
        content={"status": "ok" if ok else "degraded", "services": status},
    )


@app.get("/tracks/{track_id}", response_model=Track)
def get_track(track_id: int, conn=Depends(deps.get_pg_conn)) -> Track:
    return _fetch_track_row(conn, track_id)


@app.get("/tracks/{track_id}/similar", response_model=list[SimilarTrack])
def get_similar_tracks(
    track_id: int,
    k: int = 10,
    conn=Depends(deps.get_pg_conn),
    collection: Collection = Depends(deps.get_milvus_collection),
) -> list[SimilarTrack]:
    _fetch_track_row(conn, track_id)  # 404s if the track doesn't exist

    query_result = collection.query(expr=f"track_id == {track_id}", output_fields=["embedding"])
    if not query_result:
        raise HTTPException(status_code=404, detail=f"no embedding for track {track_id}")
    embedding = query_result[0]["embedding"]

    search_result = collection.search(
        data=[embedding],
        anns_field="embedding",
        param={"metric_type": "COSINE", "params": {"nprobe": 16}},
        limit=k,
        expr=f"track_id != {track_id}",
        output_fields=["track_id"],
    )
    hits = [(hit.entity.get("track_id"), hit.distance) for hit in search_result[0]]
    if not hits:
        return []

    ids = [tid for tid, _ in hits]
    with conn.cursor() as cur:
        cur.execute("SELECT id, title, artist_name FROM tracks WHERE id = ANY(%s)", (ids,))
        rows = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    return [
        SimilarTrack(track_id=tid, title=rows[tid][0], artist_name=rows[tid][1], score=score)
        for tid, score in hits
        if tid in rows
    ]


@app.get("/tracks/{track_id}/graph", response_model=TrackGraph)
def get_track_graph(
    track_id: int, conn=Depends(deps.get_pg_conn), session=Depends(deps.get_neo4j_session)
) -> TrackGraph:
    _fetch_track_row(conn, track_id)  # 404s if the track doesn't exist

    result = session.run(
        """
        MATCH (t:Track {track_id: $id})
        OPTIONAL MATCH (a:Artist)-[:PERFORMED]->(t)
        OPTIONAL MATCH (t)-[:HAS_GENRE]->(g:Genre)
        RETURN a.name AS artist, collect(DISTINCT g.name) AS genres
        """,
        id=track_id,
    ).single()

    related = session.run(
        """
        MATCH (t:Track {track_id: $id})-[:HAS_GENRE]->(:Genre)<-[:HAS_GENRE]-(other:Track)
        WHERE other.track_id <> $id
        WITH other, count(*) AS shared_genres
        RETURN other.track_id AS track_id, other.title AS title
        ORDER BY shared_genres DESC, other.track_id ASC
        LIMIT 10
        """,
        id=track_id,
    ).data()

    return TrackGraph(
        track_id=track_id,
        artist=result["artist"] if result else None,
        genres=result["genres"] if result else [],
        related_by_genre=[RelatedTrack(**r) for r in related],
    )


@app.get("/artists/{name}/tracks", response_model=list[ArtistOrGenreTrack])
def get_artist_tracks(name: str, session=Depends(deps.get_neo4j_session)) -> list[ArtistOrGenreTrack]:
    rows = session.run(
        "MATCH (a:Artist {name: $name})-[:PERFORMED]->(t:Track) "
        "RETURN t.track_id AS track_id, t.title AS title",
        name=name,
    ).data()
    return [ArtistOrGenreTrack(**r) for r in rows]


@app.get("/genres/{name}/tracks", response_model=list[ArtistOrGenreTrack])
def get_genre_tracks(name: str, session=Depends(deps.get_neo4j_session)) -> list[ArtistOrGenreTrack]:
    rows = session.run(
        "MATCH (g:Genre {name: $name})<-[:HAS_GENRE]-(t:Track) "
        "RETURN t.track_id AS track_id, t.title AS title",
        name=name,
    ).data()
    return [ArtistOrGenreTrack(**r) for r in rows]
