"""
Stage 5 (8.1): context builder.

For one seed track, gathers three raw candidate sources entirely over HTTP
from the stage 4 Semantic API — never touches Postgres/Milvus/Neo4j
directly. This is the module that owns the "don't bypass the Semantic API"
rule for anything relating to a track's *content*.

Sources:
  - similar:        GET /tracks/{id}/similar?k=candidate_k   (Milvus cosine)
  - genre_siblings:  GET /tracks/{id}/graph -> related_by_genre
                     (Neo4j, capped at 10 by the API itself)
  - same_artist:     GET /tracks/{id}/graph -> artist, then
                     GET /artists/{artist}/tracks, seed excluded

ranking.py does the merging/scoring; this module only fetches and reshapes.
`candidate_k` is deliberately wider than the final top_k a caller will ask
ranking.py for, so a same-artist/genre-sibling track just outside the raw
similarity top-10 can still be boosted back into the final result.
"""
from dataclasses import dataclass, field
from urllib.parse import quote

import httpx


class SeedTrackNotFoundError(Exception):
    def __init__(self, track_id: int):
        super().__init__(f"seed track {track_id} not found")
        self.track_id = track_id


@dataclass
class TrackContext:
    seed_track_id: int
    seed_title: str
    seed_artist_name: str
    similar: list[dict] = field(default_factory=list)
    genre_siblings: list[dict] = field(default_factory=list)
    same_artist: list[dict] = field(default_factory=list)


def build_context(client: httpx.Client, seed_track_id: int, *, candidate_k: int = 25) -> TrackContext:
    seed_resp = client.get(f"/tracks/{seed_track_id}")
    if seed_resp.status_code == 404:
        raise SeedTrackNotFoundError(seed_track_id)
    seed_resp.raise_for_status()
    seed = seed_resp.json()

    similar_resp = client.get(f"/tracks/{seed_track_id}/similar", params={"k": candidate_k})
    similar_resp.raise_for_status()
    similar = similar_resp.json()

    graph_resp = client.get(f"/tracks/{seed_track_id}/graph")
    graph_resp.raise_for_status()
    graph = graph_resp.json()
    genre_siblings = graph["related_by_genre"]

    same_artist: list[dict] = []
    artist = graph.get("artist")
    if artist:
        artist_resp = client.get(f"/artists/{quote(artist, safe='')}/tracks")
        artist_resp.raise_for_status()
        same_artist = [t for t in artist_resp.json() if t["track_id"] != seed_track_id]

    return TrackContext(
        seed_track_id=seed_track_id,
        seed_title=seed["title"],
        seed_artist_name=seed["artist_name"],
        similar=similar,
        genre_siblings=genre_siblings,
        same_artist=same_artist,
    )
