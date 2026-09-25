"""
Demo-only HTTP wrapper around 8.1's real recommendation path, for the
dashboard (demo/uc81.html) to call live. NOT a change to the Semantic API's
frozen contract (contracts/semantic-api-v1.json) and not a new stage --
this calls exactly the same two functions recommend.py's run() calls per
seed (context_builder.build_context() then ranking.score_recommendations()),
just exposed over one endpoint instead of run as a batch over all 411
tracks and written to Postgres. No new computation, no write path.

CORS is enabled and permissive (Origin: *) because this exists only to be
opened from a local static HTML file during thesis demos -- it is not
meant to run anywhere else.

Run (needs the Semantic API up):
    platform/enrichment/.venv/bin/python -m uvicorn demo_api:app \
        --app-dir usecases/8_1_batch_reactive --port 8010
"""
import sys
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent.parent / "platform"))

from recommender import context_builder  # noqa: E402
from scoring import ranking  # noqa: E402

DEFAULT_SEMANTIC_API_URL = "http://127.0.0.1:8000"
DEFAULT_TOP_K = 10
DEFAULT_CANDIDATE_K = 25

app = FastAPI(title="8.1 Demo API")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"],
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/recommend/{track_id}")
def recommend(track_id: int) -> dict:
    with httpx.Client(base_url=DEFAULT_SEMANTIC_API_URL, timeout=30.0) as http_client:
        try:
            ctx = context_builder.build_context(http_client, track_id, candidate_k=DEFAULT_CANDIDATE_K)
        except context_builder.SeedTrackNotFoundError:
            raise HTTPException(status_code=404, detail=f"track {track_id} not found")
        ranked = ranking.score_recommendations(
            track_id, ctx.similar, ctx.genre_siblings, ctx.same_artist, top_k=DEFAULT_TOP_K
        )
    return {
        "seed": {"track_id": track_id, "title": ctx.seed_title, "artist_name": ctx.seed_artist_name},
        "recommendations": ranked,
    }
