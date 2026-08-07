"""
Stage 5 results: score distribution across the full 411-seed batch, plus
one fully-detailed worked example showing the ranking formula's boosts in
action (raw similarity vs. genre-sibling vs. same-artist contributions).

Writes:
  - reports/stage5/score_distribution.png
  - reports/stage5/results.json  (distribution stats + worked example, for
    the results report)

Requires: docker compose stack up, and api/main.py already running
(default http://localhost:8010) — pass --api-base-url to override.
"""
import argparse
import json
import os
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import httpx
import matplotlib.pyplot as plt
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from recommender import context_builder, ranking  # noqa: E402

load_dotenv(ROOT / ".env")

OUT_DIR = ROOT / "reports" / "stage5"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PALETTE_LIGHT_BLUE = "#2a78d6"

WORKED_EXAMPLE_SEED_TRACK_ID = 1


def fetch_score_distribution(conn) -> list[float]:
    with conn.cursor() as cur:
        cur.execute("SELECT score FROM recommendations ORDER BY score")
        return [row[0] for row in cur.fetchall()]


def build_worked_example(client: httpx.Client, seed_track_id: int, top_k: int = 5) -> dict:
    ctx = context_builder.build_context(client, seed_track_id, candidate_k=25)
    ranked = ranking.score_recommendations(
        seed_track_id, ctx.similar, ctx.genre_siblings, ctx.same_artist, top_k=top_k
    )

    similar_by_id = {c["track_id"]: c["score"] for c in ctx.similar}
    genre_ids = {c["track_id"] for c in ctx.genre_siblings}
    artist_ids = {c["track_id"] for c in ctx.same_artist}

    breakdown = []
    for r in ranked:
        tid = r["track_id"]
        breakdown.append(
            {
                "track_id": tid,
                "title": r["title"],
                "raw_similarity": round(similar_by_id.get(tid, 0.0), 4),
                "genre_boost_applied": tid in genre_ids,
                "artist_boost_applied": tid in artist_ids,
                "final_score": round(r["score"], 4),
            }
        )

    return {
        "seed_track_id": seed_track_id,
        "seed_title": ctx.seed_title,
        "seed_artist_name": ctx.seed_artist_name,
        "recommendations": breakdown,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-base-url", default=os.environ.get("SEMANTIC_API_BASE_URL", "http://localhost:8010"))
    args = parser.parse_args()

    conn = psycopg2.connect(
        host="localhost",
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    scores = fetch_score_distribution(conn)
    print(f"Pulled {len(scores)} recommendation scores.")

    with httpx.Client(base_url=args.api_base_url, timeout=10.0) as client:
        worked_example = build_worked_example(client, WORKED_EXAMPLE_SEED_TRACK_ID)

    n = len(scores)
    mean = sum(scores) / n
    result = {
        "score_distribution": {
            "count": n,
            "min": round(min(scores), 4),
            "max": round(max(scores), 4),
            "mean": round(mean, 4),
            "scores": [round(s, 4) for s in scores],
        },
        "worked_example": worked_example,
    }
    with open(OUT_DIR / "results.json", "w") as f:
        json.dump(result, f, indent=2)
    print(f"Wrote {OUT_DIR / 'results.json'}")

    fig, ax = plt.subplots(figsize=(8, 5), dpi=150)
    fig.patch.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")
    ax.hist(scores, bins=30, color=PALETTE_LIGHT_BLUE, edgecolor="#fcfcfb", linewidth=0.5)
    ax.set_title(f"Recommendation score distribution (n={n}, all 411 seeds × top_k=10)", fontsize=11)
    ax.set_xlabel("final score (similarity + boosts)")
    ax.set_ylabel("count")
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "score_distribution.png")
    print(f"Wrote {OUT_DIR / 'score_distribution.png'}")


if __name__ == "__main__":
    main()
