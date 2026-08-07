"""
Stage 3 results: 2D projection of the 411 CLAP audio embeddings.

Pulls all embeddings from Milvus + genre/artist/title from Postgres,
computes a t-SNE projection to 2D, and writes:
  - reports/stage3/embedding_tsne.png       (static plot)
  - reports/stage3/embedding_projection.json (x, y, genre, title, artist
    per track — for the interactive chart in the results report)

Primary genre per track = the first of its top-3-by-frequency genre tags
present on that track, else "Other" (kept to 3 hue slots + neutral gray,
per the dataviz skill's all-pairs scatter cap).
"""
import json
import os
from collections import Counter
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import psycopg2
from dotenv import load_dotenv
from pymilvus import Collection, connections
from sklearn.manifold import TSNE

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

OUT_DIR = ROOT / "reports" / "stage3"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PALETTE_LIGHT = {
    "top1": "#2a78d6",
    "top2": "#eb6834",
    "top3": "#1baf7a",
    "other": "#9a9990",
}


def main() -> None:
    conn = psycopg2.connect(
        host="localhost",
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    with conn.cursor() as cur:
        cur.execute("SELECT id, title, artist_name, genre_tags FROM tracks ORDER BY id")
        rows = cur.fetchall()
    track_meta = {
        r[0]: {"title": r[1], "artist_name": r[2], "genre_tags": r[3] or []} for r in rows
    }

    genre_counts = Counter(g for m in track_meta.values() for g in m["genre_tags"])
    top3_genres = [g for g, _ in genre_counts.most_common(3)]
    print(f"Top 3 genres by frequency: {top3_genres}")

    connections.connect(host="localhost", port=os.environ.get("MILVUS_PORT", "19530"))
    collection = Collection("track_embeddings")
    collection.load()

    track_ids = list(track_meta.keys())
    results = collection.query(
        expr=f"track_id in {track_ids}", output_fields=["track_id", "embedding"]
    )
    results.sort(key=lambda r: r["track_id"])
    ids = [r["track_id"] for r in results]
    embeddings = [r["embedding"] for r in results]
    print(f"Pulled {len(embeddings)} embeddings, dim={len(embeddings[0])}")

    tsne = TSNE(n_components=2, random_state=42, perplexity=30, init="pca")
    coords = tsne.fit_transform(__import__("numpy").array(embeddings))

    def primary_genre(track_id: int) -> str:
        tags = track_meta[track_id]["genre_tags"]
        for g in top3_genres:
            if g in tags:
                return g
        return "Other"

    points = []
    for i, tid in enumerate(ids):
        m = track_meta[tid]
        points.append(
            {
                "track_id": tid,
                "title": m["title"],
                "artist_name": m["artist_name"],
                "genre": primary_genre(tid),
                "x": round(float(coords[i][0]), 3),
                "y": round(float(coords[i][1]), 3),
            }
        )

    with open(OUT_DIR / "embedding_projection.json", "w") as f:
        json.dump({"top3_genres": top3_genres, "points": points}, f, indent=2)
    print(f"Wrote {OUT_DIR / 'embedding_projection.json'}")

    fig, ax = plt.subplots(figsize=(9, 7), dpi=150)
    fig.patch.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")
    for genre in [*top3_genres, "Other"]:
        color_key = {top3_genres[0]: "top1", top3_genres[1]: "top2", top3_genres[2]: "top3"}.get(
            genre, "other"
        )
        gp = [p for p in points if p["genre"] == genre]
        ax.scatter(
            [p["x"] for p in gp],
            [p["y"] for p in gp],
            c=PALETTE_LIGHT[color_key],
            s=28,
            alpha=0.85 if genre != "Other" else 0.5,
            linewidths=0,
            label=f"{genre} (n={len(gp)})",
        )
    ax.set_title("CLAP embedding space (t-SNE projection), colored by genre", fontsize=12)
    ax.set_xlabel("t-SNE dim 1")
    ax.set_ylabel("t-SNE dim 2")
    ax.legend(frameon=False, loc="best", fontsize=9)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "embedding_tsne.png")
    print(f"Wrote {OUT_DIR / 'embedding_tsne.png'}")


if __name__ == "__main__":
    main()
