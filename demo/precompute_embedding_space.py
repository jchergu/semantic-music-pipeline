"""
Demo-only precompute: projects all 411 track CLAP embeddings down to 2D via
PCA, for the dashboards' scatter-plot visualization. Not part of the
pipeline itself -- purely a rendering aid so the dashboards can show "the
shared semantic space" (Figure 3.1's concept, made concrete) as an actual
picture instead of a static illustration, using this thesis's own real
embeddings, not synthetic points.

PCA (not t-SNE/UMAP) deliberately: it's a fixed, deterministic linear
projection, cheap enough to recompute from scratch every run, and needs no
extra dependency beyond numpy (sklearn happens to be present in this venv
too, used here for convenience, not because a non-linear embedding was
required).

Run once (needs the stack up):
    platform/enrichment/.venv/bin/python demo/precompute_embedding_space.py

Writes demo/data/embedding_space.json:
    {"tracks": [{"id", "title", "artist_name", "genre_tags", "x", "y"}, ...]}
x/y are min-max normalized to [0, 1] so the frontend never has to know the
actual PCA scale.
"""
import json
import os
import sys
from pathlib import Path

import numpy as np
import psycopg2
from dotenv import load_dotenv
from pymilvus import Collection, connections
from sklearn.decomposition import PCA

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

MILVUS_ALIAS = "demo-projection"
MILVUS_COLLECTION = "track_embeddings"
OUT_PATH = Path(__file__).resolve().parent / "data" / "embedding_space.json"

PG_DSN = (
    f"host=localhost port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)


def main() -> None:
    connections.connect(alias=MILVUS_ALIAS, host="localhost", port="19530")
    collection = Collection(MILVUS_COLLECTION, using=MILVUS_ALIAS)
    collection.load()
    rows = collection.query(expr="track_id >= 0", output_fields=["track_id", "embedding"], limit=1000)
    connections.disconnect(alias=MILVUS_ALIAS)

    ids = [r["track_id"] for r in rows]
    matrix = np.array([r["embedding"] for r in rows], dtype=np.float64)

    pca = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(matrix)
    x_min, x_max = coords[:, 0].min(), coords[:, 0].max()
    y_min, y_max = coords[:, 1].min(), coords[:, 1].max()
    coords[:, 0] = (coords[:, 0] - x_min) / (x_max - x_min)
    coords[:, 1] = (coords[:, 1] - y_min) / (y_max - y_min)

    pg_conn = psycopg2.connect(PG_DSN)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT id, title, artist_name, genre_tags FROM tracks WHERE id = ANY(%s)", (ids,))
        meta = {row[0]: {"title": row[1], "artist_name": row[2], "genre_tags": row[3]} for row in cur.fetchall()}
    pg_conn.close()

    tracks = []
    for track_id, (x, y) in zip(ids, coords):
        m = meta.get(track_id)
        if m is None:
            continue
        tracks.append({
            "id": track_id, "title": m["title"], "artist_name": m["artist_name"],
            "genre_tags": m["genre_tags"], "x": round(float(x), 4), "y": round(float(y), 4),
        })

    OUT_PATH.parent.mkdir(exist_ok=True)
    OUT_PATH.write_text(json.dumps({"tracks": tracks, "explained_variance": pca.explained_variance_ratio_.tolist()}))
    print(f"Wrote {len(tracks)} projected tracks to {OUT_PATH}")
    print(f"Explained variance (2 components): {sum(pca.explained_variance_ratio_):.1%}")


if __name__ == "__main__":
    main()
