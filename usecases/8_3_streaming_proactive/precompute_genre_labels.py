"""
8.3 prep: one-off script computing a CLAP TEXT embedding for every genre tag
in the catalog (the same 77 tags stage 3 wrote to Neo4j/Postgres), so the
live classifier (live_classifier.py) can do CLAP zero-shot classification --
cosine similarity between a track's already-computed AUDIO embedding and
each label's TEXT embedding -- entirely offline at serving time. This is
what "auto-tagging reused as live classifier" (thesis §6.2) means in code:
the same CLAP model stage 3 already uses for audio embeddings, applied to
its text tower against a currently-playing track, with no write path to
Neo4j at all.

Run once (needs platform/enrichment/.venv's laion_clap install; downloads
the pretrained checkpoint on first run, same as enrich.py):
    platform/enrichment/.venv/bin/python \
        usecases/8_3_streaming_proactive/precompute_genre_labels.py

Writes genre_label_embeddings.json next to this file: {tag: [512 floats]}.
Re-run only if the genre vocabulary changes -- it hasn't since stage 3.
"""
import json
import os
from pathlib import Path

import laion_clap
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / ".env")

OUT_PATH = Path(__file__).resolve().parent / "genre_label_embeddings.json"

PG_DSN = (
    f"host=localhost port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)


def fetch_genre_vocabulary() -> list[str]:
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT unnest(genre_tags) AS g FROM tracks ORDER BY g")
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def main() -> None:
    tags = fetch_genre_vocabulary()
    print(f"Embedding {len(tags)} genre labels via CLAP's text tower...")

    model = laion_clap.CLAP_Module(enable_fusion=False)
    model.load_ckpt()

    prompts = [f"a {tag} music track" for tag in tags]
    embeddings = model.get_text_embedding(prompts, use_tensor=False)

    out = {tag: emb.tolist() for tag, emb in zip(tags, embeddings)}
    OUT_PATH.write_text(json.dumps(out))
    print(f"Wrote {len(out)} label embeddings to {OUT_PATH}")


if __name__ == "__main__":
    main()
