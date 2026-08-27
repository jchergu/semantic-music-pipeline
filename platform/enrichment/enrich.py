"""
Stage 3 (8.1): L2 semantic enrichment.

Batch job: computes a CLAP audio embedding for every track ingested in
stage 2, indexes it in Milvus for similarity search, and writes a
Track/Artist/Genre subgraph into Neo4j. Idempotent — only processes tracks
where tracks.enriched_at IS NULL, and marks each row once its embedding and
graph writes both land.

Usage:
    python enrich.py [--limit N] [--batch-size 8] [--force]
"""
import argparse
import os
import sys
import tempfile
from pathlib import Path

import boto3
import laion_clap
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from neo4j import GraphDatabase
from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
)

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

MINIO_ENDPOINT = f"http://localhost:{os.environ.get('MINIO_API_PORT', '9000')}"

PG_DSN = (
    f"host=localhost port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)

NEO4J_URI = f"bolt://localhost:{os.environ.get('NEO4J_BOLT_PORT', '7687')}"
NEO4J_AUTH = (
    os.environ.get("NEO4J_USER", "neo4j"),
    os.environ.get("NEO4J_PASSWORD", "neo4j_password"),
)

MILVUS_HOST = "localhost"
MILVUS_PORT = os.environ.get("MILVUS_PORT", "19530")
MILVUS_COLLECTION = "track_embeddings"
CLAP_EMBED_DIM = 512

BATCH_SIZE_DEFAULT = 8


def fetch_pending_tracks(conn, limit: int | None, force: bool) -> list[dict]:
    filters = [] if force else ["enriched_at IS NULL"]
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    limit_clause = "LIMIT %s" if limit else ""
    query = f"""
        SELECT id, jamendo_id, title, artist_name, genre_tags,
               minio_bucket, minio_object_key
        FROM tracks
        {where_clause}
        ORDER BY id
        {limit_clause}
    """
    params = (limit,) if limit else None
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, params)
        return cur.fetchall()


def ensure_milvus_collection() -> Collection:
    if utility.has_collection(MILVUS_COLLECTION):
        return Collection(MILVUS_COLLECTION)

    fields = [
        FieldSchema(name="track_id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=CLAP_EMBED_DIM),
    ]
    schema = CollectionSchema(fields, description="CLAP audio embeddings for stage 3 enrichment")
    collection = Collection(MILVUS_COLLECTION, schema)
    collection.create_index(
        field_name="embedding",
        index_params={"index_type": "IVF_FLAT", "metric_type": "COSINE", "params": {"nlist": 128}},
    )
    return collection


def download_track(s3, bucket: str, key: str) -> Path:
    fd, path = tempfile.mkstemp(suffix=".mp3", prefix="enrich_")
    os.close(fd)
    s3.download_file(bucket, key, path)
    return Path(path)


def write_neo4j_track(session, track: dict) -> None:
    session.run(
        """
        MERGE (t:Track {track_id: $track_id})
        SET t.jamendo_id = $jamendo_id, t.title = $title
        MERGE (a:Artist {name: $artist_name})
        MERGE (a)-[:PERFORMED]->(t)
        WITH t
        UNWIND $genres AS genre
        MERGE (g:Genre {name: genre})
        MERGE (t)-[:HAS_GENRE]->(g)
        """,
        track_id=track["id"],
        jamendo_id=track["jamendo_id"],
        title=track["title"],
        artist_name=track["artist_name"],
        genres=track["genre_tags"] or [],
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Max number of tracks to enrich")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT)
    parser.add_argument(
        "--force", action="store_true", help="Re-enrich tracks that already have enriched_at set"
    )
    args = parser.parse_args()

    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute((ROOT / "ingestion" / "schema.sql").read_text())

    tracks = fetch_pending_tracks(conn, limit=args.limit, force=args.force)
    if not tracks:
        print("No tracks pending enrichment.")
        return
    print(f"Enriching {len(tracks)} tracks...")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )

    connections.connect(alias="default", host=MILVUS_HOST, port=MILVUS_PORT)
    collection = ensure_milvus_collection()

    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

    print("Loading CLAP checkpoint (first run downloads the pretrained weights, cached after)...")
    model = laion_clap.CLAP_Module(enable_fusion=False)
    model.load_ckpt()

    enriched = 0
    with driver.session() as neo_session:
        for batch_start in range(0, len(tracks), args.batch_size):
            batch = tracks[batch_start : batch_start + args.batch_size]
            tmp_paths: list[Path] = []
            batch_ok: list[dict] = []
            try:
                for track in batch:
                    try:
                        tmp_paths.append(
                            download_track(s3, track["minio_bucket"], track["minio_object_key"])
                        )
                        batch_ok.append(track)
                    except Exception as e:
                        print(f"SKIP '{track['title']}' (download failed: {e})", file=sys.stderr)

                if not batch_ok:
                    continue

                embeddings = model.get_audio_embedding_from_filelist(
                    x=[str(p) for p in tmp_paths], use_tensor=False
                )

                milvus_ids = [t["id"] for t in batch_ok]
                collection.upsert([milvus_ids, embeddings.tolist()])

                track_ids_done = []
                for track in batch_ok:
                    write_neo4j_track(neo_session, track)
                    track_ids_done.append(track["id"])

                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE tracks SET enriched_at = now() WHERE id = ANY(%s)",
                        (track_ids_done,),
                    )

                enriched += len(batch_ok)
                print(f"[{enriched}/{len(tracks)}] enriched batch of {len(batch_ok)}")
            except Exception as e:
                print(f"FAIL batch starting at {batch_start}: {e}", file=sys.stderr)
            finally:
                for p in tmp_paths:
                    if p.exists():
                        p.unlink()

    collection.flush()
    driver.close()
    conn.close()
    print(f"Done. Enriched {enriched}/{len(tracks)} tracks.")


if __name__ == "__main__":
    main()
