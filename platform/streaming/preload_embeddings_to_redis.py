"""
Stage 13 prep: one-off host-side script that copies every track's CLAP
embedding AND duration_sec from Milvus/Postgres into Redis
(track:{id}:embedding, track:{id}:duration_sec), so the PyFlink job's
Python UDF workers -- which run inside the TaskManager container and
don't have pymilvus installed (deliberately; adding it would mean another
image rebuild for a connection this job only needs once at worker start)
-- can read them from Redis, which is already reachable from the Flink
containers via REDIS_HOST=redis (docker-compose.yml). duration_sec is
needed to classify skip timing (>80% of duration -- see
flink_session_profile_job.py's weight table). Run once before submitting
the job; re-run if the dataset changes.
"""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg2
import redis
from dotenv import load_dotenv
from pymilvus import Collection, connections

load_dotenv(ROOT.parent / ".env")

MILVUS_ALIAS = "preload"
MILVUS_COLLECTION = "track_embeddings"


def main() -> None:
    connections.connect(alias=MILVUS_ALIAS, host="localhost", port="19530")
    collection = Collection(MILVUS_COLLECTION, using=MILVUS_ALIAS)
    collection.load()
    rows = collection.query(expr="track_id >= 0", output_fields=["track_id", "embedding"], limit=1000)
    connections.disconnect(alias=MILVUS_ALIAS)

    pg_conn = psycopg2.connect(
        host="localhost",
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    with pg_conn.cursor() as cur:
        cur.execute("SELECT id, duration_sec FROM tracks")
        durations = {row[0]: row[1] for row in cur.fetchall()}
    pg_conn.close()

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    for row in rows:
        track_id = row["track_id"]
        r.set(f"track:{track_id}:embedding", json.dumps([float(v) for v in row["embedding"]]))
        if track_id in durations and durations[track_id] is not None:
            r.set(f"track:{track_id}:duration_sec", durations[track_id])
    print(f"Preloaded {len(rows)} track embeddings and {len(durations)} durations into Redis.")


if __name__ == "__main__":
    main()
