"""
Connection helpers for eval/8_2, against the existing platform stores.

Same small-local-connect() convention as eval/8_1/store_access.py and
platform/semantic_api/dependencies.py, with one difference that matters:
this harness runs platform/streaming/recommendation_refresh.py *in its own
process*, and that module opens Milvus under alias "recs-refresh". Sharing
an alias between two independently-managed connection lifecycles in one
process is exactly the bug CLAUDE.md's Platform contracts rule exists to
prevent, so this pack uses its own "eval82" (also distinct from eval/8_1's
"eval", the API's "api", and tests/conftest.py's "default").

Unlike eval/8_1 this pack is not read-only: it deletes and re-reads Redis
keys under the session ids it generates itself (see harness.reset_session).
It never writes to Postgres, Milvus or Neo4j.
"""
import os
from pathlib import Path

import psycopg2
import redis
from dotenv import load_dotenv
from pymilvus import Collection, connections

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / ".env")

MILVUS_COLLECTION = "track_embeddings"
MILVUS_ALIAS = "eval82"


def connect_postgres():
    return psycopg2.connect(
        host="localhost",
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def connect_milvus() -> Collection:
    connections.connect(
        alias=MILVUS_ALIAS,
        host="localhost",
        port=os.environ.get("MILVUS_PORT", "19530"),
    )
    collection = Collection(MILVUS_COLLECTION, using=MILVUS_ALIAS)
    collection.load()
    return collection


def disconnect_milvus() -> None:
    connections.disconnect(alias=MILVUS_ALIAS)


def connect_redis() -> redis.Redis:
    """redis-py clients are connection-pool backed and safe to share across
    the harness's threads -- unlike the psycopg2 connections, which are not
    (each thread that touches Postgres opens its own)."""
    return redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
        decode_responses=True,
    )


def fetch_embeddings(collection: Collection, track_ids: list[int]) -> dict[str, list[float]]:
    """CLAP embeddings for the given tracks, keyed by track id AS A STRING --
    behavioral events carry track_id as a string (platform/simulator/events.py
    builds it that way), and metric 4 joins these against event payloads.

    Read from Milvus, the system of record for embeddings, rather than from
    the track:{id}:embedding copies preload_embeddings_to_redis.py leaves in
    Redis for the Flink workers. The two should agree, but the Redis copy is
    a convenience for containers without pymilvus, not a second source of
    truth, and metric 4 compares against the profile those very copies fed --
    reading the reference from the same copy could hide a stale preload.
    """
    if not track_ids:
        return {}
    expr = f"track_id in [{','.join(str(int(t)) for t in sorted(set(track_ids)))}]"
    rows = collection.query(expr=expr, output_fields=["track_id", "embedding"], limit=len(set(track_ids)))
    return {str(row["track_id"]): [float(v) for v in row["embedding"]] for row in rows}
