"""
Connection lifecycle for the stage 4 Semantic API.

One Postgres connection pool, one Milvus collection handle, one Neo4j
driver, created at FastAPI startup and torn down at shutdown. Read-only
usage throughout — this API serves L3 consumers, it doesn't write to any
of the three stores.
"""
import os
from pathlib import Path

from dotenv import load_dotenv
from neo4j import Driver, GraphDatabase
from psycopg2.pool import ThreadedConnectionPool
from pymilvus import Collection, connections

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

MILVUS_COLLECTION = "track_embeddings"
# A dedicated alias, not "default": when this app runs in-process (e.g.
# FastAPI's TestClient in tests/test_stage4_api.py) rather than as a
# separate OS process, pymilvus's `connections` registry is process-global
# — sharing "default" with tests/conftest.py's own `milvus_collection`
# fixture meant this module's disconnect_all() tore down the alias out
# from under any later test still using that fixture. Out-of-process runs
# (uvicorn subprocess, e.g. tests/conftest.py's semantic_api_server) don't
# need this, but it's harmless and keeps both call sites consistent.
MILVUS_ALIAS = "api"

_pg_pool: ThreadedConnectionPool | None = None
_milvus_collection: Collection | None = None
_neo4j_driver: Driver | None = None


def connect_all() -> None:
    global _pg_pool, _milvus_collection, _neo4j_driver

    _pg_pool = ThreadedConnectionPool(
        1,
        10,
        host="localhost",
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    connections.connect(
        alias=MILVUS_ALIAS,
        host="localhost",
        port=os.environ.get("MILVUS_PORT", "19530"),
    )
    _milvus_collection = Collection(MILVUS_COLLECTION, using=MILVUS_ALIAS)
    _milvus_collection.load()

    _neo4j_driver = GraphDatabase.driver(
        f"bolt://localhost:{os.environ.get('NEO4J_BOLT_PORT', '7687')}",
        auth=(
            os.environ.get("NEO4J_USER", "neo4j"),
            os.environ.get("NEO4J_PASSWORD", "neo4j_password"),
        ),
    )


def disconnect_all() -> None:
    global _pg_pool, _milvus_collection, _neo4j_driver
    if _pg_pool is not None:
        _pg_pool.closeall()
        _pg_pool = None
    connections.disconnect(alias=MILVUS_ALIAS)
    _milvus_collection = None
    if _neo4j_driver is not None:
        _neo4j_driver.close()
        _neo4j_driver = None


def get_pg_conn():
    conn = _pg_pool.getconn()
    conn.autocommit = True
    try:
        yield conn
    finally:
        _pg_pool.putconn(conn)


def get_milvus_collection() -> Collection:
    return _milvus_collection


def get_neo4j_session():
    session = _neo4j_driver.session()
    try:
        yield session
    finally:
        session.close()
