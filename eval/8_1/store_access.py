"""
Connection helpers for eval/8_1, against the existing platform stores.

Read-only throughout. Mirrors platform/semantic_api/dependencies.py's
connection pattern (own Postgres connection, own Milvus alias, own Neo4j
driver) rather than importing a shared helper -- this repo's convention is a
small local connect() per component (dependencies.py, recommend.py,
reports/_db.py each have their own), not a cross-package import.
"""
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from neo4j import GraphDatabase
from pymilvus import Collection, connections

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / ".env")

MILVUS_COLLECTION = "track_embeddings"
# Own alias, never pymilvus's "default" (shared by tests/conftest.py) or
# "api" (the Semantic API) -- see CLAUDE.md's Platform contracts section.
MILVUS_ALIAS = "eval"


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


def connect_neo4j():
    return GraphDatabase.driver(
        f"bolt://localhost:{os.environ.get('NEO4J_BOLT_PORT', '7687')}",
        auth=(
            os.environ.get("NEO4J_USER", "neo4j"),
            os.environ.get("NEO4J_PASSWORD", "neo4j_password"),
        ),
    )
