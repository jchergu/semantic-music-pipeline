"""Thin re-export of the shared platform fixtures for 8.1's tests.

pytest's conftest.py auto-discovery only walks up a test file's own
directory tree to rootdir; usecases/8_1_batch_reactive/tests/ isn't a
descendant of tests/, so tests/conftest.py's fixtures (pg_conn, s3_client,
milvus_collection, neo4j_driver, semantic_api_server) aren't automatically
visible here. They're shared across both platform tests (tests/) and 8.1's
own tests (here) — same fixtures, same live stack — so this file imports
them rather than duplicating them.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT))

from tests.conftest import (  # noqa: F401,E402
    milvus_collection,
    neo4j_driver,
    pg_conn,
    s3_client,
    semantic_api_server,
)
