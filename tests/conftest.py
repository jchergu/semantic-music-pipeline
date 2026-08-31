import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import boto3
import httpx
import psycopg2
import pytest
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


@pytest.fixture(scope="session")
def pg_conn():
    conn = psycopg2.connect(
        host="localhost",
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{os.environ.get('MINIO_API_PORT', '9000')}",
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )


@pytest.fixture(scope="session")
def milvus_collection():
    from pymilvus import Collection, connections

    connections.connect(alias="default", host="localhost", port=os.environ.get("MILVUS_PORT", "19530"))
    collection = Collection("track_embeddings")
    collection.load()
    yield collection
    connections.disconnect(alias="default")


@pytest.fixture(scope="session")
def neo4j_driver():
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(
        f"bolt://localhost:{os.environ.get('NEO4J_BOLT_PORT', '7687')}",
        auth=(os.environ.get("NEO4J_USER", "neo4j"), os.environ.get("NEO4J_PASSWORD", "neo4j_password")),
    )
    yield driver
    driver.close()


def _free_port() -> int:
    """Binds to port 0 to get an OS-assigned free port, then releases it.
    Small TOCTOU race window before uvicorn binds it — acceptable for a
    local dev/test fixture; avoids hardcoding a port that might collide
    with something else running on the dev machine (port 8000 is known to
    be occupied by something unrelated here; 8010 is also used for the
    manual `uvicorn --port 8010` example in docs/platform/stage4-semantic-api.md)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def semantic_api_server():
    """Launches `uvicorn semantic_api.main:app` (via --app-dir platform/) as
    a subprocess on a free port,
    polls /health until it's ready, yields the base URL, and tears the
    process down afterward. Session-scoped: one server for the whole test
    run, shared by every test that needs a live Semantic API instance.

    Uses `sys.executable -m uvicorn` rather than an `uvicorn` console
    script, so it always runs under whichever interpreter/venv pytest
    itself is running under (avoids PATH ambiguity between venvs).
    """
    port = _free_port()
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "semantic_api.main:app",
            "--app-dir",
            str(ROOT / "platform"),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            # no --reload: it spawns a supervisor + worker process pair,
            # which complicates clean subprocess teardown for no test benefit
        ],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    base_url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 30
        last_error: Exception | None = None
        ready = False
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                out = proc.stdout.read() if proc.stdout else ""
                raise RuntimeError(f"uvicorn exited early (code {proc.returncode}):\n{out}")
            try:
                resp = httpx.get(f"{base_url}/health", timeout=1.0)
                if resp.status_code == 200:
                    ready = True
                    break
            except httpx.TransportError as e:
                last_error = e
            time.sleep(0.3)
        if not ready:
            raise RuntimeError(f"uvicorn did not become healthy within 30s: {last_error}")

        yield base_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


@pytest.fixture(scope="session")
def event_ingestion_server():
    """Same shape as semantic_api_server, for the stage 11 event
    ingestion service (`uvicorn event_ingestion.main:app`)."""
    port = _free_port()
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "event_ingestion.main:app",
            "--app-dir",
            str(ROOT / "platform"),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    base_url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 30
        last_error: Exception | None = None
        ready = False
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                out = proc.stdout.read() if proc.stdout else ""
                raise RuntimeError(f"uvicorn exited early (code {proc.returncode}):\n{out}")
            try:
                resp = httpx.get(f"{base_url}/health", timeout=1.0)
                if resp.status_code == 200:
                    ready = True
                    break
            except httpx.TransportError as e:
                last_error = e
            time.sleep(0.3)
        if not ready:
            raise RuntimeError(f"uvicorn did not become healthy within 30s: {last_error}")

        yield base_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
