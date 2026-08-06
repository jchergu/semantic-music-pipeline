import os
from pathlib import Path

import boto3
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
