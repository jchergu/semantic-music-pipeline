"""Verifies the Stage 2 seed dataset: Postgres/MinIO parity and data sanity."""
MINIO_BUCKET = "raw-tracks"


def _row_count(pg_conn) -> int:
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM tracks")
        return cur.fetchone()[0]


def _minio_keys(s3_client) -> set[str]:
    keys = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=MINIO_BUCKET):
        for obj in page.get("Contents", []):
            keys.add(obj["Key"])
    return keys


def test_dataset_within_seed_range(pg_conn):
    count = _row_count(pg_conn)
    assert 200 <= count <= 500, f"expected 200-500 seed tracks, got {count}"


def test_no_duplicate_jamendo_ids(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*), count(DISTINCT jamendo_id) FROM tracks")
        total, distinct = cur.fetchone()
    assert total == distinct


def test_every_track_has_a_fingerprint(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM tracks WHERE chromaprint_fingerprint IS NULL")
        assert cur.fetchone()[0] == 0


def test_postgres_minio_parity(pg_conn, s3_client):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT minio_object_key FROM tracks")
        pg_keys = {row[0] for row in cur.fetchall()}

    minio_keys = _minio_keys(s3_client)

    assert pg_keys == minio_keys, (
        f"{len(pg_keys - minio_keys)} rows point to missing MinIO objects, "
        f"{len(minio_keys - pg_keys)} MinIO objects have no Postgres row"
    )


def test_durations_are_plausible(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT min(duration_sec), max(duration_sec) FROM tracks")
        min_dur, max_dur = cur.fetchone()
    assert min_dur > 10, "a track under 10s is likely a bad download, not music"
    assert max_dur < 1800, "a track over 30min is likely a bad audiodownload URL"
