"""
Stage 2 (8.1): seed dataset ingestion.

Pulls up to TARGET_TRACK_COUNT full-length, Creative-Commons-licensed tracks
from the Jamendo API, uploads the audio to MinIO, computes a Chromaprint
fingerprint, best-effort links it to a MusicBrainz recording via AcoustID,
and writes the metadata row to PostgreSQL (system of record).

Usage:
    python ingest.py [--limit 500]
"""
import argparse
import os
import sys
import tempfile
import time
from pathlib import Path

import acoustid
import boto3
import psycopg2
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

JAMENDO_CLIENT_ID = os.environ["JAMENDO_CLIENT_ID"]
ACOUSTID_API_KEY = os.environ["ACOUSTID_API_KEY"]

JAMENDO_API_URL = "https://api.jamendo.com/v3.0/tracks/"
JAMENDO_PAGE_SIZE = 200

MINIO_ENDPOINT = f"http://localhost:{os.environ.get('MINIO_API_PORT', '9000')}"
MINIO_BUCKET = "raw-tracks"

PG_DSN = (
    f"host=localhost port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)

# AcoustID asks clients to stay at/under 3 requests/second.
ACOUSTID_MIN_INTERVAL_SEC = 0.4


def fetch_jamendo_tracks(target_count: int, start_offset: int = 0) -> list[dict]:
    tracks: list[dict] = []
    offset = start_offset
    while len(tracks) < target_count:
        page_limit = min(JAMENDO_PAGE_SIZE, target_count - len(tracks))
        params = {
            "client_id": JAMENDO_CLIENT_ID,
            "format": "json",
            "limit": page_limit,
            "offset": offset,
            "include": "musicinfo",
            "audioformat": "mp32",
            # NB: "boost" (relevance re-ranking) silently returns 0 results
            # past offset 200 on this API. "order" (actual sort) paginates
            # correctly to arbitrary depth.
            "order": "popularity_total",
        }

        page: list[dict] = []
        # The API occasionally returns a transient empty page for a valid
        # offset; retry a few times before concluding we've exhausted the
        # catalog.
        for attempt in range(4):
            if attempt > 0:
                time.sleep(1.5 * attempt)
            resp = requests.get(JAMENDO_API_URL, params=params, timeout=30)
            resp.raise_for_status()
            page = resp.json().get("results", [])
            if page:
                break

        if not page:
            break
        tracks.extend(page)
        offset += len(page)
    return tracks[:target_count]


def ensure_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=MINIO_BUCKET)


def download_to_tempfile(url: str) -> Path:
    fd, path = tempfile.mkstemp(suffix=".mp3", prefix="jamendo_")
    os.close(fd)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 16):
                f.write(chunk)
    return Path(path)


def fingerprint_and_lookup(path: Path) -> tuple[str | None, str | None, float | None]:
    """Returns (fingerprint, musicbrainz_recording_id, match_score)."""
    try:
        duration, fingerprint = acoustid.fingerprint_file(str(path))
    except acoustid.FingerprintGenerationError:
        return None, None, None

    fingerprint_str = fingerprint.decode("ascii")

    try:
        result = acoustid.lookup(ACOUSTID_API_KEY, fingerprint, duration, meta="recordings")
    except acoustid.WebServiceError:
        return fingerprint_str, None, None
    finally:
        time.sleep(ACOUSTID_MIN_INTERVAL_SEC)

    best_score = None
    best_recording_id = None
    for r in result.get("results", []):
        recordings = r.get("recordings") or []
        if recordings and (best_score is None or r["score"] > best_score):
            best_score = r["score"]
            best_recording_id = recordings[0]["id"]

    return fingerprint_str, best_recording_id, best_score


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--offset", type=int, default=0, help="Jamendo listing offset, for resuming a partial run")
    args = parser.parse_args()

    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute((ROOT / "ingestion" / "schema.sql").read_text())

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )
    ensure_bucket(s3)

    print(f"Fetching {args.limit} track listings from Jamendo (offset={args.offset})...")
    jamendo_tracks = fetch_jamendo_tracks(args.limit, start_offset=args.offset)
    print(f"Got {len(jamendo_tracks)} track listings. Ingesting...")

    ingested = 0
    for i, t in enumerate(jamendo_tracks, start=1):
        jamendo_id = t["id"]
        title = t["name"]
        artist_name = t["artist_name"]
        duration_sec = t.get("duration")
        genre_tags = (t.get("musicinfo") or {}).get("tags", {}).get("genres", [])
        license_ccurl = t.get("license_ccurl")
        audio_url = t.get("audiodownload") or t.get("audio")
        object_key = f"{jamendo_id}.mp3"

        if not audio_url:
            print(f"[{i}/{len(jamendo_tracks)}] SKIP '{title}' (no audio URL)")
            continue

        tmp_path = None
        try:
            tmp_path = download_to_tempfile(audio_url)
            s3.upload_file(str(tmp_path), MINIO_BUCKET, object_key)
            fingerprint, mb_recording_id, mb_score = fingerprint_and_lookup(tmp_path)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tracks (
                        jamendo_id, title, artist_name, duration_sec, genre_tags,
                        license_ccurl, minio_bucket, minio_object_key,
                        chromaprint_fingerprint, musicbrainz_recording_id,
                        musicbrainz_match_score
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (jamendo_id) DO NOTHING
                    """,
                    (
                        jamendo_id, title, artist_name, duration_sec, genre_tags,
                        license_ccurl, MINIO_BUCKET, object_key,
                        fingerprint, mb_recording_id, mb_score,
                    ),
                )
            ingested += 1
            mb_note = f"MB={mb_recording_id}" if mb_recording_id else "MB=no match"
            print(f"[{i}/{len(jamendo_tracks)}] OK '{title}' by {artist_name} ({mb_note})")
        except Exception as e:
            print(f"[{i}/{len(jamendo_tracks)}] FAIL '{title}': {e}", file=sys.stderr)
        finally:
            if tmp_path and tmp_path.exists():
                tmp_path.unlink()

    print(f"Done. Ingested {ingested}/{len(jamendo_tracks)} tracks.")
    conn.close()


if __name__ == "__main__":
    main()
