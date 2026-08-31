"""
Stage 12: deterministic behavioral-event simulator CLI.

Replays scripted listening sessions against the live 411-track catalog by
POSTing to platform/event_ingestion's /events endpoint (must already be
running -- same expectation usecases/8_1_batch_reactive/recommender/recommend.py
has of the Semantic API, not started by this module). See
docs/platform/stage12-event-simulator.md and this directory's README.md.

Usage (from the repo root):
    PYTHONPATH=platform platform/enrichment/.venv/bin/python -m simulator.cli \
        --seed 42 --speed 10 --sessions 3
"""
import argparse
import datetime as dt
import os
import random
import sys
import threading
import time
from pathlib import Path

import httpx
import psycopg2
from dotenv import load_dotenv

from simulator import events, scripts_io

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / ".env")

SCRIPTS_DIR = Path(__file__).resolve().parent / "scripts"
DEFAULT_INGESTION_URL = os.environ.get("EVENT_INGESTION_URL", "http://localhost:8020")

PG_DSN = (
    f"host=localhost port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)


def fetch_durations(track_ids: set[int]) -> dict[int, int]:
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, duration_sec FROM tracks WHERE id = ANY(%s)", (list(track_ids),))
            return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()


# Fixed reference instant for simulated event_time -- deliberately NOT
# wall-clock now(): "same seed, byte-identical event stream" requires
# event_time itself to be reproducible across separate invocations, not
# just the gaps between events. Each session's start is offset by its
# index only, so concurrent sessions don't share identical timestamps.
SIMULATION_EPOCH = dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc)


def run_session(
    session_id: str, script: dict, durations: dict, speed: float, ingestion_url: str, start_time: dt.datetime
) -> list[dict]:
    planned = events.build_session_events(script, session_id, durations, start_time)
    posted = []
    with httpx.Client(base_url=ingestion_url, timeout=10.0) as client:
        for item in planned:
            gap = item["wall_clock_gap_seconds"]
            if gap > 0:
                time.sleep(gap / speed)
            resp = client.post("/events", json=item["event"])
            resp.raise_for_status()
            posted.append(item["event"])
    return posted


def main(argv: list[str] | None = None) -> dict[str, list[dict]]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, required=True, help="Reproducibility seed")
    parser.add_argument("--speed", type=float, default=1.0, help="Wall-clock compression factor")
    parser.add_argument("--sessions", type=int, default=1, help="Number of concurrent sessions")
    parser.add_argument(
        "--script",
        default=None,
        help="Path to a YAML script; every session uses it. Omit to pick (seeded) among the canned scripts per session.",
    )
    parser.add_argument("--ingestion-url", default=DEFAULT_INGESTION_URL)
    args = parser.parse_args(argv)

    rng = random.Random(args.seed)
    canned_scripts = sorted(SCRIPTS_DIR.glob("*.yaml"))
    if not canned_scripts and args.script is None:
        print("No canned scripts found and no --script given.", file=sys.stderr)
        sys.exit(1)

    session_scripts = []
    for i in range(args.sessions):
        script_path = Path(args.script) if args.script else rng.choice(canned_scripts)
        session_scripts.append((f"sim-{args.seed}-{i}", scripts_io.load_script(script_path)))

    all_track_ids = {t["track_id"] for _, s in session_scripts for t in s["tracks"]}
    durations = fetch_durations(all_track_ids)
    missing = all_track_ids - durations.keys()
    if missing:
        print(f"Track ids not found in the live catalog: {sorted(missing)}", file=sys.stderr)
        sys.exit(1)

    results: dict[str, list[dict]] = {}
    lock = threading.Lock()

    def worker(session_id: str, script: dict, index: int) -> None:
        start_time = SIMULATION_EPOCH + dt.timedelta(hours=index)
        posted = run_session(session_id, script, durations, args.speed, args.ingestion_url, start_time)
        with lock:
            results[session_id] = posted

    threads = [
        threading.Thread(target=worker, args=(sid, s, i)) for i, (sid, s) in enumerate(session_scripts)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total = sum(len(v) for v in results.values())
    print(f"Done. {len(results)} session(s), {total} event(s) posted to {args.ingestion_url}.")
    for sid, posted in results.items():
        print(f"  {sid}: {len(posted)} events")
    return results


if __name__ == "__main__":
    main()
