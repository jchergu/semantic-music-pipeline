"""
Stage 5 (8.1): Recommender Engine — batch entrypoint.

Trigger model: seed-track only (no fabricated user/session identity). One
"trigger" = one existing track ID, standing in for "this track was just
played." For each seed track, gathers three candidate sources from the
Semantic API (stage 4) — CLAP similarity, genre-sibling graph neighbors,
same-artist catalog — merges and scores them (ranking.py, pure function),
and writes the top-K ranked recommendations to Postgres, tagged with a
run_id so a batch invocation's output is queryable as one unit.

Usage:
    python recommend.py                          # batch: every track in `tracks`
    python recommend.py --limit 20                # batch: first 20 tracks by id
    python recommend.py --seed-track-id 7          # single seed
    python recommend.py --top-k 5 --candidate-k 30
"""
import argparse
import os
import sys
import uuid
from pathlib import Path

import httpx
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from recommender import context_builder, ranking, trigger_handler  # noqa: E402

load_dotenv(ROOT / ".env")

PG_DSN = (
    f"host=localhost port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)

DEFAULT_API_BASE_URL = os.environ.get("SEMANTIC_API_BASE_URL", "http://localhost:8010")
DEFAULT_TOP_K = 10
DEFAULT_CANDIDATE_K = 25  # wider than top_k — see context_builder.py docstring


def insert_recommendations(conn, run_id: uuid.UUID, seed_track_id: int, ranked: list[dict]) -> None:
    rows = [
        (str(run_id), seed_track_id, r["track_id"], rank, r["score"])
        for rank, r in enumerate(ranked, start=1)
    ]
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO recommendations (run_id, seed_track_id, recommended_track_id, rank, score)
            VALUES %s
            ON CONFLICT (run_id, seed_track_id, rank) DO NOTHING
            """,
            rows,
        )


def run(
    conn,
    client: httpx.Client,
    seed_track_ids: list[int],
    run_id: uuid.UUID,
    top_k: int,
    candidate_k: int,
) -> int:
    written = 0
    for i, seed_track_id in enumerate(seed_track_ids, start=1):
        try:
            ctx = context_builder.build_context(client, seed_track_id, candidate_k=candidate_k)
            ranked = ranking.score_recommendations(
                seed_track_id,
                ctx.similar,
                ctx.genre_siblings,
                ctx.same_artist,
                top_k=top_k,
            )
            insert_recommendations(conn, run_id, seed_track_id, ranked)
            written += len(ranked)
            print(f"[{i}/{len(seed_track_ids)}] seed={seed_track_id} -> {len(ranked)} recommendations")
        except context_builder.SeedTrackNotFoundError:
            print(f"[{i}/{len(seed_track_ids)}] SKIP seed={seed_track_id} (not found)", file=sys.stderr)
        except Exception as e:
            print(f"[{i}/{len(seed_track_ids)}] FAIL seed={seed_track_id}: {e}", file=sys.stderr)
    return written


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed-track-id", type=int, default=None, help="Run for a single seed track")
    parser.add_argument("--limit", type=int, default=None, help="Batch mode: first N tracks by id")
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--candidate-k", type=int, default=DEFAULT_CANDIDATE_K)
    parser.add_argument("--api-base-url", default=DEFAULT_API_BASE_URL)
    parser.add_argument("--run-id", default=None, help="UUID for this batch; generated if omitted")
    args = parser.parse_args(argv)

    run_id = uuid.UUID(args.run_id) if args.run_id else uuid.uuid4()

    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute((ROOT / "recommender" / "schema.sql").read_text())

    seed_track_ids = trigger_handler.get_seed_track_ids(
        conn, seed_track_id=args.seed_track_id, limit=args.limit
    )
    print(f"run_id={run_id}: {len(seed_track_ids)} seed track(s), top_k={args.top_k}")

    with httpx.Client(base_url=args.api_base_url, timeout=10.0) as client:
        written = run(conn, client, seed_track_ids, run_id, args.top_k, args.candidate_k)

    print(f"Done. Wrote {written} recommendation rows under run_id={run_id}.")
    conn.close()


if __name__ == "__main__":
    main()
