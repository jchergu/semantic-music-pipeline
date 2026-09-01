"""
Stage 15A follow-up: regenerate the recommendation set against CURRENT
code (post related_by_genre ORDER BY fix, post Decision D's ranking.py
move), without touching the frozen 4110-row `recommendations` Postgres
table.

Reuses the exact per-seed logic usecases/8_1_batch_reactive/recommender/
recommend.py::run() uses -- context_builder.build_context() +
scoring.ranking.score_recommendations() -- but never opens a Postgres
write path. Output goes to a JSON file instead: the frozen table has no
`run_id` filter in eval/8_1/run.py's queries, so inserting a second run
into the same table would silently corrupt every downstream metric.

Requires the Semantic API running (context_builder talks to it over
HTTP only):
    platform/enrichment/.venv/bin/python -m uvicorn semantic_api.main:app \
        --app-dir platform --port 8010

Usage:
    platform/enrichment/.venv/bin/python -m eval.8_1.regenerate_recommendations

Writes:
    eval/8_1/regenerated_recommendations.json
"""
import json
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
USECASE_RECOMMENDER = ROOT.parent.parent / "usecases" / "8_1_batch_reactive"
sys.path.insert(0, str(USECASE_RECOMMENDER))
PLATFORM = ROOT.parent.parent / "platform"
sys.path.insert(0, str(PLATFORM))

import store_access  # noqa: E402
from recommender import context_builder, trigger_handler  # noqa: E402
from scoring import ranking  # noqa: E402

DEFAULT_API_BASE_URL = "http://localhost:8010"
DEFAULT_TOP_K = 10
DEFAULT_CANDIDATE_K = 25  # matches recommend.py's own default

OUT_PATH = ROOT / "regenerated_recommendations.json"


def main() -> None:
    conn = store_access.connect_postgres()
    try:
        seed_track_ids = trigger_handler.get_seed_track_ids(conn, seed_track_id=None, limit=None)
    finally:
        conn.close()

    print(f"Regenerating recommendations for {len(seed_track_ids)} seeds against {DEFAULT_API_BASE_URL}...")

    rows: list[dict] = []
    with httpx.Client(base_url=DEFAULT_API_BASE_URL, timeout=10.0) as client:
        for i, seed_track_id in enumerate(seed_track_ids, start=1):
            try:
                ctx = context_builder.build_context(client, seed_track_id, candidate_k=DEFAULT_CANDIDATE_K)
                ranked = ranking.score_recommendations(
                    seed_track_id,
                    ctx.similar,
                    ctx.genre_siblings,
                    ctx.same_artist,
                    top_k=DEFAULT_TOP_K,
                )
                for rank, r in enumerate(ranked, start=1):
                    rows.append(
                        {
                            "seed_track_id": seed_track_id,
                            "recommended_track_id": r["track_id"],
                            "rank": rank,
                            "score": r["score"],
                        }
                    )
                print(f"[{i}/{len(seed_track_ids)}] seed={seed_track_id} -> {len(ranked)} recommendations")
            except context_builder.SeedTrackNotFoundError:
                print(f"[{i}/{len(seed_track_ids)}] SKIP seed={seed_track_id} (not found)", file=sys.stderr)
            except Exception as e:
                print(f"[{i}/{len(seed_track_ids)}] FAIL seed={seed_track_id}: {e}", file=sys.stderr)

    OUT_PATH.write_text(json.dumps(rows, indent=2))
    print(f"Wrote {len(rows)} rows for {len(seed_track_ids)} seeds to {OUT_PATH}")


if __name__ == "__main__":
    main()
