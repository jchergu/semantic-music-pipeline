"""
Stage 15A follow-up: smoke test for eval/8_1/run.py's own import path and
pipeline wiring. Decision D's git mv of ranking.py to platform/scoring/
(2026-08-31) broke run.py's `from recommender import ranking` import, and
it sat undetected on main until this session re-ran the eval pack by
hand. Nothing in the existing test suite imported eval/8_1/run.py at all.

Two layers:
  - test_run_module_imports: a bare import of eval.8_1.run. Alone, this
    would have caught the Decision-D-style break, since it failed at
    import time (before any function ran).
  - test_run_pipeline_smoke: a live call to run_pipeline() against a tiny
    real subset (3 seeds from the frozen table), writing to tmp_path so
    the real eval/8_1 output files are never touched, asserting the
    returned results dict is well-formed.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import store_access  # noqa: E402


def test_run_module_imports():
    import run  # noqa: F401


def test_run_pipeline_smoke(tmp_path):
    import run

    conn = store_access.connect_postgres()
    collection = store_access.connect_milvus()
    neo4j_driver = store_access.connect_neo4j()

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT seed_track_id, recommended_track_id, rank, score FROM recommendations "
                "WHERE seed_track_id IN (SELECT DISTINCT seed_track_id FROM recommendations ORDER BY seed_track_id LIMIT 3) "
                "ORDER BY seed_track_id, rank"
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        assert rows, "expected at least one row from the live recommendations table"
        seed_ids = sorted({r["seed_track_id"] for r in rows})
        assert len(seed_ids) == 3

        results = run.run_pipeline(
            conn,
            collection,
            neo4j_driver,
            rows,
            all_track_ids=seed_ids,
            out_dir=tmp_path,
            figures_dir=tmp_path / "figures",
            out_prefix="smoke_",
            measure_latency=False,
        )
    finally:
        conn.close()
        store_access.disconnect_milvus()
        neo4j_driver.close()

    for key in ("signal_contribution", "catalog_coverage", "intra_list_diversity", "kg_connectivity", "failure_edge_cases"):
        assert key in results

    assert (tmp_path / "smoke_results.json").exists()
    assert (tmp_path / "smoke_tables.md").exists()
    # The real eval/8_1 output files must never be touched by this test.
    assert not (ROOT / "smoke_results.json").exists()
