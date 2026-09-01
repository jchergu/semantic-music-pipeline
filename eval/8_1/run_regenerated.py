"""
Stage 15A follow-up: run the eval/8_1 pipeline (run.py::run_pipeline)
against eval/8_1/regenerate_recommendations.py's output instead of the
frozen Postgres `recommendations` table -- a self-consistent read of
current behavior now that kg_connectivity.py::capped_genre_sibling_ids
has been un-frozen to match the current (post Stage 15A fix) live query.

The frozen results.json/tables.md/figures are untouched; this writes a
"regenerated_"-prefixed parallel set. Latency isn't re-measured here --
it's identical pipeline code regardless of which table is evaluated, and
Stage 15A already recorded a fresh reading.

Usage:
    platform/enrichment/.venv/bin/python -m eval.8_1.run_regenerated

Writes:
    eval/8_1/results_regenerated.json
    eval/8_1/tables_regenerated.md
    eval/8_1/figures/regenerated/*.png
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import store_access  # noqa: E402
from run import OUT_DIR, run_pipeline  # noqa: E402

REGENERATED_PATH = ROOT / "regenerated_recommendations.json"


def main() -> None:
    rows = json.loads(REGENERATED_PATH.read_text())

    conn = store_access.connect_postgres()
    collection = store_access.connect_milvus()
    neo4j_driver = store_access.connect_neo4j()

    try:
        run_pipeline(
            conn,
            collection,
            neo4j_driver,
            rows,
            out_dir=OUT_DIR,
            figures_dir=OUT_DIR / "figures" / "regenerated",
            out_prefix="regenerated_",
            measure_latency=False,
        )
    finally:
        conn.close()
        store_access.disconnect_milvus()
        neo4j_driver.close()


if __name__ == "__main__":
    main()
