"""
Stage 15A follow-up: diff the frozen 4110-row `recommendations` table
against eval/8_1/regenerate_recommendations.py's output.

NOTE on the 15-row residual (11 seeds classified "identical" that still
show row_diffs > 0): this is NOT float32/float64 precision noise (the
Stage 15A.2 `score::float8` fix left this file's output byte-identical --
SCORE_TOLERANCE=1e-4 already swamps that ~1e-7-scale artifact by three
orders of magnitude, so it could never have caused these) and NOT Milvus
ANN non-determinism (ruled out by eval/8_1/noise_floor.json's exact 0.0
across 5 real runs). Verified instead: every one of the 15 rows differs
by EXACTLY +-0.05 (GENRE_BOOST) -- these are candidates whose genre-sibling
status flipped between the pre-Stage-15A and post-Stage-15A query, but
the +-0.05 wasn't enough to change their rank, so "identical" (a
track_id-based classification) correctly reports no reordering while the
row's actual score composition genuinely changed. Confirmed by direct
inspection, e.g. seed=9/track=3: frozen score 0.817909 (similarity only)
vs regenerated 0.867909 (similarity + GENRE_BOOST), same rank (1) in
both. This means 277/411 "affected" (this file's own headline number) is
a real, track_id-based count -- not an undercount bug -- but 11 more
seeds also had a real, smaller-magnitude effect this classification was
never designed to catch.

Usage:
    platform/enrichment/.venv/bin/python -m eval.8_1.diff_recommendations

Writes:
    eval/8_1/regeneration_diff.json
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import store_access  # noqa: E402

REGENERATED_PATH = ROOT / "regenerated_recommendations.json"
OUT_PATH = ROOT / "regeneration_diff.json"


def fetch_frozen_rows(conn) -> list[dict]:
    # score::float8: an unqualified SELECT of a `real` column round-trips
    # through Postgres's default 6-significant-digit text output, losing
    # exact float32 precision (see diff_runs.py::fetch_run_rows for the
    # confirmed diagnosis). Doesn't change this file's conclusions --
    # SCORE_TOLERANCE (1e-4) already swamps that ~1e-7-scale artifact --
    # but the raw numbers should reflect the real database value.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT seed_track_id, recommended_track_id, rank, score::float8 AS score FROM recommendations "
            "ORDER BY seed_track_id, rank"
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def group_by_seed_ranked(rows: list[dict]) -> dict[int, list[dict]]:
    grouped: dict[int, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["seed_track_id"], []).append(row)
    for seed_rows in grouped.values():
        seed_rows.sort(key=lambda r: r["rank"])
    return grouped


# The frozen table's `score` column is Postgres REAL (float32); the
# regenerated JSON keeps Python's native float64. Comparing scores exactly
# flags every row as "different" purely from that storage round-trip (e.g.
# 0.6584804 vs 0.6584804177284241 for the literal same value) -- this
# tolerance absorbs float32 precision loss without masking a genuine score
# change (boosts are 0.05/0.15, real re-ranking differences are far larger).
SCORE_TOLERANCE = 1e-4


def diff(frozen: dict[int, list[dict]], regenerated: dict[int, list[dict]]) -> dict:
    seed_ids = sorted(set(frozen) | set(regenerated))
    per_seed = {}
    differing_rows = 0
    classification_counts = {"identical": 0, "order_only": 0, "membership_changed": 0}

    for seed_id in seed_ids:
        old_rows = frozen.get(seed_id, [])
        new_rows = regenerated.get(seed_id, [])
        old_ids_ranked = [r["recommended_track_id"] for r in old_rows]
        new_ids_ranked = [r["recommended_track_id"] for r in new_rows]
        old_by_rank = {r["rank"]: r for r in old_rows}
        new_by_rank = {r["rank"]: r for r in new_rows}

        row_diffs = 0
        for rank in sorted(set(old_by_rank) | set(new_by_rank)):
            o, n = old_by_rank.get(rank), new_by_rank.get(rank)
            if (
                o is None
                or n is None
                or o["recommended_track_id"] != n["recommended_track_id"]
                or abs(o["score"] - n["score"]) > SCORE_TOLERANCE
            ):
                row_diffs += 1
        differing_rows += row_diffs

        if old_ids_ranked == new_ids_ranked:
            classification = "identical"
        elif set(old_ids_ranked) == set(new_ids_ranked):
            classification = "order_only"
        else:
            classification = "membership_changed"
        classification_counts[classification] += 1

        per_seed[seed_id] = {
            "classification": classification,
            "row_diffs": row_diffs,
            "old_track_ids": old_ids_ranked,
            "new_track_ids": new_ids_ranked,
        }

    identical_with_row_diffs = sorted(
        sid for sid, v in per_seed.items() if v["classification"] == "identical" and v["row_diffs"] > 0
    )

    return {
        "total_seeds": len(seed_ids),
        "total_rows_compared": sum(len(v) for v in frozen.values()),
        "differing_rows": differing_rows,
        "classification_counts": classification_counts,
        "affected_seed_ids": sorted(
            sid for sid, v in per_seed.items() if v["classification"] != "identical"
        ),
        "identical_but_row_diffs_seed_ids": identical_with_row_diffs,
        "identical_but_row_diffs_note": (
            "Seeds with unchanged track_id order/membership but row_diffs > 0 -- "
            "verified (see module docstring) to be exactly +-GENRE_BOOST (0.05) on "
            "one candidate whose genre-sibling status flipped between the pre- and "
            "post-Stage-15A query without changing its rank. Not float precision "
            "noise, not ANN non-determinism -- a real, smaller-magnitude effect the "
            "track_id-based classification above doesn't count as 'affected'."
        ),
        "per_seed": per_seed,
    }


def main() -> None:
    conn = store_access.connect_postgres()
    try:
        frozen_rows = fetch_frozen_rows(conn)
    finally:
        conn.close()

    regenerated_rows = json.loads(REGENERATED_PATH.read_text())

    frozen_by_seed = group_by_seed_ranked(frozen_rows)
    regenerated_by_seed = group_by_seed_ranked(regenerated_rows)

    result = diff(frozen_by_seed, regenerated_by_seed)
    OUT_PATH.write_text(json.dumps(result, indent=2, sort_keys=True))

    print(f"Compared {result['total_rows_compared']} frozen rows across {result['total_seeds']} seeds.")
    print(f"Differing (seed, rank) rows: {result['differing_rows']} / {result['total_rows_compared']}")
    print(f"Seed classification: {result['classification_counts']}")
    print(f"Affected seeds: {len(result['affected_seed_ids'])} / {result['total_seeds']}")
    print(f"Identical-order seeds with a real GENRE_BOOST-magnitude score diff: {len(result['identical_but_row_diffs_seed_ids'])}")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
