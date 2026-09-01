"""
Stage 15A.2: ANN noise floor + reconstruction-path validation.

Two questions this answers:
  1. How much does Milvus vector search alone move recommendation set
     membership/order/scores, run to run, under a PROVABLY IDENTICAL code
     path (the same recommend.py, invoked N times)? -- the noise floor.
  2. Does eval/8_1/regenerate_recommendations.py's reconstruction path (which
     calls build_context()/score_recommendations() directly, not through
     recommend.py) agree with the real recommender within that noise floor,
     or is there a second, real bug?

A naive diff can't separate these two causes. This script establishes (1)
first, then tests (2) against it -- and the PASS/FAIL rule is fixed in
verdict() below, before any Task 2 number exists, so it can't be tuned
after the fact.

RESULT (2026-09-01, this dataset): the noise floor came back at EXACTLY
0.0 across all 10 pairs / 41,100 matched pairs -- zero drift, zero
membership/order changes. The Milvus collection's index is IVF_FLAT
(nlist=128, queried at nprobe=16 -- see platform/semantic_api/main.py),
not FLAT/exhaustive -- it IS an approximate index in the sense that a
true nearest neighbor could be missed if it falls in an unprobed
cluster. But cluster assignment is fixed at index-build time, and this
session never rebuilt the index between runs, so repeated queries
against the same static index returned bit-identical results. Do not
generalize "Milvus is deterministic" from this -- what was measured is
"a static IVF_FLAT index over an unchanging 411-vector collection,
queried repeatedly, was deterministic here." A larger or dynamically
updated collection, index rebuilds, or concurrent writes could all
reintroduce real non-determinism; recall (whether IVF_FLAT finds the
TRUE top-k at all) was also not measured here, only run-to-run
repeatability of whatever it does find.

Also produces the ANN instability number 8.2's determinism metric will need
(N>=5 repeated runs, pairwise membership-instability rate) -- one
measurement, reused later. Given the 0.0 result above, that number is
currently a floor of "not observed in N=5 at this scale," not a proven
upper bound -- 8.2's own candidate pool and concurrency profile may
differ enough to matter.

Usage:
    platform/enrichment/.venv/bin/python -m eval.8_1.diff_runs \\
        --run-ids <uuid1> <uuid2> <uuid3> <uuid4> <uuid5> \\
        --regenerated-path eval/8_1/regenerated_recommendations.json

Writes:
    eval/8_1/noise_floor.json
    eval/8_1/path_validation.json
"""
import argparse
import itertools
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import store_access  # noqa: E402
from diff_recommendations import group_by_seed_ranked  # noqa: E402

NOISE_FLOOR_PATH = ROOT / "noise_floor.json"
PATH_VALIDATION_PATH = ROOT / "path_validation.json"


def fetch_run_rows(conn, run_id: str) -> list[dict]:
    """score::float8 matters here: Postgres's default text-protocol output
    for `real` (float4) uses ~6 significant decimal digits
    (extra_float_digits=0), which psycopg2 then parses back into a Python
    float64 that is close to -- but not bit-identical to -- the actual
    stored float32 value (confirmed via float4send(): the raw stored bytes
    match a numpy float32 cast exactly; a plain unqualified SELECT does
    not, off by ~5.8e-8 for at least one real row). Casting to float8
    server-side is an exact, lossless float4->float8 widening -- avoids
    relying on client-side extra_float_digits settings entirely."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT seed_track_id, recommended_track_id, rank, score::float8 AS score FROM recommendations "
            "WHERE run_id = %s ORDER BY seed_track_id, rank",
            (run_id,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def load_regenerated_rows_float32(path: Path) -> list[dict]:
    """The frozen/run tables live in Postgres REAL (float32); the
    regenerated JSON keeps Python's native float64. Comparing across that
    width difference without reconciling it inflates every score-delta
    with a quantization artifact that has nothing to do with path drift --
    this is what made the first-pass 15A.1 diff flag all 4110 rows as
    differing. Fix: round to float32 here, at load time, so both sides of
    the Task 2 comparison are quantized identically. Do NOT compensate by
    widening the comparison tolerance instead -- that hides real drift."""
    rows = json.loads(path.read_text())
    for r in rows:
        r["score"] = float(np.float32(r["score"]))
    return rows


def percentile_stats(deltas: list[float]) -> dict:
    if not deltas:
        return {"count": 0, "max": 0.0, "p99": 0.0, "p50": 0.0, "mean": 0.0}
    arr = np.array(deltas, dtype=np.float64)
    return {
        "count": len(deltas),
        "max": float(np.max(arr)),
        "p99": float(np.percentile(arr, 99)),
        "p50": float(np.percentile(arr, 50)),
        "mean": float(np.mean(arr)),
    }


def compare_runs(a_by_seed: dict[int, list[dict]], b_by_seed: dict[int, list[dict]]) -> dict:
    """Per-seed classification (identical/order_only/membership_changed) by
    comparing the ranked track_id list, plus a score-drift distribution
    built by matching track_id WITHIN each seed's top-10 regardless of rank
    position -- not rank-aligned, so an order-only shuffle's underlying
    score deltas still get measured, not masked by the rank comparison
    itself."""
    seed_ids = sorted(set(a_by_seed) | set(b_by_seed))
    per_seed = {}
    classification_counts = {"identical": 0, "order_only": 0, "membership_changed": 0}
    all_deltas: list[float] = []

    for seed_id in seed_ids:
        a_rows = a_by_seed.get(seed_id, [])
        b_rows = b_by_seed.get(seed_id, [])
        a_ids_ranked = [r["recommended_track_id"] for r in a_rows]
        b_ids_ranked = [r["recommended_track_id"] for r in b_rows]

        if a_ids_ranked == b_ids_ranked:
            classification = "identical"
        elif set(a_ids_ranked) == set(b_ids_ranked):
            classification = "order_only"
        else:
            classification = "membership_changed"
        classification_counts[classification] += 1

        a_score_by_tid = {r["recommended_track_id"]: r["score"] for r in a_rows}
        b_score_by_tid = {r["recommended_track_id"]: r["score"] for r in b_rows}
        common_tids = set(a_score_by_tid) & set(b_score_by_tid)
        seed_deltas = [abs(a_score_by_tid[tid] - b_score_by_tid[tid]) for tid in common_tids]
        all_deltas.extend(seed_deltas)

        per_seed[seed_id] = {
            "classification": classification,
            "a_track_ids": a_ids_ranked,
            "b_track_ids": b_ids_ranked,
            "matched_track_id_count": len(common_tids),
            "max_delta": max(seed_deltas) if seed_deltas else 0.0,
        }

    return {
        "total_seeds": len(seed_ids),
        "classification_counts": classification_counts,
        "affected_seed_ids": sorted(
            sid for sid, v in per_seed.items() if v["classification"] != "identical"
        ),
        "score_deltas": all_deltas,
        "per_seed": per_seed,
    }


def compute_noise_floor(runs_by_seed: dict[str, dict]) -> dict:
    run_ids = sorted(runs_by_seed)
    pair_results = {}
    pooled_deltas: list[float] = []
    membership_changed_counts = []
    order_only_counts = []

    for a, b in itertools.combinations(run_ids, 2):
        cmp = compare_runs(runs_by_seed[a], runs_by_seed[b])
        key = f"{a}__{b}"
        pair_results[key] = {
            "classification_counts": cmp["classification_counts"],
            "affected_seed_ids": cmp["affected_seed_ids"],
            "score_delta_stats": percentile_stats(cmp["score_deltas"]),
        }
        pooled_deltas.extend(cmp["score_deltas"])
        membership_changed_counts.append(cmp["classification_counts"]["membership_changed"])
        order_only_counts.append(cmp["classification_counts"]["order_only"])

    return {
        "run_ids": run_ids,
        "pair_count": len(pair_results),
        "pairs": pair_results,
        "pooled_score_delta_stats": percentile_stats(pooled_deltas),
        # The noise CEILING: the worst observed under a provably identical
        # code path, across all 10 pairs -- not a single pair's number.
        "max_score_delta": max(pooled_deltas) if pooled_deltas else 0.0,
        "max_membership_changed_seed_count": max(membership_changed_counts) if membership_changed_counts else 0,
        "max_order_only_seed_count": max(order_only_counts) if order_only_counts else 0,
        "per_pair_membership_changed_counts": {
            f"{a}__{b}": c for (a, b), c in zip(itertools.combinations(run_ids, 2), membership_changed_counts)
        },
    }


def verdict(noise_floor: dict, path_validation_classification_counts: dict, path_validation_max_delta: float) -> dict:
    """Pre-registered PASS/FAIL rule -- fixed here, before Task 2's numbers
    exist, so it cannot be tuned after seeing them.

    PASS iff:
      - path_validation's membership_changed seed count does not exceed the
        worst membership_changed count observed in the noise floor, AND
      - path_validation's max score delta does not exceed the noise floor's
        max score delta.
    """
    observed_membership = path_validation_classification_counts["membership_changed"]
    ceiling_membership = noise_floor["max_membership_changed_seed_count"]
    pass_membership = observed_membership <= ceiling_membership

    ceiling_delta = noise_floor["max_score_delta"]
    pass_delta = path_validation_max_delta <= ceiling_delta

    return {
        "pass": bool(pass_membership and pass_delta),
        "membership_check": {
            "observed_membership_changed_seeds": observed_membership,
            "noise_floor_ceiling": ceiling_membership,
            "pass": pass_membership,
        },
        "delta_check": {
            "observed_max_delta": path_validation_max_delta,
            "noise_floor_ceiling": ceiling_delta,
            "pass": pass_delta,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-ids", nargs="+", required=True, help="5+ Postgres run_ids from repeated recommend.py invocations")
    parser.add_argument("--regenerated-path", default=str(ROOT / "regenerated_recommendations.json"))
    args = parser.parse_args()

    conn = store_access.connect_postgres()
    try:
        runs_by_seed = {
            run_id: group_by_seed_ranked(fetch_run_rows(conn, run_id)) for run_id in args.run_ids
        }
    finally:
        conn.close()

    for run_id, by_seed in runs_by_seed.items():
        n_rows = sum(len(v) for v in by_seed.values())
        print(f"run {run_id}: {n_rows} rows, {len(by_seed)} seeds")

    print("\n=== Task 1: noise floor (all pairs) ===")
    nf = compute_noise_floor(runs_by_seed)
    NOISE_FLOOR_PATH.write_text(json.dumps(nf, indent=2, sort_keys=True, default=str))
    print(f"Pairs compared: {nf['pair_count']}")
    print(f"Pooled score delta stats: {nf['pooled_score_delta_stats']}")
    print(f"Noise floor -- max score delta (tolerance): {nf['max_score_delta']}")
    print(f"Noise floor -- max membership_changed seeds in any pair: {nf['max_membership_changed_seed_count']}")
    print(f"Noise floor -- max order_only seeds in any pair: {nf['max_order_only_seed_count']}")
    print(f"Wrote {NOISE_FLOOR_PATH}")

    print("\n=== Task 2: path validation (anchor run vs regenerated) ===")
    anchor_run_id = sorted(runs_by_seed)[0]
    anchor_by_seed = runs_by_seed[anchor_run_id]
    regenerated_rows = load_regenerated_rows_float32(Path(args.regenerated_path))
    regenerated_by_seed = group_by_seed_ranked(regenerated_rows)

    pv_cmp = compare_runs(anchor_by_seed, regenerated_by_seed)
    pv_delta_stats = percentile_stats(pv_cmp["score_deltas"])
    pv_max_delta = pv_delta_stats["max"]
    pv_verdict = verdict(nf, pv_cmp["classification_counts"], pv_max_delta)

    path_validation_out = {
        "anchor_run_id": anchor_run_id,
        "classification_counts": pv_cmp["classification_counts"],
        "affected_seed_ids": pv_cmp["affected_seed_ids"],
        "score_delta_stats": pv_delta_stats,
        "verdict": pv_verdict,
        "per_seed": pv_cmp["per_seed"],
    }
    PATH_VALIDATION_PATH.write_text(json.dumps(path_validation_out, indent=2, sort_keys=True, default=str))
    print(f"Anchor run: {anchor_run_id}")
    print(f"Classification: {pv_cmp['classification_counts']}")
    print(f"Score delta stats: {pv_delta_stats}")
    print(f"VERDICT: {'PASS' if pv_verdict['pass'] else 'FAIL'}")
    print(f"  membership check: {pv_verdict['membership_check']}")
    print(f"  delta check: {pv_verdict['delta_check']}")
    print(f"Wrote {PATH_VALIDATION_PATH}")


if __name__ == "__main__":
    main()
