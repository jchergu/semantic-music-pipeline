"""
Session B: 8.1 evaluation data pack.

Systems and behavior evaluation of the existing 411-seed / 4110-row
`recommendations` table -- no accuracy metric (no precision@k/recall@k/NDCG),
since there is no ground truth and no real users for this dataset. Read-only
against the existing recommendations table and the existing stores
(Postgres/Milvus/Neo4j); usecases/8_1_batch_reactive/recommender/recommend.py
is never re-run.

Usage:
    platform/enrichment/.venv/bin/python -m eval.8_1.run

Writes:
    eval/8_1/results.json   -- metrics 1/2/3/5/6, fully deterministic
    eval/8_1/latency.json   -- metric 4, fresh wall-clock timings (varies run-to-run)
    eval/8_1/tables.md      -- Markdown tables for the thesis
    eval/8_1/figures/*.png
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
USECASE_RECOMMENDER = ROOT.parent.parent / "usecases" / "8_1_batch_reactive"
sys.path.insert(0, str(USECASE_RECOMMENDER))
PLATFORM = ROOT.parent.parent / "platform"
sys.path.insert(0, str(PLATFORM))

import kg_connectivity  # noqa: E402
import latency as latency_module  # noqa: E402
import metrics  # noqa: E402
import store_access  # noqa: E402
from scoring import ranking  # noqa: E402

OUT_DIR = ROOT
FIGURES_DIR = ROOT / "figures"
FIGURES_DIR.mkdir(exist_ok=True)


def fetch_recommendations(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT seed_track_id, recommended_track_id, rank, score FROM recommendations ORDER BY seed_track_id, rank")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_track_meta(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT id, artist_name, genre_tags FROM tracks")
        return {row[0]: {"artist_name": row[1], "genre_tags": row[2]} for row in cur.fetchall()}


def fetch_embeddings(collection) -> dict:
    results = collection.query(expr="track_id >= 0", output_fields=["track_id", "embedding"], limit=1000)
    return {r["track_id"]: np.array(r["embedding"], dtype=np.float64) for r in results}


def group_by_seed(rows: list[dict]) -> dict:
    grouped: dict[int, list[int]] = {}
    for row in rows:
        grouped.setdefault(row["seed_track_id"], []).append(row["recommended_track_id"])
    return grouped


def write_figures(coverage: dict, diversity: dict, latency_summary: dict | None, figures_dir: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figures_dir.mkdir(parents=True, exist_ok=True)

    # Coverage long tail
    counts = sorted((int(v) for v in coverage["appearance_counts_by_track_id"].values()), reverse=True)
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(range(len(counts)), counts, width=1.0)
    ax.set_xlabel("Track rank (by appearance count, descending)")
    ax.set_ylabel("Times recommended (top-10 appearances)")
    ax.set_title("Catalog coverage long tail")
    fig.tight_layout()
    fig.savefig(figures_dir / "coverage_long_tail.png", dpi=150)
    plt.close(fig)

    # Diversity histogram
    values = list(diversity["per_seed"].values())
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(values, bins=30)
    ax.set_xlabel("Mean pairwise cosine distance within top-10")
    ax.set_ylabel("Number of seeds")
    ax.set_title("Intra-list diversity distribution")
    fig.tight_layout()
    fig.savefig(figures_dir / "diversity_histogram.png", dpi=150)
    plt.close(fig)

    # Latency breakdown -- only if latency was measured for this pass
    if latency_summary is not None:
        stages = ["milvus_ms", "neo4j_ms", "ranking_ms"]
        p50s = [latency_summary[s]["p50"] for s in stages]
        p95s = [latency_summary[s]["p95"] for s in stages]
        x = range(len(stages))
        fig, ax = plt.subplots(figsize=(6, 4))
        width = 0.35
        ax.bar([i - width / 2 for i in x], p50s, width, label="p50")
        ax.bar([i + width / 2 for i in x], p95s, width, label="p95")
        ax.set_xticks(list(x))
        ax.set_xticklabels(["Milvus ANN", "Neo4j", "Ranking"])
        ax.set_ylabel("Latency (ms)")
        ax.set_title("Per-stage latency breakdown")
        ax.legend()
        fig.tight_layout()
        fig.savefig(figures_dir / "latency_breakdown.png", dpi=150)
        plt.close(fig)


def write_tables_md(results: dict, latency_summary: dict | None, out_dir: Path, out_prefix: str = "") -> None:
    sig = results["signal_contribution"]
    cov = results["catalog_coverage"]
    div = results["intra_list_diversity"]
    kg = results["kg_connectivity"]
    fail = results["failure_edge_cases"]

    lines = ["# 8.1 Evaluation Tables", ""]

    lines += [
        "## Signal contribution",
        "",
        "| Combination | Rows | % |",
        "|---|---|---|",
    ]
    for combo, count in sig["combination_counts"].items():
        lines.append(f"| {combo} | {count} | {sig['combination_pct'][combo]} |")
    gap = sig["genre_boost_coverage_gap"]
    lines += [
        "",
        f"In similarity pool: {sig['pct_in_similarity_pool']}%.",
        f"Reconstruction-inconsistent rows (see note in results.json): "
        f"{sig['reconstruction_inconsistent_count']} / {sig['total_rows']} ({sig['reconstruction_inconsistent_pct']}%).",
        "",
        f"Genre-boost coverage gap: of {gap['genre_tag_overlap_rows']} rows that genuinely share a genre "
        f"tag with their seed, {gap['missed_by_cap_count']} ({gap['missed_by_cap_pct_of_tag_overlap']}%) "
        "never received GENRE_BOOST because the Semantic API's related_by_genre response is capped at "
        "10 candidates (ordered by shared-genre-count descending as of Stage 15A) -- a confirmed, "
        "still-unaddressed pipeline limitation (the cap itself, not the ordering), not a reconstruction "
        "artifact.",
        "",
    ]

    lines += [
        "## Catalog coverage",
        "",
        f"- Covered: {cov['covered_tracks']} / {cov['total_tracks']} tracks ({cov['pct_covered']}%)",
        f"- Gini coefficient: {cov['gini_coefficient']}",
        f"- Appearance range: {cov['min_appearances']}–{cov['max_appearances']}",
        "",
    ]

    lines += [
        "## Intra-list diversity",
        "",
        f"- Seeds evaluated: {div['seeds_evaluated']} (skipped {div['seeds_skipped_lt_2_recs']} with <2 recommendations)",
        f"- Mean: {div['mean']}, median: {div['median']}, p10: {div['p10']}, p90: {div['p90']}",
        "",
    ]

    if latency_summary is not None:
        lines += [
            "## Latency (measured fresh against the current environment; not the original batch run's historical timing)",
            "",
            "| Stage | p50 (ms) | p95 (ms) | mean (ms) |",
            "|---|---|---|---|",
        ]
        for stage_key, label in [("milvus_ms", "Milvus ANN"), ("neo4j_ms", "Neo4j"), ("ranking_ms", "Ranking"), ("total_ms", "Total")]:
            s = latency_summary[stage_key]
            lines.append(f"| {label} | {s['p50']} | {s['p95']} | {s['mean']} |")
        lines.append("")
    else:
        lines += [
            "## Latency",
            "",
            "Not measured for this pass -- the ranking pipeline's per-stage "
            "latency doesn't depend on which recommendations table is being "
            "evaluated; see the frozen table's tables.md for a fresh reading.",
            "",
        ]

    lines += [
        "## KG connectivity",
        "",
        f"- Tracks / Artists / Genres: {kg['total_tracks']} / {kg['total_artists']} / {kg['total_genres']}",
        f"- Track genre out-degree: mean {kg['track_genre_out_degree']['mean']}, median {kg['track_genre_out_degree']['median']}",
        f"- Artist track out-degree: mean {kg['artist_track_out_degree']['mean']}, median {kg['artist_track_out_degree']['median']}",
        f"- Genre track in-degree: mean {kg['genre_track_in_degree']['mean']}, median {kg['genre_track_in_degree']['median']}",
        f"- Seeds with zero genre siblings (cold start): {kg['zero_genre_sibling_seed_count']}",
        "",
    ]

    lines += [
        "## Failure / edge cases",
        "",
        f"- Seeds with fewer than {fail['expected_top_k']} recommendations: {fail['count']}",
        "",
    ]

    (out_dir / f"{out_prefix}tables.md").write_text("\n".join(lines))


def run_pipeline(
    conn,
    collection,
    neo4j_driver,
    rows: list[dict],
    *,
    all_track_ids: list[int] | None = None,
    out_dir: Path = OUT_DIR,
    figures_dir: Path | None = None,
    out_prefix: str = "",
    measure_latency: bool = True,
) -> dict:
    """Shared evaluation pipeline: given a set of recommendation rows
    (frozen Postgres table, a regenerated table, or a small subset), computes
    all eval/8_1 metrics and writes results.json/tables.md/figures under
    out_dir (namespaced by out_prefix so multiple passes can coexist).
    Returns the results dict. Reused by main() (frozen table), by
    run_regenerated.py (the regenerated table), and by the Stage 15A
    follow-up smoke test (a tiny live subset, written to a tmp dir)."""
    track_meta = fetch_track_meta(conn)
    if all_track_ids is None:
        all_track_ids = sorted(track_meta.keys())
    embeddings = fetch_embeddings(collection)
    rows_by_seed = group_by_seed(rows)
    figures_dir = figures_dir if figures_dir is not None else out_dir / "figures"

    print(f"Evaluating {len(rows)} recommendation rows for {len(rows_by_seed)} seeds, {len(all_track_ids)} tracks.")

    with neo4j_driver.session() as session:
        capped_genre_siblings = kg_connectivity.capped_genre_sibling_ids(session, all_track_ids)
        kg_summary = kg_connectivity.summarize(session, all_track_ids)

    enriched = metrics.reconstruct_signals(rows, track_meta, embeddings, capped_genre_siblings)
    signal_contribution = metrics.signal_contribution_summary(enriched)
    catalog_coverage = metrics.catalog_coverage(rows, all_track_ids)
    intra_list_diversity = metrics.intra_list_diversity(rows_by_seed, embeddings)
    failure_edge_cases = metrics.failure_edge_cases(rows_by_seed, expected_top_k=10)

    results = {
        "signal_contribution": signal_contribution,
        "catalog_coverage": catalog_coverage,
        "intra_list_diversity": intra_list_diversity,
        "kg_connectivity": kg_summary,
        "failure_edge_cases": failure_edge_cases,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{out_prefix}results.json").write_text(json.dumps(results, indent=2, sort_keys=True))
    print(f"Wrote {out_dir / f'{out_prefix}results.json'}")

    latency_summary = None
    if measure_latency:
        print("Measuring latency (fresh, per-stage) across all seeds...")
        per_seed_timings = []
        with neo4j_driver.session() as session:
            for seed_id in all_track_ids:
                per_seed_timings.append(latency_module.time_one_seed(collection, session, ranking.score_recommendations, seed_id))
        latency_summary = latency_module.summarize(per_seed_timings)
        (out_dir / f"{out_prefix}latency.json").write_text(
            json.dumps(
                {
                    "note": "Measured fresh against the current environment. The original stage-5 "
                    "batch run's ~8.8s/411-seed wall-clock (CLAUDE.md) was never broken down by "
                    "stage, so this is not a decomposition of that historical figure.",
                    "summary": latency_summary,
                    "seeds_measured": len(per_seed_timings),
                },
                indent=2,
                sort_keys=True,
            )
        )
        print(f"Wrote {out_dir / f'{out_prefix}latency.json'}")

    write_tables_md(results, latency_summary, out_dir=out_dir, out_prefix=out_prefix)
    print(f"Wrote {out_dir / f'{out_prefix}tables.md'}")

    write_figures(catalog_coverage, intra_list_diversity, latency_summary, figures_dir=figures_dir)
    print(f"Wrote figures to {figures_dir}")

    return results


def main() -> None:
    conn = store_access.connect_postgres()
    collection = store_access.connect_milvus()
    neo4j_driver = store_access.connect_neo4j()

    try:
        rows = fetch_recommendations(conn)
        run_pipeline(conn, collection, neo4j_driver, rows, out_dir=OUT_DIR, figures_dir=FIGURES_DIR)
    finally:
        conn.close()
        store_access.disconnect_milvus()
        neo4j_driver.close()


if __name__ == "__main__":
    main()
