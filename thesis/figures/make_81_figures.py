"""Generate the measurement figures for thesis chapter 5 (use case 8.1).

Companion to make_82_diagrams.py, which draws chapter 6.1's schematic
diagrams. This script plots *data*, so it follows the eval packs' plotting
conventions (eval/8_1/run.py::write_figures, eval/8_2/run.py::write_figures)
for figure mechanics, and make_82_diagrams.py's conventions for anything
thesis-facing: the print-safe palette, the in-figure "Figure 5.N" title, and
dpi/facecolor on save. Run from the repository root:

    platform/enrichment/.venv/bin/python thesis/figures/make_81_figures.py

Writes fig-5-1-score-distribution.png, fig-5-2-concentration.png and
fig-5-3-signal-contribution.png into this directory.

WHY THESE ARE THESIS-OWNED RATHER THAN EVAL-PACK OUTPUT
-------------------------------------------------------
Chapter 6.1 references eval/8_2/figures/*.png in place, so the thesis cites
the evaluation pack's own output and cannot drift from it. That is the better
pattern and it is used here wherever it applies -- but none of these three
figures is an eval-pack output, for a specific reason each:

  5.1  plots reports/uc81/histogram.json, which belongs to the 2026-08-28
       reports/ pack, not to eval/8_1 at all.
  5.2  is a BEFORE/AFTER comparison spanning both eval/8_1 packs. run.py
       computes one pack per invocation and has no notion of the legacy
       comparison; adding it there would extend that script's output contract
       and put its byte-identical-results.json guarantee at risk for a
       thesis-presentation concern.
  5.3  partitions signal_contribution into four mutually exclusive classes,
       which eval/8_1 reports as a table and deliberately does not plot.

Every value plotted here is read from a frozen artifact on disk. Nothing is
recomputed against a live store, and no service needs to be running. The
provenance of each input is recorded in thesis/facts/8_1_eval_facts.md and
restated in FIGURE_MANIFEST.md next to this file.
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUT_DIR = Path(__file__).resolve().parent
REPO = OUT_DIR.parent.parent

# Sources -- frozen artifacts only. See thesis/facts/8_1_eval_facts.md §0 for
# why there are two eval packs and which one is canonical.
CANONICAL = REPO / "eval" / "8_1" / "results.json"          # post-fix, ordered query
LEGACY = REPO / "eval" / "8_1" / "frozen_legacy_results.json"  # pre-fix, unordered query
UC81_HISTOGRAM = REPO / "reports" / "uc81" / "histogram.json"  # pre-fix score histogram
UC81_RESULTS = REPO / "reports" / "uc81" / "results.json"      # pre-fix score summary

# Counted over reports/uc81/results.json::score_distribution.scores; recorded in
# thesis/facts/8_1_eval_facts.md §11 so no figure states a number the facts file lacks.
ABOVE_ONE_COUNT = 203
ABOVE_ONE_PCT = 4.94

# Palette carried over from make_82_diagrams.py: muted, print-safe, and
# distinguishable in greyscale once paired with distinct linestyles.
BEFORE = "#a8792a"   # amber
AFTER = "#2f5c8a"    # blue
ACCENT = "#6b4a86"   # purple
NEUTRAL = "#4a7a3c"  # green


def load(path: Path) -> dict:
    with path.open() as fh:
        return json.load(fh)


def save(fig, name: str) -> None:
    path = OUT_DIR / name
    fig.tight_layout()
    fig.savefig(path, dpi=200, facecolor="white")
    plt.close(fig)
    print(f"wrote {path}")


# --------------------------------------------------------------------------
# Figure 5.1 -- score distribution over the 4,110 rows of the batch run
# --------------------------------------------------------------------------
def fig_score_distribution() -> None:
    """Bars are reports/uc81/histogram.json's OWN pre-computed bins, plotted as
    stored -- not a re-binning of the raw score list. The summary statistics
    come from reports/uc81/results.json::score_distribution.

    This is the PRE-FIX (frozen batch run) distribution, deliberately: chapter 5
    narrates that run, and Section 5.5 reports the canonical post-fix pack
    separately. The caption must say so; see thesis/facts/8_1_eval_facts.md §7
    item E3 for the decision and its rationale.
    """
    hist = load(UC81_HISTOGRAM)
    summary = load(UC81_RESULTS)["score_distribution"]

    lo, width, counts = hist["lo"], hist["bin_width"], hist["counts"]
    edges = [lo + i * width for i in range(len(counts) + 1)]
    centres = [(edges[i] + edges[i + 1]) / 2 for i in range(len(counts))]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar(centres, counts, width=width * 0.94, color=AFTER, edgecolor="white", linewidth=0.4)

    # The >1.0 region is shaded rather than colour-coded per bar. One of the
    # artifact's own bins spans the boundary (left edge 0.9873, right edge
    # 1.0257), so recolouring whole bars would assign that bin entirely to one
    # side of a line it actually straddles.
    ax.axvspan(1.0, edges[-1], color=ACCENT, alpha=0.13, zorder=0)
    ax.axvline(1.0, color="#333333", linewidth=1.0, linestyle=(0, (5, 4)), zorder=3)
    ax.axvline(summary["mean"], color=NEUTRAL, linewidth=1.2, linestyle=":", zorder=3)

    top = max(counts)
    ax.set_ylim(0, top * 1.18)
    ax.annotate(f"mean {summary['mean']}", xy=(summary["mean"], top * 1.13),
                xytext=(-6, 0), textcoords="offset points", ha="right", va="center",
                fontsize=8.5, color=NEUTRAL, fontweight="bold")
    # Top-left is the one large empty region of this distribution; anchoring the
    # note there keeps it off the bars at any y-limit.
    ax.annotate(f"shaded: {ABOVE_ONE_COUNT:,} rows ({ABOVE_ONE_PCT}%) score above 1.0.\n"
                "The additive boosts stack on a similarity score\n"
                "that is not re-normalized back into [0, 1].",
                xy=(0.02, 0.93), xycoords="axes fraction", ha="left", va="top",
                fontsize=8.5, color=ACCENT)

    ax.set_xlabel("Recommendation score")
    ax.set_ylabel("Number of rows (of 4,110)")
    ax.set_title("Figure 5.1 — Score distribution across the 411-seed batch run",
                 fontsize=11.5, pad=10)
    ax.margins(x=0.01)
    save(fig, "fig-5-1-score-distribution.png")


# --------------------------------------------------------------------------
# Figure 5.2 -- popularity concentration, before vs after the ordering fix
# --------------------------------------------------------------------------
def fig_concentration() -> None:
    """The finding figure. Both curves are catalog_coverage.appearance_counts_by_track_id
    read out of the two packs and sorted descending -- the same key in two files,
    so the comparison needs no recomputation.

    Note what the two annotated statistics do NOT agree about: the peak falls by
    38.8% while the Gini coefficient moves by 0.0044. That disagreement is the
    honest reading of the fix and Section 5.5 states it rather than leading with
    whichever number flatters it.
    """
    after = load(CANONICAL)["catalog_coverage"]
    before = load(LEGACY)["catalog_coverage"]

    ys_before = sorted((int(v) for v in before["appearance_counts_by_track_id"].values()), reverse=True)
    ys_after = sorted((int(v) for v in after["appearance_counts_by_track_id"].values()), reverse=True)
    xs = range(1, len(ys_before) + 1)

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(xs, ys_before, color=BEFORE, linestyle="--", linewidth=1.6,
            label=f"before — unordered LIMIT 10 (Gini {before['gini_coefficient']}, "
                  f"{before['covered_tracks']}/{before['total_tracks']} covered)")
    ax.plot(xs, ys_after, color=AFTER, linestyle="-", linewidth=1.6,
            label=f"after — ordered by shared-genre-count (Gini {after['gini_coefficient']}, "
                  f"{after['covered_tracks']}/{after['total_tracks']} covered)")

    for ys, colour, value, dy in ((ys_before, BEFORE, before["max_appearances"], 6),
                                  (ys_after, AFTER, after["max_appearances"], -14)):
        ax.plot([1], [ys[0]], marker="o", ms=6, color=colour, zorder=4)
        ax.annotate(f"peak {value}", xy=(1, ys[0]), xytext=(10, dy),
                    textcoords="offset points", fontsize=8.5, color=colour, fontweight="bold")

    ax.set_xlabel("Track rank (by appearance count, descending)")
    ax.set_ylabel("Times recommended (top-10 appearances)")
    ax.set_title("Figure 5.2 — Popularity concentration before and after the related_by_genre ordering fix",
                 fontsize=11.5, pad=10)
    ax.legend(fontsize=8.5, loc="upper right", framealpha=0.95)
    ax.margins(x=0.01)
    ax.set_ylim(0, max(ys_before) * 1.12)

    # The two curves are indistinguishable past about rank 40; the entire effect
    # of the fix lives in the head, so the head gets its own axes rather than
    # being asserted in the caption.
    inset = ax.inset_axes([0.42, 0.34, 0.34, 0.44])
    head = 50
    inset.plot(range(1, head + 1), ys_before[:head], color=BEFORE, linestyle="--", linewidth=1.5)
    inset.plot(range(1, head + 1), ys_after[:head], color=AFTER, linestyle="-", linewidth=1.5)
    inset.set_title(f"first {head} ranks", fontsize=8, pad=3)
    inset.tick_params(labelsize=7)
    inset.set_facecolor("#fbfbfb")
    save(fig, "fig-5-2-concentration.png")


# --------------------------------------------------------------------------
# Figure 5.3 -- which signals explain each recommendation
# --------------------------------------------------------------------------
def fig_signal_contribution() -> None:
    """signal_contribution.combination_counts from the canonical pack. The four
    classes are mutually exclusive and sum to 4,110 exactly.

    The labels say "similarity + ..." rather than the artifact's raw key names
    ("genre_only", "artist_only") because pct_in_similarity_pool is 100.0: every
    one of the 4,110 rows reached the ranking function through the similarity
    pool, so a boost is always an addition to a similarity score, never an
    alternative route into the list. Using the raw key names in a thesis figure
    would imply four independent sources, which is the opposite of what the
    pipeline does.
    """
    sig = load(CANONICAL)["signal_contribution"]
    counts, pcts = sig["combination_counts"], sig["combination_pct"]

    rows = [
        ("Similarity only", "similarity_only", AFTER),
        ("Similarity + same artist", "artist_only", BEFORE),
        ("Similarity + genre sibling", "genre_only", NEUTRAL),
        ("Similarity + both boosts", "genre_and_artist", ACCENT),
    ]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ys = range(len(rows))
    ax.barh(list(ys), [counts[k] for _, k, _ in rows],
            color=[c for _, _, c in rows], height=0.62, edgecolor="white", linewidth=0.6)

    for i, (_, key, colour) in enumerate(rows):
        ax.annotate(f"{counts[key]:,}  ({pcts[key]}%)", xy=(counts[key], i),
                    xytext=(6, 0), textcoords="offset points",
                    va="center", fontsize=9, color=colour, fontweight="bold")

    ax.set_yticks(list(ys))
    ax.set_yticklabels([label for label, _, _ in rows], fontsize=9.5)
    ax.invert_yaxis()
    ax.set_xlabel(f"Recommendation rows (of {sig['total_rows']:,})")
    ax.set_xlim(0, max(counts.values()) * 1.26)
    ax.set_title("Figure 5.3 — Which signals explain each recommendation",
                 fontsize=11.5, pad=10)
    ax.annotate(f"All {sig['total_rows']:,} rows entered ranking through the similarity pool "
                f"({sig['pct_in_similarity_pool']}%);\nthe graph signals are additive boosts on top of it, "
                f"never an independent route into the list.",
                xy=(0.98, 0.24), xycoords="axes fraction", ha="right", va="center",
                fontsize=8, color="#333333", style="italic")
    save(fig, "fig-5-3-signal-contribution.png")


if __name__ == "__main__":
    fig_score_distribution()
    fig_concentration()
    fig_signal_contribution()
