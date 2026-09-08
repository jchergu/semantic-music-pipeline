"""Generate the architecture/sequence diagrams for thesis chapter 6.1 (use case 8.2).

Drawn with matplotlib rather than Graphviz so the thesis figures need no
tooling beyond `platform/enrichment/.venv`, which already carries matplotlib
for the eval packs. Run from the repository root:

    platform/enrichment/.venv/bin/python thesis/figures/make_82_diagrams.py

Writes fig-6-1-runtime.png, fig-6-2-cold-warm.png and fig-6-3-warm-path.png
into this directory. The measurement figures in chapter 6.1 are NOT generated
here -- they are eval/8_2/figures/*.png, referenced in place so the thesis
cites the evaluation pack's own output rather than a copy of it.
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

OUT_DIR = Path(__file__).resolve().parent

# Muted, print-safe palette: distinguishable in colour and in greyscale.
STYLES = {
    "service": dict(fc="#dbe7f3", ec="#2f5c8a", lw=1.4),
    "broker": dict(fc="#fbeed3", ec="#a8792a", lw=1.4),
    "store": dict(fc="#dfeeda", ec="#4a7a3c", lw=1.4),
    "external": dict(fc="#ececec", ec="#5f5f5f", lw=1.4),
    "compute": dict(fc="#e8dff0", ec="#6b4a86", lw=1.4),
}


def box(ax, x, y, w, h, title, subtitle=None, style="service", fs=10, sub_fs=8):
    """Draw a rounded box anchored at its lower-left corner."""
    ax.add_patch(
        FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.0,rounding_size=1.2",
            mutation_aspect=1.0, zorder=2, **STYLES[style],
        )
    )
    if subtitle:
        ax.text(x + w / 2, y + h * 0.62, title, ha="center", va="center",
                fontsize=fs, fontweight="bold", zorder=3)
        ax.text(x + w / 2, y + h * 0.27, subtitle, ha="center", va="center",
                fontsize=sub_fs, color="#333333", zorder=3, linespacing=1.35)
    else:
        ax.text(x + w / 2, y + h / 2, title, ha="center", va="center",
                fontsize=fs, fontweight="bold", zorder=3, linespacing=1.35)
    return (x, y, w, h)


def arrow(ax, xy_from, xy_to, label=None, dashed=False, color="#333333",
          rad=0.0, fs=8, lx=0.0, ly=0.0, ha="center"):
    ax.add_patch(
        FancyArrowPatch(
            xy_from, xy_to,
            arrowstyle="-|>", mutation_scale=13,
            linewidth=1.2, color=color, zorder=4,
            linestyle=(0, (4, 3)) if dashed else "solid",
            connectionstyle=f"arc3,rad={rad}",
            shrinkA=1, shrinkB=1,
        )
    )
    if label:
        mx = (xy_from[0] + xy_to[0]) / 2 + lx
        my = (xy_from[1] + xy_to[1]) / 2 + ly
        ax.text(mx, my, label, ha=ha, va="center", fontsize=fs,
                color=color, zorder=5, linespacing=1.3,
                bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none", alpha=0.85))


def canvas(w_in, h_in, xlim=(0, 100), ylim=(0, 100)):
    fig, ax = plt.subplots(figsize=(w_in, h_in))
    ax.set_xlim(*xlim)
    ax.set_ylim(*ylim)
    ax.axis("off")
    return fig, ax


def save(fig, name):
    path = OUT_DIR / name
    fig.savefig(path, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"wrote {path}")


# --------------------------------------------------------------------------
# Figure 6.1 -- 8.2 runtime: one topic, three independent consumer groups
# --------------------------------------------------------------------------
def fig_runtime():
    fig, ax = canvas(15.5, 7.6, xlim=(0, 120), ylim=(0, 100))

    # --- producers -------------------------------------------------------
    box(ax, 1, 60, 16, 14, "Listening session",
        "stage 12 simulator:\nscripted sessions,\n--seed / --speed", "external")
    box(ax, 20, 60, 16, 14, "Event ingestion", "FastAPI :8020\nPOST /events\n(stage 11)")
    arrow(ax, (17, 67), (20, 67))

    # --- Kafka spine -----------------------------------------------------
    box(ax, 40, 28, 8, 56, "behavioral-events", None, "broker", fs=10)
    ax.text(44, 25.5, "Kafka topic\n(never purged)", ha="center", va="top",
            fontsize=8, color="#8a6318")
    arrow(ax, (36, 67), (40, 67), "produce", ly=3.4)
    ax.text(44, 88.5, "three independent consumer groups —\nKafka fan-out gives each the full stream",
            ha="center", va="bottom", fontsize=8.5, color="#8a6318")

    # --- consumers -------------------------------------------------------
    box(ax, 53, 71, 24, 13, "Flink session-profile job",
        "5 min window / 30 s slide,\nevent-time watermarks (5 s)\n— stage 13", "compute")
    box(ax, 53, 50, 24, 13, "session_consumer_daemon",
        "group platform-session-consumer\nRAW state (Decision C)\n— stages 9–10, 16")
    box(ax, 53, 29, 24, 13, "refresh_daemon",
        "group platform-recs-refresh\nDERIVED recs, debounced\n— stages 14, 16")
    for y in (77.5, 56.5, 35.5):
        arrow(ax, (48, y), (53, y))

    # --- session state ---------------------------------------------------
    box(ax, 84, 71, 18, 13, "Redis",
        "session:{id}:profile\nsession:{id}:profile_meta", "store")
    box(ax, 84, 50, 18, 13, "Redis + PostgreSQL",
        "session:{id}:events\nevents table\n(system of record)", "store", fs=9.5)
    box(ax, 84, 29, 18, 13, "Redis",
        "session:{id}:recs\nsession:{id}:refresh_meta", "store")
    for y in (79.5, 58.5, 37.5):
        arrow(ax, (77, y), (84, y), "write", ly=2.6, fs=7.5)

    # refresh_daemon's two reads, bowed through the corridor
    arrow(ax, (84, 73.5), (77, 40.5), None, dashed=True, color="#3d6b31", rad=0.32)
    arrow(ax, (84, 52.5), (77, 38.5), None, dashed=True, color="#3d6b31", rad=0.28)
    ax.text(80.4, 47.0, "read", ha="center", va="center", fontsize=7.5, color="#3d6b31",
            bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.9))

    # --- Semantic API ----------------------------------------------------
    box(ax, 53, 6, 24, 13, "Semantic API",
        "FastAPI :8000, read-only\n(stage 4, shared with 8.1)")
    box(ax, 82, 6, 21, 13, "PostgreSQL · Milvus · Neo4j",
        "catalog, embeddings, graph", "store", fs=8.5, sub_fs=8)
    arrow(ax, (77, 10), (82, 10))
    arrow(ax, (58, 29), (58, 19), None, dashed=True)
    ax.text(59.5, 24.0, "HTTP: genre siblings,\nsame artist", ha="left", va="center",
            fontsize=7.5, color="#333333")
    arrow(ax, (72, 29), (89, 19), None, dashed=True, rad=-0.22)
    ax.text(85.5, 24.0, "Milvus ANN over the profile\nvector (alias recs-refresh)",
            ha="center", va="center", fontsize=7.5, color="#333333",
            bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.9))

    # --- delivery --------------------------------------------------------
    box(ax, 104, 48, 15, 15, "session_api",
        "FastAPI :8030\nread-only, Redis only\n— stage 16", fs=9.5, sub_fs=7.5)
    box(ax, 104, 24, 15, 16, "Client",
        "GET /sessions/{id}/\nrecommendations · /profile\n(request/response,\nnot WebSocket)",
        "external", fs=9.5, sub_fs=7.5)
    arrow(ax, (102, 40), (108, 48), None, dashed=True, color="#3d6b31", rad=-0.2)
    arrow(ax, (114, 48), (114, 40))
    ax.text(111.5, 66.0, "serves what is already\nwritten; computes nothing",
            ha="center", va="bottom", fontsize=7.5, color="#3d6b31")

    ax.set_title("Figure 6.1 — Use case 8.2 runtime: one behavioural-event topic, three independent consumer groups",
                 fontsize=11.5, pad=12)
    save(fig, "fig-6-1-runtime.png")


# --------------------------------------------------------------------------
# Figure 6.2 -- the cold-start -> warm handoff as a race between two groups
# --------------------------------------------------------------------------
def fig_cold_warm():
    fig, ax = canvas(13.0, 6.6, xlim=(0, 100), ylim=(0, 78))

    lanes = [
        (64, "behavioral-events", "#8a6318"),
        (50, "Flink job\n(group: Flink's own)", "#6b4a86"),
        (36, "refresh_daemon\n(group platform-recs-refresh)", "#2f5c8a"),
        (20, "session:{id}:recs\n(what the client can read)", "#3d6b31"),
    ]
    for y, label, colour in lanes:
        ax.plot([16, 96], [y, y], color="#b8b8b8", lw=1.0, zorder=1)
        ax.text(15, y, label, ha="right", va="center", fontsize=8.5,
                color=colour, linespacing=1.35)

    # event ticks on the topic lane
    ev_x = [20, 27, 34, 41, 48, 55, 62, 69, 76, 83, 90]
    for i, x in enumerate(ev_x):
        ax.plot([x], [64], marker="|", ms=11, mew=1.6, color="#8a6318", zorder=3)
        ax.text(x, 68.2, f"e{i + 1}", ha="center", va="bottom", fontsize=7, color="#8a6318")

    # Flink: window fires only once enough event time has passed
    ax.add_patch(FancyBboxPatch((20, 47), 34, 6,
                                boxstyle="round,pad=0.0,rounding_size=1.0",
                                fc="#efe7f5", ec="#6b4a86", lw=1.1, zorder=2))
    ax.text(37, 50, "5 min sliding window accumulating — no profile key yet",
            ha="center", va="center", fontsize=8, color="#4a3060", zorder=3)
    ax.plot([54], [50], marker="o", ms=8, color="#6b4a86", zorder=4)
    ax.text(50, 56.5, "first window fires:\nsession:{id}:profile written", ha="right",
            va="bottom", fontsize=8, color="#6b4a86")

    # refresh daemon decisions
    for x, kind in [(27, "cold"), (41, "cold"), (62, "warm"), (76, "warm"), (90, "warm")]:
        colour = "#a8792a" if kind == "cold" else "#2f5c8a"
        ax.plot([x], [36], marker="o", ms=8, color=colour, zorder=4)
        arrow(ax, (x, 34.6), (x, 22.4), color=colour)
        ax.plot([x], [20], marker="s", ms=7, color=colour, zorder=4)

    ax.text(34, 41.0, "cold start: 8.1's own batch path\n(seed = session's first track)",
            ha="center", va="bottom", fontsize=8, color="#a8792a")
    ax.text(76, 41.0, "warm path: Milvus ANN over the profile vector,\nplayed tracks excluded, re-ranked on active context",
            ha="center", va="bottom", fontsize=8, color="#2f5c8a")

    # the handoff
    ax.plot([58, 58], [12, 70], color="#333333", lw=1.1, ls=(0, (5, 4)), zorder=1)
    ax.text(58, 9.0, "handoff — measured at 5.3 s and 1 cold-start refresh\n(median profile-compute lag H2 = 2.12 s)",
            ha="center", va="top", fontsize=8.5, fontweight="bold")

    ax.text(50, 3.0,
            "The handoff is a race between two independently scheduled consumer groups, not a fixed event count:\n"
            "the refresh loop uses the profile as soon as one exists, and falls back to the batch path until then.",
            ha="center", va="top", fontsize=8, color="#333333", style="italic")

    ax.text(15, 28, "debounce: at most one refresh\nper 5 s or per 3 events",
            ha="right", va="center", fontsize=8, color="#3d6b31")

    ax.set_title("Figure 6.2 — Cold start to warm path: the handoff is a race, not a threshold",
                 fontsize=11.5, pad=12)
    save(fig, "fig-6-2-cold-warm.png")


# --------------------------------------------------------------------------
# Figure 6.3 -- what a warm refresh actually computes
# --------------------------------------------------------------------------
def fig_warm_path():
    fig, ax = canvas(13.0, 6.2, xlim=(0, 100), ylim=(0, 72))

    box(ax, 1, 52, 20, 13, "session:{id}:profile",
        "recency-decayed weighted\ncentroid of session track\nembeddings (512-d)", "store", fs=9.5)
    box(ax, 1, 30, 20, 13, "session:{id}:events",
        "raw event log:\nplayed tracks, and the\nmost recent track", "store", fs=9.5)

    box(ax, 29, 52, 24, 13, "Milvus ANN search",
        "over the profile vector,\nexpr excludes every\ntrack already played", "compute", fs=9.5)
    box(ax, 29, 30, 24, 13, "Semantic API",
        "/tracks/{id}/graph,\n/artists/{name}/tracks\nanchored on the last track", fs=9.5)
    box(ax, 29, 8, 24, 13, "Played-track filter",
        "re-applied to the graph\ncandidates: Neo4j does not\nknow session history", "compute", fs=9.5)

    box(ax, 58, 27, 26, 19, "ranking.score_recommendations()",
        "score = similarity + 0.05 genre sibling\n+ 0.15 same artist\n\nunchanged from 8.1 (Decision D)",
        fs=9, sub_fs=8)
    box(ax, 87, 29, 12, 15, "session:{id}:recs",
        "top-10,\nself-contained rows", "store", fs=8.5, sub_fs=7.5)

    arrow(ax, (21, 58), (29, 58))
    arrow(ax, (21, 36), (29, 36))
    arrow(ax, (41, 30), (41, 21), None, dashed=True)
    arrow(ax, (53, 58), (58, 42), rad=-0.18)
    arrow(ax, (53, 14), (58, 31), rad=0.18)
    arrow(ax, (84, 36.5), (87, 36.5))

    ax.text(55.5, 51.0, "candidate set A", ha="center", va="center", fontsize=8, color="#333333")
    ax.text(57.0, 20.0, "candidate set B", ha="left", va="center", fontsize=8, color="#333333")

    ax.text(50, 3.0,
            "The warm path is the only part of 8.2 that is genuinely new: the acoustic signal comes from the session's own centroid\n"
            "rather than from a single seed track, while the graph signals and the scoring function are 8.1's, reused unchanged.",
            ha="center", va="bottom", fontsize=8, color="#333333", style="italic")

    ax.set_title("Figure 6.3 — What one warm refresh computes",
                 fontsize=11.5, pad=12)
    save(fig, "fig-6-3-warm-path.png")


if __name__ == "__main__":
    fig_runtime()
    fig_cold_warm()
    fig_warm_path()
