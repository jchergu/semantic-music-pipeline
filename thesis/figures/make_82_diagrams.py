"""Generate the architecture/sequence diagrams for thesis chapters 4 and 6.

Drawn with matplotlib rather than Graphviz so the thesis figures need no
tooling beyond `platform/enrichment/.venv`, which already carries matplotlib
for the eval packs. Run from the repository root:

    platform/enrichment/.venv/bin/python thesis/figures/make_82_diagrams.py

Writes fig-4-1-architecture.png, fig-6-1-runtime.png, fig-6-2-cold-warm.png,
fig-6-3-warm-path.png and fig-6-9-uc83-design.png into this directory. The
measurement figures in chapter 6.1 are NOT generated here -- they are
eval/8_2/figures/*.png, referenced in place so the thesis cites the
evaluation pack's own output rather than a copy of it.

Figure 4.1 (Chapter 4's three-layer architecture overview) and Figure 6.9
(Chapter 6.2, use case 8.3's design) both live in this file rather than in
one of their own, for the same reason: everything here is *drawn* from
hardcoded coordinates rather than plotted from data, so a cross-chapter or
unimplemented-use-case diagram costs nothing and claims nothing -- unlike
make_81_figures.py, which cannot produce a figure without a completed
evaluation run. Figure 4.1 replaces a hand-drawn PNG that had drifted out of
sync with the built system (OWL/Protégé/Jena, "ChromaDB for MVP",
LightGCN+CF, `media.*`/`events.*` topic names, the pre-rename title); this
version is regenerated from the same source of truth as every other diagram
here, so it can't drift the same way again.
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Arc, Ellipse, FancyArrowPatch, FancyBboxPatch, Rectangle

OUT_DIR = Path(__file__).resolve().parent

# Muted, print-safe palette: distinguishable in colour and in greyscale.
STYLES = {
    "service": dict(fc="#dbe7f3", ec="#2f5c8a", lw=1.4),
    "broker": dict(fc="#fbeed3", ec="#a8792a", lw=1.4),
    "store": dict(fc="#dfeeda", ec="#4a7a3c", lw=1.4),
    "external": dict(fc="#ececec", ec="#5f5f5f", lw=1.4),
    "compute": dict(fc="#e8dff0", ec="#6b4a86", lw=1.4),
}


def box(ax, x, y, w, h, title, subtitle=None, style="service", fs=10, sub_fs=8,
        dashed=False):
    """Draw a rounded box anchored at its lower-left corner.

    `dashed=True` marks a component that does not exist: a dashed edge over a
    white fill, which stays distinguishable from a solid filled box in
    greyscale as well as in colour. Used only by Figure 6.9, where the whole
    point is which half of the diagram is built and which half is proposed.
    """
    style_kw = dict(STYLES[style])
    if dashed:
        style_kw["linestyle"] = (0, (4, 3))
        style_kw["fc"] = "white"
    ax.add_patch(
        FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.0,rounding_size=1.2",
            mutation_aspect=1.0, zorder=2, **style_kw,
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


def cylinder(ax, x, y, w, h, title, subtitle=None, style="store", fs=10, sub_fs=8):
    """Draw a database cylinder, anchored at its lower-left corner.

    Reserved for actual storage systems (Postgres, Milvus, Neo4j, Redis,
    object/feature stores) so a reader can tell "stores data" from "does
    work" at a glance, independently of the colour legend. Built from two
    ellipses (top lid, bottom cap) plus straight sides, rather than
    FancyBboxPatch's rounded rectangle that every other component uses.
    """
    style_kw = dict(STYLES[style])
    ec, fc, lw = style_kw["ec"], style_kw["fc"], style_kw["lw"]
    ry = min(h * 0.12, w * 0.10, 2.0)
    cx = x + w / 2
    ax.add_patch(Rectangle((x, y + ry), w, h - 2 * ry, fc=fc, ec="none", zorder=2))
    ax.add_patch(Ellipse((cx, y + ry), w, 2 * ry, fc=fc, ec="none", zorder=1.8))
    ax.add_patch(Arc((cx, y + ry), w, 2 * ry, theta1=180, theta2=360,
                      ec=ec, lw=lw, zorder=2.3))
    ax.plot([x, x], [y + ry, y + h - ry], color=ec, lw=lw, zorder=2.3)
    ax.plot([x + w, x + w], [y + ry, y + h - ry], color=ec, lw=lw, zorder=2.3)
    ax.add_patch(Ellipse((cx, y + h - ry), w, 2 * ry, fc=fc, ec=ec, lw=lw, zorder=3))
    body_lo, body_hi = y + ry, y + h - 2 * ry
    if subtitle:
        ax.text(cx, body_lo + (body_hi - body_lo) * 0.68, title, ha="center", va="center",
                fontsize=fs, fontweight="bold", zorder=4)
        ax.text(cx, body_lo + (body_hi - body_lo) * 0.28, subtitle, ha="center", va="center",
                fontsize=sub_fs, color="#333333", zorder=4, linespacing=1.35)
    else:
        ax.text(cx, (body_lo + body_hi) / 2, title, ha="center", va="center",
                fontsize=fs, fontweight="bold", zorder=4, linespacing=1.35)
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
                bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none", alpha=1.0))


def canvas(w_in, h_in, xlim=(0, 100), ylim=(0, 100)):
    fig, ax = plt.subplots(figsize=(w_in, h_in))
    ax.set_xlim(*xlim)
    ax.set_ylim(*ylim)
    ax.axis("off")
    return fig, ax


def save(fig, name):
    path = OUT_DIR / name
    fig.savefig(path, dpi=300, bbox_inches="tight", facecolor="white")
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
    cylinder(ax, 84, 71, 18, 13, "Redis",
             "session:{id}:profile\nsession:{id}:profile_meta")
    cylinder(ax, 84, 50, 18, 13, "Redis + PostgreSQL",
             "session:{id}:events\nevents table (system of record)", fs=9.5, sub_fs=7.3)
    cylinder(ax, 84, 29, 18, 13, "Redis",
             "session:{id}:recs\nsession:{id}:refresh_meta")
    for y in (79.5, 58.5, 37.5):
        arrow(ax, (77, y), (84, y), "write", ly=2.6, fs=7.5)

    # refresh_daemon's two reads. Straight (rad=0), not bowed: the corridor
    # between the daemon column (x<=77) and the store column (x>=84) is
    # already clear, so any curvature only risks pushing the line into a
    # cylinder it isn't pointing at. Entry points into refresh_daemon are
    # spread top vs. bottom of its right edge rather than converging on
    # nearly the same point. Labels are placed a quarter of the way along
    # each line (not at its midpoint, arrow()'s default) specifically to
    # avoid landing on top of the row "write" labels, which all sit at the
    # corridor's x-midpoint (80.5) by construction.
    arrow(ax, (84, 75), (77, 41.5), None, dashed=True, color="#3d6b31")
    ax.text(82.25, 66.6, "read", ha="center", va="center", fontsize=7.5, color="#3d6b31",
            zorder=5, bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=1.0))
    arrow(ax, (84, 54), (77, 30), None, dashed=True, color="#3d6b31")
    ax.text(82.25, 48.0, "read", ha="center", va="center", fontsize=7.5, color="#3d6b31",
            zorder=5, bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=1.0))

    # --- Semantic API ----------------------------------------------------
    box(ax, 53, 6, 24, 13, "Semantic API",
        "FastAPI :8000, read-only\n(stage 4, shared with 8.1)")
    cylinder(ax, 82, 6, 21, 13, "PostgreSQL · Milvus · Neo4j",
             "catalog, embeddings, graph", fs=8.5, sub_fs=8)
    arrow(ax, (77, 10), (82, 10))
    arrow(ax, (58, 29), (58, 19), None, dashed=True)
    ax.text(59.5, 24.0, "HTTP: genre siblings,\nsame artist", ha="left", va="center",
            fontsize=7.5, color="#333333",
            bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=1.0))
    arrow(ax, (72, 29), (89, 19), None, dashed=True, rad=-0.22)
    ax.text(85.5, 24.0, "Milvus ANN over the profile\nvector (alias recs-refresh)",
            ha="center", va="center", fontsize=7.5, color="#333333",
            bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=1.0))

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

    cylinder(ax, 1, 52, 20, 13, "session:{id}:profile",
             "recency-decayed weighted\ncentroid of session track\nembeddings (512-d)", fs=9.5, sub_fs=7.3)
    cylinder(ax, 1, 30, 20, 13, "session:{id}:events",
             "raw event log:\nplayed tracks, and the\nmost recent track", fs=9.5, sub_fs=7.3)

    box(ax, 29, 52, 24, 13, "Milvus ANN search",
        "over the profile vector,\nexpr excludes every\ntrack already played", "compute", fs=9.5)
    box(ax, 29, 30, 24, 13, "Semantic API",
        "/tracks/{id}/graph,\n/artists/{name}/tracks\nanchored on the last track", fs=9.5)
    box(ax, 29, 8, 24, 13, "Played-track filter",
        "re-applied to the graph\ncandidates: Neo4j does not\nknow session history", "compute", fs=9.5)

    box(ax, 58, 27, 26, 19, "ranking.score_recommendations()",
        "score = similarity + 0.05 genre sibling\n+ 0.15 same artist\n\nunchanged from 8.1 (Decision D)",
        fs=9, sub_fs=8)
    cylinder(ax, 87, 29, 12, 15, "session:{id}:recs",
             "top-10,\nself-contained rows", fs=8.5, sub_fs=7.5)

    arrow(ax, (21, 58), (29, 58))
    arrow(ax, (21, 36), (29, 36))
    arrow(ax, (41, 30), (41, 21), None, dashed=True)
    arrow(ax, (53, 58), (58, 42))
    arrow(ax, (53, 14), (58, 31))
    arrow(ax, (84, 36.5), (87, 36.5))

    ax.text(55.5, 51.0, "candidate set A", ha="center", va="center", fontsize=8, color="#333333",
            zorder=5, bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none", alpha=1.0))
    ax.text(57.0, 20.0, "candidate set B", ha="left", va="center", fontsize=8, color="#333333",
            zorder=5, bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none", alpha=1.0))

    ax.text(50, 3.0,
            "The warm path is the only part of 8.2 that is genuinely new: the acoustic signal comes from the session's own centroid\n"
            "rather than from a single seed track, while the graph signals and the scoring function are 8.1's, reused unchanged.",
            ha="center", va="bottom", fontsize=8, color="#333333", style="italic")

    ax.set_title("Figure 6.3 — What one warm refresh computes",
                 fontsize=11.5, pad=12)
    save(fig, "fig-6-3-warm-path.png")


# --------------------------------------------------------------------------
# Figure 6.9 -- 8.3 as designed: the built lower lane, the proposed upper one
# --------------------------------------------------------------------------
def fig_uc83_design():
    fig, ax = canvas(15.5, 8.6, xlim=(0, 124), ylim=(0, 104))

    # --- upper lane: proposed, none of it implemented ---------------------
    ax.text(62, 95.5, "PROPOSED — none of this is implemented (Sections 6.2.3, 6.2.5)",
            ha="center", va="bottom", fontsize=9.5, color="#5f5f5f",
            fontweight="bold", zorder=6,
            bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none"))

    box(ax, 1, 76, 17, 15, "Freesound clip",
        "CC audio served as a\nsimulated live stream,\nnot hardware capture",
        "external", dashed=True, fs=9.5, sub_fs=7.5)
    box(ax, 23, 76, 15, 15, "media-stream",
        "Kafka topic:\nexists since stage 7,\nnever produced to",
        "broker", fs=9.5, sub_fs=7.5)
    box(ax, 43, 76, 21, 15, "Live CLAP + auto-tagger",
        "classifier only —\nno live writes to Neo4j\n(Semantic API endpoint:\ncontract revision)",
        "compute", dashed=True, fs=9.5, sub_fs=7.5)
    box(ax, 69, 76, 19, 15, "Trigger policy",
        "when to interrupt —\nthe question the design\ndoes not settle (6.2.7)",
        "compute", dashed=True, fs=9.5, sub_fs=7.5)
    box(ax, 93, 76, 16, 15, "Push delivery",
        "WebSocket: the user\nnever asked\n(Decision E)",
        dashed=True, fs=9.5, sub_fs=7.5)
    box(ax, 112, 76, 11, 15, "Client", None, "external", dashed=True, fs=9.5)

    for x0, x1 in ((18, 23), (38, 43), (64, 69), (88, 93), (109, 112)):
        arrow(ax, (x0, 83.5), (x1, 83.5), dashed=True, color="#5f5f5f")

    # --- lower lane: built, verified and measured in 6.1 ------------------
    ax.text(62, 47.5,
            "BUILT, VERIFIED AND MEASURED IN SECTION 6.1 — reused unchanged (Table 6.9)",
            ha="center", va="bottom", fontsize=9.5, color="#2f5c8a",
            fontweight="bold", zorder=6,
            bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none"))

    box(ax, 1, 28, 17, 15, "Listening session",
        "stage 12 simulator:\nscripted, --seed / --speed", "external",
        fs=9.5, sub_fs=7.5)
    box(ax, 23, 28, 15, 15, "behavioral-events",
        "Kafka topic\n(stages 7, 11)", "broker", fs=9.5, sub_fs=7.5)
    box(ax, 43, 28, 21, 15, "Three consumer groups",
        "Flink profile job ·\nraw-state daemon ·\nrefresh daemon", "compute",
        fs=9.5, sub_fs=7.5)
    cylinder(ax, 69, 28, 19, 15, "Redis + PostgreSQL",
             "session:{id}:events\n:profile · :recs\n(Decision C)",
             fs=9.5, sub_fs=7.5)
    box(ax, 93, 28, 16, 15, "session_api",
        "read-only, Redis only\nrequest/response\n(stage 16)", fs=9.5, sub_fs=7.5)
    box(ax, 112, 28, 11, 15, "Client", None, "external", fs=9.5)

    for x0, x1 in ((18, 35.5), (38, 43), (64, 69), (88, 93), (109, 112)):
        arrow(ax, (x0, 35.5), (x1, 35.5))

    # --- the whole of 8.3's coupling to the platform: two arrows ----------
    arrow(ax, (49, 76), (31, 43), None, dashed=True, color="#6b4a86")
    ax.text(2.0, 62.0,
            "the recognised track enters\nas an ordinary behavioural event —\nno new ingestion path",
            ha="left", va="center", fontsize=7.8, color="#6b4a86", zorder=6,
            bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none", alpha=1.0))

    arrow(ax, (78.5, 43), (78.5, 76), None, dashed=True, color="#4a7a3c")
    ax.text(81.0, 62.0,
            "reads :profile and :recs,\nalready written and measured\n(Sections 6.1.8–6.1.10)",
            ha="left", va="center", fontsize=7.8, color="#4a7a3c",
            bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none", alpha=1.0))

    # --- legend ----------------------------------------------------------
    box(ax, 3, 13, 8, 6.5, "", None, "service")
    ax.text(12.5, 16.25, "built and verified in Section 6.1", ha="left",
            va="center", fontsize=8.5)
    box(ax, 3, 4, 8, 6.5, "", None, "service", dashed=True)
    ax.text(12.5, 7.25, "proposed — not implemented", ha="left",
            va="center", fontsize=8.5)

    ax.text(122, 10.0,
            "The two vertical arrows are the whole of 8.3's coupling to the platform: it produces ordinary behavioural events\n"
            "and reads derived session state. That is why the lower lane needs no change, and why nothing on the upper lane\n"
            "can be given a measured result in this chapter.",
            ha="right", va="center", fontsize=8, color="#333333", style="italic")

    ax.set_title("Figure 6.9 — Use case 8.3 as designed: what already exists, and what would have to be built",
                 fontsize=11.5, pad=12)
    save(fig, "fig-6-9-uc83-design.png")


# --------------------------------------------------------------------------
# Figure 4.1 -- the three-layer architecture (Chapter 4)
# --------------------------------------------------------------------------
def fig_architecture():
    fig, ax = canvas(16.0, 10.4, xlim=(0, 128), ylim=(0, 108))

    ax.text(64, 105.5,
            "Data-Driven Context-Aware Pipelining for Multimodal Music Systems",
            ha="center", va="top", fontsize=11, fontweight="bold", color="#333333")

    # --- Layer 1: Data Preparation ----------------------------------------
    ax.text(2, 99.5, "LAYER 1 -- DATA PREPARATION", ha="left", va="bottom",
            fontsize=9, fontweight="bold", color="#a8792a")

    box(ax, 2, 84, 18, 13, "media-stream", "Kafka topic", "broker", fs=9.5)
    box(ax, 22, 84, 20, 13, "behavioral-events", "Kafka topic", "broker", fs=9.5)
    box(ax, 46, 84, 24, 13, "Ingestion / Preparation",
        "cleaning, dedup,\nmodality-specific\nfeature extraction", fs=9, sub_fs=7.5)
    cylinder(ax, 74, 84, 16, 13, "MinIO / S3", "raw media\n(object store)", fs=9, sub_fs=7.5)
    cylinder(ax, 92, 84, 17, 13, "Parquet / Delta",
             "feature store\n(columnar)", fs=9, sub_fs=7.5)
    cylinder(ax, 111, 84, 15, 13, "PostgreSQL",
             "system of\nrecord", fs=9, sub_fs=7.5)

    arrow(ax, (20, 90.5), (22, 90.5))
    arrow(ax, (42, 90.5), (46, 90.5))
    arrow(ax, (70, 90.5), (74, 90.5))
    arrow(ax, (90, 90.5), (92, 90.5))
    arrow(ax, (109, 90.5), (111, 90.5))
    ax.text(101, 81.5, "joined on a common MusicBrainz recording ID",
            ha="center", va="top", fontsize=7.5, color="#4a7a3c")

    # --- Layer 2: Semantic Enrichment --------------------------------------
    ax.text(2, 76.5, "LAYER 2 -- SEMANTIC ENRICHMENT", ha="left", va="bottom",
            fontsize=9, fontweight="bold", color="#6b4a86")

    box(ax, 6, 58, 26, 13, "Spark",
        "nightly batch\nenrichment", "compute", fs=9.5)
    box(ax, 36, 58, 28, 13, "Kafka + Flink",
        "real-time streaming\nenrichment (stateful,\nwindowed)", "compute", fs=9.5, sub_fs=7.5)
    box(ax, 78, 58, 22, 13, "CLAP embeddings",
        "audio-text joint\nembedding model", "compute", fs=9.5, sub_fs=7.5)

    # L1 stores feed both enrichment paths (batch and streaming) and the
    # embedding computation; the two Kafka topics feed the streaming path
    # directly too, since Layer 2 is stateful over the live event stream.
    arrow(ax, (54, 84), (19, 71))
    arrow(ax, (60, 84), (50, 71))
    arrow(ax, (100, 84), (89, 71))

    cylinder(ax, 8, 40, 20, 13, "Neo4j",
             "knowledge graph\n(artist / genre /\nrelations)", fs=9.5, sub_fs=7.5)
    cylinder(ax, 32, 40, 20, 13, "Milvus",
             "vector index\n(similarity search)", fs=9.5, sub_fs=7.5)

    # Structural enrichment (batch or streaming) projects the KG; only the
    # embedding step feeds the vector index -- these do not cross.
    arrow(ax, (19, 58), (17, 53), None, color="#6b4a86")
    arrow(ax, (42, 58), (26, 53), None, color="#6b4a86")
    arrow(ax, (85, 58), (46, 53), None, color="#6b4a86")
    ax.text(60, 37, "joined on the same MusicBrainz / internal track ID",
            ha="center", va="top", fontsize=7.5, color="#4a7a3c")

    # --- Layer 3: Semantic API / Application -------------------------------
    ax.text(2, 33, "LAYER 3 -- SEMANTIC API / APPLICATION", ha="left", va="bottom",
            fontsize=9, fontweight="bold", color="#2f5c8a")

    box(ax, 40, 18, 30, 12, "Semantic API",
        "FastAPI, read-only,\nshared by every\nconsumer below", fs=10, sub_fs=8)
    arrow(ax, (18, 40), (48, 30), None, color="#4a7a3c")
    arrow(ax, (42, 40), (57, 30), None, color="#4a7a3c")

    box(ax, 2, 2, 26, 12, "Recommender\nEngine  ★", None, fs=9.5)
    box(ax, 32, 2, 22, 12, "Similarity\nSearch", None, fs=9.5)
    box(ax, 58, 2, 20, 12, "Auto-tagging", None, fs=9.5)
    box(ax, 82, 2, 24, 12, "Playlist\nGeneration", None, fs=9.5)
    arrow(ax, (45, 18), (15, 14))
    arrow(ax, (48, 18), (43, 14))
    arrow(ax, (62, 18), (68, 14))
    arrow(ax, (65, 18), (94, 14))

    ax.text(126, 6,
            "Recommender Engine (★) is one\nsibling consumer among several --\nnot the API's specification.",
            ha="right", va="center", fontsize=7.8, color="#333333", style="italic")

    # --- legend (top-right, beside the LAYER 1 label) -----------------------
    legend_items = [("service", "service"), ("broker", "Kafka topic"),
                    ("store", "store"), ("compute", "compute / enrichment")]
    for i, (style, label) in enumerate(legend_items):
        lx = 52 + i * 19
        if style == "store":
            cylinder(ax, lx, 100.3, 3, 3, "")
        else:
            box(ax, lx, 100.3, 3, 3, "", None, style)
        ax.text(lx + 4, 101.8, label, ha="left", va="center", fontsize=7.8)

    save(fig, "fig-4-1-architecture.png")


# --------------------------------------------------------------------------
# Figure 5.5 -- 8.1's internal module structure (Chapter 5, Section 5.6.1)
# --------------------------------------------------------------------------
def fig_uc81_modules():
    fig, ax = canvas(14.5, 6.4, xlim=(0, 116), ylim=(26, 84))

    # Two rows, four columns, each store paired with its one and only
    # caller's column: tracks with trigger_handler.py (the one module that
    # reads it), recommendations with recommend.py (the only writer).
    # scoring/ranking.py has no relationship to Semantic API in this
    # diagram -- it is placed in the top row next to recommend.py (its only
    # caller) purely because nothing else constrains where it goes, which
    # turns recommend.py -> scoring/ranking.py into a same-row, same-height
    # straight line instead of a detour around Semantic API below it.
    cylinder(ax, 2, 62, 18, 16, "recommendations", "Postgres\n(written)", fs=8.5, sub_fs=7.5)
    box(ax, 37, 62, 30, 15, "recommend.py",
        "batch entrypoint /\norchestrator (stage 5)", fs=10, sub_fs=8)
    box(ax, 84, 62, 26, 16, "scoring/ranking.py",
        "pure function, no I/O --\nshared with 8.2's refresh\nloop (Decision D)", "compute", fs=9, sub_fs=7.3)

    cylinder(ax, 2, 34, 18, 16, "tracks", "Postgres\n(read)", fs=9.5, sub_fs=7.5)
    box(ax, 24, 34, 26, 16, "trigger_handler.py",
        "resolves seed track\nID(s) -- the one module\nthat reads Postgres\ndirectly", "compute", fs=9, sub_fs=7.3)
    box(ax, 54, 34, 26, 16, "context_builder.py",
        "gathers similarity /\ngraph / artist candidates\n-- HTTP only, never\nPostgres/Milvus/Neo4j", "compute", fs=9, sub_fs=7.3)
    box(ax, 84, 34, 26, 16, "Semantic API",
        "6 read-only endpoints\n(contracts/semantic-api-v1\n.json, frozen)", fs=9, sub_fs=7.3)

    # recommend.py calls all three modules directly -- every arrow below is
    # a straight line within its own row or straight down into the row
    # beneath, none crossing a third box, since each source/target pair now
    # sits in adjacent or vertically-aligned columns.
    arrow(ax, (48, 62), (35, 50))
    arrow(ax, (56, 62), (64, 50))
    arrow(ax, (67, 68), (84, 68), None, color="#6b4a86")

    arrow(ax, (37, 68), (20, 68), None, color="#4a7a3c")
    ax.text(28.5, 71.5, "writes top-k,\ntagged with run_id", ha="center", va="bottom",
            fontsize=7.3, color="#4a7a3c")

    # trigger_handler is the one module that bypasses the Semantic API
    arrow(ax, (24, 42), (20, 42), None, color="#4a7a3c")
    ax.text(22, 32.5, "the one exception:\ndirect SQL read (tracks.id only)",
            ha="center", va="top", fontsize=7.6, color="#4a7a3c")

    # context_builder talks only to the Semantic API
    arrow(ax, (80, 42), (84, 42))
    ax.text(82, 45.5, "HTTP", ha="center", va="bottom", fontsize=7.3, color="#333333")

    ax.set_title("Figure 5.5 — 8.1's Recommender Engine: internal module structure",
                 fontsize=11.5, pad=12)
    save(fig, "fig-5-5-uc81-modules.png")


# --------------------------------------------------------------------------
# Figure 5.6 -- one seed-track recommendation, request sequence
# (Chapter 5, Section 5.6.2)
# --------------------------------------------------------------------------
def fig_uc81_sequence():
    fig, ax = canvas(16.0, 10.5, xlim=(0, 148), ylim=(0, 100))

    # Uniform 24-unit spacing for every actor -- each header box is 20 wide,
    # so this leaves a 4-unit gap between adjacent boxes. The previous
    # version used uneven spacing (last gap only 16) which let the
    # scoring/ranking and Postgres boxes overlap, and started its first
    # actor at x=8 (box left edge at -2), which the canvas silently clipped.
    RECOMMEND, TRIGGER, CONTEXT, SEMANTIC_API, SCORING, POSTGRES = 12, 36, 60, 84, 108, 132
    actors = [
        ("recommend.py", RECOMMEND),
        ("trigger_handler", TRIGGER),
        ("context_builder", CONTEXT),
        ("Semantic API", SEMANTIC_API),
        ("scoring/ranking", SCORING),
        ("Postgres", POSTGRES),
    ]
    top_y, bottom_y = 94, 4
    for name, x in actors:
        box(ax, x - 10, top_y, 20, 6, name, None, fs=8.5)
        ax.plot([x, x], [top_y, bottom_y], color="#999999", lw=1.0, linestyle=(0, (2, 2)), zorder=1)

    def msg(y, x_from, x_to, label, dashed=False, fs=7.6):
        color = "#333333"
        arrow(ax, (x_from, y), (x_to, y), None, dashed=dashed, color=color, fs=fs)
        mx = (x_from + x_to) / 2
        ha = "center"
        ax.text(mx, y + 1.2, label, ha=ha, va="bottom", fontsize=fs, color=color)

    y = 83
    step = 5.7
    steps = [
        (RECOMMEND, TRIGGER, "1. get_seed_track_ids()", False),
        (TRIGGER, POSTGRES, "2. SELECT id FROM tracks", False),
        (POSTGRES, TRIGGER, "3. [seed_track_id, ...]", True),
        (TRIGGER, RECOMMEND, "4. seed_track_id(s)", True),
        (RECOMMEND, CONTEXT, "5. build_context(seed_track_id)", False),
        (CONTEXT, SEMANTIC_API, "6. GET /tracks/{id}", False),
        (CONTEXT, SEMANTIC_API, "7. GET /tracks/{id}/similar?k=candidate_k", False),
        (CONTEXT, SEMANTIC_API, "8. GET /tracks/{id}/graph", False),
        (CONTEXT, SEMANTIC_API, "9. GET /artists/{name}/tracks", False),
        (SEMANTIC_API, CONTEXT, "10. TrackContext", True),
        (CONTEXT, RECOMMEND, "11. TrackContext", True),
        (RECOMMEND, SCORING, "12. score_recommendations(...)", False),
        (SCORING, RECOMMEND, "13. ranked list (top_k)", True),
        (RECOMMEND, POSTGRES, "14. INSERT INTO recommendations", False),
    ]
    for x_from, x_to, label, dashed in steps:
        msg(y, x_from, x_to, label, dashed=dashed)
        y -= step

    ax.text(146, 90.5, "solid = call\ndashed = return",
            ha="right", va="top", fontsize=7.8, color="#5f5f5f", style="italic")

    ax.set_title("Figure 5.6 — One seed-track recommendation: request sequence",
                 fontsize=11.5, pad=12)
    save(fig, "fig-5-6-uc81-sequence.png")


def fig_shared_layer():
    """Figure 1.1 (Chapter 1, Motivation) -- the one deliberately NON-technical
    figure in this file: no protocol names, no store names, no code paths.
    Contrasts four applications each maintaining their own private
    understanding of a track (left) against four applications reading one
    shared semantic layer (right) -- the intuitive version of the
    architectural argument Section 1.1's prose already makes from the
    literature. Every other figure in this module explains a mechanism;
    this one explains why the mechanism is worth having at all."""
    fig, ax = canvas(11, 5.2, xlim=(0, 100), ylim=(0, 52))

    apps = ["Recommender", "Similarity\nSearch", "Auto-tagging", "Playlist\nGeneration"]

    # --- left panel: one private model per application ---
    ax.text(20, 49, "Without a shared layer", ha="center", fontsize=12, fontweight="bold")
    app_y = [37, 26, 15, 4]
    for label, y in zip(apps, app_y):
        box(ax, 2, y, 17, 8, label, style="external", fs=9)
        box(ax, 22, y, 16, 8, "own private\ntrack model", style="compute", fs=8)
        arrow(ax, (19, y + 4), (22, y + 4), color="#8a3a3a")
    ax.text(20, -2.5, "4 duplicated models -> 4x the cold-start problem,\nno shared improvement",
            ha="center", va="top", fontsize=8.5, color="#8a3a3a", style="italic")

    # --- right panel: one shared semantic layer ---
    ax.text(78, 49, "With a shared semantic layer", ha="center", fontsize=12, fontweight="bold")
    app_y2 = [37, 26, 15, 4]
    hub = box(ax, 68, 15.5, 22, 14, "Shared semantic\nlayer", "(what a track IS,\nonce)", style="service", fs=10, sub_fs=8.5)
    # Straight lines (no curvature) into four distinct points spread across the
    # hub's left edge, highest source to highest entry point and so on --
    # avoids both the mid-air crossing curved arrows produced and the
    # bunching-up all four had converging on the exact same center point.
    entry_ys = [27.5, 24.17, 20.83, 17.5]
    for label, y, entry_y in zip(apps, app_y2, entry_ys):
        box(ax, 44, y, 17, 8, label, style="external", fs=9)
        arrow(ax, (61, y + 4), (68, entry_y), color="#2f6a3a")
    ax.text(78, -2.5, "one enriched representation, reused --\nan improvement to it helps every application",
            ha="center", va="top", fontsize=8.5, color="#2f6a3a", style="italic")

    ax.plot([50, 50], [-4, 50], color="#cccccc", lw=1.0, linestyle=(0, (2, 3)))

    fig.subplots_adjust(bottom=0.12)
    save(fig, "fig-1-1-shared-layer.png")


if __name__ == "__main__":
    fig_architecture()
    fig_uc81_modules()
    fig_uc81_sequence()
    fig_runtime()
    fig_cold_warm()
    fig_warm_path()
    fig_uc83_design()
    fig_shared_layer()
