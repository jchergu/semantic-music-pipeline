// Shared knowledge-graph view: a real force-directed layout of Track/Artist/
// Genre nodes, built live from the Semantic API's /tracks/{id}/graph
// endpoint (Neo4j -- artist, genres, and up to 10 genre-sibling tracks per
// call). A genuinely different real view of the same catalog from
// scatter.js's embedding-space projection: that one shows continuous
// similarity, this one shows the discrete graph structure the ranking
// function's genre-sibling/same-artist signals are actually computed from.
//
// Same headless-screenshot problem scatter.js already solved, worse here:
// a force simulation never "finishes" on its own. ?static=1 runs the
// simulation synchronously to a fixed tick count before first paint instead
// of relying on the animated tick loop, so a screenshot capture is
// deterministic; interactive browsing (no query param) gets the live,
// settling simulation.
const GRAPH_STATIC = new URLSearchParams(location.search).has("static");

const NODE_COLOR = {
  artist: "#b892e8",
  genre: "#5fc9d8",
  tag: "#e8956e", // live CLAP zero-shot tag (8.3 only) -- predicted, not ground truth
};
const TRACK_ROLE_COLOR = {
  seed: "#6ea8fe",
  session: "#6ea8fe",
  "now-playing": "#6ea8fe",
  recommended: "#f2b84b",
  suggestion: "#5fd88f",
  context: "#5b6178",
};

function nodeColor(d) {
  if (d.type === "track") return TRACK_ROLE_COLOR[d.role] || "#3d4560";
  return NODE_COLOR[d.type] || "#7d8496";
}

function nodeRadius(d) {
  if (d.type !== "track") return 6;
  return d.role === "context" ? 5 : 9;
}

async function fetchTrackGraph(semanticApiBase, trackId) {
  const resp = await fetch(`${semanticApiBase}/tracks/${trackId}/graph`);
  if (!resp.ok) throw new Error(`tracks/${trackId}/graph: HTTP ${resp.status}`);
  return resp.json();
}

// tracks: [{id, title, role, siblings: true|false}]. A track with
// siblings:true also pulls in up to `maxSiblings` of its real genre-sibling
// tracks (from the same response, no extra fetch) as small "context" nodes,
// linked directly to it rather than routed through a genre node -- the
// Semantic API's related_by_genre doesn't say *which* shared genre drove
// the match, so a direct edge is the honest claim, not an invented one
// through a specific genre node.
async function buildGraphData(semanticApiBase, tracks, { maxSiblings = 5 } = {}) {
  const nodes = new Map();
  const links = [];
  // force:true for a track's own primary role (seed/session/recommended/...)
  // so it always wins over an earlier "context" insert from someone else's
  // genre-sibling list -- a recommended/suggested track that also happens
  // to be a sibling of another shown track must still render in its real role.
  const addNode = (id, node, force = false) => {
    if (!nodes.has(id) || force) nodes.set(id, node);
  };

  for (const t of tracks) {
    let g;
    try {
      g = await fetchTrackGraph(semanticApiBase, t.id);
    } catch (e) {
      continue; // not enriched, or API hiccup -- skip this one track, not the whole graph
    }
    const trackNodeId = `track:${t.id}`;
    addNode(trackNodeId, { id: trackNodeId, type: "track", label: t.title || `#${t.id}`, role: t.role, trackId: t.id }, true);

    if (g.artist) {
      const artistNodeId = `artist:${g.artist}`;
      addNode(artistNodeId, { id: artistNodeId, type: "artist", label: g.artist });
      links.push({ source: trackNodeId, target: artistNodeId, kind: "performed" });
    }
    (g.genres || []).forEach((genre) => {
      const genreNodeId = `genre:${genre}`;
      addNode(genreNodeId, { id: genreNodeId, type: "genre", label: genre });
      links.push({ source: trackNodeId, target: genreNodeId, kind: "has_genre" });
    });
    if (t.siblings) {
      (g.related_by_genre || []).slice(0, maxSiblings).forEach((rel) => {
        const relNodeId = `track:${rel.track_id}`;
        addNode(relNodeId, { id: relNodeId, type: "track", label: rel.title, role: "context", trackId: rel.track_id });
        links.push({ source: trackNodeId, target: relNodeId, kind: "genre_sibling" });
      });
    }
  }

  return { nodes: Array.from(nodes.values()), links };
}

function linkStyle(kind) {
  if (kind === "performed") return { stroke: "#8a6fc9", width: 1.4, dash: null, opacity: 0.7 };
  if (kind === "has_genre") return { stroke: "#3f8a94", width: 1.2, dash: null, opacity: 0.6 };
  if (kind === "predicted_tag") return { stroke: "#c96f3f", width: 1.3, dash: "3,3", opacity: 0.75 };
  return { stroke: "#4b5166", width: 1, dash: "2,3", opacity: 0.45 }; // genre_sibling
}

// A track that shares no artist or genre with anything else shown (a real,
// if infrequent, case -- e.g. 8.1's #8 recommendation "Ballad of a Townie")
// forms its own connected component with only its own artist node, and the
// force layout can leave that pair well away from the main cluster (nothing
// but weak centering opposes charge repulsion for a node with no links back
// to the rest of the graph). Fitting the viewport to *every* node's bounding
// box then either shrinks the main cluster to keep that one pair in frame,
// or clips it at the edge; neither is right. fitToNodes instead fits to the
// largest connected component only, via union-find over `links` -- smaller
// components may render partially outside the initial view, reachable by
// panning, which is the right trade-off for a screenshot meant to show the
// main structure, not every isolated pair.
function largestComponentNodes(nodes, links) {
  const parent = new Map(nodes.map((n) => [n.id, n.id]));
  const find = (id) => {
    while (parent.get(id) !== id) {
      parent.set(id, parent.get(parent.get(id)));
      id = parent.get(id);
    }
    return id;
  };
  const union = (a, b) => {
    const ra = find(a), rb = find(b);
    if (ra !== rb) parent.set(ra, rb);
  };
  links.forEach((l) => union(typeof l.source === "object" ? l.source.id : l.source, typeof l.target === "object" ? l.target.id : l.target));

  const groups = new Map();
  nodes.forEach((n) => {
    const root = find(n.id);
    if (!groups.has(root)) groups.set(root, []);
    groups.get(root).push(n);
  });
  return Array.from(groups.values()).reduce((a, b) => (b.length > a.length ? b : a), []);
}

// Once the layout has settled, zooms/pans so the actual node cluster fills
// the panel instead of sitting small in a mostly-empty 640x560 canvas --
// the simulation's forces size themselves to spread nodes apart, not to
// fill any particular viewport, so without this every graph (especially
// 8.3's sparse ~10-node one) renders tiny regardless of how well-laid-out
// it is. Padding is generous because node labels extend well past the
// circles themselves and must not be cropped by the fit.
function fitToNodes(svg, zoomBehavior, allNodes, links, width, height, { padding = 70, maxScale = 2.4 } = {}) {
  const nodes = largestComponentNodes(allNodes, links);
  if (!nodes.length) return;
  const xs = nodes.map((d) => d.x ?? width / 2);
  const ys = nodes.map((d) => d.y ?? height / 2);
  const minX = Math.min(...xs), maxX = Math.max(...xs);
  const minY = Math.min(...ys), maxY = Math.max(...ys);
  const boundsW = Math.max(maxX - minX, 1);
  const boundsH = Math.max(maxY - minY, 1);
  const scale = Math.min(maxScale, (width - padding * 2) / boundsW, (height - padding * 2) / boundsH);
  const cx = (minX + maxX) / 2, cy = (minY + maxY) / 2;
  const transform = d3.zoomIdentity.translate(width / 2 - scale * cx, height / 2 - scale * cy).scale(scale);
  if (GRAPH_STATIC) {
    svg.call(zoomBehavior.transform, transform);
  } else {
    svg.transition().duration(500).call(zoomBehavior.transform, transform);
  }
}

function createGraph(svgSelector, { nodes, links }, width = 640, height = 560) {
  const svg = d3.select(svgSelector).attr("viewBox", `0 0 ${width} ${height}`);
  svg.selectAll("*").remove();

  const zoomLayer = svg.append("g");
  const zoomBehavior = d3.zoom().scaleExtent([0.35, 4]).on("zoom", (e) => zoomLayer.attr("transform", e.transform));
  svg.call(zoomBehavior);

  const simulation = d3
    .forceSimulation(nodes)
    .force("link", d3.forceLink(links).id((d) => d.id).distance((d) => (d.kind === "performed" ? 52 : d.kind === "genre_sibling" ? 36 : 42)).strength(0.75))
    .force("charge", d3.forceManyBody().strength(-110))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .force("collide", d3.forceCollide().radius((d) => nodeRadius(d) + 14))
    .force("x", d3.forceX(width / 2).strength(0.045))
    .force("y", d3.forceY(height / 2).strength(0.045));

  const linkSel = zoomLayer
    .append("g")
    .attr("class", "graph-links")
    .selectAll("line")
    .data(links)
    .join("line")
    .attr("stroke", (d) => linkStyle(d.kind).stroke)
    .attr("stroke-width", (d) => linkStyle(d.kind).width)
    .attr("stroke-dasharray", (d) => linkStyle(d.kind).dash)
    .attr("opacity", (d) => linkStyle(d.kind).opacity);

  const drag = d3
    .drag()
    .on("start", (event, d) => {
      if (!event.active) simulation.alphaTarget(0.25).restart();
      d.fx = d.x;
      d.fy = d.y;
    })
    .on("drag", (event, d) => {
      d.fx = event.x;
      d.fy = event.y;
    })
    .on("end", (event, d) => {
      if (!event.active) simulation.alphaTarget(0);
      d.fx = null;
      d.fy = null;
    });

  const nodeSel = zoomLayer
    .append("g")
    .attr("class", "graph-nodes")
    .selectAll("g.graph-node")
    .data(nodes, (d) => d.id)
    .join("g")
    .attr("class", "graph-node")
    .call(drag);

  nodeSel
    .append("circle")
    .attr("r", (d) => nodeRadius(d))
    .attr("fill", (d) => nodeColor(d))
    .attr("stroke", "#12141a")
    .attr("stroke-width", 1.2);

  nodeSel
    .append("text")
    .text((d) => d.label)
    .attr("dy", (d) => -(nodeRadius(d) + 4))
    .attr("text-anchor", "middle")
    .attr("fill", "#e8eaf0")
    .attr("font-size", (d) => (d.type === "track" && d.role !== "context" ? 11 : 9))
    .attr("font-weight", (d) => (d.type === "track" && d.role !== "context" ? 600 : 400))
    .attr("opacity", (d) => (d.role === "context" ? 0.75 : 1))
    .style("pointer-events", "none");

  const tick = () => {
    linkSel
      .attr("x1", (d) => d.source.x)
      .attr("y1", (d) => d.source.y)
      .attr("x2", (d) => d.target.x)
      .attr("y2", (d) => d.target.y);
    nodeSel.attr("transform", (d) => `translate(${d.x},${d.y})`);
  };

  if (GRAPH_STATIC) {
    simulation.stop();
    for (let i = 0; i < 400; i++) simulation.tick();
    tick();
    fitToNodes(svg, zoomBehavior, nodes, links, width, height);
  } else {
    simulation.on("tick", tick).on("end", () => fitToNodes(svg, zoomBehavior, nodes, links, width, height));
  }

  return { svg, simulation, nodeSel, linkSel, nodes, links };
}

// Shared tab toggle inside a .viz-panel: swaps which .viz-view is visible
// without touching the surrounding panel chrome. All three demo pages wire
// this to the same two buttons (Embedding space / Knowledge graph).
function switchView(viewId, btn) {
  const tabs = btn.parentElement.querySelectorAll(".viz-tab");
  const views = btn.closest(".viz-panel").querySelectorAll(".viz-view");
  tabs.forEach((t) => t.classList.remove("active"));
  views.forEach((v) => v.classList.remove("active"));
  btn.classList.add("active");
  document.getElementById(viewId).classList.add("active");
}

// Screenshot-only convenience: ?view=graph activates the Knowledge graph tab
// after a fixed delay, long enough for the graph's own sequential
// /tracks/{id}/graph fetches (each page awaits several, one per shown
// track) to resolve and for createGraph's ?static=1 synchronous tick loop
// to finish. Interactive browsing never sets this param.
function activateRequestedView(delayMs = 3500) {
  if (new URLSearchParams(location.search).get("view") !== "graph") return;
  setTimeout(() => {
    const tabs = document.querySelectorAll(".viz-tab");
    if (tabs[1]) switchView("graph-view", tabs[1]);
  }, delayMs);
}

// Adds the live CLAP zero-shot top-3 tags (8.3 only) as a small ring of
// "predicted" nodes around the now-playing track, distinct in colour and
// edge style from the real HAS_GENRE catalog genres -- the point being to
// make the gap between the two directly visible on the same graph, the
// same honest-limitation Section 6.2.8 already reports in prose.
function addPredictedTags(graphCtx, trackNodeId, tags) {
  const newNodes = tags.map(([label, score]) => ({
    id: `tag:${label}`,
    type: "tag",
    label: `${label} (${score.toFixed(2)})`,
    x: graphCtx.nodes.find((n) => n.id === trackNodeId)?.x ?? 320,
    y: graphCtx.nodes.find((n) => n.id === trackNodeId)?.y ?? 280,
  }));
  const newLinks = newNodes.map((n) => ({ source: trackNodeId, target: n.id, kind: "predicted_tag" }));
  return { nodes: graphCtx.nodes.concat(newNodes), links: graphCtx.links.concat(newLinks) };
}
