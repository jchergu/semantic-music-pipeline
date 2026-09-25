// Shared semantic-space scatter plot, built once per page from the real
// PCA projection of all 411 CLAP embeddings (demo/data/embedding_space.json
// -- see precompute_embedding_space.py). Deliberately not a generic charting
// library wrapper: this is small enough to read end to end, and every page
// needs slightly different highlight/animation behaviour on top of the same
// base scatter.

// Headless-Chrome screenshot capture (used for the thesis figures) turned
// out to be unreliable with rAF-driven d3 .transition() calls under
// --virtual-time-budget: the same page, same data, same budget, produced
// complete-DOM-but-visually-incomplete captures on some runs and fully
// correct ones on others, with no code-level bug behind it (verified via
// --dump-dom: the underlying content was always right). Rather than chase
// headless timing nondeterminism further, ?static=1 skips every transition
// and jumps straight to final attribute values -- deterministic for
// screenshots, while normal interactive browsing (no query param) keeps
// the animated version.
const SCATTER_INSTANT = new URLSearchParams(location.search).has("static");

async function loadEmbeddingSpace() {
  const resp = await fetch("data/embedding_space.json");
  if (!resp.ok) throw new Error(`embedding_space.json: HTTP ${resp.status}`);
  return resp.json();
}

function createScatter(svgSelector, tracks, width = 640, height = 560) {
  const pad = 24;
  const svg = d3.select(svgSelector).attr("viewBox", `0 0 ${width} ${height}`);
  svg.selectAll("*").remove();

  const x = d3.scaleLinear().domain([0, 1]).range([pad, width - pad]);
  const y = d3.scaleLinear().domain([0, 1]).range([height - pad, pad]);

  const zoomLayer = svg.append("g");
  svg.call(
    d3.zoom()
      .scaleExtent([0.6, 8])
      .on("zoom", (event) => zoomLayer.attr("transform", event.transform))
  );

  const linesLayer = zoomLayer.append("g").attr("class", "lines-layer");
  const dotsLayer = zoomLayer.append("g").attr("class", "dots-layer");
  const pulseLayer = zoomLayer.append("g").attr("class", "pulse-layer");

  const byId = new Map(tracks.map((t) => [t.id, t]));

  dotsLayer
    .selectAll("circle.track-dot")
    .data(tracks, (d) => d.id)
    .join("circle")
    .attr("class", "track-dot")
    .attr("cx", (d) => x(d.x))
    .attr("cy", (d) => y(d.y))
    .attr("r", 2.6)
    .attr("fill", "#3d4560")
    .attr("opacity", 0.5);

  return { svg, x, y, dotsLayer, linesLayer, pulseLayer, byId, tracks };
}

// Fades a set of track ids to a highlight colour/radius, in a staggered
// sequence so the eye can follow each one lighting up rather than seeing a
// single instant repaint.
function highlightTracks(ctx, ids, { color = "#6ea8fe", radius = 6, stagger = 90, ring = false } = {}) {
  ids.forEach((id, i) => {
    const node = ctx.dotsLayer.selectAll("circle.track-dot").filter((d) => d.id === id);
    if (SCATTER_INSTANT) {
      node.attr("r", radius).attr("fill", color).attr("opacity", 1);
    } else {
      node
        .transition()
        .delay(i * stagger)
        .duration(500)
        .ease(d3.easeBackOut.overshoot(1.6))
        .attr("r", radius)
        .attr("fill", color)
        .attr("opacity", 1);
    }
    if (ring) {
      const d = ctx.byId.get(id);
      if (!d) return;
      ctx.pulseLayer
        .append("circle")
        .attr("cx", ctx.x(d.x))
        .attr("cy", ctx.y(d.y))
        .attr("class", "pulse-ring")
        .attr("stroke", color)
        .style("animation-delay", `${i * stagger}ms`);
    }
  });
}

// Draws animated "connecting" lines from one origin point to each target id,
// each line drawing itself in via stroke-dashoffset rather than appearing
// instantly.
function connectFrom(ctx, originXY, ids, { color = "#6ea8fe", stagger = 90 } = {}) {
  ids.forEach((id, i) => {
    const d = ctx.byId.get(id);
    if (!d) return;
    const x2 = ctx.x(d.x), y2 = ctx.y(d.y);
    const line = ctx.linesLayer
      .append("line")
      .attr("x1", originXY[0]).attr("y1", originXY[1])
      .attr("x2", SCATTER_INSTANT ? x2 : originXY[0])
      .attr("y2", SCATTER_INSTANT ? y2 : originXY[1])
      .attr("stroke", color)
      .attr("stroke-width", 1.1)
      .attr("opacity", 0.55);
    if (!SCATTER_INSTANT) {
      line
        .transition()
        .delay(i * stagger)
        .duration(550)
        .ease(d3.easeCubicOut)
        .attr("x2", x2)
        .attr("y2", y2);
    }
  });
}

function clearOverlay(ctx) {
  ctx.linesLayer.selectAll("*").remove();
  ctx.pulseLayer.selectAll("*").remove();
  const sel = ctx.dotsLayer.selectAll("circle.track-dot");
  if (SCATTER_INSTANT) {
    sel.attr("r", 2.6).attr("fill", "#3d4560").attr("opacity", 0.5);
  } else {
    sel.transition().duration(300).attr("r", 2.6).attr("fill", "#3d4560").attr("opacity", 0.5);
  }
}
