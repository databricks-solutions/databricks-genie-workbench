/**
 * Phase 3e Step B: Estate Graph rendering (MV-D48). Renders the OntologyGraph as an
 * interactive cytoscape canvas — domain hubs + asset nodes at the backend's precomputed
 * layout positions (`fr`), lineage (solid) and co-query (dashed) edges — with pan / zoom /
 * select. Degrades to an honest-empty state (MV-D43) if the graph is empty. The backend
 * caps assets to the top-N by centrality, so this renders a bounded, readable estate view.
 */
import { useMemo } from "react"
import { AlertTriangle } from "lucide-react"
import cytoscape from "cytoscape"
import fcose from "cytoscape-fcose"
import CytoscapeComponent from "react-cytoscapejs"
import type { OntologyGraph, OntologyGraphNode } from "@/ontology/types"

// Register the fcose layout once (fallback when the backend positions are degenerate).
// cytoscape.use throws if already registered (HMR) — swallow it.
try {
  cytoscape.use(fcose)
} catch {
  /* already registered */
}

// Asset node fill by kind; domains get the accent. Dark-friendly hexes that track
// the app's tokens (accent indigo / cyan / violet) without reading CSS at runtime.
const KIND_COLOR: Record<string, string> = {
  metric_view: "#22D3EE",
  agent: "#A78BFA",
  dashboard: "#F59E0B",
  table: "#64748B",
  view: "#64748B",
}

interface CyElement {
  data: Record<string, unknown>
  position?: { x: number; y: number }
}

/** Fit the backend layout coordinates into a virtual canvas sized to the node count. */
function normalizePositions(nodes: OntologyGraphNode[]): { scale: (n: OntologyGraphNode) => { x: number; y: number }; degenerate: boolean } {
  let minX = Infinity
  let maxX = -Infinity
  let minY = Infinity
  let maxY = -Infinity
  for (const n of nodes) {
    if (n.x < minX) minX = n.x
    if (n.x > maxX) maxX = n.x
    if (n.y < minY) minY = n.y
    if (n.y > maxY) maxY = n.y
  }
  const spanX = maxX - minX
  const spanY = maxY - minY
  const degenerate = !isFinite(spanX) || !isFinite(spanY) || spanX === 0 || spanY === 0
  // Spread the canvas with the estate size so dense graphs don't collapse.
  const side = Math.max(1200, Math.round(Math.sqrt(Math.max(nodes.length, 1)) * 90))
  const scale = (n: OntologyGraphNode) => ({
    x: ((n.x - minX) / (spanX || 1)) * side,
    y: ((n.y - minY) / (spanY || 1)) * side,
  })
  return { scale, degenerate }
}

export function EstateGraph({ graph }: { graph: OntologyGraph }) {
  const domainCount = graph.domains.nodes.length
  const assetCount = graph.assets.nodes.length
  const edgeCount = graph.domains.edges.length + graph.assets.edges.length
  const isEmpty = domainCount + assetCount === 0
  const truncated = graph.domains.truncated || graph.assets.truncated

  const { elements, useFcose } = useMemo(() => {
    const allNodes = [...graph.domains.nodes, ...graph.assets.nodes]
    const { scale, degenerate } = normalizePositions(allNodes)
    const nodeIds = new Set(allNodes.map((n) => n.id))
    const els: CyElement[] = []

    for (const n of graph.domains.nodes) {
      els.push({
        data: { id: n.id, label: n.label, ntype: "domain", px: 22 + Math.min(n.size, 3) * 12 },
        position: degenerate ? undefined : scale(n),
      })
    }
    for (const n of graph.assets.nodes) {
      els.push({
        data: {
          id: n.id,
          label: n.label,
          ntype: "asset",
          color: KIND_COLOR[n.kind] ?? KIND_COLOR.table,
          px: 6 + Math.min(n.size, 3) * 5,
        },
        position: degenerate ? undefined : scale(n),
      })
    }

    let ei = 0
    for (const e of [...graph.domains.edges, ...graph.assets.edges]) {
      if (!nodeIds.has(e.src) || !nodeIds.has(e.dst)) continue // drop dangling (truncation)
      const etype = e.kind === "lineage" ? "lineage" : /quer|co/i.test(e.kind) ? "coquery" : "other"
      els.push({
        data: {
          id: `e${ei++}`,
          source: e.src,
          target: e.dst,
          etype,
          w: 1 + Math.min(e.weight ?? 0, 4),
        },
      })
    }
    return { elements: els, useFcose: degenerate }
  }, [graph])

  if (isEmpty) {
    return (
      <div className="flex items-start gap-2.5 rounded-xl border border-info/30 bg-info/5 px-4 py-3.5">
        <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0 text-info-foreground" />
        <div>
          <p className="text-sm font-semibold text-primary">No data in the estate graph</p>
          <p className="mt-1 max-w-prose text-xs text-secondary">
            The graph is empty — there are no domains or assets to display. Run a refresh to populate the estate.
          </p>
        </div>
      </div>
    )
  }

  const stylesheet = [
    {
      selector: 'node[ntype="domain"]',
      style: {
        "background-color": "#818CF8",
        "border-color": "#4F46E5",
        "border-width": 2,
        width: "data(px)",
        height: "data(px)",
        label: "data(label)",
        "font-size": 11,
        color: "#E2E8F0",
        "text-valign": "center",
        "text-halign": "center",
        "text-outline-color": "#0f172a",
        "text-outline-width": 2,
        "z-index": 10,
      },
    },
    {
      selector: 'node[ntype="asset"]',
      style: {
        "background-color": "data(color)",
        width: "data(px)",
        height: "data(px)",
        label: "data(label)",
        "font-size": 8,
        color: "#94A3B8",
        "min-zoomed-font-size": 14, // hide the 2k asset labels until zoomed in
        "text-valign": "bottom",
        "text-halign": "center",
      },
    },
    {
      selector: "edge",
      style: { width: "data(w)", "line-color": "#334155", "curve-style": "straight", opacity: 0.45 },
    },
    {
      selector: 'edge[etype="coquery"]',
      style: { "line-color": "#6366F1", "line-style": "dashed", opacity: 0.55 },
    },
    { selector: "node:selected", style: { "border-width": 3, "border-color": "#22D3EE" } },
  ]

  const layout = useFcose
    ? { name: "fcose", animate: false, quality: "default", nodeRepulsion: 5000, randomize: false }
    : { name: "preset", fit: true, padding: 40 }

  return (
    <div className="rounded-xl border border-default bg-surface overflow-hidden">
      {/* Overview header */}
      <div className="flex flex-wrap items-center gap-x-6 gap-y-1 border-b border-default bg-elevated/40 px-4 py-2.5">
        <p className="text-xs font-semibold uppercase tracking-wide text-muted">Estate Overview</p>
        <span className="text-xs text-secondary">
          <span className="font-semibold text-accent">{domainCount}</span> Domains
        </span>
        <span className="text-xs text-secondary">
          <span className="font-semibold text-accent">{assetCount}</span> Assets
        </span>
        <span className="text-xs text-secondary">
          <span className="font-semibold text-accent">{edgeCount}</span> Edges
        </span>
        {truncated && <span className="text-xs text-muted">— showing the most-connected assets</span>}
      </div>

      {/* Interactive canvas */}
      <div className="relative w-full bg-elevated/25" style={{ height: "560px" }}>
        <CytoscapeComponent
          elements={elements}
          stylesheet={stylesheet}
          layout={layout}
          style={{ width: "100%", height: "100%" }}
          minZoom={0.05}
          maxZoom={3}
          wheelSensitivity={0.2}
          boxSelectionEnabled={false}
          autoungrabify
          hideEdgesOnViewport
          textureOnViewport
          pixelRatio={1}
          cy={(cy: { ready: (fn: () => void) => void; fit: (a?: unknown, b?: number) => void }) => {
            cy.ready(() => cy.fit(undefined, 40))
          }}
        />
      </div>

      {/* Legend */}
      <div className="border-t border-default bg-elevated/50 px-4 py-3">
        <p className="text-xs font-semibold uppercase tracking-wide text-muted">Legend</p>
        <div className="mt-2 grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-4">
          <div className="flex items-center gap-2">
            <div className="h-3 w-3 rounded-full" style={{ backgroundColor: "#818CF8" }} />
            <span className="text-xs text-secondary">Domain</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="h-2 w-2 rounded-full" style={{ backgroundColor: "#64748B" }} />
            <span className="text-xs text-secondary">Asset</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="h-px w-3" style={{ backgroundColor: "#334155" }} />
            <span className="text-xs text-secondary">Lineage</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="h-px w-3 border-b-2 border-dashed" style={{ borderColor: "#6366F1" }} />
            <span className="text-xs text-secondary">Co-query</span>
          </div>
        </div>
      </div>

      {/* Graph state */}
      <div className="border-t border-default bg-surface px-4 py-3 space-y-2">
        <p className="text-xs font-semibold uppercase tracking-wide text-muted">Graph state</p>
        <div className="text-xs text-secondary">
          <p>Layout: {graph.layout}</p>
          <p>State: {graph.state}</p>
          {graph.as_of && <p>Last updated: {graph.as_of}</p>}
        </div>
      </div>
    </div>
  )
}
