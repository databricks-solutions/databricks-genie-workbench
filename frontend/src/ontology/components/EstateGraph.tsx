/**
 * Phase 3e Step B: Estate Graph — the "Ontology Map" (MV-D48, MV-D72). Renders the
 * OntologyGraph as an aggregate-first, compound-container cytoscape canvas that matches
 * mockup 17.0j: a Domains | Sub-domains | Assets level-of-detail toggle, domains as
 * coloured boxes, drill-down into a single business area, focus+context highlighting,
 * a plain-language detail popover, and label declutter so a 2,000-asset estate stays
 * readable. Degrades to an honest-empty state (MV-D43) when the graph is empty.
 */
import { useMemo, useState } from "react"
import { AlertTriangle, X } from "lucide-react"
import cytoscape from "cytoscape"
import fcose from "cytoscape-fcose"
import CytoscapeComponent from "react-cytoscapejs"
import type { OntologyGraph } from "@/ontology/types"
import { buildElements, nodeFacts, type Lod } from "@/ontology/estateGraphModel"

// Register the fcose layout once. cytoscape.use throws if already registered (HMR).
try {
  cytoscape.use(fcose)
} catch {
  /* already registered */
}

const LODS: { id: Lod; label: string }[] = [
  { id: "domains", label: "Domains" },
  { id: "subdomains", label: "Sub-domains" },
  { id: "assets", label: "Assets" },
]

// Minimal structural types for the bits of the cytoscape API this component uses.
// The library ships no types (see cytoscape-shims.d.ts, MV-D45); these keep tsc/lint
// honest without an `any` and without a new @types dependency.
interface CyCollection {
  addClass(cls: string): void
  removeClass(cls: string): void
}
interface CyNode extends CyCollection {
  data(): Record<string, unknown>
  closedNeighborhood(): CyCollection
  connectedEdges(): CyCollection
  ancestors(): CyCollection
  descendants(): CyCollection
}
interface CyCore {
  removeListener(ev: string): void
  ready(fn: () => void): void
  fit(eles?: unknown, padding?: number): void
  batch(fn: () => void): void
  elements(): CyCollection
  nodes(selector?: string): CyCollection
  edges(): CyCollection
  on(events: string, selector: string, handler: (evt: { target: CyNode }) => void): void
  on(events: string, handler: (evt: { target: CyNode | CyCore }) => void): void
}

const STYLESHEET = [
  {
    selector: 'node[ntype="container"]',
    style: {
      shape: "round-rectangle",
      "background-color": "data(color)",
      "background-opacity": 0.08,
      "border-color": "data(color)",
      "border-width": 1.5,
      "border-opacity": 0.55,
      label: "data(label)",
      "text-valign": "top",
      "text-halign": "center",
      "text-margin-y": -6,
      "font-size": 12,
      "font-weight": 700,
      color: "#E2E8F0",
      "text-outline-color": "#0f172a",
      "text-outline-width": 2,
      padding: 20,
      "z-index": 1,
    },
  },
  {
    // Sub-domain container: an inner, dashed box nested inside its top domain so
    // the sub-domain grouping stays visible at the Assets LOD.
    selector: 'node[ntype="subcontainer"]',
    style: {
      shape: "round-rectangle",
      "background-color": "data(color)",
      "background-opacity": 0.05,
      "border-color": "data(color)",
      "border-width": 1,
      "border-opacity": 0.45,
      "border-style": "dashed",
      label: "data(label)",
      "text-valign": "top",
      "text-halign": "center",
      "text-margin-y": -4,
      "font-size": 10,
      "font-weight": 600,
      color: "#CBD5E1",
      "text-outline-color": "#0f172a",
      "text-outline-width": 1.5,
      padding: 12,
      "z-index": 2,
    },
  },
  {
    selector: 'node[ntype="domain"]',
    style: {
      "background-color": "data(color)",
      width: "data(px)",
      height: "data(px)",
      label: "data(label)",
      "font-size": 12,
      "font-weight": 700,
      color: "#0B1120",
      "text-valign": "center",
      "text-halign": "center",
      "text-max-width": "data(px)",
      "text-wrap": "ellipsis",
      "z-index": 10,
    },
  },
  {
    selector: 'node[ntype="subdomain"]',
    style: {
      "background-color": "data(color)",
      "background-opacity": 0.9,
      width: "data(px)",
      height: "data(px)",
      label: "data(label)",
      "font-size": 10,
      color: "#CBD5E1",
      "text-valign": "bottom",
      "text-halign": "center",
      "text-margin-y": 2,
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
      "min-zoomed-font-size": 12, // declutter: hide asset labels until zoomed in
      "text-valign": "bottom",
      "text-halign": "center",
    },
  },
  {
    selector: 'node[kind="metric_view"]',
    style: { "border-color": "#22D3EE", "border-width": 2 },
  },
  {
    selector: 'node[kind="agent"]',
    style: { "border-color": "#A78BFA", "border-width": 2, shape: "diamond" },
  },
  {
    selector: 'node[ntype="more"]',
    style: {
      shape: "round-rectangle",
      "background-color": "#1E293B",
      "border-color": "data(color)",
      "border-width": 1,
      "border-style": "dashed",
      label: "data(label)",
      "font-size": 9,
      color: "#94A3B8",
      "text-valign": "center",
      "text-halign": "center",
      width: 46,
      height: 18,
    },
  },
  {
    selector: "edge",
    style: { width: 1.4, "line-color": "#475569", "curve-style": "bezier", opacity: 0.5 },
  },
  { selector: 'edge[etype="coquery"]', style: { "line-color": "#818CF8", "line-style": "dashed", opacity: 0.7 } },
  { selector: 'edge[etype="lineage"]', style: { "line-color": "#64748B" } },
  { selector: "node.faded", style: { opacity: 0.12 } },
  { selector: "edge.faded", style: { opacity: 0.05 } },
  { selector: "node.focused", style: { "border-color": "#22D3EE", "border-width": 3 } },
  // Edge-on-demand: `visibility:hidden` keeps the edge in the fcose simulation (so
  // clusters still emerge from connectivity) but off-screen until a node is tapped.
  { selector: "edge.hidden", style: { visibility: "hidden" } },
]

function layoutFor(lod: Lod) {
  // Seeded compound fcose = the Group-in-a-Box pattern: physics refines within/between
  // the domain boxes so clusters emerge, but `randomize:false` starts from the model's
  // deterministic id-hashed seed positions, so the map looks the same every render AND
  // every page load (mental-map preservation) instead of jittering.
  return {
    name: "fcose",
    animate: false,
    quality: lod === "assets" ? "default" : "proof",
    randomize: false, // start from the seed positions → deterministic, stable
    fit: true,
    padding: 28,
    nodeSeparation: 80,
    idealEdgeLength: 90,
    nodeRepulsion: 6500,
    packComponents: true,
    nestingFactor: 0.1,
    gravity: 0.3,
    gravityCompound: 1.2,
    tile: true,
  }
}

export function EstateGraph({ graph }: { graph: OntologyGraph }) {
  const [lod, setLod] = useState<Lod>("domains")
  const [focusTop, setFocusTop] = useState<string | null>(null)
  const [focusName, setFocusName] = useState<string | null>(null)
  const [selected, setSelected] = useState<Record<string, unknown> | null>(null)

  const domainCount = graph.domains.nodes.filter((n) => n.kind !== "ungrouped").length
  const assetCount = graph.assets.nodes.length
  const isEmpty = graph.domains.nodes.length + assetCount === 0
  const truncated = graph.domains.truncated || graph.assets.truncated

  const elements = useMemo(
    () =>
      buildElements(graph, lod, focusTop).map((el) =>
        el.position ? { data: el.data, position: el.position } : { data: el.data },
      ),
    [graph, lod, focusTop],
  )
  const layout = useMemo(() => layoutFor(lod), [lod])

  // The CytoscapeComponent remounts on any (lod, focus, snapshot) change (see `key`),
  // so highlighting resets itself; handlers just clear the popover.
  const changeLod = (next: Lod) => {
    setLod(next)
    setSelected(null)
    // Domains is the aggregate overview — leaving a drill-down focus set there would
    // hide tops; clear it so the overview always shows the whole estate.
    if (next === "domains") {
      setFocusTop(null)
      setFocusName(null)
    }
  }
  const drillInto = (topId: string, name: string | null) => {
    setFocusTop(topId)
    setFocusName(name)
    setLod("assets")
    setSelected(null)
  }
  const clearFocus = () => {
    setFocusTop(null)
    setFocusName(null)
    setSelected(null)
  }

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

  const facts = selected ? nodeFacts(selected) : null

  return (
    <div className="rounded-xl border border-default bg-surface overflow-hidden">
      {/* Controls: LOD toggle + focus chip + counts */}
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-default bg-elevated/40 px-4 py-2.5">
        <div className="flex flex-wrap items-center gap-2">
          <div className="inline-flex overflow-hidden rounded-lg border border-default text-xs">
            {LODS.map((l) => (
              <button
                key={l.id}
                onClick={() => changeLod(l.id)}
                className={`border-l border-default px-2.5 py-1 first:border-l-0 transition-colors ${
                  lod === l.id ? "bg-accent font-semibold text-white" : "text-secondary hover:text-primary"
                }`}
              >
                {l.label}
              </button>
            ))}
          </div>
          {focusTop && (
            <button
              onClick={clearFocus}
              className="inline-flex items-center gap-1 rounded-full border border-accent/40 bg-accent/10 px-2.5 py-1 text-xs text-accent hover:bg-accent/20"
            >
              {focusName || "Focused"} <X className="h-3 w-3" />
            </button>
          )}
        </div>
        <div className="flex items-center gap-x-4 text-xs text-secondary">
          <span><span className="font-semibold text-accent">{domainCount}</span> Domains</span>
          <span><span className="font-semibold text-accent">{assetCount}</span> Assets</span>
          {truncated && <span className="text-muted">Top 2,000 · centrality</span>}
        </div>
      </div>

      {/* Interactive canvas */}
      <div className="relative w-full bg-elevated/25" style={{ height: "560px" }}>
        <CytoscapeComponent
          key={`${lod}:${focusTop ?? "all"}:${graph.as_of ?? ""}`}
          elements={elements}
          stylesheet={STYLESHEET}
          layout={layout}
          style={{ width: "100%", height: "100%" }}
          minZoom={0.05}
          maxZoom={3}
          wheelSensitivity={0.2}
          boxSelectionEnabled={false}
          hideEdgesOnViewport
          textureOnViewport
          pixelRatio={1}
          cy={(cy: CyCore) => {
            cy.removeListener("tap")
            cy.ready(() => {
              // Edge-on-demand: at Sub-domains/Assets the graph starts edge-free (clean),
              // edges reveal on tap. Domains keeps its few aggregated ties visible.
              if (lod !== "domains") cy.edges().addClass("hidden")
              cy.fit(undefined, 28)
            })
            cy.on("tap", "node", (evt: { target: CyNode }) => {
              const node = evt.target
              setSelected({ ...node.data() })
              cy.batch(() => {
                cy.nodes('[ntype != "container"][ntype != "subcontainer"]').addClass("faded")
                cy.edges().addClass("faded")
                node.removeClass("faded")
                const nb = node.closedNeighborhood()
                nb.removeClass("faded")
                // Reveal (and un-fade) only the tapped node's own connections — the
                // "FK view on demand" pattern that keeps the canvas readable.
                node.connectedEdges().removeClass("hidden faded")
                node.ancestors().removeClass("faded")
                node.descendants().removeClass("faded")
                cy.nodes().removeClass("focused")
                node.addClass("focused")
              })
            })
            cy.on("tap", (evt: { target: CyNode | CyCore }) => {
              if (evt.target === cy) {
                setSelected(null)
                cy.batch(() => {
                  cy.elements().removeClass("faded focused")
                  if (lod !== "domains") cy.edges().addClass("hidden")
                })
              }
            })
          }}
        />

        {/* Detail popover (plain language, no SQL/ids — MV-D23) */}
        {facts && (
          <div className="absolute right-3 top-3 w-64 rounded-lg border border-default bg-elevated p-3 shadow-lg">
            <div className="flex items-start justify-between gap-2">
              <p className="text-sm font-semibold text-primary break-words">{facts.title}</p>
              <button onClick={() => setSelected(null)} className="shrink-0 text-muted hover:text-primary">
                <X className="h-4 w-4" />
              </button>
            </div>
            <span className="mt-1 inline-block rounded-full bg-accent/15 px-2 py-0.5 text-xs text-accent">{facts.chip}</span>
            {facts.lines.length > 0 && (
              <div className="mt-2 space-y-1 text-xs text-secondary">
                {facts.lines.map((line, i) => (
                  <p key={i}>{line}</p>
                ))}
              </div>
            )}
            {facts.drillTopId && (
              <button
                onClick={() => drillInto(facts.drillTopId!, facts.title)}
                className="mt-3 w-full rounded-lg bg-accent px-3 py-1.5 text-xs font-semibold text-white hover:opacity-90"
              >
                View its assets
              </button>
            )}
          </div>
        )}
      </div>

      {/* Legend */}
      <div className="border-t border-default bg-elevated/50 px-4 py-3">
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2 text-xs text-muted">
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2.5 w-3.5 rounded border" style={{ borderColor: "#818CF8", background: "#818cf814" }} /> Domain (container)
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full" style={{ backgroundColor: "#64748B" }} /> Asset
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full border-2" style={{ borderColor: "#22D3EE" }} /> Metric view
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block h-0 w-6 border-t" style={{ borderColor: "#64748b" }} /> Lineage
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block h-0 w-6 border-t border-dashed" style={{ borderColor: "#818cf8" }} /> Co-query
          </span>
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
