/**
 * Ontology Map v2 (MV-D48, MV-D72, MV-D74, MV-D75). Renders the OntologyGraph as an
 * aggregate-first, compound-container cytoscape canvas with a modern shell:
 *  - an **Applied | Proposed** source toggle (default Applied, MV-D74) — proposed rollups
 *    render dashed + a "Suggested" chip so they never read as current state;
 *  - a Domains | Sub-domains | Assets LOD toggle; the **Assets LOD requires a focused
 *    domain** (drill in, not the all-assets mush, MV-D75) + a breadcrumb;
 *  - **expand-on-demand**: tapping a metric view / sub-domain fetches its measures & Pages
 *    as satellite nodes (§2.3);
 *  - a docked **right-rail inspector** (plain language, MV-D23), **search-to-focus**, a
 *    **minimap**, per-type icons, semantic colour, curved edges revealed on select.
 * Degrades to honest loading / empty / error / stale states throughout (MV-D43).
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { AlertTriangle, Loader2, MousePointerClick } from "lucide-react"
import cytoscape from "cytoscape"
import fcose from "cytoscape-fcose"
import CytoscapeComponent from "react-cytoscapejs"
import type { GraphOrigin, OntologyGraph, OntologyGraphExpand } from "@/ontology/types"
import { mergeExpand, nodeFacts, viewElements, type CyEl, type Lod } from "@/ontology/estateGraphModel"
import { expandNode, getGraph } from "@/ontology/api"
import { GraphInspector, type ExpandState } from "./GraphInspector"
import { GraphSearch } from "./GraphSearch"
import { GraphMinimap, type MiniPoint, type MiniViewport } from "./GraphMinimap"

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

const ORIGINS: { id: GraphOrigin; label: string }[] = [
  { id: "applied", label: "Applied" },
  { id: "proposed", label: "Proposed" },
]

// Per-type inline-SVG icons (MV-D75) as data URIs — NO new dependency (MV-D45). A light
// stroke reads on the coloured node fills. `%23` is an escaped '#' so the URI stays valid.
function svgIcon(inner: string): string {
  return `url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='%23F8FAFC' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'>${inner}</svg>")`
}
const ICONS = {
  table: svgIcon("<rect x='3' y='4' width='18' height='16' rx='2'/><path d='M3 10h18M9 4v16'/>"),
  metric_view: svgIcon("<path d='M4 20V10M10 20V4M16 20v-8M22 20H2'/>"),
  agent: svgIcon("<path d='M12 3v4M8 21h8M12 15v6'/><rect x='5' y='7' width='14' height='8' rx='3'/>"),
  dashboard: svgIcon("<rect x='3' y='3' width='7' height='7' rx='1'/><rect x='14' y='3' width='7' height='7' rx='1'/><rect x='14' y='14' width='7' height='7' rx='1'/><rect x='3' y='14' width='7' height='7' rx='1'/>"),
  measure: svgIcon("<path d='M4 9h16M4 15h16M10 3 8 21M16 3l-2 18'/>"),
  page: svgIcon("<path d='M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z'/><path d='M14 2v6h6M8 13h8M8 17h5'/>"),
}
const ICON_STYLE = {
  "background-fit": "none",
  "background-width": "55%",
  "background-height": "55%",
  "background-clip": "none",
  "background-image-opacity": 0.9,
} as const

// ── Visual system (Map v3 §1A, MV-D76) ──────────────────────────────────────
// The shipped webfonts, loaded by index.css and measured by the canvas renderer.
// Cabinet Grotesk = display (container titles); General Sans = everything else.
const FONT_DISPLAY = "Cabinet Grotesk, Inter, system-ui, sans-serif"
const FONT_BODY = "General Sans, Inter, system-ui, sans-serif"

// Crisp label plate instead of the old heavy text-outline halo (§1A defect #1):
// a translucent sunken chip under every label keeps text readable over edges and
// fills at any zoom without warping the glyphs.
const LABEL_PLATE = {
  "text-outline-width": 0,
  "text-background-color": "#0D1321",
  "text-background-opacity": 0.82,
  "text-background-padding": 3,
  "text-background-shape": "round-rectangle",
} as const

const STYLESHEET = [
  {
    selector: 'node[ntype="container"]',
    style: {
      shape: "round-rectangle",
      "corner-radius": 14,
      "background-color": "data(color)",
      "background-opacity": 0.06,
      "border-color": "data(color)",
      "border-width": 1.5,
      "border-opacity": 0.6,
      label: "data(label)",
      "font-family": FONT_DISPLAY,
      "font-size": 13,
      "font-weight": 700,
      color: "#F8FAFC",
      "text-valign": "top",
      "text-halign": "center",
      "text-margin-y": -8,
      ...LABEL_PLATE,
      padding: 26,
      "z-index": 1,
    },
  },
  {
    // Sub-domain container nested inside its top domain. SOLID by default —
    // provenance styling is origin-driven only (§1E; old defect #2 was dashing
    // every subcontainer regardless of origin).
    selector: 'node[ntype="subcontainer"]',
    style: {
      shape: "round-rectangle",
      "corner-radius": 10,
      "background-color": "data(color)",
      "background-opacity": 0.05,
      "border-color": "data(color)",
      "border-width": 1,
      "border-opacity": 0.5,
      label: "data(label)",
      "font-family": FONT_BODY,
      "font-size": 10.5,
      "font-weight": 600,
      color: "#CBD5E1",
      "text-valign": "top",
      "text-halign": "center",
      "text-margin-y": -5,
      ...LABEL_PLATE,
      padding: 16,
      "z-index": 2,
    },
  },
  {
    // Domain hub (Domains LOD): a filled disc with a soft rim; the two-line
    // caption (`display` = name + member count) sits BELOW on a plate so long
    // names never truncate into the fill (§1A defect: warped centered labels).
    selector: 'node[ntype="domain"]',
    style: {
      "background-color": "data(color)",
      "background-opacity": 0.92,
      "border-width": 2,
      "border-color": "#F8FAFC",
      "border-opacity": 0.22,
      width: "data(px)",
      height: "data(px)",
      label: "data(label)",
      "font-family": FONT_BODY,
      "font-size": 11.5,
      "font-weight": 600,
      color: "#E2E8F0",
      "line-height": 1.25,
      "text-valign": "bottom",
      "text-halign": "center",
      "text-margin-y": 7,
      "text-wrap": "wrap",
      "text-max-width": 150,
      ...LABEL_PLATE,
      "z-index": 10,
    },
  },
  {
    selector: 'node[ntype="subdomain"]',
    style: {
      "background-color": "data(color)",
      "background-opacity": 0.9,
      "border-width": 1.5,
      "border-color": "#F8FAFC",
      "border-opacity": 0.18,
      width: "data(px)",
      height: "data(px)",
      label: "data(label)",
      "font-family": FONT_BODY,
      "font-size": 10,
      "font-weight": 500,
      color: "#CBD5E1",
      "text-valign": "bottom",
      "text-halign": "center",
      "text-margin-y": 5,
      "text-wrap": "wrap",
      "text-max-width": 110,
      ...LABEL_PLATE,
    },
  },
  {
    selector: 'node[ntype="asset"]',
    style: {
      "background-color": "data(color)",
      width: "data(px)",
      height: "data(px)",
      label: "data(label)",
      "font-family": FONT_BODY,
      "font-size": 9,
      "font-weight": 500,
      color: "#94A3B8",
      "min-zoomed-font-size": 11, // declutter: hide asset labels until zoomed in
      "text-valign": "bottom",
      "text-halign": "center",
      "text-margin-y": 4,
      ...LABEL_PLATE,
    },
  },
  // Two-line hub caption (name + count) wherever the model provides one.
  { selector: "node[display]", style: { label: "data(display)" } },
  // Expand-on-demand satellites (MV-D73/D75): small "snippet" chips off their parent.
  {
    selector: 'node[ntype="measure"]',
    style: {
      shape: "round-rectangle",
      "corner-radius": 3,
      "background-color": "data(color)",
      width: "data(px)",
      height: "data(px)",
      label: "data(label)",
      "font-family": FONT_BODY,
      "font-size": 8.5,
      color: "#CBD5E1",
      "text-valign": "bottom",
      "text-halign": "center",
      "text-margin-y": 3,
      "min-zoomed-font-size": 8,
      ...LABEL_PLATE,
      ...ICON_STYLE,
      "background-image": ICONS.measure,
    },
  },
  {
    selector: 'node[ntype="page"]',
    style: {
      shape: "round-rectangle",
      "corner-radius": 3,
      "background-color": "data(color)",
      width: "data(px)",
      height: "data(px)",
      label: "data(label)",
      "font-family": FONT_BODY,
      "font-size": 8.5,
      color: "#CBD5E1",
      "text-valign": "bottom",
      "text-halign": "center",
      "text-margin-y": 3,
      "min-zoomed-font-size": 8,
      ...LABEL_PLATE,
      ...ICON_STYLE,
      "background-image": ICONS.page,
    },
  },
  // Per-type icons + accents on the asset dots (§1A: type = icon + shape).
  { selector: 'node[kind="table"]', style: { ...ICON_STYLE, "background-image": ICONS.table } },
  { selector: 'node[kind="view"]', style: { ...ICON_STYLE, "background-image": ICONS.table } },
  {
    selector: 'node[kind="metric_view"]',
    style: { "border-color": "#22D3EE", "border-width": 2, "border-opacity": 1, ...ICON_STYLE, "background-image": ICONS.metric_view },
  },
  {
    selector: 'node[kind="dashboard"]',
    style: { ...ICON_STYLE, "background-image": ICONS.dashboard },
  },
  // Agents render hollow (sunken fill + violet rim) like the mockup's evidence set.
  {
    selector: 'node[kind="agent"], node[kind="genie_agent"]',
    style: {
      "background-color": "#0D1321",
      "background-opacity": 1,
      "border-color": "#A78BFA",
      "border-width": 2,
      "border-opacity": 1,
      ...ICON_STYLE,
      "background-image": ICONS.agent,
    },
  },
  // Provenance (MV-D74, §1E): driven by `origin` ONLY. Proposed rollups — dashed
  // at every level; applied stays solid/current-state.
  {
    selector: 'node[origin="proposed"][ntype="container"]',
    style: { "border-style": "dashed", "border-opacity": 0.75 },
  },
  {
    selector: 'node[origin="proposed"][ntype="subcontainer"]',
    style: { "border-style": "dashed", "border-opacity": 0.7 },
  },
  {
    selector: 'node[origin="proposed"][ntype="domain"]',
    style: { "border-style": "dashed", "border-color": "#CBD5E1", "border-opacity": 0.8 },
  },
  {
    selector: 'node[origin="proposed"][ntype="subdomain"]',
    style: { "border-style": "dashed", "border-color": "#CBD5E1", "border-opacity": 0.8 },
  },
  // Ungrouped is neither Applied nor Suggested — it's the honest leftover bucket.
  // Neutral hollow + dotted rim, distinct from the provenance encodings (placed
  // after them so it wins for the Ungrouped rollup).
  {
    selector: "node[?isUngrouped]",
    style: {
      "background-color": "#64748B",
      "background-opacity": 0.18,
      "border-color": "#64748B",
      "border-style": "dotted",
      "border-width": 1.5,
      "border-opacity": 0.8,
      color: "#94A3B8",
    },
  },
  {
    selector: 'node[ntype="more"]',
    style: {
      shape: "round-rectangle",
      "corner-radius": 4,
      "background-color": "#1E293B",
      "border-color": "data(color)",
      "border-width": 1,
      "border-style": "dashed",
      "border-opacity": 0.7,
      label: "data(label)",
      "font-family": FONT_BODY,
      "font-size": 9,
      color: "#94A3B8",
      "text-valign": "center",
      "text-halign": "center",
      width: 52,
      height: 18,
    },
  },
  // Edges: weight = line weight (§1A encoding); hue = relationship type.
  {
    selector: "edge",
    style: { width: 1.2, "line-color": "#475569", "curve-style": "bezier", opacity: 0.45 },
  },
  { selector: "edge[w]", style: { width: "data(w)" } },
  { selector: 'edge[etype="coquery"]', style: { "line-color": "#818CF8", "line-style": "dashed", opacity: 0.65 } },
  { selector: 'edge[etype="lineage"]', style: { "line-color": "#64748B" } },
  // Satellite edges (measure / Page links) — hairline, distinct hue, always shown.
  { selector: 'edge[etype="snippet"]', style: { width: 1, "line-color": "#38BDF8", "line-style": "dotted", opacity: 0.55, "curve-style": "bezier" } },
  { selector: "node.faded", style: { opacity: 0.12 } },
  { selector: "edge.faded", style: { opacity: 0.05 } },
  { selector: "node.focused", style: { "border-color": "#22D3EE", "border-width": 3, "border-opacity": 1, "border-style": "solid" } },
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
    padding: 36,
    // Domains: few large hubs with two-line captions BELOW them — spread wide so
    // labels never collide with a neighbouring hub. Sub-domains: labelled leaves
    // inside compound boxes — enough separation that sibling labels and the boxes
    // themselves never overlap (§1A no-overlap at default zoom).
    nodeSeparation: lod === "domains" ? 160 : lod === "subdomains" ? 130 : 90,
    // Long leash for edges that CROSS compound boxes so a connected sub-domain is
    // never dragged into a neighbouring domain's box (box-overlap fix); short
    // intra-box edges keep siblings clustered.
    idealEdgeLength:
      lod === "domains"
        ? 190
        : (edge: { source(): { data(k: string): unknown }; target(): { data(k: string): unknown } }) => {
            const sp = edge.source().data("parent")
            const tp = edge.target().data("parent")
            return sp && tp && sp === tp ? 90 : 320
          },
    nodeRepulsion: lod === "domains" ? 18000 : lod === "subdomains" ? 16000 : 7500,
    packComponents: true,
    // A touch more breathing room between sibling compound boxes than fcose's default.
    nestingFactor: 0.15,
    gravity: lod === "domains" ? 0.2 : 0.25,
    gravityCompound: lod === "assets" ? 1.2 : 0.8,
    tile: true,
  }
}

// Minimal structural types for the bits of the cytoscape API this component uses. The
// library ships no types (see cytoscape-shims.d.ts, MV-D45); these keep tsc/lint honest
// without an `any` and without a new @types dependency.
interface CyCollection {
  addClass(cls: string): void
  removeClass(cls: string): void
}
interface CyNode extends CyCollection {
  data(): Record<string, unknown>
  position(): { x: number; y: number }
  closedNeighborhood(): CyCollection
  connectedEdges(): CyCollection
  ancestors(): CyCollection
  descendants(): CyCollection
}
interface CyNodeCollection extends CyCollection {
  length: number
  forEach(fn: (n: CyNode) => void): void
  filter(fn: (n: CyNode) => boolean): CyNodeCollection
  first(): CyNode
}
interface CyExtent {
  x1: number
  y1: number
  x2: number
  y2: number
}
interface CyCore {
  removeListener(ev: string): void
  ready(fn: () => void): void
  fit(eles?: unknown, padding?: number): void
  center(eles?: unknown): void
  batch(fn: () => void): void
  elements(): CyCollection
  nodes(selector?: string): CyNodeCollection
  edges(selector?: string): CyCollection
  extent(): CyExtent
  on(events: string, selector: string, handler: (evt: { target: CyNode }) => void): void
  on(events: string, handler: (evt: { target: CyNode | CyCore }) => void): void
}

interface ExpandRecord {
  nodeId: string
  parentId: string
  data: OntologyGraphExpand
}

/**
 * Injectable API seam (Map v3 §2.2, MV-D77): the two runtime calls the map makes, threaded
 * as an optional prop so the dev-only harness can substitute fixture-backed implementations
 * (NO new dependency, MV-D45). Prod never passes it and keeps the real `api.ts` functions.
 */
export interface EstateGraphApi {
  getGraph: (origin: GraphOrigin) => Promise<OntologyGraph>
  expandNode: (node: string, origin: GraphOrigin) => Promise<OntologyGraphExpand>
}

// Module-level singleton so the default prop value is referentially stable across renders
// (it participates in hook dependency arrays).
const REAL_API: EstateGraphApi = { getGraph, expandNode }

/**
 * Props. Everything beyond `graph` is optional and additive:
 *  - `api` — the injectable data seam above (harness/testing only).
 *  - `initialOrigin` / `initialLod` / `initialFocus` — deep-link style initial view state,
 *    used by the harness to make every state URL-addressable (and therefore screenshotable)
 *    without simulated clicks. Defaults reproduce the exact pre-v3 behaviour.
 *  - `onCyReady` — dev hook: receives the cytoscape instance once per mount (inside the
 *    idempotency guard), so the harness can drive taps/asserts. Unused in prod.
 */
export interface EstateGraphProps {
  graph: OntologyGraph
  api?: EstateGraphApi
  initialOrigin?: GraphOrigin
  initialLod?: Lod
  initialFocus?: { topId: string; name: string } | null
  onCyReady?: (cy: unknown) => void
}

// A node is expandable (§2.3) when it's a metric view (→ measures) or a sub-domain (→ Pages).
function isExpandable(data: Record<string, unknown> | null): boolean {
  if (!data) return false
  const ntype = String(data.ntype ?? "")
  if (ntype === "subdomain" || ntype === "subcontainer") return true
  return ntype === "asset" && String(data.kind ?? "") === "metric_view"
}

export function EstateGraph({
  graph,
  api = REAL_API,
  initialOrigin = "applied",
  initialLod = "domains",
  initialFocus = null,
  onCyReady,
}: EstateGraphProps) {
  const [origin, setOrigin] = useState<GraphOrigin>(initialOrigin)
  const [lod, setLod] = useState<Lod>(initialLod)
  const [focusTop, setFocusTop] = useState<string | null>(initialFocus?.topId ?? null)
  const [focusName, setFocusName] = useState<string | null>(initialFocus?.name ?? null)
  const [subCrumb, setSubCrumb] = useState<string | null>(null)
  const [selected, setSelected] = useState<Record<string, unknown> | null>(null)

  // Non-applied graph cache. Applied always mirrors the prop (OntologyPage fetches it as the
  // default); Proposed is fetched lazily the first time the toggle asks for it.
  const [cache, setCache] = useState<Partial<Record<GraphOrigin, OntologyGraph>>>({})
  const [loadingGraph, setLoadingGraph] = useState(false)
  const [graphError, setGraphError] = useState<string | null>(null)

  // Expand-on-demand state (keyed by tapped node id, deduped in mergeExpand).
  const [expands, setExpands] = useState<ExpandRecord[]>([])
  const [expandStateById, setExpandStateById] = useState<Record<string, ExpandState>>({})
  const [expandVersion, setExpandVersion] = useState(0)

  const [search, setSearch] = useState("")
  const [searchHint, setSearchHint] = useState<string | null>(null)
  const [mini, setMini] = useState<{ points: MiniPoint[]; viewport: MiniViewport | null }>({
    points: [],
    viewport: null,
  })

  const cyRef = useRef<CyCore | null>(null)
  // react-cytoscapejs@2.0.0 invokes the `cy` prop on EVERY update, not just mount
  // (updateCytoscape → `t.cy(n)`), so the wiring below must be idempotent per instance:
  // re-running `cy.ready(() => cy.fit())` + re-binding viewport listeners on each render
  // makes fit re-emit pan/zoom → refreshMini → setMini → re-render → … "Maximum update
  // depth exceeded" (React #185). A new instance only appears on remount (the `key`
  // already changes on origin/lod/focus/expand), so gate init on instance identity.
  const initedCyRef = useRef<CyCore | null>(null)

  // Applied mirrors the prop; other origins come from the lazy cache.
  const activeGraph = origin === "applied" ? graph : cache[origin]

  // Lazily fetch the proposed graph (or any uncached origin) when the toggle asks for it.
  // The setState calls live inside this async helper (not directly in the effect body) so
  // the effect only synchronises with an external system (the graph fetch), mirroring the
  // OntologyPage load pattern.
  const loadOrigin = useCallback(
    async (which: GraphOrigin) => {
      setLoadingGraph(true)
      setGraphError(null)
      try {
        const g = await api.getGraph(which)
        setCache((prev) => ({ ...prev, [which]: g }))
      } catch (e) {
        setGraphError(e instanceof Error ? e.message : "Couldn't load the map")
      } finally {
        setLoadingGraph(false)
      }
    },
    [api],
  )

  useEffect(() => {
    if (origin === "applied" || cache[origin]) return
    void loadOrigin(origin)
  }, [origin, cache, loadOrigin])

  const resetView = useCallback(() => {
    setSelected(null)
    setExpands([])
    setExpandStateById({})
    setSubCrumb(null)
  }, [])

  const changeOrigin = (next: GraphOrigin) => {
    if (next === origin) return
    setOrigin(next)
    resetView()
  }

  const changeLod = (next: Lod) => {
    setLod(next)
    resetView()
    // Domains is the aggregate overview — leaving a drill-down focus set there would hide
    // tops; clear it so the overview always shows the whole estate.
    if (next === "domains") {
      setFocusTop(null)
      setFocusName(null)
    }
  }

  const drillInto = (topId: string, name: string) => {
    setFocusTop(topId)
    setFocusName(name)
    setLod("assets")
    resetView()
  }

  const clearFocus = () => {
    setFocusTop(null)
    setFocusName(null)
    setLod("domains")
    resetView()
  }

  // Elements: pure model build, then fold each expand payload in (deduped, dangling-safe).
  const baseEls = useMemo<CyEl[]>(() => {
    if (!activeGraph) return []
    // viewElements enforces the Assets-LOD-requires-focus gate (MV-D75).
    return viewElements(activeGraph, lod, focusTop)
  }, [activeGraph, lod, focusTop])

  const mergedEls = useMemo<CyEl[]>(() => {
    let els = baseEls
    for (const ex of expands) els = mergeExpand(els, ex.data, ex.parentId)
    return els
  }, [baseEls, expands])

  const elements = useMemo(
    () => mergedEls.map((el) => (el.position ? { data: el.data, position: el.position } : { data: el.data })),
    [mergedEls],
  )
  const layout = useMemo(() => layoutFor(lod), [lod])

  const triggerExpand = useCallback(
    (data: Record<string, unknown>) => {
      const nodeId = String(data.id)
      const parentId = String(data.parent ?? data.id)
      const state = expandStateById[nodeId]
      if (state === "loading" || state === "done") return
      setExpandStateById((prev) => ({ ...prev, [nodeId]: "loading" }))
      api
        .expandNode(nodeId, origin)
        .then((exp) => {
          setExpands((prev) => [...prev.filter((e) => e.nodeId !== nodeId), { nodeId, parentId, data: exp }])
          setExpandStateById((prev) => ({ ...prev, [nodeId]: "done" }))
          setExpandVersion((v) => v + 1) // force a relayout so satellites settle
        })
        .catch(() => {
          setExpandStateById((prev) => ({ ...prev, [nodeId]: "error" }))
        })
    },
    [expandStateById, origin, api],
  )

  const refreshMini = useCallback((cy: CyCore) => {
    const points: MiniPoint[] = []
    cy.nodes().forEach((n) => {
      const nt = String(n.data().ntype ?? "")
      if (nt === "container" || nt === "subcontainer") return
      const p = n.position()
      if (Number.isFinite(p.x) && Number.isFinite(p.y)) {
        points.push({ x: p.x, y: p.y, color: String(n.data().color ?? "#64748B") })
      }
    })
    const e = cy.extent()
    setMini({ points, viewport: { x1: e.x1, y1: e.y1, x2: e.x2, y2: e.y2 } })
  }, [])

  const handleSelect = useCallback(
    (cy: CyCore, node: CyNode) => {
      const data = { ...node.data() }
      setSelected(data)
      const nt = String(data.ntype ?? "")
      if (nt === "subdomain" || nt === "subcontainer") setSubCrumb(String(data.label ?? ""))
      cy.batch(() => {
        cy.nodes('[ntype != "container"][ntype != "subcontainer"]').addClass("faded")
        cy.edges().addClass("faded")
        node.removeClass("faded")
        node.closedNeighborhood().removeClass("faded")
        // Reveal (and un-fade) only the tapped node's own connections — "FK view on demand".
        node.connectedEdges().removeClass("hidden faded")
        node.ancestors().removeClass("faded")
        node.descendants().removeClass("faded")
        cy.nodes().removeClass("focused")
        node.addClass("focused")
      })
      if (isExpandable(data)) triggerExpand(data)
    },
    [triggerExpand],
  )

  const runSearch = () => {
    const cy = cyRef.current
    const q = search.trim().toLowerCase()
    if (!cy || !q) return
    const matches = cy.nodes().filter((n) => String(n.data().label ?? "").toLowerCase().includes(q))
    if (matches.length === 0) {
      setSearchHint("No match on this view")
      return
    }
    setSearchHint(null)
    const node = matches.first()
    cy.center(node)
    handleSelect(cy, node)
  }

  // Counts describe the graph actually on screen (activeGraph, not always the applied
  // prop) and count top-level business areas only — sub-domains are not "Domains".
  const countGraph = activeGraph ?? graph
  const domainCount = countGraph.domains.nodes.filter(
    (n) => !n.parent_id && n.kind !== "ungrouped" && n.id !== "ungrouped",
  ).length
  const subdomainCount = countGraph.domains.nodes.filter((n) => !!n.parent_id).length
  const assetCount = countGraph.assets.nodes.length
  const truncated = (activeGraph?.domains.truncated || activeGraph?.assets.truncated) ?? false
  const isEmpty = !!activeGraph && activeGraph.domains.nodes.length + activeGraph.assets.nodes.length === 0
  const assetsNeedFocus = lod === "assets" && !focusTop
  const facts = selected ? nodeFacts(selected) : null
  const selectedExpandState: ExpandState = selected ? expandStateById[String(selected.id)] ?? "idle" : "idle"

  // ── Loading / error before any graph exists for this origin (MV-D43) ──────
  if (!activeGraph) {
    if (graphError) {
      return (
        <div className="flex items-start gap-2.5 rounded-xl border border-danger/30 bg-danger/5 px-4 py-3.5">
          <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0 text-danger-foreground" />
          <div>
            <p className="text-sm font-semibold text-primary">Couldn&apos;t load the map</p>
            <p className="mt-1 max-w-prose text-xs text-secondary">{graphError}</p>
            <button
              onClick={() => setOrigin("applied")}
              className="mt-2 rounded-lg border border-default px-2.5 py-1 text-xs text-secondary hover:text-primary"
            >
              Back to current view
            </button>
          </div>
        </div>
      )
    }
    return (
      <div className="flex items-center gap-2 rounded-xl border border-default bg-surface px-4 py-6 text-sm text-secondary">
        <Loader2 className="h-4 w-4 animate-spin text-accent" /> Building the suggested map…
      </div>
    )
  }

  // ── Honest-empty (MV-D43) with a one-tap nudge to Proposed when applied is bare (MV-D74) ──
  if (isEmpty) {
    return (
      <div className="flex items-start gap-2.5 rounded-xl border border-info/30 bg-info/5 px-4 py-3.5">
        <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0 text-info-foreground" />
        <div>
          <p className="text-sm font-semibold text-primary">No data in the estate graph</p>
          <p className="mt-1 max-w-prose text-xs text-secondary">
            {origin === "applied"
              ? "Nothing has been organised into business areas yet — this is the honest current state. You can preview what the engine suggests."
              : "The graph is empty — there are no domains or assets to display. Run a refresh to populate the estate."}
          </p>
          {origin === "applied" && (
            <button
              onClick={() => changeOrigin("proposed")}
              className="mt-2 rounded-lg bg-accent px-3 py-1.5 text-xs font-semibold text-white hover:opacity-90"
            >
              View suggested
            </button>
          )}
        </div>
      </div>
    )
  }

  return (
    <div className="rounded-xl border border-default bg-surface overflow-hidden">
      {/* Controls: source toggle + LOD toggle + breadcrumb / search / counts */}
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-default bg-elevated/40 px-4 py-2.5">
        <div className="flex flex-wrap items-center gap-2">
          {/* Applied | Proposed source toggle (MV-D74) */}
          <div className="inline-flex overflow-hidden rounded-lg border border-default text-xs">
            {ORIGINS.map((o) => (
              <button
                key={o.id}
                onClick={() => changeOrigin(o.id)}
                disabled={loadingGraph && o.id !== origin}
                className={`border-l border-default px-2.5 py-1 first:border-l-0 transition-colors disabled:opacity-50 ${
                  origin === o.id ? "bg-accent font-semibold text-white" : "text-secondary hover:text-primary"
                }`}
              >
                {o.label}
              </button>
            ))}
          </div>
          {origin === "proposed" && (
            <span className="inline-flex items-center rounded-full border border-dashed border-secondary/50 bg-secondary/10 px-2 py-0.5 text-xs text-secondary">
              Suggested
            </span>
          )}
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
        </div>
        <div className="flex items-center gap-3">
          <GraphSearch
            value={search}
            onChange={(v) => {
              setSearch(v)
              setSearchHint(null)
            }}
            onSubmit={runSearch}
            onClear={() => {
              setSearch("")
              setSearchHint(null)
            }}
            hint={searchHint}
          />
          <div className="flex items-center gap-x-4 text-xs text-secondary">
            <span><span className="font-semibold text-accent">{domainCount}</span> Domains</span>
            <span><span className="font-semibold text-accent">{subdomainCount}</span> Sub-domains</span>
            <span><span className="font-semibold text-accent">{assetCount}</span> Assets</span>
            {truncated && <span className="text-muted">Top 2,000 · centrality</span>}
          </div>
        </div>
      </div>

      {/* Breadcrumb: Estate ▸ Domain ▸ Sub-domain */}
      <div className="flex items-center gap-1.5 border-b border-default bg-surface px-4 py-1.5 text-xs text-muted">
        <button onClick={clearFocus} className="hover:text-primary">
          Estate
        </button>
        {focusName && (
          <>
            <span aria-hidden>▸</span>
            <span className="text-secondary">{focusName}</span>
          </>
        )}
        {subCrumb && (
          <>
            <span aria-hidden>▸</span>
            <span className="text-secondary">{subCrumb}</span>
          </>
        )}
      </div>

      {/* Body: canvas + docked right-rail inspector */}
      <div className="flex">
        {/* min-w-0 lets the canvas column shrink below the canvas bitmap's intrinsic
            width — without it the fixed-width inspector rail gets pushed off-screen.
            The sunken ground + faint dot grid give the map a drafting-table depth
            distinct from the surrounding panel (§1A whitespace/density). */}
        <div
          className="relative min-w-0 flex-1"
          style={{
            height: "560px",
            backgroundColor: "var(--bg-sunken)",
            backgroundImage: "radial-gradient(circle at 1px 1px, rgba(148, 163, 184, 0.07) 1px, transparent 0)",
            backgroundSize: "22px 22px",
          }}
        >
          {assetsNeedFocus ? (
            <div className="flex h-full flex-col items-center justify-center gap-2 px-6 text-center">
              <MousePointerClick className="h-6 w-6 text-muted" />
              <p className="text-sm font-semibold text-primary">Pick a business area to see its assets</p>
              <p className="max-w-sm text-xs text-secondary">
                Open a domain from the Domains or Sub-domains view, then its tables and metrics show here — no
                overwhelming all-at-once map.
              </p>
              <button
                onClick={() => changeLod("domains")}
                className="mt-1 rounded-lg border border-default px-2.5 py-1 text-xs text-secondary hover:text-primary"
              >
                Back to Domains
              </button>
            </div>
          ) : (
            <>
              <CytoscapeComponent
                key={`${origin}:${lod}:${focusTop ?? "all"}:${activeGraph.as_of ?? ""}:${expandVersion}`}
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
                  cyRef.current = cy
                  // Idempotency guard: wire this cytoscape instance exactly once. On plain
                  // re-renders react-cytoscapejs calls this again with the SAME instance —
                  // returning early avoids the fit→pan/zoom→setMini→re-render feedback loop
                  // (React #185). Remounts (key change) yield a new instance → re-wire.
                  if (initedCyRef.current === cy) return
                  initedCyRef.current = cy
                  cy.removeListener("tap")
                  cy.removeListener("layoutstop")
                  cy.removeListener("pan")
                  cy.removeListener("zoom")
                  cy.ready(() => {
                    // Edge-on-demand: hide FK/lineage/co-query edges at deep LODs (kept in the
                    // sim so clusters emerge). Satellite edges stay visible so an expand shows.
                    if (lod !== "domains") cy.edges('[etype != "snippet"]').addClass("hidden")
                    cy.fit(undefined, 28)
                    refreshMini(cy)
                  })
                  cy.on("layoutstop", () => refreshMini(cy))
                  cy.on("pan", () => refreshMini(cy))
                  cy.on("zoom", () => refreshMini(cy))
                  cy.on("tap", "node", (evt: { target: CyNode }) => handleSelect(cy, evt.target))
                  cy.on("tap", (evt: { target: CyNode | CyCore }) => {
                    if (evt.target === cy) {
                      setSelected(null)
                      cy.batch(() => {
                        cy.elements().removeClass("faded focused")
                        if (lod !== "domains") cy.edges('[etype != "snippet"]').addClass("hidden")
                      })
                    }
                  })
                  onCyReady?.(cy)
                }}
              />
              <div className="pointer-events-none absolute bottom-3 left-3">
                <GraphMinimap points={mini.points} viewport={mini.viewport} />
              </div>
            </>
          )}
        </div>

        {/* Right-rail inspector (upgrade of the popover; plain language, MV-D23) */}
        <div className="w-72 shrink-0 border-l border-default bg-elevated/40">
          <GraphInspector
            facts={facts}
            canExpand={isExpandable(selected)}
            expandState={selectedExpandState}
            onExpand={() => selected && triggerExpand(selected)}
            onDrill={drillInto}
            onClose={() => setSelected(null)}
          />
        </div>
      </div>

      {/* Legend */}
      <div className="border-t border-default bg-elevated/50 px-4 py-3">
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2 text-xs text-muted">
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2.5 w-3.5 rounded border" style={{ borderColor: "#818CF8", background: "#818cf814" }} /> Domain (container)
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2.5 w-3.5 rounded border border-dashed" style={{ borderColor: "#CBD5E1" }} /> Suggested
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full" style={{ backgroundColor: "#64748B" }} /> Asset
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full border-2" style={{ borderColor: "#22D3EE" }} /> Metric view
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-sm" style={{ backgroundColor: "#FBBF24" }} /> Page
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
          <p>Source: {origin === "applied" ? "Applied (current governed tags)" : "Proposed (suggested by the engine)"}</p>
          <p>Layout: {activeGraph.layout}</p>
          <p>State: {activeGraph.state}</p>
          {activeGraph.as_of && <p>Last updated: {activeGraph.as_of}</p>}
        </div>
      </div>
    </div>
  )
}
