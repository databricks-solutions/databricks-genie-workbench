/**
 * Ontology Map north-star (MV-D81/D83/D84, §9-A) — deterministic d3 tidy-tree renderer.
 *
 * Replaces the Cytoscape-fcose-LOD core with a React-controlled SVG: a pure `d3.tree`
 * layout (`ontologyTreeLayout`), a typed verb overlay, an off-tree Ungrouped tray, and
 * dashed proposal hulls, dual-theme via `graphTokens`. d3 is used ONLY imperatively via two
 * refs — `d3.zoom` on the <svg> and `d3.drag` on nodes — never for rendering (React owns the
 * DOM). Expand/collapse happens in place (no LOD control). Degrades to the shallow contract
 * (MV-D43) when Lane-D fields are absent. Honest loading / empty / error / stale states.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import {
  AlertTriangle,
  ChevronsDownUp,
  Eye,
  Loader2,
  Maximize,
  Maximize2,
  Minimize,
  RotateCcw,
  Sparkles,
} from "lucide-react"
import { drag as d3drag } from "d3-drag"
import { select } from "d3-selection"
import { zoom as d3zoom, zoomIdentity, type ZoomBehavior } from "d3-zoom"
import type {
  GraphOrigin,
  OntologyDrafts,
  OntologyGraph,
  OntologyGraphExpand,
  OntologyTaxonomy,
} from "@/ontology/types"
import { expandNode as apiExpandNode } from "@/ontology/api"
import {
  buildEstateModel,
  filterModelByVisibility,
  fmtCount,
  fmtMoney,
  mergeHydration,
  pagesFromExpansions,
  relTypeLegend,
  type EstateModel,
  type NodeType,
} from "@/ontology/estateGraphModel"
import {
  DEFAULT_LAYOUT,
  ancestorPath,
  centerOnTransform,
  collapsedToDomainTier,
  contentBounds,
  initialExpanded,
  layoutHash,
  layoutTree,
  viewportContentRect,
  type LaidNode,
  type Layout,
  type Point,
} from "@/ontology/ontologyTreeLayout"
import { bandColor, domainTintFor, graphTokens } from "@/ontology/graphTokens"
import { useTheme } from "@/hooks/useTheme"
import { GraphInspector, type InspectorData } from "./GraphInspector"
import { GraphSearch } from "./GraphSearch"
import { GraphMinimap, type MiniPoint, type MiniRect, type MiniViewport } from "./GraphMinimap"
import {
  GraphEdgeTooltip,
  GraphTooltip,
  assembleEdgeTooltip,
  assembleTooltip,
  type EdgeTooltipData,
  type TooltipData,
} from "./GraphTooltip"
import { DomainVisibilityPanel, type VisibilityGroup } from "./DomainVisibilityPanel"

/** Injectable data seam (the harness supplies a fixture-backed mock). */
export interface EstateGraphApi {
  getGraph: (origin: GraphOrigin) => Promise<OntologyGraph>
  /** Expand-on-demand hydration (MV-D73): a node's measures / attached Pages. */
  expandNode?: (node: string, origin: GraphOrigin) => Promise<OntologyGraphExpand>
}

/** Handle exposed to the dev harness for the deterministic screenshot loop. */
export interface EstateGraphHandle {
  ready: boolean
  nodeCount: number
  layoutHash: string
  tapByLabel: (q: string) => boolean
  positions: () => { id: string; x: number; y: number }[]
}

type Provenance = "applied" | "proposed" | "both"

const PROVENANCE: { id: Provenance; label: string }[] = [
  { id: "applied", label: "Applied" },
  { id: "proposed", label: "Proposed" },
  { id: "both", label: "Both" },
]

const TYPE_LABEL: Record<NodeType, string> = {
  org: "Organization",
  domain: "Business area",
  subdomain: "Sub-area",
  agent: "Genie Agent",
  dashboard: "Dashboard",
  metric_view: "Metric View",
  measure: "Measure",
  table: "Table",
}

const LEGEND: { type: NodeType; label: string }[] = [
  { type: "domain", label: "Business area" },
  { type: "agent", label: "Genie Agent" },
  { type: "dashboard", label: "Dashboard" },
  { type: "metric_view", label: "Metric View" },
  { type: "measure", label: "Measure" },
  { type: "table", label: "Table" },
]

// Per-type inline-SVG glyph paths (NO new dependency, MV-D45). A single stroke reads on the
// saturated fills in both themes; the label plate carries the theme flip.
const GLYPHS: Record<NodeType, string> = {
  org: "M12 3 3 8v8l9 5 9-5V8z",
  domain: "M4 7h16M4 12h16M4 17h16",
  subdomain: "M6 8h12M6 12h12M9 16h9",
  agent: "M12 3v3M8 21h8M9 10h.01M15 10h.01",
  dashboard: "M3 3h7v7H3zM14 3h7v7h-7zM14 14h7v7h-7zM3 14h7v7H3z",
  metric_view: "M4 20V10M10 20V4M16 20v-8M22 20H2",
  measure: "M4 9h16M4 15h16M10 3 8 21M16 3l-2 18",
  table: "M3 4h18v16H3zM3 10h18M9 4v16",
}

/**
 * Canvas caption declutter (R12d): ellipsize an over-long name so plated sibling labels
 * never collide or hard-clip mid-word ("Revenue Accountin_"). The full name stays in the
 * inspector, breadcrumb, and a hover `<title>`, so nothing is lost.
 */
const LABEL_MAX = 16
function clampLabel(s: string): string {
  return s.length > LABEL_MAX ? `${s.slice(0, LABEL_MAX - 1).trimEnd()}…` : s
}

/** Generic plain-language copy by type — the fallback when a node has no real description. */
function describeType(type: NodeType): string {
  switch (type) {
    case "org":
      return "The whole estate — every governed business area lives under here."
    case "domain":
      return "A business area of the estate."
    case "subdomain":
      return "A sub-area within its business area."
    case "agent":
      return "Answers questions about this area in plain language."
    case "dashboard":
      return "A chart page built on this area's data."
    case "metric_view":
      return "A curated set of business measures."
    case "measure":
      return "A number a metric view reports."
    default:
      return "A data table."
  }
}

function describe(node: LaidNode): string {
  return describeType(node.type)
}

export function EstateGraph({
  graph,
  api,
  initialOrigin = "applied",
  drafts = null,
  taxonomy = null,
  estateName = null,
  onReady,
}: {
  graph: OntologyGraph
  api?: EstateGraphApi
  initialOrigin?: GraphOrigin
  drafts?: OntologyDrafts | null
  taxonomy?: OntologyTaxonomy | null
  estateName?: string | null
  onReady?: (handle: EstateGraphHandle) => void
}) {
  const { resolvedTheme } = useTheme()
  const theme = resolvedTheme === "light" ? "light" : "dark"
  const tokens = graphTokens(theme)

  const [provenance, setProvenance] = useState<Provenance>(initialOrigin === "proposed" ? "proposed" : "applied")
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [query, setQuery] = useState("")
  const [searchHits, setSearchHits] = useState<Set<string>>(new Set())
  const [typeFocus, setTypeFocus] = useState<NodeType | null>(null)
  const [hint, setHint] = useState<string | null>(null)
  // Hover-snippet (R6/R21): which node the cursor is over + its viewport position.
  const [hoveredId, setHoveredId] = useState<string | null>(null)
  const [hoverPos, setHoverPos] = useState({ x: 0, y: 0 })
  // Edge hover (R25) — which cross-link arc the cursor is over (mutually exclusive w/ node hover).
  const [hoveredEdgeId, setHoveredEdgeId] = useState<string | null>(null)
  // Relationship-type highlight (R25, Bloom idiom) — a legend verb, or null.
  const [relVerbFocus, setRelVerbFocus] = useState<string | null>(null)
  // Fullscreen overlay (P0-a) + right-rail domain show/hide panel (P1-b).
  const [fullscreen, setFullscreen] = useState(false)
  const [showDomainPanel, setShowDomainPanel] = useState(false)
  // Domain show/hide (R27) — top-domain / sub-domain container ids the curator has hidden.
  const [hiddenDomains, setHiddenDomains] = useState<Set<string>>(new Set())
  // Live viewport rect (content coords) for the minimap you-are-here box (R26).
  const [viewportRect, setViewportRect] = useState<MiniViewport | null>(null)

  // Data seam (MV-D43 honest states): the app passes a `graph` prop and owns its own
  // loading/error shell (OntologyPage), so with no `api` we render the prop directly. When
  // an `api` IS injected (the harness), we fetch per-provenance and surface loading/error.
  const [fetched, setFetched] = useState<OntologyGraph | null>(null)
  const [fetchState, setFetchState] = useState<"idle" | "loading" | "error">(api ? "loading" : "idle")
  useEffect(() => {
    if (!api) return
    let alive = true
    setFetchState("loading")
    api
      .getGraph(provenance === "proposed" ? "proposed" : "applied")
      .then((g) => {
        if (alive) {
          setFetched(g)
          setFetchState("idle")
        }
      })
      .catch(() => {
        if (alive) setFetchState("error")
      })
    return () => {
      alive = false
    }
  }, [api, provenance])
  const effectiveGraph = fetched ?? graph

  // Expand-on-demand hydration (MV-D73, §2.3): a node's measures / attached Pages, fetched
  // on expand and folded into the graph. Held in a ref + version counter (mirrors the
  // drag-offset seam) so a merge grows the tree in place without resetting view state.
  const hydrationRef = useRef<Map<string, OntologyGraphExpand>>(new Map())
  const hydrationInFlight = useRef<Set<string>>(new Set())
  const [hydrationVersion, setHydrationVersion] = useState(0)

  const augmentedGraph = useMemo(
    () => mergeHydration(effectiveGraph, hydrationRef.current.values()),
    // hydrationRef is mutated in place; hydrationVersion bumps on each successful merge.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [effectiveGraph, hydrationVersion],
  )

  const model: EstateModel = useMemo(
    () => buildEstateModel(augmentedGraph, { drafts, taxonomy, estateName }),
    [augmentedGraph, drafts, taxonomy, estateName],
  )

  // View-only domain show/hide (R27): the model the map actually renders — the full model with
  // any hidden domain/sub-domain subtree (and its cross-links + legend counts) removed. With
  // nothing hidden this IS `model` (identity), so the default paint's layout is byte-identical.
  const vizModel: EstateModel = useMemo(
    () => filterModelByVisibility(model, hiddenDomains),
    [model, hiddenDomains],
  )

  const [expanded, setExpanded] = useState<Set<string>>(() => initialExpanded(model))
  // Reset the open set + hydration ONLY when the underlying estate changes (provenance
  // toggle / new snapshot) — never when a hydration merge grows the model in place, which
  // would collapse the node the curator just expanded (R7). Keyed on the base graph +
  // provenance, so a measure/Page merge (which changes `model` but not the estate) is inert.
  const estateSig = useMemo(
    () =>
      `${effectiveGraph.root?.id ?? ""}|${effectiveGraph.node_count}|${provenance === "proposed" ? "p" : "a"}`,
    [effectiveGraph, provenance],
  )
  const lastSig = useRef(estateSig)
  useEffect(() => {
    if (lastSig.current === estateSig) return
    lastSig.current = estateSig
    hydrationRef.current = new Map()
    hydrationInFlight.current = new Set()
    setExpanded(initialExpanded(model))
    setSelectedId(null)
    setHoveredId(null)
    setUncapped(new Set())
  }, [estateSig, model])

  // Hydrate a newly-expanded metric view (its measures, which promote as amber children)
  // or subdomain (its attached Pages, surfaced in the inspector). Cached per node id and
  // guarded against concurrent fetches (bounded); an error degrades to an empty payload so
  // the node still expands. In production `EstateGraph` runs without an injected `api`, so
  // the real `expandNode` client is the default; the harness supplies a fixture-backed one.
  const doExpand = api?.expandNode ?? apiExpandNode
  useEffect(() => {
    const origin: GraphOrigin = provenance === "proposed" ? "proposed" : "applied"
    const typeById = new Map(model.nodes.map((n) => [n.id, n.type]))
    for (const id of expanded) {
      const t = typeById.get(id)
      if (t !== "metric_view" && t !== "subdomain") continue
      if (hydrationRef.current.has(id) || hydrationInFlight.current.has(id)) continue
      hydrationInFlight.current.add(id)
      doExpand(id, origin)
        .then((exp) => hydrationRef.current.set(id, exp))
        .catch(() => hydrationRef.current.set(id, { nodes: [], edges: [], parent_id: id, as_of: null }))
        .finally(() => {
          hydrationInFlight.current.delete(id)
          setHydrationVersion((v) => v + 1)
        })
    }
  }, [expanded, model, provenance, doExpand])

  // Manual drag offsets — persisted deltas applied post-layout (R17). A tick bumps relayout.
  const offsetsRef = useRef<Map<string, Point>>(new Map())
  const [dragTick, setDragTick] = useState(0)

  // Parents whose per-parent child cap has been lifted via their `+N more` chip (§6/R3).
  const [uncapped, setUncapped] = useState<Set<string>>(new Set())

  const layout: Layout = useMemo(
    () =>
      layoutTree(vizModel, expanded, offsetsRef.current, DEFAULT_LAYOUT, {
        focusId: selectedId,
        verbFocus: relVerbFocus,
        uncapped,
      }),
    // dragTick is a deliberate relayout trigger; offsetsRef is mutated in place.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [vizModel, expanded, dragTick, selectedId, relVerbFocus, uncapped],
  )

  const nodeById = useMemo(() => new Map(layout.nodes.map((n) => [n.id, n])), [layout])
  const showTree = provenance !== "proposed"
  const showProposals = provenance !== "applied"

  // Honest states (MV-D43). Computed early so the fit effect can gate on them.
  const isEmpty = layout.nodes.length <= 1 && layout.trayItems.length === 0 && model.proposals.length === 0
  // The applied tree has no governed structure (only the org root) but the engine has
  // suggestions: nudge the curator to view them without hiding the tray (MV-D43/D74).
  const treeBare = layout.nodes.length <= 1
  const showNudge = treeBare && provenance === "applied" && model.proposals.length > 0
  const isStale = effectiveGraph.state === "stale"

  // ── Camera (d3.zoom) ───────────────────────────────────────────────────────
  const svgRef = useRef<SVGSVGElement | null>(null)
  const gRef = useRef<SVGGElement | null>(null)
  const zoomRef = useRef<ZoomBehavior<SVGSVGElement, unknown> | null>(null)
  const [transform, setTransform] = useState({ x: 40, y: 40, k: 0.75 })

  useEffect(() => {
    const svg = svgRef.current
    if (!svg) return
    const z = d3zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.3, 2.5])
      .filter((e: Event) => {
        // Let node drags win over canvas pan; allow wheel + background pointer.
        const t = e.target as Element
        return !(t && t.closest && t.closest("[data-node-id]"))
      })
      .on("zoom", (e) => setTransform({ x: e.transform.x, y: e.transform.y, k: e.transform.k }))
    zoomRef.current = z
    select(svg).call(z)
    return () => {
      select(svg).on(".zoom", null)
    }
  }, [])

  const applyTransform = useCallback((t: { x: number; y: number; k: number }) => {
    const svg = svgRef.current
    if (!svg || !zoomRef.current) {
      setTransform(t)
      return
    }
    select(svg).call(zoomRef.current.transform, zoomIdentity.translate(t.x, t.y).scale(t.k))
  }, [])

  const fit = useCallback(() => {
    const svg = svgRef.current
    if (!svg) return
    // Frame the laid-out CONTENT bbox — nodes + any visible verb-arc extents (R12b), plus
    // the tray only when it's on screen. Center it and zoom so it fills the viewport with a
    // modest margin (R12a — no dead canvas, no crammed corner, no arcs off the edge).
    const b = contentBounds(layout, { tree: showTree, tray: showProposals || !showTree })
    if (b.width <= 0 || b.height <= 0) return
    const padX = 40
    // Reserve extra headroom at the top so the org / top-domain tier (and its label plate)
    // is never cropped by the canvas edge on fit (§5/R21e).
    const padTop = 60
    const padBottom = 40
    const bw = b.width + padX * 2
    const bh = b.height + padTop + padBottom
    const rect = svg.getBoundingClientRect()
    if (rect.width === 0 || rect.height === 0) return
    const k = Math.min(2.2, Math.max(0.28, Math.min(rect.width / bw, rect.height / bh) * 0.98))
    const x = rect.width / 2 - (b.minX + b.width / 2) * k
    const centeredY = rect.height / 2 - (b.minY + b.height / 2) * k
    // Center vertically, but keep at least `padTop` screen px above the content top.
    const y = Math.max(padTop - b.minY * k, centeredY)
    applyTransform({ x, y, k })
  }, [layout, showTree, showProposals, applyTransform])

  // Fit once the canvas is actually on screen. Guard against firing during the loading /
  // error / empty phases (the <svg> isn't mounted then) — otherwise the one-shot fires
  // against a null svg and never reframes once the graph arrives.
  const didFit = useRef(false)
  useEffect(() => {
    if (didFit.current) return
    if (fetchState !== "idle" || isEmpty) return
    if (layout.nodes.length === 0) return
    const id = requestAnimationFrame(() => {
      if (!svgRef.current) return
      didFit.current = true
      fit()
    })
    return () => cancelAnimationFrame(id)
  }, [fetchState, isEmpty, layout.nodes.length, fit])

  // Reframe once after a reveal has relayed out (see `reveal`). Runs on the post-reveal
  // layout so the newly-expanded path + its verb arcs are framed (R12b), then clears.
  useEffect(() => {
    if (!pendingFit.current) return
    pendingFit.current = false
    if (!didFit.current) return // initial fit will cover the first paint
    const id = requestAnimationFrame(() => {
      if (svgRef.current) fit()
    })
    return () => cancelAnimationFrame(id)
  }, [layout, fit])

  // ── Drag (d3.drag on nodes) ──────────────────────────────────────────────────
  useEffect(() => {
    const g = gRef.current
    if (!g) return
    const sel = select(g).selectAll<SVGGElement, unknown>("[data-node-id]")
    const dragged = d3drag<SVGGElement, unknown>()
      .on("start", function (e) {
        e.sourceEvent?.stopPropagation?.()
        setHoveredId(null) // hide the hover snippet while dragging (R21)
      })
      .on("drag", function (e) {
        const id = (this as SVGGElement).getAttribute("data-node-id")
        if (!id) return
        const cur = offsetsRef.current.get(id) ?? { x: 0, y: 0 }
        offsetsRef.current.set(id, { x: cur.x + e.dx / transform.k, y: cur.y + e.dy / transform.k })
        setDragTick((t) => t + 1)
      })
    sel.call(dragged)
    return () => {
      sel.on(".drag", null)
    }
  }, [layout.nodes, transform.k])

  // ── Selection / expand / navigation ─────────────────────────────────────────
  const toggleExpand = useCallback((id: string) => {
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }, [])

  const onNodeClick = useCallback(
    (id: string) => {
      setSelectedId(id)
      setRelVerbFocus(null) // a node selection is the more specific navigation (R25 precedence)
      const n = nodeById.get(id)
      if (n && (n.collapsed || (vizModel.childrenByParent.get(id)?.length ?? 0) > 0)) toggleExpand(id)
    },
    [nodeById, vizModel, toggleExpand],
  )

  // A reveal (search hit, relationship nav, harness `select`) auto-expands paths and can
  // grow the tree past the last Fit — request one reframe so the revealed node and its
  // now-visible verb arcs stay on-canvas (R12b: no arcs spilling off the edge).
  const pendingFit = useRef(false)

  // Reveal a node: open the whole ancestor path, then select it.
  const reveal = useCallback(
    (id: string) => {
      const path = ancestorPath(vizModel, id)
      setExpanded((prev) => {
        const next = new Set(prev)
        for (const a of path) next.add(a.id)
        return next
      })
      setSelectedId(id)
      setRelVerbFocus(null)
      pendingFit.current = true
    },
    [vizModel],
  )

  // ── Search-to-reveal ─────────────────────────────────────────────────────────
  const runSearch = useCallback(() => {
    const q = query.trim().toLowerCase()
    if (!q) {
      setSearchHits(new Set())
      setHint(null)
      return
    }
    const hits = vizModel.nodes.filter((n) => n.label.toLowerCase().includes(q))
    if (!hits.length) {
      setSearchHits(new Set())
      setHint("No match in the estate.")
      return
    }
    setExpanded((prev) => {
      const next = new Set(prev)
      for (const h of hits) for (const a of ancestorPath(vizModel, h.id)) next.add(a.id)
      return next
    })
    setSearchHits(new Set(hits.map((h) => h.id)))
    setSelectedId(hits[0].id)
    setHint(hits.length === 1 ? null : `${hits.length} matches`)
  }, [query, vizModel])

  const clearSearch = useCallback(() => {
    setQuery("")
    setSearchHits(new Set())
    setHint(null)
  }, [])

  const expandAll = useCallback(() => {
    setExpanded(new Set(vizModel.nodes.map((n) => n.id)))
    pendingFit.current = true
  }, [vizModel])

  // Collapse-all (P0-a): fold every container back to the domain tier, then re-fit (R24).
  const collapseAll = useCallback(() => {
    setExpanded(collapsedToDomainTier(vizModel))
    setUncapped(new Set())
    pendingFit.current = true
  }, [vizModel])

  const resetView = useCallback(() => {
    offsetsRef.current = new Map()
    setExpanded(initialExpanded(model))
    setSelectedId(null)
    setTypeFocus(null)
    setRelVerbFocus(null)
    setUncapped(new Set())
    didFit.current = false
    setDragTick((t) => t + 1)
  }, [model])

  // A `+N more` chip lifts its parent's child cap (§6/R3) — reveal the rest in place.
  const expandMore = useCallback((parentId: string) => {
    setUncapped((prev) => {
      const next = new Set(prev)
      next.add(parentId)
      return next
    })
  }, [])

  // ── Harness handle ───────────────────────────────────────────────────────────
  const hash = useMemo(() => layoutHash(layout), [layout])
  useEffect(() => {
    if (!onReady) return
    onReady({
      ready: true,
      nodeCount: layout.nodes.length,
      layoutHash: hash,
      tapByLabel: (q: string) => {
        const needle = q.toLowerCase()
        const hit = vizModel.nodes.find((n) => n.label.toLowerCase().includes(needle) || n.id === q)
        if (hit) {
          reveal(hit.id)
          return true
        }
        return false
      },
      positions: () => layout.nodes.map((n) => ({ id: n.id, x: n.x, y: n.y })),
    })
  }, [onReady, layout, hash, vizModel, reveal])

  // ── Inspector data ─────────────────────────────────────────────────────────
  const inspector: InspectorData | null = useMemo(() => {
    if (!selectedId) return null
    const proposal = vizModel.proposals.find((p) => p.id === selectedId)
    if (proposal) {
      return {
        title: proposal.name,
        typeLabel: proposal.kind === "domain" ? "Suggested area" : "Suggested sub-area",
        description: "A grouping the engine suggests for ungrouped assets. Nothing is applied yet.",
        facts: [`${proposal.memberIds.length} ${proposal.memberIds.length === 1 ? "asset" : "assets"} would move in`],
        technical: [],
        relationships: [],
        isProposal: true,
        band: proposal.band,
      }
    }
    const n = nodeById.get(selectedId)
    const mn = vizModel.nodes.find((x) => x.id === selectedId)
    if (!n || !mn) return null
    const facts: string[] = []
    if (mn.memberCount != null && mn.memberCount > 0)
      facts.push(`${fmtCount(mn.memberCount)} ${mn.memberCount === 1 ? "asset" : "assets"} in this area`)
    if (mn.cost != null && mn.cost > 0) facts.push(`About ${fmtMoney(mn.cost)} / month`)
    const rels: InspectorData["relationships"] = []
    // Hierarchy relationships (part of / contains).
    if (mn.parentId) {
      const p = vizModel.nodes.find((x) => x.id === mn.parentId)
      if (p) rels.push({ targetId: p.id, label: p.label, verb: "part of", xdom: false })
    }
    for (const c of vizModel.childrenByParent.get(mn.id) ?? []) {
      rels.push({ targetId: c.id, label: c.label, verb: "contains", xdom: false })
    }
    // Typed cross-links.
    for (const e of vizModel.crossEdges) {
      if (e.src === mn.id || e.dst === mn.id) {
        const otherId = e.src === mn.id ? e.dst : e.src
        const other = vizModel.nodes.find((x) => x.id === otherId)
        if (other) rels.push({ targetId: otherId, label: other.label, verb: e.verb, xdom: e.relClass === "xdom" })
      }
    }
    const technical: string[] = []
    if (mn.type === "table" || mn.type === "metric_view") technical.push(`Path: ${mn.id.replace(/^asset:|^mv:|^table:/, "")}`)
    // Prefer the snapshot's real description/meta (MV-D86); fall back to generic copy (R13).
    const meta = mn.meta
      ? Object.entries(mn.meta).map(([k, v]) => [k, `${v}`] as [string, string])
      : undefined
    const pages = mn.type === "subdomain" ? pagesFromExpansions(hydrationRef.current.get(mn.id)) : []
    return {
      title: mn.label,
      typeLabel: TYPE_LABEL[mn.type],
      description: mn.description && mn.description.trim() ? mn.description : describe(n),
      facts,
      meta,
      pages: pages.length ? pages : undefined,
      technical,
      relationships: rels.slice(0, 24),
    }
    // hydrationRef is read for attached Pages; hydrationVersion gates recompute.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedId, vizModel, nodeById, hydrationVersion])

  // Hover-snippet content (R6/R21) — prefers real description/meta, degrades to generic copy
  // (R13). Skips the synthetic "+N more" chip (not a model node → no tooltip).
  const tooltip: TooltipData | null = useMemo(() => {
    if (!hoveredId) return null
    const mn = vizModel.nodes.find((n) => n.id === hoveredId)
    if (!mn) return null
    return assembleTooltip({
      typeLabel: TYPE_LABEL[mn.type],
      name: mn.label,
      description: mn.description,
      meta: mn.meta,
      fallbackDescription: describeType(mn.type),
    })
  }, [hoveredId, vizModel])

  // Edge hover content (R25) — From → To, verb, class, and the pre-seed evidence bag; degrades
  // to verb + endpoints + class when the edge carries no `detail` (never requires Lane E).
  const edgeTooltip: EdgeTooltipData | null = useMemo(() => {
    if (!hoveredEdgeId) return null
    const c = layout.crossLinks.find((x) => x.id === hoveredEdgeId)
    if (!c) return null
    const from = vizModel.nodes.find((x) => x.id === c.sourceId)
    const to = vizModel.nodes.find((x) => x.id === c.targetId)
    if (!from || !to) return null
    return assembleEdgeTooltip({
      fromName: from.label,
      toName: to.label,
      verb: c.verb,
      xdom: c.relClass === "xdom",
      detail: c.detail,
    })
  }, [hoveredEdgeId, layout, vizModel])

  const breadcrumb = useMemo(
    () => (selectedId ? ancestorPath(vizModel, selectedId) : vizModel.root ? [vizModel.root] : []),
    [selectedId, vizModel],
  )

  // Relationship-type legend rows (R25) — one per verb present, with a live count (drops as
  // domains are hidden). Computed from the visible cross-edges.
  const relLegend = useMemo(() => relTypeLegend(vizModel.crossEdges), [vizModel])

  // Domain show/hide panel groups (R27) — listed from the FULL model so a hidden area can be
  // re-shown; each top domain expandable to its sub-areas.
  const visGroups: VisibilityGroup[] = useMemo(
    () =>
      model.nodes
        .filter((n) => n.type === "domain")
        .map((d) => ({
          id: d.id,
          label: d.label,
          subdomains: (model.childrenByParent.get(d.id) ?? [])
            .filter((c) => c.type === "subdomain")
            .map((c) => ({ id: c.id, label: c.label })),
        })),
    [model],
  )

  const toggleDomainVisibility = useCallback((id: string) => {
    setHiddenDomains((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }, [])
  const showAllDomains = useCallback(() => setHiddenDomains(new Set()), [])
  const hideAllDomains = useCallback(
    () => setHiddenDomains(new Set(model.nodes.filter((n) => n.type === "domain").map((n) => n.id))),
    [model],
  )

  // Minimap points + rects.
  const miniPoints: MiniPoint[] = useMemo(
    () => layout.nodes.map((n) => ({ x: n.x, y: n.y, color: tokens.typeFill[n.type] })),
    [layout, tokens],
  )
  const miniRects: MiniRect[] = useMemo(() => {
    if (!layout.trayBounds) return []
    const b = layout.trayBounds
    return [{ x1: b.minX, y1: b.minY, x2: b.maxX, y2: b.maxY, color: tokens.trayNodeStroke }]
  }, [layout, tokens])

  // Keep the minimap "you-are-here" box in sync with the live camera (R26). Reads the on-screen
  // canvas rect + current transform, inverts to content coords. Recomputes on pan/zoom, on a
  // fit, and when the canvas resizes (fullscreen enter/exit / new layout).
  useEffect(() => {
    const svg = svgRef.current
    if (!svg) {
      setViewportRect(null)
      return
    }
    const rect = svg.getBoundingClientRect()
    if (rect.width === 0 || rect.height === 0) return
    setViewportRect(viewportContentRect(transform, { width: rect.width, height: rect.height }))
  }, [transform, fullscreen, fetchState, layout.nodes.length])

  // Minimap click/drag → recenter the main camera on that content point at the current zoom (R26).
  const onMinimapNavigate = useCallback(
    (cx: number, cy: number) => {
      const svg = svgRef.current
      if (!svg) return
      const rect = svg.getBoundingClientRect()
      if (rect.width === 0) return
      applyTransform(centerOnTransform({ x: cx, y: cy }, { width: rect.width, height: rect.height }, transform.k))
    },
    [applyTransform, transform.k],
  )

  // Fullscreen (P0-a): Esc restores; re-fit on enter AND exit so the tree reframes to the new
  // canvas size (R24 — never stranded in a thin band).
  useEffect(() => {
    if (!fullscreen) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setFullscreen(false)
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [fullscreen])
  useEffect(() => {
    if (!didFit.current) return
    const id = requestAnimationFrame(() => {
      if (svgRef.current) fit()
    })
    return () => cancelAnimationFrame(id)
  }, [fullscreen, fit])

  const dimmed = useCallback((t: NodeType) => typeFocus != null && typeFocus !== t, [typeFocus])
  // Dim an arc when a rel-type is highlighted and this arc isn't that verb (Bloom idiom, R25).
  const edgeDimmed = useCallback(
    (verb: string) => relVerbFocus != null && relVerbFocus !== verb,
    [relVerbFocus],
  )

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div
      className={
        fullscreen
          ? "fixed inset-0 z-50 flex flex-col gap-2 overflow-hidden bg-sunken p-4"
          : "flex flex-col gap-2"
      }
    >
      {/* Toolbar */}
      <div className="flex flex-wrap items-center gap-2">
        <div className="inline-flex rounded-lg border border-default bg-elevated p-0.5" role="tablist" aria-label="Provenance">
          {PROVENANCE.map((p) => (
            <button
              key={p.id}
              role="tab"
              aria-selected={provenance === p.id}
              onClick={() => setProvenance(p.id)}
              className={`rounded-md px-2.5 py-1 text-xs font-semibold ${
                provenance === p.id ? "bg-accent text-white" : "text-secondary hover:text-primary"
              }`}
            >
              {p.label}
            </button>
          ))}
        </div>
        <GraphSearch value={query} onChange={setQuery} onSubmit={runSearch} onClear={clearSearch} hint={hint} />
        <div className="ml-auto flex flex-wrap items-center gap-1">
          <button onClick={fit} className="flex items-center gap-1 rounded-md border border-default px-2 py-1 text-xs text-secondary hover:text-primary" aria-label="Fit">
            <Maximize2 className="h-3.5 w-3.5" /> Fit
          </button>
          <button onClick={expandAll} className="flex items-center gap-1 rounded-md border border-default px-2 py-1 text-xs text-secondary hover:text-primary">
            <Sparkles className="h-3.5 w-3.5" /> Expand all
          </button>
          <button onClick={collapseAll} className="flex items-center gap-1 rounded-md border border-default px-2 py-1 text-xs text-secondary hover:text-primary" aria-label="Collapse all">
            <ChevronsDownUp className="h-3.5 w-3.5" /> Collapse all
          </button>
          <button
            onClick={() => setShowDomainPanel((v) => !v)}
            aria-pressed={showDomainPanel}
            className={`flex items-center gap-1 rounded-md border border-default px-2 py-1 text-xs hover:text-primary ${
              showDomainPanel ? "bg-accent/15 text-primary" : "text-secondary"
            }`}
          >
            <Eye className="h-3.5 w-3.5" /> Domains
          </button>
          <button onClick={resetView} className="flex items-center gap-1 rounded-md border border-default px-2 py-1 text-xs text-secondary hover:text-primary" aria-label="Reset view">
            <RotateCcw className="h-3.5 w-3.5" /> Reset
          </button>
          <button
            onClick={() => setFullscreen((v) => !v)}
            aria-pressed={fullscreen}
            className="flex items-center gap-1 rounded-md border border-default px-2 py-1 text-xs text-secondary hover:text-primary"
            aria-label={fullscreen ? "Exit fullscreen" : "Fullscreen"}
          >
            {fullscreen ? <Minimize className="h-3.5 w-3.5" /> : <Maximize className="h-3.5 w-3.5" />}
            {fullscreen ? "Exit" : "Fullscreen"}
          </button>
        </div>
      </div>

      {/* Breadcrumb */}
      <nav aria-label="Breadcrumb" className="flex flex-wrap items-center gap-1 text-xs text-muted">
        {breadcrumb.map((n, i) => (
          <span key={n.id} className="flex items-center gap-1">
            {i > 0 && <span aria-hidden>▸</span>}
            <button onClick={() => reveal(n.id)} className={i === breadcrumb.length - 1 ? "text-primary" : "hover:text-secondary"}>
              {n.displayName}
            </button>
          </span>
        ))}
      </nav>

      <div className={`relative flex gap-2${fullscreen ? " min-h-0 flex-1" : ""}`}>
        {/* Canvas — a bigger default so the tree is framed, not stranded in a thin band (R24). */}
        <div
          className="relative flex-1 overflow-hidden rounded-xl border border-default bg-sunken"
          style={fullscreen ? { height: "100%" } : { height: "max(70vh, 520px)" }}
        >
          {isStale && (
            <div className="absolute left-3 top-3 z-10 rounded-md bg-warning/15 px-2 py-1 text-[11px] text-warning-foreground">
              Snapshot may be out of date
            </div>
          )}
          {/* Cross-link overlay is gated at realistic scale (R4): show the count and point
              the curator at selection instead of painting a full-width arc hairball. */}
          {showTree && !selectedId && layout.crossLinkOverflow > 0 && (
            <div className="absolute right-3 top-3 z-10 rounded-md border border-default bg-elevated/95 px-2 py-1 text-[11px] text-secondary shadow-sm">
              +{fmtCount(layout.crossLinkOverflow)} links — select a node to trace its relationships
            </div>
          )}
          {showNudge && (
            <div className="absolute left-1/2 top-3 z-10 flex -translate-x-1/2 items-center gap-2 rounded-lg border border-default bg-elevated/95 px-3 py-1.5 text-xs shadow-sm">
              <span className="text-secondary">No governed groups yet — the engine has suggestions.</span>
              <button onClick={() => setProvenance("proposed")} className="rounded-md bg-accent px-2 py-0.5 font-semibold text-white">
                View suggested
              </button>
            </div>
          )}
          {fetchState === "loading" ? (
            <div className="flex h-full flex-col items-center justify-center gap-2 text-muted">
              <Loader2 className="h-6 w-6 animate-spin" />
              <p className="text-xs">Building the estate graph…</p>
            </div>
          ) : fetchState === "error" ? (
            <div className="flex h-full flex-col items-center justify-center gap-2 text-center text-muted">
              <AlertTriangle className="h-6 w-6 text-warning-foreground" />
              <p className="text-sm font-medium text-secondary">The estate snapshot could not be read</p>
              <p className="max-w-xs text-xs">Try refreshing in a moment.</p>
            </div>
          ) : isEmpty ? (
            <div className="flex h-full flex-col items-center justify-center gap-2 text-center text-muted">
              <p className="text-sm font-medium text-secondary">No data in the estate graph yet</p>
              <p className="max-w-xs text-xs">
                {provenance === "proposed"
                  ? "Nothing to suggest — every asset in scope is already organized."
                  : "Once the estate is scanned, its business areas appear here."}
              </p>
              {provenance === "applied" && model.proposals.length > 0 && (
                <button onClick={() => setProvenance("proposed")} className="rounded-lg bg-accent px-3 py-1.5 text-xs font-semibold text-white">
                  View suggested groupings
                </button>
              )}
            </div>
          ) : (
            <svg
              ref={svgRef}
              role="img"
              aria-label="Estate ontology map"
              className="h-full w-full cursor-grab active:cursor-grabbing"
              style={{ background: tokens.ground }}
            >
              <defs>
                <pattern id="ontgrid" width="24" height="24" patternUnits="userSpaceOnUse">
                  <circle cx="1" cy="1" r="1" fill={tokens.dotGrid} />
                </pattern>
                {/* Direction arrowheads (R25) — one per edge class, themed to the edge hue. */}
                {[
                  { id: "arrow-shared", color: tokens.sharedEdge },
                  { id: "arrow-xdom", color: tokens.xdomEdge },
                ].map((m) => (
                  <marker
                    key={m.id}
                    id={m.id}
                    viewBox="0 0 10 10"
                    refX="9"
                    refY="5"
                    markerWidth="6"
                    markerHeight="6"
                    orient="auto-start-reverse"
                  >
                    <path d="M0,0 L10,5 L0,10 z" fill={m.color} />
                  </marker>
                ))}
              </defs>
              <rect x="0" y="0" width="100%" height="100%" fill="url(#ontgrid)" />
              <g ref={gRef} transform={`translate(${transform.x},${transform.y}) scale(${transform.k})`}>
                {/* Spine links — a NEUTRAL hierarchy stroke (§5/R21e), not the domain tint, so
                    containment reads as structure and never sweeps a coloured arc off-canvas.
                    Drawn only when BOTH endpoints are laid out (clip-to-visible). */}
                {showTree &&
                  layout.spineLinks.map((l) => {
                    if (!nodeById.has(l.sourceId) || !nodeById.has(l.targetId)) return null
                    const child = nodeById.get(l.targetId)
                    const fade = child ? dimmed(child.type) : false
                    return (
                      <path
                        key={l.id}
                        d={l.path}
                        fill="none"
                        stroke={tokens.spine}
                        strokeWidth={1.2}
                        strokeOpacity={fade ? 0.08 : tokens.spineOpacity}
                      />
                    )
                  })}

                {/* Typed cross-links + verb plates. KG idiom (R25): every VISIBLE arc is
                    hoverable (a fat transparent hit path), carries a direction arrowhead, and
                    dims when a different rel-type is highlighted from the legend. */}
                {showTree &&
                  layout.crossLinks.map((c) => {
                    const s = nodeById.get(c.sourceId)
                    const d = nodeById.get(c.targetId)
                    // Clip-to-visible (§5/R4): never draw an arc to an off-tree endpoint.
                    if (!s || !d) return null
                    const fade = dimmed(s.type) || dimmed(d.type) || edgeDimmed(c.verb)
                    const stroke = c.relClass === "xdom" ? tokens.xdomEdge : tokens.sharedEdge
                    const marker = c.relClass === "xdom" ? "url(#arrow-xdom)" : "url(#arrow-shared)"
                    return (
                      <g
                        key={c.id}
                        opacity={fade ? 0.08 : 1}
                        style={{ cursor: "help" }}
                        onMouseEnter={(e) => {
                          setHoveredId(null)
                          setHoveredEdgeId(c.id)
                          setHoverPos({ x: e.clientX, y: e.clientY })
                        }}
                        onMouseMove={(e) => setHoverPos({ x: e.clientX, y: e.clientY })}
                        onMouseLeave={() => setHoveredEdgeId((cur) => (cur === c.id ? null : cur))}
                      >
                        {/* Invisible fat hit path so a thin dashed arc is easy to hover (R25). */}
                        <path d={c.path} fill="none" stroke="transparent" strokeWidth={12} />
                        <path
                          d={c.path}
                          fill="none"
                          stroke={stroke}
                          strokeWidth={c.relClass === "xdom" ? 1.4 : 1}
                          strokeDasharray="4 3"
                          strokeOpacity={c.showLabel ? 0.9 : 0.6}
                          markerEnd={marker}
                          pointerEvents="none"
                        />
                        {/* Verb plate — only on the focused node's arcs (R4): no label cloud. */}
                        {c.showLabel && (
                          <g transform={`translate(${c.labelAt.x},${c.labelAt.y})`}>
                            <rect
                              x={-c.verb.length * 3.1 - 4}
                              y={-7}
                              width={c.verb.length * 6.2 + 8}
                              height={14}
                              rx={3}
                              fill={tokens.plateBg}
                              fillOpacity={tokens.plateOpacity}
                            />
                            <text
                              textAnchor="middle"
                              dy="3.5"
                              fontSize={9}
                              fontWeight={c.relClass === "xdom" ? 700 : 500}
                              fill={c.relClass === "xdom" ? tokens.verbXdomText : tokens.verbText}
                            >
                              {c.verb}
                            </text>
                          </g>
                        )}
                      </g>
                    )
                  })}

                {/* Ungrouped tray + proposal hulls (§3.5) — the "mess" surfaces only under
                    Proposed/Both; Applied is the solid tree only (§5), so no nameless
                    ghost discs float beside the northstar tree (R3/R12c). */}
                {showProposals && layout.trayItems.length > 0 && layout.trayBounds && (
                  <g>
                    <line
                      x1={layout.trayBounds.minX - 20}
                      y1={layout.trayBounds.minY - 20}
                      x2={layout.trayBounds.minX - 20}
                      y2={layout.trayBounds.maxY + 20}
                      stroke={tokens.trayDivider}
                      strokeWidth={1}
                      strokeDasharray="2 4"
                    />
                    <text x={layout.trayBounds.minX} y={layout.trayBounds.minY - 26} fontSize={11} fontWeight={600} fill={tokens.trayText}>
                      Ungrouped · {fmtCount(model.trayItems.length)}
                    </text>
                    {showProposals &&
                      layout.proposalHulls.map((h) => (
                        <g key={h.id} onClick={() => setSelectedId(h.id)} style={{ cursor: "pointer" }}>
                          <rect
                            x={h.x}
                            y={h.y}
                            width={h.width}
                            height={h.height}
                            rx={12}
                            fill="none"
                            stroke={selectedId === h.id ? tokens.selectedRing : tokens.proposalStroke}
                            strokeWidth={1.4}
                            strokeDasharray="6 4"
                          />
                          <g transform={`translate(${h.x + 6},${h.y - 8})`}>
                            <rect x={-4} y={-11} width={h.name.length * 6.4 + 60} height={16} rx={3} fill={tokens.plateBg} fillOpacity={tokens.plateOpacity} />
                            <text fontSize={10} fontWeight={600} fill={tokens.proposalText}>
                              Suggested: {h.name}
                            </text>
                            {h.band && (
                              <text x={h.name.length * 6.4 + 12} fontSize={9} fill={bandColor(tokens, h.band)}>
                                {h.band}
                              </text>
                            )}
                          </g>
                        </g>
                      ))}
                    {layout.trayItems.map((t) => (
                      <g key={t.id} onClick={() => setSelectedId(t.id)} style={{ cursor: "pointer" }}>
                        <circle cx={t.x} cy={t.y} r={t.radius} fill={tokens.trayNodeFill} stroke={tokens.trayNodeStroke} strokeWidth={1} strokeDasharray="2 2" />
                      </g>
                    ))}
                    {layout.trayOverflow > 0 && (
                      <text x={layout.trayBounds.minX} y={layout.trayBounds.maxY + 16} fontSize={10} fill={tokens.trayText}>
                        +{fmtCount(layout.trayOverflow)} more
                      </text>
                    )}
                  </g>
                )}

                {/* Nodes */}
                {showTree &&
                  layout.nodes.map((n) => {
                    // Synthetic "+N more" truncation chip (§6/R3): a labelled pill, not a
                    // nameless disc; clicking it lifts its parent's child cap in place.
                    if (n.isMore) {
                      const label = `+${fmtCount(n.moreCount)} more`
                      const w = label.length * 6.4 + 16
                      return (
                        <g
                          key={n.id}
                          data-node-id={n.id}
                          transform={`translate(${n.x},${n.y})`}
                          style={{ cursor: "pointer" }}
                          tabIndex={0}
                          role="button"
                          aria-label={`Show ${n.moreCount} more`}
                          onClick={(e) => {
                            if ((e.nativeEvent as { defaultPrevented?: boolean }).defaultPrevented) return
                            if (n.parentId) expandMore(n.parentId)
                          }}
                          onKeyDown={(e) => {
                            if ((e.key === "Enter" || e.key === " ") && n.parentId) {
                              e.preventDefault()
                              expandMore(n.parentId)
                            }
                          }}
                        >
                          <rect
                            x={-w / 2}
                            y={-11}
                            width={w}
                            height={22}
                            rx={11}
                            fill={tokens.plateBg}
                            fillOpacity={tokens.plateOpacity}
                            stroke={tokens.trayNodeStroke}
                            strokeWidth={1}
                            strokeDasharray="3 2"
                          />
                          <text textAnchor="middle" dy="3.5" fontSize={10.5} fontWeight={600} fill={tokens.trayText}>
                            {label}
                          </text>
                        </g>
                      )
                    }
                    const caption = clampLabel(n.displayName)
                    const fill = tokens.typeFill[n.type]
                    const tint = domainTintFor(tokens, n.domainId)
                    const isContainer = n.type === "org" || n.type === "domain" || n.type === "subdomain"
                    const ring =
                      selectedId === n.id
                        ? tokens.selectedRing
                        : searchHits.has(n.id)
                          ? tokens.searchRing
                          : isContainer && tint
                            ? tint
                            : tokens.nodeStroke
                    const fade = dimmed(n.type)
                    return (
                      <g
                        key={n.id}
                        data-node-id={n.id}
                        transform={`translate(${n.x},${n.y})`}
                        opacity={fade ? 0.2 : 1}
                        style={{ cursor: "pointer" }}
                        tabIndex={0}
                        role="treeitem"
                        aria-expanded={vizModel.childrenByParent.get(n.id)?.length ? !n.collapsed : undefined}
                        aria-label={`${TYPE_LABEL[n.type]}: ${n.label}`}
                        onClick={(e) => {
                          if ((e.nativeEvent as { defaultPrevented?: boolean }).defaultPrevented) return
                          onNodeClick(n.id)
                        }}
                        onKeyDown={(e) => {
                          if (e.key === "Enter" || e.key === " ") {
                            e.preventDefault()
                            onNodeClick(n.id)
                          }
                        }}
                        onMouseEnter={(e) => {
                          setHoveredId(n.id)
                          setHoverPos({ x: e.clientX, y: e.clientY })
                        }}
                        onMouseMove={(e) => setHoverPos({ x: e.clientX, y: e.clientY })}
                        onMouseLeave={() => setHoveredId((cur) => (cur === n.id ? null : cur))}
                        onBlur={() => setHoveredId((cur) => (cur === n.id ? null : cur))}
                      >
                        <circle
                          r={n.radius}
                          fill={fill}
                          stroke={ring}
                          strokeWidth={selectedId === n.id || searchHits.has(n.id) ? 2.5 : isContainer ? 2 : 1.25}
                          strokeOpacity={selectedId === n.id || searchHits.has(n.id) ? 1 : isContainer ? tokens.ringOpacity : 1}
                        />
                        {/* De-chrome containers (§5/R23): org/domain/subdomain carry NO glyph —
                            their identity is fill + ring + size — so sub-areas never read as a
                            "hamburger menu". Leaf assets keep their type glyph (R11). */}
                        {!isContainer && (
                          <path
                            d={GLYPHS[n.type]}
                            fill="none"
                            stroke={tokens.glyphStroke}
                            strokeWidth={1.6}
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            transform={`translate(-12,-12) scale(${(n.radius * 1.1) / 24})`}
                            opacity={0.9}
                          />
                        )}
                        {/* Label plate (ellipsized caption; full name in the hover title) */}
                        <g transform={`translate(0,${n.radius + 12})`}>
                          <rect
                            x={-caption.length * 3.2 - 4}
                            y={-9}
                            width={caption.length * 6.4 + 8}
                            height={16}
                            rx={3}
                            fill={tokens.plateBg}
                            fillOpacity={tokens.plateOpacity}
                          />
                          <text textAnchor="middle" dy="3" fontSize={n.type === "org" ? 13 : n.type === "domain" ? 12 : 10.5} fontWeight={isContainer ? 600 : 500} fill={tokens.plateText}>
                            {caption}
                          </text>
                        </g>
                        <title>{n.label}</title>
                        {/* Collapse +N badge */}
                        {n.collapsed && n.badge > 0 && (
                          <g transform={`translate(${n.radius * 0.7},${-n.radius * 0.7})`}>
                            <circle r={8} fill={tokens.badgeFill} />
                            <text textAnchor="middle" dy="3" fontSize={8} fontWeight={700} fill={tokens.badgeText}>
                              +{n.badge > 99 ? "99" : n.badge}
                            </text>
                          </g>
                        )}
                      </g>
                    )
                  })}
              </g>
            </svg>
          )}

          {/* Minimap — navigable: you-are-here box + click/drag to pan (R26) */}
          <div className="absolute bottom-3 right-3">
            <GraphMinimap points={miniPoints} rects={miniRects} viewport={viewportRect} onNavigate={onMinimapNavigate} />
          </div>

          {/* Legend — node types (click to focus) + rel-types (verb + count, click to highlight)
              + edge-class key + a one-line interaction hint */}
          <div className="absolute left-3 bottom-3 flex max-w-[calc(100%-1.5rem)] flex-col gap-1.5 rounded-lg border border-default bg-elevated/90 p-2">
            <div className="flex flex-wrap gap-1.5">
              {LEGEND.map((l) => (
                <button
                  key={l.type}
                  onClick={() => setTypeFocus((cur) => (cur === l.type ? null : l.type))}
                  className={`flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] ${
                    typeFocus === l.type ? "bg-accent/20 text-primary" : "text-secondary hover:text-primary"
                  }`}
                  aria-pressed={typeFocus === l.type}
                >
                  <span className="h-2.5 w-2.5 rounded-full" style={{ background: tokens.typeFill[l.type] }} />
                  {l.label}
                </button>
              ))}
            </div>
            {/* Relationship types present (R25, Bloom idiom): verb + live count, click to
                highlight that type across the map; swatch coloured by its within/cross class. */}
            {relLegend.length > 0 && (
              <div className="flex flex-wrap gap-1.5">
                {relLegend.map((r) => {
                  const active = relVerbFocus === r.verb
                  const swatch = r.xdom > 0 ? tokens.xdomEdge : tokens.sharedEdge
                  return (
                    <button
                      key={r.verb}
                      onClick={() =>
                        setRelVerbFocus((cur) => {
                          const next = cur === r.verb ? null : r.verb
                          if (next) setSelectedId(null) // rel-type highlight and node focus are exclusive (R25)
                          return next
                        })
                      }
                      className={`flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] ${
                        active ? "bg-accent/20 text-primary" : "text-secondary hover:text-primary"
                      }`}
                      aria-pressed={active}
                    >
                      <svg width="16" height="6" aria-hidden className="shrink-0">
                        <line x1="1" y1="3" x2="15" y2="3" stroke={swatch} strokeWidth="1.4" strokeDasharray="3 2" />
                      </svg>
                      {r.verb}
                      <span className="text-muted">· {fmtCount(r.count)}</span>
                    </button>
                  )
                })}
              </div>
            )}
            {/* Edge-class key (§4.5/R14): hierarchy solid · shared dashed-slate · cross-domain dashed-maroon */}
            <div className="flex flex-wrap items-center gap-2.5 text-[10px] text-secondary">
              {[
                { label: "Hierarchy", stroke: tokens.spine, dash: undefined },
                { label: "Shared key", stroke: tokens.sharedEdge, dash: "3 2" },
                { label: "Cross-domain", stroke: tokens.xdomEdge, dash: "3 2" },
              ].map((e) => (
                <span key={e.label} className="flex items-center gap-1">
                  <svg width="18" height="6" aria-hidden className="shrink-0">
                    <line x1="1" y1="3" x2="17" y2="3" stroke={e.stroke} strokeWidth="1.4" strokeDasharray={e.dash} />
                  </svg>
                  {e.label}
                </span>
              ))}
            </div>
            <p className="text-[10px] text-muted">click to drill · drag to move · hover for details · → shows direction</p>
          </div>
        </div>

        {/* Right rail — optional domain show/hide panel above the docked inspector */}
        <div className="flex w-64 shrink-0 flex-col gap-2">
          {showDomainPanel && (
            <DomainVisibilityPanel
              groups={visGroups}
              hidden={hiddenDomains}
              onToggle={toggleDomainVisibility}
              onShowAll={showAllDomains}
              onHideAll={hideAllDomains}
              onClose={() => setShowDomainPanel(false)}
            />
          )}
          <div className="min-h-0 flex-1 rounded-xl border border-default bg-elevated">
            <GraphInspector
              data={inspector}
              onSelectRelationship={reveal}
              onClose={() => setSelectedId(null)}
              onApprove={inspector?.isProposal ? () => setHint("Approve is wired to the Phase-5 apply gate.") : undefined}
              onDismiss={inspector?.isProposal ? () => setSelectedId(null) : undefined}
            />
          </div>
        </div>
      </div>

      {/* Honest source line */}
      <p className="text-[11px] text-muted">
        {provenance === "applied"
          ? "Applied (current governed tags)"
          : provenance === "proposed"
            ? "Proposed (engine suggestions — nothing applied yet)"
            : "Applied tree + proposed groupings"}
      </p>

      {/* Hover snippets — node (R6/R21) + edge (R25); cursor-following, clamped, pointer-events:none */}
      <GraphTooltip data={tooltip} x={hoverPos.x} y={hoverPos.y} tokens={tokens} />
      <GraphEdgeTooltip data={edgeTooltip} x={hoverPos.x} y={hoverPos.y} tokens={tokens} />
    </div>
  )
}
