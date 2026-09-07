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
import { AlertTriangle, Loader2, Maximize2, RotateCcw, Sparkles } from "lucide-react"
import { drag as d3drag } from "d3-drag"
import { select } from "d3-selection"
import { zoom as d3zoom, zoomIdentity, type ZoomBehavior } from "d3-zoom"
import type {
  GraphOrigin,
  OntologyDrafts,
  OntologyGraph,
  OntologyTaxonomy,
} from "@/ontology/types"
import {
  buildEstateModel,
  fmtCount,
  fmtMoney,
  type EstateModel,
  type NodeType,
} from "@/ontology/estateGraphModel"
import {
  DEFAULT_LAYOUT,
  ancestorPath,
  initialExpanded,
  layoutHash,
  layoutTree,
  type LaidNode,
  type Layout,
  type Point,
} from "@/ontology/ontologyTreeLayout"
import { bandColor, domainTintFor, graphTokens } from "@/ontology/graphTokens"
import { useTheme } from "@/hooks/useTheme"
import { GraphInspector, type InspectorData } from "./GraphInspector"
import { GraphSearch } from "./GraphSearch"
import { GraphMinimap, type MiniPoint, type MiniRect } from "./GraphMinimap"

/** Injectable data seam (the harness supplies a fixture-backed mock). */
export interface EstateGraphApi {
  getGraph: (origin: GraphOrigin) => Promise<OntologyGraph>
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

function describe(node: LaidNode): string {
  switch (node.type) {
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

  const model: EstateModel = useMemo(
    () => buildEstateModel(effectiveGraph, { drafts, taxonomy, estateName }),
    [effectiveGraph, drafts, taxonomy, estateName],
  )

  const [expanded, setExpanded] = useState<Set<string>>(() => initialExpanded(model))
  // Reset the open set when the underlying estate changes.
  const modelKey = useMemo(() => `${model.nodes.length}:${model.root?.id ?? ""}`, [model])
  const lastKey = useRef(modelKey)
  if (lastKey.current !== modelKey) {
    lastKey.current = modelKey
    // Defer state set out of render via a microtask-free direct set is unsafe; use effect.
  }
  useEffect(() => {
    setExpanded(initialExpanded(model))
    setSelectedId(null)
  }, [model])

  // Manual drag offsets — persisted deltas applied post-layout (R17). A tick bumps relayout.
  const offsetsRef = useRef<Map<string, Point>>(new Map())
  const [dragTick, setDragTick] = useState(0)

  const layout: Layout = useMemo(
    () => layoutTree(model, expanded, offsetsRef.current, DEFAULT_LAYOUT),
    // dragTick is a deliberate relayout trigger; offsetsRef is mutated in place.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [model, expanded, dragTick],
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
    // Frame the CONTENT that's actually on screen: the tree when it's shown (never let the
    // off-tree tray column shrink the tree into a corner), the tray when the tree is hidden.
    const pad = 48
    let minX = Infinity
    let minY = Infinity
    let maxX = -Infinity
    let maxY = -Infinity
    const extend = (x: number, y: number, r = 0) => {
      minX = Math.min(minX, x - r)
      minY = Math.min(minY, y - r)
      maxX = Math.max(maxX, x + r)
      maxY = Math.max(maxY, y + r)
    }
    if (showTree) for (const n of layout.nodes) extend(n.x, n.y, n.radius + 20)
    if ((showProposals || !showTree) && layout.trayBounds) {
      extend(layout.trayBounds.minX, layout.trayBounds.minY)
      extend(layout.trayBounds.maxX, layout.trayBounds.maxY)
    }
    if (!isFinite(minX)) return
    const bw = maxX - minX + pad * 2
    const bh = maxY - minY + pad * 2
    if (bw <= 0 || bh <= 0) return
    const rect = svg.getBoundingClientRect()
    const k = Math.min(2.5, Math.max(0.3, Math.min(rect.width / bw, rect.height / bh) * 0.95))
    const x = rect.width / 2 - (minX - pad + bw / 2) * k
    const y = rect.height / 2 - (minY - pad + bh / 2) * k
    applyTransform({ x, y, k })
  }, [layout.nodes, layout.trayBounds, showTree, showProposals, applyTransform])

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

  // ── Drag (d3.drag on nodes) ──────────────────────────────────────────────────
  useEffect(() => {
    const g = gRef.current
    if (!g) return
    const sel = select(g).selectAll<SVGGElement, unknown>("[data-node-id]")
    const dragged = d3drag<SVGGElement, unknown>()
      .on("start", function (e) {
        e.sourceEvent?.stopPropagation?.()
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
      const n = nodeById.get(id)
      if (n && (n.collapsed || (model.childrenByParent.get(id)?.length ?? 0) > 0)) toggleExpand(id)
    },
    [nodeById, model, toggleExpand],
  )

  // Reveal a node: open the whole ancestor path, then select it.
  const reveal = useCallback(
    (id: string) => {
      const path = ancestorPath(model, id)
      setExpanded((prev) => {
        const next = new Set(prev)
        for (const a of path) next.add(a.id)
        return next
      })
      setSelectedId(id)
    },
    [model],
  )

  // ── Search-to-reveal ─────────────────────────────────────────────────────────
  const runSearch = useCallback(() => {
    const q = query.trim().toLowerCase()
    if (!q) {
      setSearchHits(new Set())
      setHint(null)
      return
    }
    const hits = model.nodes.filter((n) => n.label.toLowerCase().includes(q))
    if (!hits.length) {
      setSearchHits(new Set())
      setHint("No match in the estate.")
      return
    }
    setExpanded((prev) => {
      const next = new Set(prev)
      for (const h of hits) for (const a of ancestorPath(model, h.id)) next.add(a.id)
      return next
    })
    setSearchHits(new Set(hits.map((h) => h.id)))
    setSelectedId(hits[0].id)
    setHint(hits.length === 1 ? null : `${hits.length} matches`)
  }, [query, model])

  const clearSearch = useCallback(() => {
    setQuery("")
    setSearchHits(new Set())
    setHint(null)
  }, [])

  const expandAll = useCallback(() => {
    setExpanded(new Set(model.nodes.map((n) => n.id)))
  }, [model])

  const resetView = useCallback(() => {
    offsetsRef.current = new Map()
    setExpanded(initialExpanded(model))
    setSelectedId(null)
    setTypeFocus(null)
    didFit.current = false
    setDragTick((t) => t + 1)
  }, [model])

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
        const hit = model.nodes.find((n) => n.label.toLowerCase().includes(needle) || n.id === q)
        if (hit) {
          reveal(hit.id)
          return true
        }
        return false
      },
      positions: () => layout.nodes.map((n) => ({ id: n.id, x: n.x, y: n.y })),
    })
  }, [onReady, layout, hash, model, reveal])

  // ── Inspector data ─────────────────────────────────────────────────────────
  const inspector: InspectorData | null = useMemo(() => {
    if (!selectedId) return null
    const proposal = model.proposals.find((p) => p.id === selectedId)
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
    const mn = model.nodes.find((x) => x.id === selectedId)
    if (!n || !mn) return null
    const facts: string[] = []
    if (mn.memberCount != null && mn.memberCount > 0)
      facts.push(`${fmtCount(mn.memberCount)} ${mn.memberCount === 1 ? "asset" : "assets"} in this area`)
    if (mn.cost != null && mn.cost > 0) facts.push(`About ${fmtMoney(mn.cost)} / month`)
    const rels: InspectorData["relationships"] = []
    // Hierarchy relationships (part of / contains).
    if (mn.parentId) {
      const p = model.nodes.find((x) => x.id === mn.parentId)
      if (p) rels.push({ targetId: p.id, label: p.label, verb: "part of", xdom: false })
    }
    for (const c of model.childrenByParent.get(mn.id) ?? []) {
      rels.push({ targetId: c.id, label: c.label, verb: "contains", xdom: false })
    }
    // Typed cross-links.
    for (const e of model.crossEdges) {
      if (e.src === mn.id || e.dst === mn.id) {
        const otherId = e.src === mn.id ? e.dst : e.src
        const other = model.nodes.find((x) => x.id === otherId)
        if (other) rels.push({ targetId: otherId, label: other.label, verb: e.verb, xdom: e.relClass === "xdom" })
      }
    }
    const technical: string[] = []
    if (mn.type === "table" || mn.type === "metric_view") technical.push(`Path: ${mn.id.replace(/^asset:|^mv:|^table:/, "")}`)
    return {
      title: mn.label,
      typeLabel: TYPE_LABEL[mn.type],
      description: describe(n),
      facts,
      technical,
      relationships: rels.slice(0, 24),
    }
  }, [selectedId, model, nodeById])

  const breadcrumb = useMemo(
    () => (selectedId ? ancestorPath(model, selectedId) : model.root ? [model.root] : []),
    [selectedId, model],
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

  const dimmed = useCallback((t: NodeType) => typeFocus != null && typeFocus !== t, [typeFocus])

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col gap-2">
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
        <div className="ml-auto flex items-center gap-1">
          <button onClick={fit} className="flex items-center gap-1 rounded-md border border-default px-2 py-1 text-xs text-secondary hover:text-primary" aria-label="Fit">
            <Maximize2 className="h-3.5 w-3.5" /> Fit
          </button>
          <button onClick={expandAll} className="flex items-center gap-1 rounded-md border border-default px-2 py-1 text-xs text-secondary hover:text-primary">
            <Sparkles className="h-3.5 w-3.5" /> Expand all
          </button>
          <button onClick={resetView} className="flex items-center gap-1 rounded-md border border-default px-2 py-1 text-xs text-secondary hover:text-primary" aria-label="Reset view">
            <RotateCcw className="h-3.5 w-3.5" /> Reset
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

      <div className="relative flex gap-2">
        {/* Canvas */}
        <div className="relative flex-1 overflow-hidden rounded-xl border border-default bg-sunken" style={{ minHeight: 520 }}>
          {isStale && (
            <div className="absolute left-3 top-3 z-10 rounded-md bg-warning/15 px-2 py-1 text-[11px] text-warning-foreground">
              Snapshot may be out of date
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
            <div className="flex h-[520px] flex-col items-center justify-center gap-2 text-muted">
              <Loader2 className="h-6 w-6 animate-spin" />
              <p className="text-xs">Building the estate graph…</p>
            </div>
          ) : fetchState === "error" ? (
            <div className="flex h-[520px] flex-col items-center justify-center gap-2 text-center text-muted">
              <AlertTriangle className="h-6 w-6 text-warning-foreground" />
              <p className="text-sm font-medium text-secondary">The estate snapshot could not be read</p>
              <p className="max-w-xs text-xs">Try refreshing in a moment.</p>
            </div>
          ) : isEmpty ? (
            <div className="flex h-[520px] flex-col items-center justify-center gap-2 text-center text-muted">
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
              className="h-[520px] w-full cursor-grab active:cursor-grabbing"
              style={{ background: tokens.ground }}
            >
              <defs>
                <pattern id="ontgrid" width="24" height="24" patternUnits="userSpaceOnUse">
                  <circle cx="1" cy="1" r="1" fill={tokens.dotGrid} />
                </pattern>
              </defs>
              <rect x="0" y="0" width="100%" height="100%" fill="url(#ontgrid)" />
              <g ref={gRef} transform={`translate(${transform.x},${transform.y}) scale(${transform.k})`}>
                {/* Spine links */}
                {showTree &&
                  layout.spineLinks.map((l) => {
                    const tint = domainTintFor(tokens, l.domainId) ?? tokens.spine
                    const child = nodeById.get(l.targetId)
                    const fade = child ? dimmed(child.type) : false
                    return (
                      <path
                        key={l.id}
                        d={l.path}
                        fill="none"
                        stroke={tint}
                        strokeWidth={1.2}
                        strokeOpacity={fade ? 0.08 : tokens.spineOpacity}
                      />
                    )
                  })}

                {/* Typed cross-links + verb plates */}
                {showTree &&
                  layout.crossLinks.map((c) => {
                    const s = nodeById.get(c.sourceId)
                    const d = nodeById.get(c.targetId)
                    const fade = (s && dimmed(s.type)) || (d && dimmed(d.type))
                    const stroke = c.relClass === "xdom" ? tokens.xdomEdge : tokens.sharedEdge
                    return (
                      <g key={c.id} opacity={fade ? 0.08 : 1}>
                        <path
                          d={c.path}
                          fill="none"
                          stroke={stroke}
                          strokeWidth={c.relClass === "xdom" ? 1.4 : 1}
                          strokeDasharray="4 3"
                          strokeOpacity={0.75}
                        />
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
                      </g>
                    )
                  })}

                {/* Ungrouped tray + proposal hulls (§3.5) */}
                {(layout.trayItems.length > 0 || showProposals) && layout.trayBounds && (
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
                        aria-expanded={model.childrenByParent.get(n.id)?.length ? !n.collapsed : undefined}
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
                      >
                        <circle
                          r={n.radius}
                          fill={fill}
                          stroke={ring}
                          strokeWidth={selectedId === n.id || searchHits.has(n.id) ? 2.5 : isContainer ? 2 : 1.25}
                          strokeOpacity={selectedId === n.id || searchHits.has(n.id) ? 1 : isContainer ? tokens.ringOpacity : 1}
                        />
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
                        {/* Label plate */}
                        <g transform={`translate(0,${n.radius + 12})`}>
                          <rect
                            x={-n.displayName.length * 3.2 - 4}
                            y={-9}
                            width={n.displayName.length * 6.4 + 8}
                            height={16}
                            rx={3}
                            fill={tokens.plateBg}
                            fillOpacity={tokens.plateOpacity}
                          />
                          <text textAnchor="middle" dy="3" fontSize={n.type === "org" ? 13 : n.type === "domain" ? 12 : 10.5} fontWeight={isContainer ? 600 : 500} fill={tokens.plateText}>
                            {n.displayName}
                          </text>
                        </g>
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

          {/* Minimap */}
          <div className="absolute bottom-3 right-3">
            <GraphMinimap points={miniPoints} rects={miniRects} />
          </div>

          {/* Legend (type-focus) */}
          <div className="absolute left-3 bottom-3 flex flex-wrap gap-1.5 rounded-lg border border-default bg-elevated/90 p-2">
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
        </div>

        {/* Inspector rail */}
        <div className="w-64 shrink-0 rounded-xl border border-default bg-elevated">
          <GraphInspector
            data={inspector}
            onSelectRelationship={reveal}
            onClose={() => setSelectedId(null)}
            onApprove={inspector?.isProposal ? () => setHint("Approve is wired to the Phase-5 apply gate.") : undefined}
            onDismiss={inspector?.isProposal ? () => setSelectedId(null) : undefined}
          />
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
    </div>
  )
}
