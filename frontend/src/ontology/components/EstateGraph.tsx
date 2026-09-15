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
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react"
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
  crossPath,
  initialExpanded,
  layoutHash,
  layoutTree,
  spinePath,
  viewportContentRect,
  type LaidNode,
  type Layout,
  type Point,
} from "@/ontology/ontologyTreeLayout"
import { MOTION, easeCubicOut, lerp, prefersReducedMotion } from "@/ontology/ontologyMotion"
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
// Containers (org/domain/subdomain) are the north-star's headline tier, so they get a
// longer budget before ellipsis — their names are what the curator reads first (R12d).
const LABEL_MAX_CONTAINER = 24
function clampLabel(s: string, max: number = LABEL_MAX): string {
  return s.length > max ? `${s.slice(0, max - 1).trimEnd()}…` : s
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

/**
 * `useLayoutEffect` on the client, `useEffect` on the server. The relayout glide must paint its
 * t=0 frame BEFORE the browser shows the freshly-committed (final) positions — otherwise the
 * tree would flash to its end state and snap back. Falling back to `useEffect` under SSR keeps
 * `renderToStaticMarkup` (the node-env test path) free of the useLayoutEffect warning.
 */
const useIsoLayoutEffect = typeof window !== "undefined" ? useLayoutEffect : useEffect

/** Escape a node id for use inside a `[data-…="…"]` CSS attribute selector. */
function escSel(id: string): string {
  return id.replace(/["\\]/g, "\\$&")
}

export function EstateGraph({
  graph,
  api,
  initialOrigin = "applied",
  drafts = null,
  taxonomy = null,
  estateName = null,
  initialDomainPanelOpen = false,
  initialExpandAll = false,
  initialSelectedId = null,
  onReady,
}: {
  graph: OntologyGraph
  api?: EstateGraphApi
  initialOrigin?: GraphOrigin
  drafts?: OntologyDrafts | null
  taxonomy?: OntologyTaxonomy | null
  estateName?: string | null
  /** Dev-harness only (R27 shot): open the domain show/hide panel on mount. */
  initialDomainPanelOpen?: boolean
  /**
   * Dev-harness / static-test only: seed BOTH the expand set and the asset-drill set with every
   * node id, so a non-interactive render shows the full tree incl. gated leaf assets. Real UI
   * mounts default to the clean Estate→Domain→Sub-domain view (asset gating on).
   */
  initialExpandAll?: boolean
  /**
   * Dev-harness / static-test only: seed the selected node so a non-interactive render shows
   * that node's relationship arcs (arcs are focus-gated — off at rest, R4). Real UI mounts start
   * with no selection (a clean tree; arcs appear on click).
   */
  initialSelectedId?: string | null
  onReady?: (handle: EstateGraphHandle) => void
}) {
  const { resolvedTheme } = useTheme()
  const theme = resolvedTheme === "light" ? "light" : "dark"
  const tokens = graphTokens(theme)

  const [provenance, setProvenance] = useState<Provenance>(initialOrigin === "proposed" ? "proposed" : "applied")
  const [selectedId, setSelectedId] = useState<string | null>(initialSelectedId)
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
  const [showDomainPanel, setShowDomainPanel] = useState(initialDomainPanelOpen)
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

  const [expanded, setExpanded] = useState<Set<string>>(() =>
    initialExpandAll ? new Set(model.nodes.map((n) => n.id)) : initialExpanded(model),
  )
  // Tier-aware asset drilling (owner directive): domain/org ids the curator has explicitly
  // drilled to reveal their directly-attached assets. Empty by default so the reset view is a
  // clean Estate→Domain→Sub-domain; passing this set to `layoutTree` turns gating ON.
  const [assetsExpanded, setAssetsExpanded] = useState<Set<string>>(() =>
    initialExpandAll ? new Set(model.nodes.map((n) => n.id)) : new Set(),
  )
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
    setAssetsExpanded(new Set())
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
        assetsExpanded, // presence turns tier-aware gating ON (domain assets need a drill)
      }),
    // dragTick is a deliberate relayout trigger; offsetsRef is mutated in place.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [vizModel, expanded, dragTick, selectedId, relVerbFocus, uncapped, assetsExpanded],
  )

  const nodeById = useMemo(() => new Map(layout.nodes.map((n) => [n.id, n])), [layout])

  // Latest-value ref for the imperative interaction paths (zoom / drag / glide) so their event
  // handlers read current geometry (position + radius) WITHOUT re-binding on every render —
  // assigned during render (idempotent, ref only). This is what lets drag be O(1)/frame with
  // no relayout.
  const nodeByIdRef = useRef(nodeById)
  nodeByIdRef.current = nodeById

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
  // Latest committed transform (for drag → content-space delta + camera-tween start), and the
  // running camera rAF so a user gesture can interrupt an in-flight Fit/Reset ease.
  const transformRef = useRef(transform)
  transformRef.current = transform
  const cameraRafRef = useRef<number | null>(null)

  const cancelCameraTween = useCallback(() => {
    if (cameraRafRef.current != null) {
      cancelAnimationFrame(cameraRafRef.current)
      cameraRafRef.current = null
    }
  }, [])

  // IMPERATIVE ZOOM (§2, O(1)/frame): write the transform straight to the <g> each tick — NO
  // per-frame React render. React `transform` state syncs only on 'end' (which also updates the
  // minimap you-are-here box via its effect). A user-initiated gesture (sourceEvent present)
  // cancels any in-flight camera ease so it doesn't fight the pan.
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
      .on("start", (e) => {
        if (e.sourceEvent) cancelCameraTween()
      })
      .on("zoom", (e) => {
        const g = gRef.current
        if (g) g.setAttribute("transform", `translate(${e.transform.x},${e.transform.y}) scale(${e.transform.k})`)
      })
      .on("end", (e) => setTransform({ x: e.transform.x, y: e.transform.y, k: e.transform.k }))
    zoomRef.current = z
    select(svg).call(z)
    return () => {
      select(svg).on(".zoom", null)
    }
  }, [cancelCameraTween])

  // Instant camera set (auto-fits, minimap, reveal): drives d3.zoom, which dispatches a
  // synchronous zoom+end → the <g> moves imperatively and React state syncs on 'end'.
  const applyTransform = useCallback((t: { x: number; y: number; k: number }) => {
    const svg = svgRef.current
    if (!svg || !zoomRef.current) {
      setTransform(t)
      return
    }
    select(svg).call(zoomRef.current.transform, zoomIdentity.translate(t.x, t.y).scale(t.k))
  }, [])

  // TRANSITIONED CAMERA (§5): rAF-tween {x,y,k} with easeCubicOut, writing the <g> transform
  // imperatively each frame, then sync d3.zoom's internal state ONCE at the end (via
  // applyTransform → zoom.transform) so the next pan doesn't jump. Reduced-motion (or no rAF)
  // ⇒ instant. Used by the Fit + Reset buttons; auto-fits stay instant.
  const animateTransform = useCallback(
    (target: { x: number; y: number; k: number }) => {
      const g = gRef.current
      if (!g || !zoomRef.current || prefersReducedMotion() || typeof requestAnimationFrame === "undefined") {
        applyTransform(target)
        return
      }
      cancelCameraTween()
      const start = { ...transformRef.current }
      const t0 = performance.now()
      const step = (now: number) => {
        const p = Math.min(1, (now - t0) / MOTION.cameraMs)
        const e = easeCubicOut(p)
        const cx = lerp(start.x, target.x, e)
        const cy = lerp(start.y, target.y, e)
        const ck = lerp(start.k, target.k, e)
        g.setAttribute("transform", `translate(${cx},${cy}) scale(${ck})`)
        if (p < 1) {
          cameraRafRef.current = requestAnimationFrame(step)
        } else {
          cameraRafRef.current = null
          applyTransform(target) // sync d3 internal + React state ONCE
        }
      }
      cameraRafRef.current = requestAnimationFrame(step)
    },
    [applyTransform, cancelCameraTween],
  )

  // Pure-ish framing math: the target camera transform that frames the laid-out CONTENT bbox —
  // nodes + any visible verb-arc extents (R12b), plus the tray only when on screen. Returns null
  // when there's nothing to frame or the canvas isn't measurable yet.
  const computeFit = useCallback((): { x: number; y: number; k: number } | null => {
    const svg = svgRef.current
    if (!svg) return null
    const b = contentBounds(layout, { tree: showTree, tray: showProposals || !showTree })
    if (b.width <= 0 || b.height <= 0) return null
    const padX = 40
    // Reserve extra headroom at the top so the org / top-domain tier (and its label plate)
    // is never cropped by the canvas edge on fit (§5/R21e).
    const padTop = 60
    const padBottom = 40
    const bw = b.width + padX * 2
    const bh = b.height + padTop + padBottom
    const rect = svg.getBoundingClientRect()
    if (rect.width === 0 || rect.height === 0) return null
    const k = Math.min(2.2, Math.max(0.28, Math.min(rect.width / bw, rect.height / bh) * 0.98))
    const x = rect.width / 2 - (b.minX + b.width / 2) * k
    const centeredY = rect.height / 2 - (b.minY + b.height / 2) * k
    // Center vertically, but keep at least `padTop` screen px above the content top.
    const y = Math.max(padTop - b.minY * k, centeredY)
    return { x, y, k }
  }, [layout, showTree, showProposals])

  // Frame the content (R12a). The Fit + Reset buttons ease the camera (§5); every AUTO-fit
  // (initial mount, fullscreen enter/exit, reveal) stays INSTANT.
  const fit = useCallback(
    (animated = false) => {
      const t = computeFit()
      if (!t) return
      if (animated) animateTransform(t)
      else applyTransform(t)
    },
    [computeFit, animateTransform, applyTransform],
  )
  // Latest-value ref for `fit` so effects that should fire on a SPECIFIC trigger (e.g. a
  // fullscreen transition) can call the current fit WITHOUT listing `fit` in their deps —
  // `fit` is recreated on every layout change (computeFit depends on `layout`), so depending
  // on it would auto-reframe the camera on every node click / expand (the "clicking zooms
  // out" bug). Assigned during render (idempotent, ref only).
  const fitRef = useRef(fit)
  fitRef.current = fit

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
  // layout so the newly-expanded path + its verb arcs are framed (R12b), then clears. Reveal is
  // an AUTO-fit, so it stays INSTANT (§5).
  useEffect(() => {
    if (!pendingFit.current) return
    pendingFit.current = false
    if (!didFit.current) return // initial fit will cover the first paint
    const id = requestAnimationFrame(() => {
      if (svgRef.current) fit()
    })
    return () => cancelAnimationFrame(id)
  }, [layout, fit])

  // Reframe with an EASED camera after a Reset has relayed out (§5) — Reset is a button, so its
  // camera transitions (unlike the auto-fits above). Fires on the post-reset layout, then clears.
  const pendingAnimatedFit = useRef(false)
  useEffect(() => {
    if (!pendingAnimatedFit.current) return
    pendingAnimatedFit.current = false
    const id = requestAnimationFrame(() => {
      if (svgRef.current) fit(true)
    })
    return () => cancelAnimationFrame(id)
  }, [layout, fit])

  // ── Imperative edge redraw (shared by drag §3 + glide §4) ────────────────────
  // Recompute ONE edge's `d` from a `posOf(id)` lookup, using the SAME pure builders the layout
  // pass uses (§1) so the geometry is byte-identical. Spine paths carry data-src/data-dst;
  // cross groups additionally carry data-relclass + hold two `data-cross-line` paths and an
  // optional `data-cross-label` verb plate.
  const redrawEdge = useCallback((el: Element, posOf: (id: string) => Point | null) => {
    const kind = el.getAttribute("data-edge-kind")
    const src = el.getAttribute("data-src")
    const dst = el.getAttribute("data-dst")
    if (!src || !dst) return
    const a = posOf(src)
    const b = posOf(dst)
    if (!a || !b) return
    if (kind === "spine") {
      el.setAttribute("d", spinePath(a.x, a.y, b.x, b.y))
      return
    }
    const xdom = el.getAttribute("data-relclass") === "xdom"
    const bow = DEFAULT_LAYOUT.bow * (xdom ? DEFAULT_LAYOUT.xdomBow : 1)
    const startTrim = (nodeByIdRef.current.get(src)?.radius ?? 0) + 2
    const endTrim = (nodeByIdRef.current.get(dst)?.radius ?? 0) + 7
    const { path, mid } = crossPath(a, b, bow, startTrim, endTrim)
    el.querySelectorAll<SVGPathElement>("path[data-cross-line]").forEach((p) => p.setAttribute("d", path))
    const label = el.querySelector<SVGGElement>("[data-cross-label]")
    if (label) label.setAttribute("transform", `translate(${mid.x},${mid.y})`)
  }, [])

  // IMPERATIVE DRAG (§3, O(1)/frame): move ONLY this node's <g> + recompute the `d` of its
  // incident spine/cross edges — never a relayout. `layoutTree` MUST NOT run mid-drag; the
  // accumulated offset is committed to `offsetsRef` and relayout is triggered ONCE, on 'end'.
  const applyLiveDrag = useCallback(
    (el: SVGGElement, id: string, dx: number, dy: number) => {
      const g = gRef.current
      const base = nodeByIdRef.current.get(id)
      if (!g || !base) return
      const nx = base.x + dx
      const ny = base.y + dy
      el.setAttribute("transform", `translate(${nx},${ny})`)
      const posOf = (q: string): Point | null =>
        q === id ? { x: nx, y: ny } : nodeByIdRef.current.get(q) ?? null
      g.querySelectorAll<SVGElement>(
        `[data-edge-kind][data-src="${escSel(id)}"],[data-edge-kind][data-dst="${escSel(id)}"]`,
      ).forEach((edge) => redrawEdge(edge, posOf))
    },
    [redrawEdge],
  )

  // ── Relayout glide (§4) ──────────────────────────────────────────────────────
  const prevPosRef = useRef<Map<string, Point>>(new Map())
  const glideRafRef = useRef<number | null>(null)
  // Set on drag-commit so the offset relayout does NOT glide the node back from its origin (it's
  // already visually at the drop point).
  const skipNextGlideRef = useRef(false)

  // ── Drag (d3.drag on nodes) ──────────────────────────────────────────────────
  useEffect(() => {
    const g = gRef.current
    if (!g) return
    const sel = select(g).selectAll<SVGGElement, unknown>("[data-node-id]")
    // Per-gesture accumulator (content units) — one drag at a time, so a single closure var is
    // enough. `k` is read from the ref so a zoom between drags never re-binds this effect.
    const live = { id: "", dx: 0, dy: 0 }
    const dragged = d3drag<SVGGElement, unknown>()
      .on("start", function (e) {
        e.sourceEvent?.stopPropagation?.()
        setHoveredId(null) // hide the hover snippet while dragging (R21)
        live.id = (this as SVGGElement).getAttribute("data-node-id") ?? ""
        live.dx = 0
        live.dy = 0
      })
      .on("drag", function (e) {
        if (!live.id) return
        // d3-drag's container defaults to the dragged element's parent — the zoom-transformed
        // <g ref={gRef}> — so `e.dx/e.dy` are ALREADY inverted through that group's CTM, i.e.
        // in CONTENT (unscaled) coordinates. Dividing by the zoom `k` here double-compensated
        // and made the node run ahead of the cursor (worse the further you zoomed). Accumulate
        // the raw content-space deltas so the node tracks the pointer 1:1 at every zoom level.
        live.dx += e.dx
        live.dy += e.dy
        applyLiveDrag(this as SVGGElement, live.id, live.dx, live.dy)
      })
      .on("end", function () {
        if (!live.id) return
        const cur = offsetsRef.current.get(live.id) ?? { x: 0, y: 0 }
        offsetsRef.current.set(live.id, { x: cur.x + live.dx, y: cur.y + live.dy })
        skipNextGlideRef.current = true
        setDragTick((t) => t + 1) // the ONE relayout for the whole gesture
        live.id = ""
      })
    sel.call(dragged)
    return () => {
      sel.on(".drag", null)
    }
  }, [layout.nodes, applyLiveDrag])

  // Background click-to-deselect (#1): a genuine click on empty canvas clears the selection
  // (so its relationship arcs disappear). We record the pointer-down position and only treat it
  // as a deselect click when the pointer barely moved — a pan (drag) must not deselect.
  const bgDownRef = useRef<{ x: number; y: number } | null>(null)
  const onBackgroundClick = useCallback((e: { clientX: number; clientY: number }) => {
    const down = bgDownRef.current
    bgDownRef.current = null
    if (down && Math.hypot(e.clientX - down.x, e.clientY - down.y) > 6) return // a pan, not a click
    setSelectedId(null)
    setRelVerbFocus(null)
  }, [])

  // ── Selection / expand / navigation ─────────────────────────────────────────
  // Owner directive (#1): click is DETERMINISTIC and predictable — select ≠ dump assets.
  //   • Any click SELECTS the node (docks the inspector, reveals its relationship arcs).
  //   • A container (org/domain/sub-domain) additionally TOGGLES open/closed: a closed one
  //     opens its sub-containers, an open one collapses. It never silently swaps sub-domains
  //     for a flood of leaf assets (the old confusing 3-state cycle).
  //   • A domain/org whose ONLY children are assets (no sub-containers) drills those on open,
  //     so opening it is never an empty no-op; collapsing always un-drills.
  //   • A domain/org that HAS sub-containers keeps its directly-attached assets gated — the
  //     curator reveals them with the explicit "Show N direct assets" control in the inspector
  //     (`toggleDirectAssets`), so a single click can't crowd the tree.
  // Expand/collapse is a functional toggle off `prev`, so a rapid double-click is a clean
  // open→close (no stale-closure flip-flop).
  const onNodeClick = useCallback(
    (id: string) => {
      setSelectedId(id)
      setRelVerbFocus(null) // a node selection is the more specific navigation (R25 precedence)
      const n = nodeById.get(id)
      const kids = vizModel.childrenByParent.get(id) ?? []
      if (!n || kids.length === 0) return // leaf → select only, nothing to expand
      const isCont = (t: NodeType) => t === "org" || t === "domain" || t === "subdomain"
      const isDomainOrOrg = n.type === "domain" || n.type === "org"
      const hasContainerKids = kids.some((k) => isCont(k.type))
      const hasAssetKids = kids.some((k) => !isCont(k.type))
      // Only asset-only domains/orgs drill on open (else the open would reveal nothing).
      const drillOnOpen = isDomainOrOrg && !hasContainerKids && hasAssetKids
      const wasExpanded = expanded.has(id)
      setExpanded((prev) => {
        const next = new Set(prev)
        if (next.has(id)) next.delete(id)
        else next.add(id)
        return next
      })
      if (!isDomainOrOrg) return // sub-domain/asset children aren't gated — the expand set is enough
      setAssetsExpanded((prev) => {
        // Keep the asset-drill flag coherent with open/close (the gate hides drilled assets
        // whenever the node is collapsed, so a residual flag is invisible either way):
        //   collapse → un-drill; open an asset-only node → drill; open a node with
        //   sub-containers → leave assets gated (inspector reveals them on demand).
        if (wasExpanded) {
          if (!prev.has(id)) return prev
          const next = new Set(prev)
          next.delete(id)
          return next
        }
        return drillOnOpen ? new Set(prev).add(id) : prev
      })
    },
    [nodeById, vizModel, expanded],
  )

  // Explicit direct-asset drill (#1): the inspector's "Show / Hide N direct assets" control for
  // a domain/org that has sub-containers AND gated directly-attached assets. Toggling ensures the
  // node is open, then flips its entry in the asset-drill set.
  const toggleDirectAssets = useCallback((id: string) => {
    setExpanded((prev) => (prev.has(id) ? prev : new Set(prev).add(id)))
    setAssetsExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }, [])

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
      // Drill every domain/org ancestor too, so a gated (directly-attached) target — e.g. an
      // agent hanging off a domain — is actually revealed and not left hidden behind the gate.
      setAssetsExpanded((prev) => {
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
    // Drill the domain/org ancestors so gated (directly-attached) hits are actually revealed.
    setAssetsExpanded((prev) => {
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
    const allIds = new Set(vizModel.nodes.map((n) => n.id))
    setExpanded(allIds)
    setAssetsExpanded(new Set(allIds)) // drill every domain so their assets show too
    pendingFit.current = true
  }, [vizModel])

  // Collapse-all (P0-a): fold every container back to the domain tier, then re-fit (R24).
  const collapseAll = useCallback(() => {
    setExpanded(collapsedToDomainTier(vizModel))
    setAssetsExpanded(new Set())
    setUncapped(new Set())
    pendingFit.current = true
  }, [vizModel])

  const resetView = useCallback(() => {
    offsetsRef.current = new Map()
    setExpanded(initialExpanded(model))
    setAssetsExpanded(new Set())
    setSelectedId(null)
    setTypeFocus(null)
    setRelVerbFocus(null)
    setUncapped(new Set())
    // dragTick guarantees a fresh layout object → the eased-reframe effect fires (didFit stays
    // true so the instant one-shot doesn't also fire).
    pendingAnimatedFit.current = true
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
    // Typed cross-links — carry the edge's evidence bag (MV-D88) so the "why" reads
    // inline in the docked inspector, not only on arc hover (R25).
    for (const e of vizModel.crossEdges) {
      if (e.src === mn.id || e.dst === mn.id) {
        const otherId = e.src === mn.id ? e.dst : e.src
        const other = vizModel.nodes.find((x) => x.id === otherId)
        if (other) {
          const detail = e.detail
            ? Object.entries(e.detail).filter(([, v]) => v != null && `${v}`.trim() !== "").slice(0, 4)
            : undefined
          rels.push({
            targetId: otherId,
            label: other.label,
            verb: e.verb,
            xdom: e.relClass === "xdom",
            detail: detail && detail.length ? detail : undefined,
          })
        }
      }
    }
    const technical: string[] = []
    if (mn.type === "table" || mn.type === "metric_view") technical.push(`Path: ${mn.id.replace(/^asset:|^mv:|^table:/, "")}`)
    // Prefer the snapshot's real description/meta (MV-D86); fall back to generic copy (R13).
    const meta = mn.meta
      ? Object.entries(mn.meta).map(([k, v]) => [k, `${v}`] as [string, string])
      : undefined
    const pages = mn.type === "subdomain" ? pagesFromExpansions(hydrationRef.current.get(mn.id)) : []
    // Direct-asset drill (#1): only offered for a domain/org that ALSO has sub-containers, so
    // its directly-attached assets stay gated behind an explicit reveal (an asset-only domain
    // reveals its assets on open, so no toggle is needed there).
    let directAssets: InspectorData["directAssets"] = null
    if (mn.type === "domain" || mn.type === "org") {
      const kids = vizModel.childrenByParent.get(mn.id) ?? []
      const isCont = (t: NodeType) => t === "org" || t === "domain" || t === "subdomain"
      const assetKidCount = kids.filter((k) => !isCont(k.type)).length
      const hasContainerKids = kids.some((k) => isCont(k.type))
      if (assetKidCount > 0 && hasContainerKids) {
        directAssets = { id: mn.id, count: assetKidCount, drilled: assetsExpanded.has(mn.id) }
      }
    }
    return {
      title: mn.label,
      typeLabel: TYPE_LABEL[mn.type],
      description: mn.description && mn.description.trim() ? mn.description : describe(n),
      facts,
      meta,
      pages: pages.length ? pages : undefined,
      technical,
      relationships: rels.slice(0, 24),
      directAssets,
    }
    // hydrationRef is read for attached Pages; hydrationVersion gates recompute.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedId, vizModel, nodeById, hydrationVersion, assetsExpanded])

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
  // Re-fit ONLY on an actual fullscreen enter/exit (the canvas size changed) — call the latest
  // fit via the ref so this does NOT re-run when `fit` is recreated by a layout change (which
  // would reframe the camera on every click/expand). R24: never stranded in a thin band.
  useEffect(() => {
    if (!didFit.current) return
    const id = requestAnimationFrame(() => {
      if (svgRef.current) fitRef.current()
    })
    return () => cancelAnimationFrame(id)
  }, [fullscreen])

  const dimmed = useCallback((t: NodeType) => typeFocus != null && typeFocus !== t, [typeFocus])
  // Dim an arc when a rel-type is highlighted and this arc isn't that verb (Bloom idiom, R25).
  const edgeDimmed = useCallback(
    (verb: string) => relVerbFocus != null && relVerbFocus !== verb,
    [relVerbFocus],
  )

  // GLIDE ON RELAYOUT (§4): on a layout change, tween every surviving node prev→next over ~380ms
  // (easeCubicOut), driving BOTH node transforms AND their incident edge `d` (via §1) so edges
  // stay glued — no snap. Entering nodes fade+scale in from their parent's previous position;
  // exiting nodes are already unmounted, so they snap (documented tradeoff). React has already
  // committed the FINAL positions in JSX, so we paint the t=0 frame in a layout-effect (before
  // the browser shows the final frame) then rAF to t=1. Reduced-motion / drag-commit / the first
  // paint settle instantly with positions byte-identical to today.
  useIsoLayoutEffect(() => {
    const g = gRef.current
    const nodes = layout.nodes
    const nextPos = new Map<string, Point>(nodes.map((n) => [n.id, { x: n.x, y: n.y }]))
    const prev = prevPosRef.current
    const settle = () => {
      prevPosRef.current = nextPos
      if (glideRafRef.current != null) {
        cancelAnimationFrame(glideRafRef.current)
        glideRafRef.current = null
      }
    }
    if (
      !g ||
      prev.size === 0 || // first paint — never fade the whole tree in
      skipNextGlideRef.current || // a drag just committed; the node is already at its drop point
      prefersReducedMotion() ||
      typeof requestAnimationFrame === "undefined"
    ) {
      skipNextGlideRef.current = false
      settle()
      return
    }

    // Entering nodes (not in prev) start from their parent's previous position; survivors that
    // actually moved trigger the tween.
    const enterFrom = new Map<string, Point>()
    let changed = false
    for (const n of nodes) {
      const p = prev.get(n.id)
      if (!p) {
        const parentPrev = n.parentId ? prev.get(n.parentId) ?? nextPos.get(n.parentId) : undefined
        enterFrom.set(n.id, parentPrev ?? { x: n.x, y: n.y })
        changed = true
      } else if (Math.abs(p.x - n.x) > 0.01 || Math.abs(p.y - n.y) > 0.01) {
        changed = true
      }
    }
    if (!changed) {
      settle()
      return
    }

    if (glideRafRef.current != null) cancelAnimationFrame(glideRafRef.current)
    // Snapshot the element + edge nodes once (positions change every frame, the DOM set doesn't).
    const elById = new Map<string, SVGGElement>()
    g.querySelectorAll<SVGGElement>("[data-node-id]").forEach((el) => {
      const id = el.getAttribute("data-node-id")
      if (id) elById.set(id, el)
    })
    const edgeEls = Array.from(g.querySelectorAll<SVGElement>("[data-edge-kind]"))
    const startPrev = prev
    const natural = (n: LaidNode) => (dimmed(n.type) ? 0.2 : 1)

    const paint = (e: number) => {
      const posNow = new Map<string, Point>()
      for (const n of nodes) {
        const entering = enterFrom.has(n.id)
        const from = entering ? enterFrom.get(n.id)! : startPrev.get(n.id) ?? { x: n.x, y: n.y }
        const cur = { x: from.x + (n.x - from.x) * e, y: from.y + (n.y - from.y) * e }
        posNow.set(n.id, cur)
        const el = elById.get(n.id)
        if (!el) continue
        if (entering) {
          el.setAttribute("transform", `translate(${cur.x},${cur.y}) scale(${0.7 + 0.3 * e})`)
          el.setAttribute("opacity", String(natural(n) * e))
        } else {
          el.setAttribute("transform", `translate(${cur.x},${cur.y})`)
          // Idempotent: keeps a node that was mid-fade in an earlier (interrupted) glide correct.
          el.setAttribute("opacity", String(natural(n)))
        }
      }
      const posOf = (id: string): Point | null => posNow.get(id) ?? nextPos.get(id) ?? null
      edgeEls.forEach((edge) => redrawEdge(edge, posOf))
    }

    paint(0) // t=0 synchronously, before the browser paints the committed final frame
    const t0 = performance.now()
    const step = (now: number) => {
      const p = Math.min(1, (now - t0) / MOTION.glideMs)
      paint(easeCubicOut(p))
      if (p < 1) {
        glideRafRef.current = requestAnimationFrame(step)
      } else {
        glideRafRef.current = null
        prevPosRef.current = nextPos
      }
    }
    glideRafRef.current = requestAnimationFrame(step)
    return () => {
      if (glideRafRef.current != null) {
        cancelAnimationFrame(glideRafRef.current)
        glideRafRef.current = null
      }
    }
    // `layout` is the real trigger; `dimmed` + `redrawEdge` are captured fresh each run (a
    // typeFocus-only change relayouts to identical positions ⇒ the tween no-ops via `changed`).
  }, [layout])

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
          <button onClick={() => fit(true)} className="flex items-center gap-1 rounded-md border border-default px-2 py-1 text-xs text-secondary hover:text-primary" aria-label="Fit">
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
              // Transparent canvas: the app-themed container (`bg-sunken`) shows through, so the
              // map background always matches the app and flips with the light/dark toggle via CSS
              // (independent of the React token theme). The north-star's oat `ground` is dropped on
              // purpose — the palette that matters (nodes/edges/plates) still comes from `tokens`.
              className="h-full w-full cursor-grab bg-transparent active:cursor-grabbing"
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
                    markerWidth="7.5"
                    markerHeight="7.5"
                    orient="auto-start-reverse"
                  >
                    <path d="M0,0 L10,5 L0,10 z" fill={m.color} />
                  </marker>
                ))}
              </defs>
              <rect
                x="0"
                y="0"
                width="100%"
                height="100%"
                fill="url(#ontgrid)"
                onPointerDown={(e) => {
                  bgDownRef.current = { x: e.clientX, y: e.clientY }
                }}
                onClick={onBackgroundClick}
              />
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
                        data-edge-kind="spine"
                        data-src={l.sourceId}
                        data-dst={l.targetId}
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
                        data-edge-kind="cross"
                        data-src={c.sourceId}
                        data-dst={c.targetId}
                        data-relclass={c.relClass}
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
                        <path data-cross-line d={c.path} fill="none" stroke="transparent" strokeWidth={12} />
                        <path
                          data-cross-line
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
                          <g data-cross-label transform={`translate(${c.labelAt.x},${c.labelAt.y})`}>
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
                    const fill = tokens.typeFill[n.type]
                    const tint = domainTintFor(tokens, n.domainId)
                    const isContainer = n.type === "org" || n.type === "domain" || n.type === "subdomain"
                    const caption = clampLabel(n.displayName, isContainer ? LABEL_MAX_CONTAINER : LABEL_MAX)
                    // Tiered label typography (north-star §5/R21e): the container tier reads
                    // bigger + bold so business areas dominate the leaf assets; the plate is
                    // sized to the font so a larger caption never hard-clips its chip.
                    const labelFont =
                      n.type === "org" ? 15 : n.type === "domain" ? 13 : n.type === "subdomain" ? 12 : 10.5
                    const charW = labelFont * 0.62
                    const plateW = caption.length * charW + 10
                    const plateH = labelFont + 7
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
                            x={-plateW / 2}
                            y={-plateH / 2}
                            width={plateW}
                            height={plateH}
                            rx={3}
                            fill={tokens.plateBg}
                            fillOpacity={tokens.plateOpacity}
                          />
                          <text textAnchor="middle" dy={labelFont * 0.35} fontSize={labelFont} fontWeight={isContainer ? 700 : 500} fill={tokens.plateText}>
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
            <p className="text-[10px] text-muted">click to open · drag to move · hover for details · → shows direction</p>
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
              onToggleDirectAssets={toggleDirectAssets}
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
