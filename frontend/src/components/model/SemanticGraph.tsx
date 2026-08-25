/* eslint-disable react-refresh/only-export-components */
/**
 * SemanticGraph — deterministic layered SVG of a Genie Agent's semantic model.
 *
 * Promoted to production from the Prompt 12.0 mockup (MvSemanticModelFrame.tsx),
 * NOT imported from it: that scaffold is disposed at Prompt 13.5, so coupling
 * ship code to it would be backwards. The layout is hand-rolled and
 * deterministic (no force layout, no graph/layout dependency, MV-D24) — two
 * renders of one space produce one picture, which is what a diff overlay
 * requires.
 *
 * Columns, left to right: 0 source/fact tables · 1 joined dimension tables ·
 * 2 metric views · 3 measure concepts. Governance ladder is a TRAFFIC LIGHT on
 * the theme's semantic tokens (governed=success / curated=warning /
 * ungoverned=danger), each rung carrying a non-color label so it never leans on
 * hue alone.
 *
 * ── 12c Part 2 (grouped layout) ──────────────────────────────────────────────
 * The v1 layout gave every measure its own row in column 3; a real space has
 * dozens, so the column drove the height and meet-scaling shrank everything past
 * legibility (finding 4; semantic-graph-v2-note.md §1). The redesign, per that
 * approved note:
 *   §2 grouping — measures render as chips INSIDE their owning card (an MV card,
 *       or a single "measure concepts" card for ungoverned/curated concepts that
 *       belong to no MV), with a card-level governance roll-up so the traffic
 *       light survives grouping. Column 3 stops being a row-driver.
 *   §3 fit — the "Fit" control computes a transform that frames the measured
 *       content in the measured viewport (computeFit), never reset()'s 100%.
 *   §4 focus/search + collapse-above-threshold — selecting a card dims all but
 *       its 1-hop neighborhood; a search box jump-focuses by name; above a
 *       derived threshold the cards open collapsed (roll-up counts kept, chip
 *       detail hidden) and expand on demand.
 *   §5 edge ports — edges attach to card left/right edge-midpoints (cardPorts),
 *       never the center, so a grouped card's own chip rows never get an edge
 *       drawn through them.
 *   §9 collapse threshold is DERIVED (collapseThreshold) from the measured
 *       viewport height and the legibility-floor scale, with 12 as the SSR/first
 *       -paint fallback — not a tuned literal.
 * Every direction stays a pure function of (nodes, edges, selection, measured
 * viewport), so renderToStaticMarkup testability holds.
 */
import { Component, useEffect, useMemo, useRef, useState, type ErrorInfo, type ReactNode } from "react"
import { AlertTriangle, Maximize2, Minus, Plus, RefreshCw, Search, ShieldCheck, Wrench } from "lucide-react"
import type { MvGovernance, SemanticGraphEdge, SemanticGraphNode } from "@/types"

export const GOVERNANCE: Record<
  MvGovernance,
  { label: string; color: string; Icon: typeof ShieldCheck }
> = {
  governed: { label: "Governed", color: "var(--color-success)", Icon: ShieldCheck },
  curated: { label: "Curated", color: "var(--color-warning)", Icon: Wrench },
  ungoverned: { label: "Ungoverned", color: "var(--color-danger)", Icon: AlertTriangle },
}

export const LADDER_ORDER: MvGovernance[] = ["governed", "curated", "ungoverned"]

export function countGovernance(nodes: SemanticGraphNode[]): Record<MvGovernance, number> {
  const counts: Record<MvGovernance, number> = { governed: 0, curated: 0, ungoverned: 0 }
  for (const n of nodes) {
    if (n.kind === "measure" && n.governance) counts[n.governance] += 1
  }
  return counts
}

// ── Grouped-card model (12c Part 2 §2) ───────────────────────────────────────
// Columns keep their product-semantic meaning (source → dim → metric view →
// concepts); measures no longer occupy a column of their own — they ride as
// chips inside the card that owns them.
const COL_X = [32, 236, 452, 700]
const COL_W = [176, 188, 220, 240]
const COL_HEADERS = ["source / fact", "dimensions", "metric views", "measure concepts"]
const ROW_TOP = 44
// ROW_GAP is the per-card stride the derived threshold solves against (§9); the
// grouped table/MV cards stack at ROW_TOP + N · ROW_GAP.
const ROW_GAP = 58
const VGAP = 18
const CARD_HDR = 44
const CHIP_STEP = 22
const CHIP_H = 18
const CARD_PAD_B = 8
const TABLE_H = 44
const CONCEPTS_ID = "__concepts__"

// The synthetic "measure concepts" card holds ungoverned/curated concepts that
// belong to no metric view; it has no backing node (node === null).
export interface GraphCard {
  id: string
  kind: "table" | "metric_view" | "concepts"
  label: string
  col: number
  row: number
  node: SemanticGraphNode | null
  proposed: boolean
  coverage: number | null
  measures: SemanticGraphNode[]
}

// buildCards folds the measure nodes into their owning cards. A membership edge
// (measure → metric_view) puts the measure's chip inside that MV card; measures
// with no membership to a rendered MV collect in the single concepts card. This
// is the §2 lever: column 3's cardinality (the 40-row driver at 30 tables)
// collapses into the cards that own those measures.
export function buildCards(nodes: SemanticGraphNode[], edges: SemanticGraphEdge[]): GraphCard[] {
  const mvIds = new Set(nodes.filter((n) => n.kind === "metric_view").map((n) => n.id))
  const membership = new Map<string, string>()
  for (const e of edges) {
    if (e.kind === "membership" && mvIds.has(e.to)) membership.set(e.from, e.to)
  }
  const measuresByMv = new Map<string, SemanticGraphNode[]>()
  const standalone: SemanticGraphNode[] = []
  for (const n of nodes) {
    if (n.kind !== "measure") continue
    const owner = membership.get(n.id)
    if (owner) {
      const list = measuresByMv.get(owner) ?? []
      list.push(n)
      measuresByMv.set(owner, list)
    } else {
      standalone.push(n)
    }
  }

  const cards: GraphCard[] = []
  for (const n of nodes) {
    if (n.kind === "table") {
      cards.push({
        id: n.id,
        kind: "table",
        label: n.label,
        col: Math.max(0, Math.min(1, n.col)),
        row: n.row,
        node: n,
        proposed: false,
        coverage: n.coverage ?? null,
        measures: [],
      })
    } else if (n.kind === "metric_view") {
      cards.push({
        id: n.id,
        kind: "metric_view",
        label: n.label,
        col: 2,
        row: n.row,
        node: n,
        proposed: !!n.proposed,
        coverage: n.coverage ?? null,
        measures: measuresByMv.get(n.id) ?? [],
      })
    }
  }
  if (standalone.length > 0) {
    cards.push({
      id: CONCEPTS_ID,
      kind: "concepts",
      label: "Measure concepts",
      col: 3,
      row: 0,
      node: null,
      proposed: false,
      coverage: null,
      measures: standalone,
    })
  }
  return cards
}

// Card-level governance roll-up (§2 / §8): the traffic light survives grouping
// as per-card counts. Present rungs only, ladder order, so an empty card says
// nothing rather than inventing zeros.
export function rollup(card: GraphCard): { rung: MvGovernance; count: number }[] {
  const counts: Record<MvGovernance, number> = { governed: 0, curated: 0, ungoverned: 0 }
  for (const m of card.measures) {
    if (m.governance) counts[m.governance] += 1
  }
  return LADDER_ORDER.filter((r) => counts[r] > 0).map((rung) => ({ rung, count: counts[rung] }))
}

export interface CardBox {
  x: number
  y: number
  w: number
  h: number
}

export interface PlacedCard {
  card: GraphCard
  box: CardBox
}

function cardHeight(card: GraphCard, collapsed: boolean): number {
  if (card.kind === "table") return TABLE_H
  if (collapsed || card.measures.length === 0) return CARD_HDR
  return CARD_HDR + card.measures.length * CHIP_STEP + CARD_PAD_B
}

// Deterministic layered placement: cards stack top-to-bottom within their
// column, ordered by the server-assigned row then label (stable). Height is a
// pure function of the cards and the collapse flag.
export function layoutCards(
  cards: GraphCard[],
  collapsed: boolean,
): { placed: Map<string, PlacedCard>; width: number; height: number } {
  const placed = new Map<string, PlacedCard>()
  const byCol: GraphCard[][] = [[], [], [], []]
  for (const c of cards) byCol[Math.max(0, Math.min(3, c.col))].push(c)
  let maxBottom = ROW_TOP
  byCol.forEach((column, col) => {
    column.sort((a, b) => a.row - b.row || a.label.localeCompare(b.label))
    let y = ROW_TOP
    for (const c of column) {
      const h = cardHeight(c, collapsed)
      placed.set(c.id, { card: c, box: { x: COL_X[col], y, w: COL_W[col], h } })
      y += h + VGAP
      maxBottom = Math.max(maxBottom, y)
    }
  })
  const width = COL_X[COL_X.length - 1] + COL_W[COL_W.length - 1] + 24
  const height = Math.max(ROW_TOP + ROW_GAP, maxBottom - VGAP + 16)
  return { placed, width, height }
}

// Edge ports (§5, reviewer note): left/right edge-midpoints of a placed card,
// so an edge stays in the inter-column gutter and never runs through the card's
// own chip rows. Pure function of the box.
export function cardPorts(box: CardBox): { left: { x: number; y: number }; right: { x: number; y: number } } {
  const midY = box.y + box.h / 2
  return { left: { x: box.x, y: midY }, right: { x: box.x + box.w, y: midY } }
}

// The collapse threshold is a DERIVATION (§9), not a magic number: the largest
// grouped-card count N whose stack (ROW_TOP + N·ROW_GAP) still fits at the
// legibility-floor scale in a viewport of height H —
//   N_max = floor( (H / floorScale - ROW_TOP) / ROW_GAP )
// A taller container opens denser, a shorter one collapses sooner, and nobody
// retunes a literal. Fallback 12 is used before a measurement exists (SSR /
// first paint) — which is floor((480/0.7 - 44)/58) ≈ 11 → 12 in the default box.
export function collapseThreshold(
  viewportHeight?: number | null,
  floorScale = 0.7,
  fallback = 12,
): number {
  if (!viewportHeight || viewportHeight <= 0) return fallback
  const n = Math.floor((viewportHeight / floorScale - ROW_TOP) / ROW_GAP)
  return Math.max(1, n)
}

// A real "fit everything" (§3): frame the measured content extents inside the
// measured viewport with padding, centered horizontally and top-aligned so a
// tall graph reads from the top. Scale is capped at 1 (fit never zooms in past
// 100%, the v1 reset() defect) and floored so it stays interactive. Pure.
export function computeFit(
  contentW: number,
  contentH: number,
  viewW: number,
  viewH: number,
  pad = 24,
): { scale: number; tx: number; ty: number } {
  if (contentW <= 0 || contentH <= 0 || viewW <= 0 || viewH <= 0) return { scale: 1, tx: 0, ty: 0 }
  const raw = Math.min((viewW - 2 * pad) / contentW, (viewH - 2 * pad) / contentH, 1)
  const scale = Math.min(1, Math.max(0.2, raw))
  const tx = Math.max(pad, (viewW - contentW * scale) / 2)
  const ty = pad
  return { scale, tx, ty }
}

// Case-insensitive substring match for the search/jump box (§4). Empty term
// matches everything, so a blank box dims nothing.
export function matchesSearch(text: string, term: string): boolean {
  const t = term.trim().toLowerCase()
  if (!t) return true
  return text.toLowerCase().includes(t)
}

// The 1-hop focus set (§4): the selected card plus every card one join/
// membership/replaces edge away. Selection may target a measure chip — its
// owning card anchors the focus. Returns null when nothing is selected (no
// dimming). Pure function of (cards, edges, selectedId).
export function focusSet(
  cards: GraphCard[],
  edges: SemanticGraphEdge[],
  selectedId: string | null | undefined,
): Set<string> | null {
  if (!selectedId) return null
  const nodeToCard = new Map<string, string>()
  for (const c of cards) {
    if (c.node) nodeToCard.set(c.id, c.id)
    for (const m of c.measures) nodeToCard.set(m.id, c.id)
  }
  const anchor = nodeToCard.get(selectedId)
  if (!anchor) return null
  const keep = new Set<string>([anchor])
  for (const e of edges) {
    const from = nodeToCard.get(e.from)
    const to = nodeToCard.get(e.to)
    if (from === undefined || to === undefined) continue
    if (from === anchor) keep.add(to)
    if (to === anchor) keep.add(from)
  }
  return keep
}

function abbreviate(text: string, max = 28): string {
  return text.length > max ? `${text.slice(0, max - 1)}…` : text
}

/**
 * Pan delta from a drag anchor to the current pointer. Null-safe by contract:
 * a null anchor (the pointerup-vs-pointermove race that used to crash the tab
 * via `drag.current!.tx`) yields null and the caller no-ops. Pure so the
 * invariant is testable without a DOM.
 */
export function panDelta(
  anchor: { x: number; y: number } | null,
  clientX: number,
  clientY: number,
): { dx: number; dy: number } | null {
  if (!anchor) return null
  return { dx: clientX - anchor.x, dy: clientY - anchor.y }
}

// Prompt 12b SQL-coverage lens: a small badge on the card's top-right corner
// carrying the curated-SQL touch count. 0 is a legible COLD SPOT (a node no
// curated query exercises), rendered plainly with a dashed ring — the lens's
// point, not an error. Absent (undefined) coverage renders nothing, so a
// lens-free Prompt 12 response is unchanged.
function CoverageBadge({ x, y, w, coverage }: { x: number; y: number; w: number; coverage?: number | null }) {
  if (coverage == null) return null
  const cold = coverage === 0
  const bx = x + w - 12
  const by = y - 6
  return (
    <g aria-label={cold ? "no curated SQL coverage" : `curated SQL coverage ${coverage}`}>
      <title>{cold ? "cold spot — no curated SQL touches this" : `${coverage} curated statement${coverage === 1 ? "" : "s"}`}</title>
      <circle cx={bx} cy={by} r="8" fill={cold ? "var(--bg-surface)" : "var(--color-accent)"} opacity={cold ? 1 : 0.85}
        stroke={cold ? "var(--color-danger)" : "var(--color-accent)"} strokeWidth="1" strokeDasharray={cold ? "2 2" : undefined} />
      <text x={bx} y={by + 3} textAnchor="middle" fontSize="8" fontWeight="700"
        fill={cold ? "var(--color-danger)" : "var(--bg-surface)"}>{coverage}</text>
    </g>
  )
}

function EdgeView({
  edge,
  from,
  to,
  active,
  onHover,
}: {
  edge: SemanticGraphEdge
  from: CardBox
  to: CardBox
  active: boolean
  onHover: (on: boolean) => void
}) {
  // Anchor at edge ports (§5): pick the facing sides so the curve lives in the
  // gutter between the two cards, never crossing a card's chip rows.
  const fromPorts = cardPorts(from)
  const toPorts = cardPorts(to)
  const leftToRight = from.x <= to.x
  const src = leftToRight ? fromPorts.right : fromPorts.left
  const dst = leftToRight ? toPorts.left : toPorts.right
  // Column-aware cubic curve: control points offset horizontally so edges bend
  // in the gutter (the §5 "orthogonal elbows or column-aware curves" call).
  const cx = (src.x + dst.x) / 2
  const path = `M ${src.x} ${src.y} C ${cx} ${src.y} ${cx} ${dst.y} ${dst.x} ${dst.y}`
  const midX = (src.x + dst.x) / 2
  const midY = (src.y + dst.y) / 2

  if (edge.kind === "replaces") {
    return (
      <g>
        <path d={path} fill="none" stroke="var(--color-danger)" strokeWidth="1.5" strokeDasharray="4 3" />
        <text x={midX} y={midY - 4} textAnchor="middle" className="fill-[var(--color-danger)]" fontSize="9">replaces</text>
      </g>
    )
  }
  if (edge.kind === "membership") {
    return <path d={path} fill="none" stroke="var(--color-accent)" strokeWidth="1.5" />
  }

  // join — declutter: relationship + SCD2 at rest, full ON predicate on hover or
  // when an endpoint card is selected. Prompt 12b coverage weight rides "×N".
  const weightLabel = typeof edge.weight === "number" && edge.weight > 0 ? `×${edge.weight}` : null
  const restLabel = [edge.relationship, edge.scd2 ? "SCD2" : null, weightLabel].filter(Boolean).join(" · ")
  return (
    <g onMouseEnter={() => onHover(true)} onMouseLeave={() => onHover(false)} style={{ cursor: "default" }}>
      <path d={path} fill="none" stroke="var(--border-color-strong)" strokeWidth={active ? 2 : 1.5} markerEnd="url(#mv-arrow)" />
      {active && edge.on ? (
        <text x={midX} y={midY - 6} textAnchor="middle" className="fill-[var(--text-secondary)]" fontSize="8.5">
          ON {abbreviate(edge.on, 42)}
        </text>
      ) : (
        restLabel && (
          <text x={midX} y={midY - 6} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="8.5">{restLabel}</text>
        )
      )}
      {active && (edge.relationship || edge.scd2) && (
        <text x={midX} y={midY + 8} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="8.5">
          {[edge.relationship, edge.scd2 ? "SCD2 (is_current)" : null].filter(Boolean).join(" · ")}
        </text>
      )}
    </g>
  )
}

function RollUp({ x, y, card }: { x: number; y: number; card: GraphCard }) {
  const rungs = rollup(card)
  if (rungs.length === 0) return null
  // Non-color labels ride the hue (§7): "1 governed · 2 curated", each colored,
  // so the traffic light never leans on color alone.
  return (
    <text x={x} y={y} className="fill-[var(--text-muted)]" fontSize="8.5" aria-label="governance roll-up">
      {rungs.map((r, i) => (
        <tspan key={r.rung} fill={GOVERNANCE[r.rung].color} fontWeight="600">
          {i > 0 ? "  ·  " : ""}
          {r.count} {GOVERNANCE[r.rung].label.toLowerCase()}
        </tspan>
      ))}
    </text>
  )
}

function MeasureChip({
  m,
  x,
  y,
  w,
  selected,
  dim,
  onSelect,
}: {
  m: SemanticGraphNode
  x: number
  y: number
  w: number
  selected: boolean
  dim: boolean
  onSelect: (n: SemanticGraphNode) => void
}) {
  const g = m.governance ? GOVERNANCE[m.governance] : null
  const color = g?.color ?? "var(--border-color-strong)"
  const tag = m.governance ? m.governance[0].toUpperCase() : "•"
  return (
    <g opacity={dim ? 0.3 : 1} onClick={() => onSelect(m)} style={{ cursor: "pointer" }}>
      <title>{m.origin ? `${m.label} — ${m.origin}` : m.label}</title>
      <rect x={x} y={y} width={w} height={CHIP_H} rx="4" fill={color} opacity="0.14"
        stroke={selected ? "var(--color-accent)" : color} strokeWidth={selected ? 2 : 1} />
      <text x={x + 6} y={y + 13} className="fill-[var(--text-primary)]" fontSize="9.5" fontWeight="600">{abbreviate(m.label, 22)}</text>
      {g && (
        <text x={x + w - 6} y={y + 13} textAnchor="end" fill={color} fontSize="8" fontWeight="700" aria-label={g.label}>{tag}</text>
      )}
    </g>
  )
}

function CardView({
  placed,
  collapsed,
  selectedId,
  dim,
  searchTerm,
  onSelect,
}: {
  placed: PlacedCard
  collapsed: boolean
  selectedId: string | null | undefined
  dim: boolean
  searchTerm: string
  onSelect: (n: SemanticGraphNode) => void
}) {
  const { card, box } = placed
  const { x, y, w, h } = box
  const selfSelected = card.node != null && card.id === selectedId
  const stroke = selfSelected ? "var(--color-accent)" : undefined
  const selWidth = selfSelected ? 2 : 1.5
  const opacity = dim ? 0.3 : 1

  if (card.kind === "table") {
    const clickable = card.node
    return (
      <g opacity={opacity} onClick={() => clickable && onSelect(clickable)} style={{ cursor: clickable ? "pointer" : "default" }}>
        <title>{card.label}</title>
        <rect x={x} y={y} width={w} height={h} rx="6" fill="var(--bg-surface)" stroke={stroke ?? "var(--border-color-strong)"} strokeWidth={selWidth} strokeDasharray={card.coverage === 0 ? "4 3" : undefined} />
        <text x={x + w / 2} y={y + 27} textAnchor="middle" className="fill-[var(--text-secondary)]" fontSize="10">{abbreviate(card.label, 22)}</text>
        <CoverageBadge x={x} y={y} w={w} coverage={card.coverage} />
      </g>
    )
  }

  // metric_view / concepts card: header (title + subtitle + roll-up), then chips
  // when expanded. Collapsed keeps the roll-up (governance story survives, §8)
  // and drops the chips.
  const isMv = card.kind === "metric_view"
  const subtitle = isMv ? (card.proposed ? "proposed metric view" : "metric view") : `${card.measures.length} concept${card.measures.length === 1 ? "" : "s"}`
  const clickableHeader = card.node
  const hidden = collapsed && card.measures.length > 0
  return (
    <g opacity={opacity}>
      <g onClick={() => clickableHeader && onSelect(clickableHeader)} style={{ cursor: clickableHeader ? "pointer" : "default" }}>
        <title>{card.proposed ? `${card.label} — proposed metric view` : card.label}</title>
        <rect x={x} y={y} width={w} height={h} rx="8"
          fill={isMv ? "var(--color-accent)" : "var(--bg-surface)"} opacity={isMv ? (card.proposed ? 0.1 : 0.15) : 1}
          stroke={stroke ?? (isMv ? "var(--color-accent)" : "var(--border-color-strong)")} strokeWidth={selWidth}
          strokeDasharray={card.proposed ? "5 3" : undefined} />
        <text x={x + 10} y={y + 16} className="fill-[var(--text-primary)]" fontSize="10" fontWeight="600">{abbreviate(card.label, 24)}</text>
        <text x={x + 10} y={y + 29} className="fill-[var(--text-muted)]" fontSize="8">{subtitle}{hidden ? " · collapsed" : ""}</text>
        <RollUp x={x + 10} y={y + 40} card={card} />
        <CoverageBadge x={x} y={y} w={w} coverage={card.coverage} />
      </g>
      {!hidden &&
        card.measures.map((m, i) => (
          <MeasureChip
            key={m.id}
            m={m}
            x={x + 8}
            y={y + CARD_HDR + i * CHIP_STEP}
            w={w - 16}
            selected={m.id === selectedId}
            dim={!matchesSearch(m.label, searchTerm)}
            onSelect={onSelect}
          />
        ))}
    </g>
  )
}

interface SemanticGraphProps {
  nodes: SemanticGraphNode[]
  edges: SemanticGraphEdge[]
  selectedId?: string | null
  onSelectNode?: (node: SemanticGraphNode) => void
  label?: string
}

function SemanticGraphInner({ nodes, edges, selectedId, onSelectNode, label = "Semantic model" }: SemanticGraphProps) {
  const cards = useMemo(() => buildCards(nodes, edges), [nodes, edges])
  const tallestColumn = useMemo(() => {
    const perCol = [0, 0, 0, 0]
    for (const c of cards) perCol[Math.max(0, Math.min(3, c.col))] += 1
    return Math.max(...perCol, 0)
  }, [cards])

  const containerRef = useRef<HTMLDivElement | null>(null)
  const svgRef = useRef<SVGSVGElement | null>(null)
  const [viewportHeight, setViewportHeight] = useState<number | null>(null)
  const [expandAll, setExpandAll] = useState(false)
  const [search, setSearch] = useState("")
  const [hoverEdge, setHoverEdge] = useState<number | null>(null)
  const [view, setView] = useState({ scale: 1, tx: 0, ty: 0 })
  const [dragging, setDragging] = useState(false)
  const drag = useRef<{ x: number; y: number; tx: number; ty: number } | null>(null)

  // Threshold is derived from the measured viewport (§9); the SSR/first-paint
  // render uses the 12 fallback until the effect below measures the box.
  const threshold = collapseThreshold(viewportHeight)
  const collapsible = tallestColumn > threshold
  const collapsed = collapsible && !expandAll

  const { placed, width, height } = useMemo(() => layoutCards(cards, collapsed), [cards, collapsed])

  const highlight = useMemo(() => focusSet(cards, edges, selectedId), [cards, edges, selectedId])

  const clampScale = (s: number) => Math.min(2.5, Math.max(0.4, s))
  const zoomBy = (f: number) => setView((v) => ({ ...v, scale: clampScale(v.scale * f) }))

  // The real Fit control (§3): frame the measured content in the measured
  // viewport. Falls back to identity only when nothing has been measured yet.
  const fitToContent = () => {
    const rect = svgRef.current?.getBoundingClientRect()
    const viewW = rect?.width ?? width
    const viewH = rect?.height ?? Math.min(480, height)
    setView(computeFit(width, height, viewW, viewH))
  }

  // Measure the rendered box once mounted and on resize: this feeds both the
  // derived collapse threshold and the initial fit. Guarded for SSR/node env.
  useEffect(() => {
    const el = svgRef.current
    if (!el || typeof ResizeObserver === "undefined") return
    const measure = () => {
      const rect = el.getBoundingClientRect()
      if (rect.height > 0) setViewportHeight(rect.height)
    }
    measure()
    const ro = new ResizeObserver(measure)
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  // Frame the content once we have a measurement (initial fit, §3.1).
  useEffect(() => {
    if (viewportHeight != null) fitToContent()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [viewportHeight, width, height])

  const onWheel = (e: React.WheelEvent) => {
    e.preventDefault()
    zoomBy(e.deltaY < 0 ? 1.1 : 0.9)
  }
  const onPointerDown = (e: React.PointerEvent) => {
    drag.current = { x: e.clientX, y: e.clientY, tx: view.tx, ty: view.ty }
    setDragging(true)
    ;(e.target as Element).setPointerCapture?.(e.pointerId)
  }
  const onPointerMove = (e: React.PointerEvent) => {
    // Read the ref ONCE into a local: a pointerup between this guard and the
    // setView closure would otherwise null drag.current and crash the tab
    // (the SemanticGraph.tsx:219 non-null-assertion race). No `!` in handlers.
    const d = drag.current
    const delta = panDelta(d, e.clientX, e.clientY)
    if (!d || !delta) return
    setView((v) => ({ ...v, tx: d.tx + delta.dx, ty: d.ty + delta.dy }))
  }
  const onPointerUp = () => {
    drag.current = null
    setDragging(false)
  }

  const select = (n: SemanticGraphNode) => onSelectNode?.(n)
  const selectedCardDim = (cardId: string) => (highlight ? !highlight.has(cardId) : false)

  // A card matches the search if the card itself or any of its chips matches.
  const cardMatchesSearch = (card: GraphCard) =>
    matchesSearch(card.label, search) || card.measures.some((m) => matchesSearch(m.label, search))

  return (
    <div ref={containerRef} className="relative overflow-hidden rounded-lg border border-default bg-sunken">
      <div className="absolute left-2 top-2 z-10 flex items-center gap-2">
        <div className="flex items-center gap-1 rounded border border-default bg-surface px-1.5 py-1">
          <Search className="h-3 w-3 text-muted" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Find a table or measure…"
            aria-label="Find a table or measure"
            className="w-40 bg-transparent text-xs text-primary placeholder:text-muted focus:outline-none"
          />
        </div>
        {collapsible && (
          <button
            type="button"
            onClick={() => setExpandAll((v) => !v)}
            className="rounded border border-default bg-surface px-2 py-1 text-xs text-muted hover:text-secondary"
          >
            {expandAll ? "Collapse all" : "Expand all"}
          </button>
        )}
      </div>
      <div className="absolute right-2 top-2 z-10 flex flex-col gap-1">
        <button type="button" aria-label="Zoom in" onClick={() => zoomBy(1.2)} className="rounded border border-default bg-surface p-1 text-muted hover:text-secondary"><Plus className="h-3.5 w-3.5" /></button>
        <button type="button" aria-label="Zoom out" onClick={() => zoomBy(0.83)} className="rounded border border-default bg-surface p-1 text-muted hover:text-secondary"><Minus className="h-3.5 w-3.5" /></button>
        <button type="button" aria-label="Fit to view" onClick={fitToContent} className="rounded border border-default bg-surface p-1 text-muted hover:text-secondary"><Maximize2 className="h-3.5 w-3.5" /></button>
      </div>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${width} ${height}`}
        className="w-full touch-none select-none"
        style={{ maxHeight: 480, cursor: dragging ? "grabbing" : "grab" }}
        role="img"
        aria-label={label}
        onWheel={onWheel}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        onPointerLeave={onPointerUp}
      >
        <defs>
          <marker id="mv-arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
            <path d="M0,0 L6,3 L0,6 Z" className="fill-[var(--text-muted)]" />
          </marker>
        </defs>
        <g transform={`translate(${view.tx} ${view.ty}) scale(${view.scale})`}>
          {COL_HEADERS.map((h, i) => (
            <text key={h} x={COL_X[i] + COL_W[i] / 2} y={20} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="9" fontWeight="600">{h}</text>
          ))}
          {edges.map((edge, i) => {
            const fromCardId = placed.has(edge.from) ? edge.from : cards.find((c) => c.measures.some((m) => m.id === edge.from))?.id
            const toCardId = placed.has(edge.to) ? edge.to : cards.find((c) => c.measures.some((m) => m.id === edge.to))?.id
            if (!fromCardId || !toCardId || fromCardId === toCardId) return null
            const from = placed.get(fromCardId)
            const to = placed.get(toCardId)
            if (!from || !to) return null
            const active =
              hoverEdge === i ||
              fromCardId === selectedId ||
              toCardId === selectedId ||
              (highlight != null && highlight.has(fromCardId) && highlight.has(toCardId))
            return <EdgeView key={i} edge={edge} from={from.box} to={to.box} active={active} onHover={(on) => setHoverEdge(on ? i : null)} />
          })}
          {[...placed.values()].map((p) => (
            <CardView
              key={p.card.id}
              placed={p}
              collapsed={collapsed}
              selectedId={selectedId}
              dim={selectedCardDim(p.card.id) || (search.trim() !== "" && !cardMatchesSearch(p.card))}
              searchTerm={search}
              onSelect={select}
            />
          ))}
        </g>
      </svg>
    </div>
  )
}

// ── Error boundary (12c Part 1) ──────────────────────────────────────────────
// A visualization must never take the page down. Any render/interaction throw
// inside the graph is caught here and replaced with a recoverable card, instead
// of an uncaught error unmounting the whole tab (smoke finding 5).
export class GraphErrorBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  constructor(props: { children: ReactNode }) {
    super(props)
    this.state = { failed: false }
  }

  static getDerivedStateFromError(): { failed: boolean } {
    return { failed: true }
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error("SemanticGraph render failed", error, info)
  }

  handleRetry = () => this.setState({ failed: false })

  render() {
    if (this.state.failed) {
      return (
        <div
          role="alert"
          className="flex flex-col items-center justify-center gap-2 rounded-lg border border-default bg-sunken px-4 py-8 text-center"
        >
          <AlertTriangle className="h-5 w-5 text-[var(--color-danger)]" />
          <p className="text-sm text-secondary">The visualization failed to render.</p>
          <button
            type="button"
            onClick={this.handleRetry}
            className="mt-1 inline-flex items-center gap-1.5 rounded border border-default bg-surface px-2.5 py-1 text-xs text-secondary hover:text-primary"
          >
            <RefreshCw className="h-3.5 w-3.5" /> Refresh to retry
          </button>
        </div>
      )
    }
    return this.props.children
  }
}

export function SemanticGraph(props: SemanticGraphProps) {
  return (
    <GraphErrorBoundary>
      <SemanticGraphInner {...props} />
    </GraphErrorBoundary>
  )
}
