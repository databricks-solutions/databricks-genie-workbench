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
import { AlertTriangle, Maximize2, Minus, Plus, RefreshCw, RotateCcw, Search, ShieldCheck, Wrench } from "lucide-react"
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
// ── Density / spacing (12f: matched to the committed v7 contract frame 9e) ────
// The v7 frame is spacious: wide columns with generous inter-column gutters,
// tall typed table cards, a larger card header, and roomier chip rows. These
// constants replace the cramped 12c values so the shipped fit reads like the
// mockup. ROW_TOP and ROW_GAP are deliberately UNCHANGED — they are the collapse
// -threshold stride (§9), pinned by SemanticGraph.grouped.test.tsx, and are a
// derivation constant, not a visual one.
const COL_X = [24, 268, 512, 792]
const COL_W = [204, 208, 244, 256]
const COL_HEADERS = ["Source · facts", "Dimensions", "Metric views · measures", "Measure concepts"]
const ROW_TOP = 44
// ROW_GAP is the per-card stride the derived threshold solves against (§9); the
// grouped table/MV cards stack at ROW_TOP + N · ROW_GAP. (Collapse math only —
// not the visual VGAP; see the 12f note above.)
const ROW_GAP = 58
const VGAP = 26
const CARD_HDR = 50
const CHIP_STEP = 24
const CHIP_H = 19
const CARD_PAD_B = 10
const TABLE_H = 56
const CONCEPTS_ID = "__concepts__"

// ── Label hygiene (12d finding 1) ────────────────────────────────────────────
// The smoke run leaked INTERNAL TOKENS into measure-concept labels: canonical
// exprs (count(?n), sum(count(?n)) — the MV-D29 placeholder form, which is the
// identity shape, not a label) and a raw suggestion id (sug_f07l2262f800). A
// label is human-usable only when it is neither. A concept with no human-usable
// name is DROPPED from the card as a chip and counted ("+N unnamed"), never
// rendered as internals — MV-D29's expr/display separation applied to the graph.
const CANONICAL_PLACEHOLDER = /\?[a-z]/i // ?n, ?s … the MV-D29 placeholder tell
const SUGGESTION_ID = /^sug_/i

export function isDisplayableMeasureLabel(label: string | null | undefined): boolean {
  const t = (label ?? "").trim()
  if (!t) return false
  if (SUGGESTION_ID.test(t)) return false
  if (CANONICAL_PLACEHOLDER.test(t)) return false
  return true
}

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
  /** Displayable measures (human-usable labels) — rendered as chips. */
  measures: SemanticGraphNode[]
  /** Measures whose only label is an internal token (canonical_expr / sug_ id).
      Counted as "+N unnamed", never chipped (12d finding 1). Kept so the
      governance roll-up still reflects them (the count is real). */
  unnamedMeasures: SemanticGraphNode[]
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

  // Split a card's measures into displayable chips and counted-but-unnamed ones
  // (12d finding 1): a canonical_expr or sug_ id is not a label.
  const splitMeasures = (ms: SemanticGraphNode[]) => ({
    measures: ms.filter((m) => isDisplayableMeasureLabel(m.label)),
    unnamedMeasures: ms.filter((m) => !isDisplayableMeasureLabel(m.label)),
  })

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
        unnamedMeasures: [],
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
        ...splitMeasures(measuresByMv.get(n.id) ?? []),
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
      ...splitMeasures(standalone),
    })
  }
  return cards
}

// Card-level governance roll-up (§2 / §8): the traffic light survives grouping
// as per-card counts. Present rungs only, ladder order, so an empty card says
// nothing rather than inventing zeros.
export function rollup(card: GraphCard): { rung: MvGovernance; count: number }[] {
  const counts: Record<MvGovernance, number> = { governed: 0, curated: 0, ungoverned: 0 }
  // Count BOTH chipped and unnamed measures — dropping a measure's chip for
  // lacking a human label (12d finding 1) must not erase it from the governance
  // story; the roll-up count is the real total.
  for (const m of [...card.measures, ...card.unnamedMeasures]) {
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
  // An "+N unnamed" summary occupies one chip row when there are dropped
  // measures (12d finding 1), so a card that is all-unnamed still has body.
  const rows = card.measures.length + (card.unnamedMeasures.length > 0 ? 1 : 0)
  if (collapsed || rows === 0) return CARD_HDR
  return CARD_HDR + rows * CHIP_STEP + CARD_PAD_B
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

// ── Edge label glyphs (12d finding 3) ────────────────────────────────────────
// Relationship text truncated into ambiguity at rest ("many-to-o…"). At rest
// we show a compact glyph (N:1, 1:1) that never truncates; the full text rides
// on hover (the labels-on-demand rule). Case/format tolerant. Unknown values
// return null so an unexpected relationship renders no glyph rather than a wrong
// one.
const RELATIONSHIP_GLYPH: Record<string, string> = {
  "many-to-one": "N:1",
  "one-to-many": "1:N",
  "one-to-one": "1:1",
  "many-to-many": "N:N",
}

export function relationshipGlyph(relationship: string | null | undefined): string | null {
  if (!relationship) return null
  const key = relationship.trim().toLowerCase().replace(/[_\s]+/g, "-")
  return RELATIONSHIP_GLYPH[key] ?? null
}

// ── Fan-out port distribution (12d finding 4) ────────────────────────────────
// At the fact column many dimension→fact edges all landed on the fact card's
// single left-midpoint, so the fan-in overlapped into spaghetti. Distribute the
// attachment points along each card's used side: edges sharing a (card, side)
// are sorted by the OTHER endpoint's mid-y (to minimize crossings) and spread
// across the card height at (i+1)/(n+1). A lone edge lands at the midpoint, so
// the single-edge case is byte-identical to cardPorts. Pure function of the
// resolved edge boxes — testable without a DOM.
export interface RenderedEdge {
  index: number
  fromCardId: string
  toCardId: string
  fromBox: CardBox
  toBox: CardBox
}

const PORT_PAD = 8

function portY(box: CardBox, slot: number, count: number): number {
  const raw = box.y + (box.h * (slot + 1)) / (count + 1)
  const lo = box.y + PORT_PAD
  const hi = box.y + box.h - PORT_PAD
  return Math.max(lo, Math.min(hi, raw))
}

export function distributeEdgePorts(
  items: RenderedEdge[],
): Map<number, { src: { x: number; y: number }; dst: { x: number; y: number } }> {
  // side used by each endpoint, and the sort key (other endpoint's mid-y).
  interface Endpoint { index: number; box: CardBox; side: "left" | "right"; sortY: number; role: "src" | "dst" }
  const groups = new Map<string, Endpoint[]>()
  const push = (key: string, e: Endpoint) => {
    const list = groups.get(key) ?? []
    list.push(e)
    groups.set(key, list)
  }
  for (const it of items) {
    const leftToRight = it.fromBox.x <= it.toBox.x
    const fromSide = leftToRight ? "right" : "left"
    const toSide = leftToRight ? "left" : "right"
    const fromMidY = it.fromBox.y + it.fromBox.h / 2
    const toMidY = it.toBox.y + it.toBox.h / 2
    push(`${it.fromCardId}:${fromSide}`, { index: it.index, box: it.fromBox, side: fromSide, sortY: toMidY, role: "src" })
    push(`${it.toCardId}:${toSide}`, { index: it.index, box: it.toBox, side: toSide, sortY: fromMidY, role: "dst" })
  }
  const out = new Map<number, { src: { x: number; y: number }; dst: { x: number; y: number } }>()
  const ensure = (index: number) => {
    const cur = out.get(index) ?? { src: { x: 0, y: 0 }, dst: { x: 0, y: 0 } }
    out.set(index, cur)
    return cur
  }
  for (const list of groups.values()) {
    // Stable sort by the facing endpoint's y, then edge index for determinism.
    list.sort((a, b) => a.sortY - b.sortY || a.index - b.index)
    list.forEach((e, i) => {
      const x = e.side === "right" ? e.box.x + e.box.w : e.box.x
      const y = portY(e.box, i, list.length)
      const rec = ensure(e.index)
      rec[e.role] = { x, y }
    })
  }
  return out
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
  // Center vertically too (12f): a wide-and-short graph (the common shape) framed
  // at width leaves vertical slack; centering makes it read as intentional rather
  // than a blob pinned to the top. A tall graph scales by height, so the centered
  // ty naturally lands near `pad` — no top clipping.
  const ty = Math.max(pad, (viewH - contentH * scale) / 2)
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
    for (const m of [...c.measures, ...c.unnamedMeasures]) nodeToCard.set(m.id, c.id)
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

// ── Prompt 12e / MV-D33: the metric view as a semantic model ─────────────────
// A metric view is not a leaf — it sources a fact and joins dimensions. The
// server emits a `uses` edge (MV → each member table) for every MV whose YAML
// parsed; these are the at-rest arrows AND the member set the select-time
// boundary wraps. An MV with definition_available === false emitted none
// (unreadable is unproven), so everything below is empty for it and it draws no
// arrows. All pure functions of (nodes, edges, placed, selection, offsets).

// The member tables an MV uses — the `uses`-edge targets. Deterministic order.
export function memberTableIds(edges: SemanticGraphEdge[], mvId: string): string[] {
  const out: string[] = []
  const seen = new Set<string>()
  for (const e of edges) {
    if (e.kind === "uses" && e.from === mvId && !seen.has(e.to)) {
      seen.add(e.to)
      out.push(e.to)
    }
  }
  return out
}

// Tables in no metric view (no incoming `uses` edge) — the unmodeled region the
// governance gap is made visible through (MV-D33). Only meaningful when there IS
// membership to contrast against: with NO `uses` edges anywhere (no MV read /
// no MV at all) membership is unknown, so we claim nothing rather than tagging
// every table "no metric view". Pure.
export function unmodeledTableIds(nodes: SemanticGraphNode[], edges: SemanticGraphEdge[]): Set<string> {
  const used = new Set<string>()
  for (const e of edges) if (e.kind === "uses") used.add(e.to)
  const out = new Set<string>()
  if (used.size === 0) return out
  for (const n of nodes) if (n.kind === "table" && !used.has(n.id)) out.add(n.id)
  return out
}

const HULL_PAD = 12

function unionBox(boxes: CardBox[]): CardBox {
  const x = Math.min(...boxes.map((b) => b.x))
  const y = Math.min(...boxes.map((b) => b.y))
  const right = Math.max(...boxes.map((b) => b.x + b.w))
  const bottom = Math.max(...boxes.map((b) => b.y + b.h))
  return { x, y, w: right - x, h: bottom - y }
}

function boxesOverlap(a: CardBox, b: CardBox): boolean {
  return a.x < b.x + b.w && a.x + a.w > b.x && a.y < b.y + b.h && a.y + a.h > b.y
}

// The select-time boundary around an MV's member tables. HULL IS THE NORM, the
// clean RECT is the lucky case (reviewer watch-item): a single enclosing
// rectangle is only honest when it contains ONLY member boxes — a dim shared by
// several MVs cannot be home-placed adjacent to all their member sets, so a
// bounding rect routinely swallows a foreign table. When it would, we fall back
// to a member-hull outline (each member box outlined individually), which never
// encloses a foreigner. This is a placement/geometry rule, never a runtime
// solver. Pure function of the placed boxes.
export function memberBoundary(
  memberBoxes: CardBox[],
  otherBoxes: CardBox[],
): { kind: "rect" | "hull"; rects: CardBox[] } {
  if (memberBoxes.length === 0) return { kind: "hull", rects: [] }
  const bounding = unionBox(memberBoxes)
  const padded = { x: bounding.x - HULL_PAD, y: bounding.y - HULL_PAD, w: bounding.w + 2 * HULL_PAD, h: bounding.h + 2 * HULL_PAD }
  const swallowsForeigner = otherBoxes.some((o) => boxesOverlap(padded, o))
  if (!swallowsForeigner) return { kind: "rect", rects: [padded] }
  // Hull: outline each member box on its own — no foreign table inside.
  return {
    kind: "hull",
    rects: memberBoxes.map((b) => ({ x: b.x - HULL_PAD / 2, y: b.y - HULL_PAD / 2, w: b.w + HULL_PAD, h: b.h + HULL_PAD })),
  }
}

// Impact (downstream blast radius, §4): what breaks if the selected TABLE
// changes — the MVs that source it (`uses`, reversed: table → its MVs) and the
// measures those MVs own (`membership`, reversed: MV → its measures). Lineage
// (focusSet) is the both-direction neighborhood; impact is one-directional and
// transitive. A selected MV/measure anchors on its own card. Pure.
export function impactSet(
  cards: GraphCard[],
  edges: SemanticGraphEdge[],
  selectedId: string | null | undefined,
): Set<string> | null {
  if (!selectedId) return null
  const nodeToCard = new Map<string, string>()
  for (const c of cards) {
    if (c.node) nodeToCard.set(c.id, c.id)
    for (const m of [...c.measures, ...c.unnamedMeasures]) nodeToCard.set(m.id, c.id)
  }
  const anchor = nodeToCard.get(selectedId)
  if (!anchor) return null
  // Downstream adjacency: uses (table depends-on ← MV) means MV is downstream of
  // its tables, so traverse table → MV; membership (measure → MV) means the
  // measure is downstream of its MV, so traverse MV → measure.
  const downstream = new Map<string, string[]>()
  const addEdge = (from: string, to: string) => {
    const list = downstream.get(from) ?? []
    list.push(to)
    downstream.set(from, list)
  }
  for (const e of edges) {
    const from = nodeToCard.get(e.from)
    const to = nodeToCard.get(e.to)
    if (from === undefined || to === undefined || from === to) continue
    if (e.kind === "uses") addEdge(to, from) // table → MV
    if (e.kind === "membership") addEdge(to, from) // MV → measure
  }
  const keep = new Set<string>([anchor])
  const stack = [anchor]
  while (stack.length) {
    const cur = stack.pop()!
    for (const next of downstream.get(cur) ?? []) {
      if (!keep.has(next)) {
        keep.add(next)
        stack.push(next)
      }
    }
  }
  return keep
}

// Session-only drag (§7): a card's offset is added to its placed box. Not
// persisted, cleared by "Reset layout", never part of the diff seed. Pure.
export function applyDragOffset(box: CardBox, offset: { dx: number; dy: number } | undefined): CardBox {
  if (!offset) return box
  return { ...box, x: box.x + offset.dx, y: box.y + offset.dy }
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
  src,
  dst,
  active,
  onHover,
}: {
  edge: SemanticGraphEdge
  // Distributed endpoints (12d finding 4): each edge attaches at its own slot on
  // the card's facing side, so a fan-in does not collapse onto one midpoint.
  src: { x: number; y: number }
  dst: { x: number; y: number }
  active: boolean
  onHover: (on: boolean) => void
}) {
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

  // uses (12e / MV-D33): the proven at-rest arrow from a metric view to a table
  // it sources. Subtle by default (it is at-rest scaffolding, not the join
  // story); brightens when its MV or table is selected/focused (active).
  if (edge.kind === "uses") {
    return (
      <path
        d={path}
        fill="none"
        stroke="var(--color-accent)"
        strokeWidth={active ? 1.75 : 1}
        strokeDasharray="1 4"
        strokeLinecap="round"
        opacity={active ? 0.9 : 0.4}
        markerEnd="url(#mv-uses-arrow)"
      />
    )
  }

  // join — declutter: a compact relationship GLYPH (12d finding 3) + SCD2 at
  // rest so nothing truncates into ambiguity; the full ON predicate and the full
  // relationship text ride hover / an endpoint selection. A relationship with no
  // known glyph is omitted at rest rather than shown truncated. Prompt 12b
  // coverage weight rides "×N".
  const weightLabel = typeof edge.weight === "number" && edge.weight > 0 ? `×${edge.weight}` : null
  const glyph = relationshipGlyph(edge.relationship)
  const restLabel = [glyph, edge.scd2 ? "SCD2" : null, weightLabel].filter(Boolean).join(" · ")
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
  unmodeled,
  draggable,
  onSelect,
  onCardPointerDown,
}: {
  placed: PlacedCard
  collapsed: boolean
  selectedId: string | null | undefined
  dim: boolean
  searchTerm: string
  unmodeled: boolean
  draggable: boolean
  onSelect: (n: SemanticGraphNode) => void
  onCardPointerDown: (cardId: string, e: React.PointerEvent) => void
}) {
  const { card, box } = placed
  const { x, y, w, h } = box
  const selfSelected = card.node != null && card.id === selectedId
  const stroke = selfSelected ? "var(--color-accent)" : undefined
  const selWidth = selfSelected ? 2 : 1.5
  const opacity = dim ? 0.3 : 1
  // A selected card is draggable (§7) — spread the canvas to create room. The
  // grab cursor advertises it; the actual offset is session-only.
  const pointerDown = (e: React.PointerEvent) => onCardPointerDown(card.id, e)
  const dragCursor = draggable ? "grab" : undefined

  if (card.kind === "table") {
    const clickable = card.node
    // 12f: typed card — a FACT (source column 0) vs DIM (joined column 1)
    // caption above the identifier, matching the v7 contract frame. Left-aligned
    // with the label so the card reads like the mockup's fact/dim cards.
    const typeCaption = card.col === 0 ? "FACT" : "DIM"
    return (
      <g opacity={opacity} onClick={() => clickable && onSelect(clickable)} onPointerDown={pointerDown} style={{ cursor: dragCursor ?? (clickable ? "pointer" : "default") }}>
        <title>{unmodeled ? `${card.label} — in no metric view` : card.label}</title>
        <rect x={x} y={y} width={w} height={h} rx="8" fill="var(--bg-surface)" stroke={stroke ?? "var(--border-color-strong)"} strokeWidth={selWidth} strokeDasharray={card.coverage === 0 ? "4 3" : undefined} />
        <text x={x + 14} y={y + 20} className="fill-[var(--text-muted)]" fontSize="8.5" fontWeight="700" letterSpacing="0.06em">{typeCaption}</text>
        <text x={x + 14} y={y + 39} className="fill-[var(--text-primary)]" fontSize="12.5" fontWeight="600" fontFamily="monospace">{abbreviate(card.label, 20)}</text>
        {/* Prompt 12e / MV-D33: the unmodeled region — a table in no metric view.
            The governance gap made visible, in words not just hue. */}
        {unmodeled && (
          <text x={x + w - 12} y={y + 20} textAnchor="end" className="fill-[var(--text-muted)]" fontSize="7.5" fontStyle="italic">no metric view</text>
        )}
        <CoverageBadge x={x} y={y} w={w} coverage={card.coverage} />
      </g>
    )
  }

  // metric_view / concepts card: header (title + subtitle + roll-up), then chips
  // when expanded. Collapsed keeps the roll-up (governance story survives, §8)
  // and drops the chips.
  const isMv = card.kind === "metric_view"
  // The concept count is the TOTAL (chipped + unnamed) so the subtitle never
  // understates the card by the measures we declined to chip (12d finding 1).
  const conceptCount = card.measures.length + card.unnamedMeasures.length
  // Prompt 12e / MV-D33: an MV whose YAML could not be read renders "definition
  // unavailable" (and drew no arrows) — unreadable is unproven.
  const defUnavailable = isMv && card.node?.definition_available === false
  // 12f: the v7 contract puts a "N measures · <governance>" chip on each box.
  // The measures-count leads the subtitle; the colored governance roll-up below
  // carries the rung breakdown, so together they read as the frame's chip.
  const subtitle = isMv
    ? card.proposed
      ? "proposed metric view"
      : defUnavailable
        ? "definition unavailable"
        : `${conceptCount} measure${conceptCount === 1 ? "" : "s"} · metric view`
    : `${conceptCount} concept${conceptCount === 1 ? "" : "s"}`
  const clickableHeader = card.node
  const hidden = collapsed
  return (
    <g opacity={opacity}>
      <g onClick={() => clickableHeader && onSelect(clickableHeader)} onPointerDown={pointerDown} style={{ cursor: dragCursor ?? (clickableHeader ? "pointer" : "default") }}>
        <title>{card.proposed ? `${card.label} — proposed metric view` : defUnavailable ? `${card.label} — definition unavailable` : card.label}</title>
        <rect x={x} y={y} width={w} height={h} rx="10"
          fill={isMv ? "var(--color-accent)" : "var(--bg-surface)"} opacity={isMv ? (card.proposed ? 0.1 : 0.15) : 1}
          stroke={stroke ?? (isMv ? "var(--color-accent)" : "var(--border-color-strong)")} strokeWidth={selWidth}
          strokeDasharray={card.proposed ? "5 3" : undefined} />
        <text x={x + 12} y={y + 20} className="fill-[var(--text-primary)]" fontSize="12" fontWeight="700">{abbreviate(card.label, 22)}</text>
        <text x={x + 12} y={y + 34} className="fill-[var(--text-muted)]" fontSize="8.5">{subtitle}{hidden ? " · collapsed" : ""}</text>
        <RollUp x={x + 12} y={y + 45} card={card} />
        <CoverageBadge x={x} y={y} w={w} coverage={card.coverage} />
      </g>
      {!hidden && (
        <>
          {card.measures.map((m, i) => (
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
          {/* 12d finding 1: measures with no human-usable name are summarized,
              never rendered as canonical_expr / sug_ internals. */}
          {card.unnamedMeasures.length > 0 && (
            <text
              x={x + 8}
              y={y + CARD_HDR + card.measures.length * CHIP_STEP + 12}
              className="fill-[var(--text-muted)]"
              fontSize="9"
              fontStyle="italic"
            >
              +{card.unnamedMeasures.length} unnamed
            </text>
          )}
        </>
      )}
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
  // 12f: the measured viewport WIDTH too. The viewBox tracks the measured pixel
  // box (1 unit = 1px) so computeFit's pixel math is self-consistent — the old
  // content-sized viewBox double-scaled against the inner transform and produced
  // the tiny-blob-in-empty-panel defect (third-look finding 1).
  const [viewportWidth, setViewportWidth] = useState<number | null>(null)
  const [expandAll, setExpandAll] = useState(false)
  const [search, setSearch] = useState("")
  const [hoverEdge, setHoverEdge] = useState<number | null>(null)
  const [view, setView] = useState({ scale: 1, tx: 0, ty: 0 })
  const [dragging, setDragging] = useState(false)
  const drag = useRef<{ x: number; y: number; tx: number; ty: number } | null>(null)
  // Prompt 12e / MV-D33 interaction state: focus mode (Lineage = both-direction
  // 1-hop neighborhood; Impact = downstream blast radius) and session-only card
  // drag offsets (spread the canvas; not persisted, cleared by Reset layout).
  const [mode, setMode] = useState<"lineage" | "impact">("lineage")
  const [offsets, setOffsets] = useState<Record<string, { dx: number; dy: number }>>({})
  const cardDrag = useRef<{ id: string; x: number; y: number; dx0: number; dy0: number; moved: boolean } | null>(null)

  // Threshold is derived from the measured viewport (§9); the SSR/first-paint
  // render uses the 12 fallback until the effect below measures the box.
  const threshold = collapseThreshold(viewportHeight)
  const collapsible = tallestColumn > threshold
  const collapsed = collapsible && !expandAll

  const { placed: placedBase, width, height } = useMemo(() => layoutCards(cards, collapsed), [cards, collapsed])

  // Session-only drag offsets (§7) applied on top of the deterministic home
  // layout. With no offsets this is byte-identical to the layout, so the home
  // layout and the diff seed are untouched — drag is honest interaction state.
  const placed = useMemo(() => {
    if (Object.keys(offsets).length === 0) return placedBase
    const m = new Map<string, PlacedCard>()
    for (const [id, pc] of placedBase) m.set(id, { card: pc.card, box: applyDragOffset(pc.box, offsets[id]) })
    return m
  }, [placedBase, offsets])

  // Focus set depends on the mode: Lineage (both directions) vs Impact
  // (downstream only). Both are pure functions of the selection.
  const highlight = useMemo(
    () => (mode === "impact" ? impactSet(cards, edges, selectedId) : focusSet(cards, edges, selectedId)),
    [mode, cards, edges, selectedId],
  )

  // Tables in no metric view — the unmodeled region cue (MV-D33).
  const unmodeled = useMemo(() => unmodeledTableIds(nodes, edges), [nodes, edges])

  // The select-time boundary: when a metric view is selected, wrap the tables it
  // uses (its `uses`-edge members). Hull is the norm, rect the lucky case.
  const boundary = useMemo(() => {
    if (!selectedId) return null
    const sel = cards.find((c) => c.id === selectedId && c.kind === "metric_view")
    if (!sel) return null
    const memberIds = memberTableIds(edges, selectedId)
    const memberBoxes = memberIds.map((id) => placed.get(id)?.box).filter((b): b is CardBox => !!b)
    if (memberBoxes.length === 0) return null
    const memberSet = new Set(memberIds)
    const others = [...placed.values()].filter((p) => p.card.id !== selectedId && !memberSet.has(p.card.id)).map((p) => p.box)
    return { ...memberBoundary(memberBoxes, others), mvLabel: sel.label }
  }, [selectedId, cards, edges, placed])

  // Resolve each edge's endpoint cards (a measure endpoint anchors on its owning
  // card), then distribute the fan-out across each card's side (12d finding 4).
  const cardIdOfNode = useMemo(() => {
    const m = new Map<string, string>()
    for (const c of cards) {
      if (placed.has(c.id)) m.set(c.id, c.id)
      for (const mm of [...c.measures, ...c.unnamedMeasures]) m.set(mm.id, c.id)
    }
    return m
  }, [cards, placed])

  const renderedEdges = useMemo(() => {
    const items: (RenderedEdge & { edge: SemanticGraphEdge })[] = []
    edges.forEach((edge, index) => {
      const fromCardId = cardIdOfNode.get(edge.from)
      const toCardId = cardIdOfNode.get(edge.to)
      if (!fromCardId || !toCardId || fromCardId === toCardId) return
      const from = placed.get(fromCardId)
      const to = placed.get(toCardId)
      if (!from || !to) return
      items.push({ index, edge, fromCardId, toCardId, fromBox: from.box, toBox: to.box })
    })
    return items
  }, [edges, cardIdOfNode, placed])

  const edgePorts = useMemo(() => distributeEdgePorts(renderedEdges), [renderedEdges])

  const clampScale = (s: number) => Math.min(2.5, Math.max(0.4, s))
  const zoomBy = (f: number) => setView((v) => ({ ...v, scale: clampScale(v.scale * f) }))

  // The real Fit control (§3): frame the measured content in the measured
  // viewport. Falls back to identity only when nothing has been measured yet.
  const fitToContent = () => {
    const rect = svgRef.current?.getBoundingClientRect()
    const viewW = rect?.width ?? viewportWidth ?? width
    const viewH = rect?.height ?? viewportHeight ?? height
    setView(computeFit(width, height, viewW, viewH))
  }

  // Measure the rendered box once mounted and on resize: this feeds the derived
  // collapse threshold, the initial fit, AND the viewBox (12f). Guarded for
  // SSR/node env.
  useEffect(() => {
    const el = svgRef.current
    if (!el || typeof ResizeObserver === "undefined") return
    const measure = () => {
      const rect = el.getBoundingClientRect()
      if (rect.height > 0) setViewportHeight(rect.height)
      if (rect.width > 0) setViewportWidth(rect.width)
    }
    measure()
    const ro = new ResizeObserver(measure)
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  // Frame the content once we have a measurement (initial fit, §3.1). Depends on
  // both measured dimensions so a resize re-fits and the ship-time fit is the
  // fit the mockup shows.
  useEffect(() => {
    if (viewportHeight != null && viewportWidth != null) fitToContent()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [viewportHeight, viewportWidth, width, height])

  const onWheel = (e: React.WheelEvent) => {
    e.preventDefault()
    zoomBy(e.deltaY < 0 ? 1.1 : 0.9)
  }
  const onPointerDown = (e: React.PointerEvent) => {
    // Empty-background press starts a canvas pan (card presses are handled by
    // onCardPointerDown, which stops propagation).
    drag.current = { x: e.clientX, y: e.clientY, tx: view.tx, ty: view.ty }
    setDragging(true)
    ;(e.target as Element).setPointerCapture?.(e.pointerId)
  }
  // A selected card's press begins a card drag (§7) instead of a pan. Deltas are
  // divided by the zoom so the card tracks the pointer at any scale.
  const onCardPointerDown = (cardId: string, e: React.PointerEvent) => {
    if (cardId !== selectedId) return // only the selected box is draggable
    e.stopPropagation()
    const cur = offsets[cardId] ?? { dx: 0, dy: 0 }
    cardDrag.current = { id: cardId, x: e.clientX, y: e.clientY, dx0: cur.dx, dy0: cur.dy, moved: false }
    setDragging(true)
    ;(e.target as Element).setPointerCapture?.(e.pointerId)
  }
  const onPointerMove = (e: React.PointerEvent) => {
    // Card drag takes precedence over canvas pan. Read each ref ONCE into a local
    // so a pointerup between the guard and the setState closure cannot null it
    // mid-flight (the SemanticGraph.tsx pan null-race — no `!` in handlers).
    const cd = cardDrag.current
    if (cd) {
      const delta = panDelta({ x: cd.x, y: cd.y }, e.clientX, e.clientY)
      if (!delta) return
      const s = view.scale || 1
      cardDrag.current = { ...cd, moved: true }
      setOffsets((o) => ({ ...o, [cd.id]: { dx: cd.dx0 + delta.dx / s, dy: cd.dy0 + delta.dy / s } }))
      return
    }
    const d = drag.current
    const delta = panDelta(d, e.clientX, e.clientY)
    if (!d || !delta) return
    setView((v) => ({ ...v, tx: d.tx + delta.dx, ty: d.ty + delta.dy }))
  }
  const onPointerUp = () => {
    drag.current = null
    cardDrag.current = null
    setDragging(false)
  }
  const resetLayout = () => setOffsets({})

  const select = (n: SemanticGraphNode) => onSelectNode?.(n)
  const selectedCardDim = (cardId: string) => (highlight ? !highlight.has(cardId) : false)

  // A card matches the search if the card itself or any of its chips matches.
  const cardMatchesSearch = (card: GraphCard) =>
    matchesSearch(card.label, search) || card.measures.some((m) => matchesSearch(m.label, search))

  // 12f: the shipped zoom readout (the v7 zoom % indicator) reflects the live
  // scale — after the initial computeFit it shows the fit that ships.
  const zoomPct = Math.round(view.scale * 100)
  // viewBox tracks the measured pixel viewport (1 unit = 1px) once measured, so
  // the inner computeFit transform is the ONLY scaling; before measurement it
  // falls back to the content box so SSR/first-paint renders the whole graph.
  const vbW = viewportWidth ?? width
  const vbH = viewportHeight ?? height

  return (
    <div ref={containerRef} className="space-y-2">
      {/* Visible control row (v7 contract 9e): search · Lineage/Impact · Fit ·
          − % + · Reset. Lifted out of the canvas corners (the deployed overlay
          that the mockup never had) into a legible row above the canvas. */}
      <div className="flex flex-wrap items-center gap-2">
        <div className="flex items-center gap-1.5 rounded-md border border-default bg-surface px-2 py-1">
          <Search className="h-3.5 w-3.5 text-muted" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Find a table or measure…"
            aria-label="Find a table or measure"
            className="w-44 bg-transparent text-xs text-primary placeholder:text-muted focus:outline-none"
          />
        </div>
        {/* Focus mode (§4 / MV-D33): Lineage traces both directions; Impact shows
            the downstream blast radius of a selected table. */}
        <div className="inline-flex items-center overflow-hidden rounded-md border border-default text-xs" role="group" aria-label="Focus mode">
          <button type="button" aria-pressed={mode === "lineage"} onClick={() => setMode("lineage")} className={`px-2.5 py-1 ${mode === "lineage" ? "bg-[var(--color-accent)] font-medium text-white" : "text-secondary hover:text-primary"}`}>Lineage</button>
          <button type="button" aria-pressed={mode === "impact"} onClick={() => setMode("impact")} className={`px-2.5 py-1 ${mode === "impact" ? "bg-[var(--color-accent)] font-medium text-white" : "text-secondary hover:text-primary"}`}>Impact</button>
        </div>
        <button type="button" onClick={fitToContent} className="inline-flex items-center gap-1 rounded-md border border-default bg-surface px-2 py-1 text-xs text-secondary hover:text-primary" title="Fit the model to the view">
          <Maximize2 className="h-3.5 w-3.5" /> Fit
        </button>
        <div className="inline-flex items-center gap-1 rounded-md border border-default bg-surface px-1 py-1 text-xs text-secondary">
          <button type="button" aria-label="Zoom out" onClick={() => zoomBy(0.83)} className="rounded p-0.5 hover:text-primary"><Minus className="h-3.5 w-3.5" /></button>
          <span className="min-w-[3ch] px-1 text-center tabular-nums" aria-label="Zoom level">{zoomPct}%</span>
          <button type="button" aria-label="Zoom in" onClick={() => zoomBy(1.2)} className="rounded p-0.5 hover:text-primary"><Plus className="h-3.5 w-3.5" /></button>
        </div>
        {collapsible && (
          <button
            type="button"
            onClick={() => setExpandAll((v) => !v)}
            className="rounded-md border border-default bg-surface px-2 py-1 text-xs text-muted hover:text-secondary"
          >
            {expandAll ? "Collapse all" : "Expand all"}
          </button>
        )}
        {Object.keys(offsets).length > 0 && (
          <button type="button" onClick={resetLayout} className="inline-flex items-center gap-1 rounded-md border border-default bg-surface px-2 py-1 text-xs text-muted hover:text-secondary" title="Restore the default layout (clears your dragging)">
            <RotateCcw className="h-3.5 w-3.5" /> Reset
          </button>
        )}
      </div>

      <div className="relative overflow-hidden rounded-lg border border-default bg-sunken" style={{ height: 420 }}>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${vbW} ${vbH}`}
        preserveAspectRatio="xMidYMid meet"
        className="h-full w-full touch-none select-none"
        style={{ cursor: dragging ? "grabbing" : "grab" }}
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
          <marker id="mv-uses-arrow" markerWidth="7" markerHeight="7" refX="5" refY="2.5" orient="auto">
            <path d="M0,0 L5,2.5 L0,5 Z" className="fill-[var(--color-accent)]" />
          </marker>
        </defs>
        <g transform={`translate(${view.tx} ${view.ty}) scale(${view.scale})`}>
          {COL_HEADERS.map((h, i) => (
            <text key={h} x={COL_X[i] + COL_W[i] / 2} y={20} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="9" fontWeight="600">{h}</text>
          ))}
          {/* Select-time MV boundary (MV-D33): wraps the tables the selected MV
              uses. Hull (per-member outlines) is the norm; a single rect is the
              lucky case where no foreign table sits inside the bounding box. */}
          {boundary && (
            <g aria-label={`tables used by ${boundary.mvLabel}`} pointerEvents="none">
              {boundary.rects.map((r, i) => (
                <rect key={i} x={r.x} y={r.y} width={r.w} height={r.h} rx="10"
                  fill="var(--color-accent)" fillOpacity="0.06"
                  stroke="var(--color-accent)" strokeWidth="1.5" strokeDasharray="6 4" />
              ))}
              {boundary.rects[0] && (
                <text x={boundary.rects[0].x + 10} y={boundary.rects[0].y - 5} className="fill-[var(--color-accent)]" fontSize="9" fontWeight="700">
                  tables used by {abbreviate(boundary.mvLabel, 22)}
                </text>
              )}
            </g>
          )}
          {renderedEdges.map(({ index, edge, fromCardId, toCardId }) => {
            const ports = edgePorts.get(index)
            if (!ports) return null
            const active =
              hoverEdge === index ||
              fromCardId === selectedId ||
              toCardId === selectedId ||
              (highlight != null && highlight.has(fromCardId) && highlight.has(toCardId))
            return (
              <EdgeView
                key={index}
                edge={edge}
                src={ports.src}
                dst={ports.dst}
                active={active}
                onHover={(on) => setHoverEdge(on ? index : null)}
              />
            )
          })}
          {[...placed.values()].map((p) => (
            <CardView
              key={p.card.id}
              placed={p}
              collapsed={collapsed}
              selectedId={selectedId}
              dim={selectedCardDim(p.card.id) || (search.trim() !== "" && !cardMatchesSearch(p.card))}
              searchTerm={search}
              unmodeled={p.card.kind === "table" && unmodeled.has(p.card.id)}
              draggable={p.card.node != null && p.card.id === selectedId}
              onSelect={select}
              onCardPointerDown={onCardPointerDown}
            />
          ))}
        </g>
      </svg>
      </div>

      {/* Legend (v7 contract 9e) — the vocabulary the canvas speaks, so a first
          reading needs no caption hunting. */}
      <Legend />

      {/* Footer tip (v7 contract 9e) — the two on-demand affordances the at-rest
          canvas can't advertise: the select-time boundary and the spread-drag. */}
      <p className="text-xs text-muted">
        Tip: click a metric view to wrap the tables in its definition · drag any box to spread the canvas · Reset restores the layout.
      </p>
    </div>
  )
}

// The canvas legend (v7 contract 9e). Static — a pure vocabulary key; kept a
// component so both the graph and its tests read one source of truth.
function Legend() {
  const items: { swatch: ReactNode; label: string }[] = [
    { swatch: <span className="inline-block h-2.5 w-3.5 rounded-sm border border-[var(--border-color-strong)] bg-[var(--bg-surface)]" />, label: "table (fact / dim)" },
    { swatch: <span className="inline-block h-2.5 w-3.5 rounded-sm border border-[var(--color-accent)] bg-[color-mix(in_srgb,var(--color-accent)_16%,transparent)]" />, label: "metric view (measures)" },
    { swatch: <span className="inline-block h-2.5 w-2.5 rounded-full bg-[var(--color-success)]" />, label: "governed / in an MV" },
    { swatch: <span className="inline-block h-2.5 w-2.5 rounded-full bg-[var(--color-danger)]" />, label: "unmodeled (no MV)" },
    { swatch: <span className="text-[var(--color-accent)]">→</span>, label: "declared join (N:1)" },
  ]
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted" aria-label="Legend">
      {items.map((it, i) => (
        <span key={i} className="inline-flex items-center gap-1.5">
          {it.swatch}
          {it.label}
        </span>
      ))}
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
