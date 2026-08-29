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
import { AlertTriangle, Crosshair, Maximize2, Minus, Plus, RefreshCw, RotateCcw, Search, ShieldCheck, Wrench } from "lucide-react"
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
// The measure columns carry the longest strings on the canvas (fully-qualified
// measure names), so 12f gives them the width the v7 frame gives them rather
// than abbreviating harder than the mockup does.
const COL_W = [204, 208, 272, 272]
// Round-5: honest column headers. Cols 0/1 describe the JOIN ROLE the layout
// actually encodes (source side vs joined side); the per-card caption asserts
// FACT/DIM only when the data proves it (node.role). Col 3 is the dedicated
// Space-config measures column the reviewer asked for.
const COL_HEADERS = ["Source tables", "Joined tables", "Metric views · measures", "Space config · measures"]
const ROW_TOP = 44
// ROW_GAP is the per-card stride the derived threshold solves against (§9); the
// grouped table/MV cards stack at ROW_TOP + N · ROW_GAP. (Collapse math only —
// not the visual VGAP; see the 12f note above.)
const ROW_GAP = 58
// Round-6 (reviewer: "columns and tables are too close for arrows to be
// legible"): more vertical air between stacked cards so a join's curve has room
// to separate from its neighbours. VGAP is visual-only (the collapse threshold
// keys off ROW_GAP), so this does not move the derivation.
const VGAP = 34
const CARD_HDR = 58
const CHIP_STEP = 24
const CHIP_H = 19
const CARD_PAD_B = 10
const TABLE_H = 56
const CONCEPTS_ID = "__concepts__"
// Round-5: the Space-config (loose measures) box gets its OWN column to the RIGHT
// of the metric-view column — a dedicated "Space config · measures" column (col 3)
// — so the loose measures sit beside the MV boxes, not buried below them. (Round 3
// put it in a full-width panel below the canvas; round 4 stacked it under the MVs
// in col 2 where it read as "below"; round 5 gives it its own right column, which
// is what the reviewer asked for.)
const CONCEPTS_COL = 3
const CONCEPTS_ROW = 1_000_000

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
      col: CONCEPTS_COL,
      row: CONCEPTS_ROW,
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
//
// 12f: EMPTY COLUMNS ARE COMPACTED AWAY. The old formula reserved every one of
// the four columns and sized width to the LAST one, so a space whose measures
// all belong to metric views (no standalone concepts) reserved ~29% dead width
// on the right — which forced computeFit to shrink the whole canvas by that much
// and is the single largest reason the shipped canvas read smaller than the
// mockup. Occupied columns now take sequential positions and width follows the
// real rightmost edge. `columns` is returned so the headers follow the compacted
// positions rather than the nominal ones.
// Round-6 (reviewer): widen the inter-column gutter so join curves have a wider
// channel to bend through — the reviewer's own diagnosis was that the columns sat
// too close for the arrows to read. Pure layout constant (used by layoutCards and
// the width calc); the fit control rescales to keep the whole canvas framed.
const COL_GUTTER = 68
// The visual top of the card stack, kept SEPARATE from ROW_TOP (which is purely
// the collapse-threshold derivation constant, §9, and stays put). The band above
// the cards has to hold the column captions AND the select-time boundary caption
// without either printing over a card border, which 44 could not.
const CARD_TOP = 66
// The canvas is at most this wide-to-tall (a 780×520-ish box). Used as the
// pre-measurement framing bound so the SSR/first-paint viewBox matches the shape
// the measured canvas will actually have.
const CANVAS_MAX_ASPECT = 1.5

export function layoutCards(
  cards: GraphCard[],
  collapsed: boolean,
): {
  placed: Map<string, PlacedCard>
  width: number
  height: number
  columns: { col: number; x: number; w: number }[]
} {
  const placed = new Map<string, PlacedCard>()
  const byCol: GraphCard[][] = [[], [], [], []]
  for (const c of cards) byCol[Math.max(0, Math.min(3, c.col))].push(c)

  // Sequential x for occupied columns only (keeps product-semantic order).
  const columns: { col: number; x: number; w: number }[] = []
  let cursor = COL_X[0]
  byCol.forEach((column, col) => {
    if (column.length === 0) return
    const w = COL_W[col]
    columns.push({ col, x: cursor, w })
    cursor += w + COL_GUTTER
  })
  const xOf = new Map(columns.map((c) => [c.col, c]))

  let maxBottom = CARD_TOP
  byCol.forEach((column, col) => {
    const slot = xOf.get(col)
    if (!slot) return
    column.sort((a, b) => a.row - b.row || a.label.localeCompare(b.label))
    let y = CARD_TOP
    for (const c of column) {
      const h = cardHeight(c, collapsed)
      placed.set(c.id, { card: c, box: { x: slot.x, y, w: slot.w, h } })
      y += h + VGAP
      maxBottom = Math.max(maxBottom, y)
    }
  })
  const rightEdge = columns.length > 0 ? cursor - COL_GUTTER : COL_X[0]
  const width = rightEdge + 24
  const height = Math.max(CARD_TOP + ROW_GAP, maxBottom - VGAP + 16)
  return { placed, width, height, columns }
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
  /** Edge kind, so bundling can count JOINS and ignore the label-less `uses`
      scaffolding that shares the same card side. Optional for port
      distribution, which spreads every kind alike. */
  kind?: SemanticGraphEdge["kind"]
}

// Round-7: one relationship = one arrow. A table pair can carry several
// `join` edges — reciprocal declarations, or an SCD2 current-row variant — which
// the backend emits per join_spec, so a single relationship reads as two
// near-parallel arrows. Collapse them to ONE join per unordered card pair,
// preferring the left→right direction so the surviving arrowhead points at the
// joined (dim) side. Non-join edges pass through untouched. Nothing is lost — the
// full predicate list still lives in the node-detail "Declared joins" panel.
// Pure function of the rendered items; order-preserving among survivors.
export function collapsePairJoins<T extends RenderedEdge>(items: T[]): T[] {
  const keptJoinByPair = new Map<string, number>()
  const dropped = new Set<number>()
  items.forEach((it, pos) => {
    if (it.kind !== "join") return
    const key = [it.fromCardId, it.toCardId].sort().join("\u0000")
    const prevPos = keptJoinByPair.get(key)
    if (prevPos === undefined) {
      keptJoinByPair.set(key, pos)
      return
    }
    const prevLtr = items[prevPos].fromBox.x <= items[prevPos].toBox.x
    const curLtr = it.fromBox.x <= it.toBox.x
    if (curLtr && !prevLtr) {
      dropped.add(prevPos)
      keptJoinByPair.set(key, pos)
    } else {
      dropped.add(pos)
    }
  })
  return dropped.size ? items.filter((_, pos) => !dropped.has(pos)) : items
}

const PORT_PAD = 8

// ── Fan bundling (12f) ───────────────────────────────────────────────────────
// A 30-table star puts 29 join edges on the fact card's facing side. Port
// distribution (above) already keeps the CURVES apart, but each edge also drew
// its own label plate, and 29 plates stacked down the gutter into a solid grey
// band — the thing that made the 30-table canvas look broken. Above this many
// edges on one side, the group is a BUNDLE: the members drop their individual
// labels (they are still hoverable, and a hover/selection still reveals the full
// predicate) and the group gets one summary count instead.
export const BUNDLE_MIN = 6

function groupKeys(it: RenderedEdge): [string, string] {
  const leftToRight = it.fromBox.x <= it.toBox.x
  return [
    `${it.fromCardId}:${leftToRight ? "right" : "left"}`,
    `${it.toCardId}:${leftToRight ? "left" : "right"}`,
  ]
}

// Only labelled edges can stack into a band, and only joins carry labels, so a
// fan is counted over JOINS. Counting `uses` here would both over-report the
// bundle and mislabel it ("30 declared joins" when one is an MV→table edge).
const bundleable = (it: RenderedEdge) => it.kind == null || it.kind === "join"

// Per-edge size of the largest (card, side) group it belongs to — the same
// grouping distributeEdgePorts uses, so bundling and port spreading always agree
// about what a fan is. Pure.
export function edgeGroupSizes(items: RenderedEdge[]): Map<number, number> {
  const joinItems = items.filter(bundleable)
  const counts = new Map<string, number>()
  for (const it of joinItems) for (const k of groupKeys(it)) counts.set(k, (counts.get(k) ?? 0) + 1)
  const out = new Map<number, number>()
  for (const it of joinItems) {
    const [a, b] = groupKeys(it)
    out.set(it.index, Math.max(counts.get(a) ?? 1, counts.get(b) ?? 1))
  }
  return out
}

// One anchor per bundled group, positioned just BELOW the card the fan converges
// on. The gutter the trunk runs through is only tens of units wide — a
// "30 declared joins" chip placed there overflows onto both neighbours — while
// the space under a fan's hub card is empty by construction (that is what makes
// it a hub). Pure. Returns the chip's left edge and text baseline.
export function edgeBundleAnchors(
  items: RenderedEdge[],
): { x: number; y: number; count: number }[] {
  const groups = new Map<string, { box: CardBox; side: "left" | "right"; count: number }>()
  for (const it of items.filter(bundleable)) {
    const [fromKey, toKey] = groupKeys(it)
    const leftToRight = it.fromBox.x <= it.toBox.x
    for (const [key, box, side] of [
      [fromKey, it.fromBox, leftToRight ? "right" : "left"] as const,
      [toKey, it.toBox, leftToRight ? "left" : "right"] as const,
    ]) {
      const cur = groups.get(key)
      if (cur) cur.count += 1
      else groups.set(key, { box, side, count: 1 })
    }
  }
  const out: { x: number; y: number; count: number }[] = []
  for (const g of groups.values()) {
    if (g.count < BUNDLE_MIN) continue
    out.push({ x: g.box.x, y: g.box.y + g.box.h + 16, count: g.count })
  }
  return out
}

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
// Below this, the card type captions and mono labels stop being readable, and a
// graph you cannot read is not a graph you have fitted.
const LEGIBILITY_FLOOR = 0.8

export function computeFit(
  contentW: number,
  contentH: number,
  viewW: number,
  viewH: number,
  pad = 24,
): { scale: number; tx: number; ty: number } {
  if (contentW <= 0 || contentH <= 0 || viewW <= 0 || viewH <= 0) return { scale: 1, tx: 0, ty: 0 }
  const byWidth = (viewW - 2 * pad) / contentW
  const byHeight = (viewH - 2 * pad) / contentH
  const meet = Math.min(byWidth, byHeight, 1)
  // 12f: "fit everything in" is the WRONG goal for a tall model. A 30-table space
  // stacks ~2400 units of cards; framing that in a 520px canvas means 20% scale,
  // which is the tiny-blob defect arriving by a different road — every label
  // illegible so the whole graph fits. So: fit to WIDTH (columns stay readable)
  // and refuse to shrink past a legibility floor, letting the reader pan the
  // overflow (drag already pans). The floor yields to the width fit, so a genuinely
  // wide model is never blown out sideways just to honour a minimum.
  const floor = Math.min(byWidth, LEGIBILITY_FLOOR, 1)
  const scale = Math.min(1, Math.max(meet, floor))
  const tx = Math.max(pad, (viewW - contentW * scale) / 2)
  // Centered when it fits — a wide-and-short graph (the common shape) framed at
  // width leaves vertical slack, and centering reads as intentional rather than
  // as a blob pinned to the top. Top-aligned when it overflows, so the reader
  // starts at the beginning of the model instead of its middle.
  const overflowsV = contentH * scale > viewH - 2 * pad
  const ty = overflowsV ? pad : Math.max(pad, (viewH - contentH * scale) / 2)
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

// Leaf of a dotted identifier — the name a reader recognizes, without the
// catalog/schema prefix a chip has no room for.
function leafName(identifier: string): string {
  const parts = identifier.split(".")
  return parts[parts.length - 1] || identifier
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
function CoverageBadge({
  x,
  y,
  w,
  coverage,
  inset,
}: {
  x: number
  y: number
  w: number
  coverage?: number | null
  // 12f: a badge centred ON the card's border half-hangs into the gutter and
  // reads unfinished next to the v7 frame, where it sits inside the card. Table
  // cards inset it; a measure card keeps the corner, because its own measure
  // pill already owns the inside of that row.
  inset?: boolean
}) {
  if (coverage == null) return null
  const cold = coverage === 0
  const bx = inset ? x + w - 17 : x + w - 12
  const by = inset ? y + 17 : y - 6
  return (
    <g aria-label={cold ? "no curated SQL coverage" : `curated SQL coverage ${coverage}`}>
      <title>{cold ? "cold spot — no curated SQL touches this" : `${coverage} curated statement${coverage === 1 ? "" : "s"}`}</title>
      {/* Round-5: a warm coverage count is a NEUTRAL grey chip, not accent — the
          accent is reserved for selection/lineage, so the resting canvas is not
          peppered with indigo dots. A cold spot keeps its dashed danger ring. */}
      <circle cx={bx} cy={by} r="8" fill={cold ? "var(--bg-surface)" : "var(--text-muted)"} opacity={cold ? 1 : 0.55}
        stroke={cold ? "var(--color-danger)" : "var(--border-color-strong)"} strokeWidth="1" strokeDasharray={cold ? "2 2" : undefined} />
      <text x={bx} y={by + 3} textAnchor="middle" fontSize="8" fontWeight="700"
        fill={cold ? "var(--color-danger)" : "var(--bg-surface)"}>{coverage}</text>
    </g>
  )
}

// An edge label always sits on top of SOMETHING (a card border, another edge, a
// boundary dash), so it carries its own opaque plate. Width is derived from the
// string rather than measured, which keeps the component renderToStaticMarkup-
// pure; the estimate runs slightly generous so it never clips.
function EdgeLabel({ x, y, text, emphasis }: { x: number; y: number; text: string; emphasis?: boolean }) {
  if (!text) return null
  const w = text.length * 4.9 + 12
  const h = 15
  return (
    <g pointerEvents="none">
      <rect x={x - w / 2} y={y - h + 3} width={w} height={h} rx="4"
        fill="var(--bg-sunken)" stroke="var(--border-color-default)" strokeWidth="0.75" />
      <text x={x} y={y + 3.5} textAnchor="middle" fontSize="8.5"
        className={emphasis ? "fill-[var(--text-secondary)]" : "fill-[var(--text-muted)]"}>{text}</text>
    </g>
  )
}

function EdgeView({
  edge,
  src,
  dst,
  active,
  dimmed,
  verbose,
  bundled,
  onHover,
}: {
  edge: SemanticGraphEdge
  // Distributed endpoints (12d finding 4): each edge attaches at its own slot on
  // the card's facing side, so a fan-in does not collapse onto one midpoint.
  src: { x: number; y: number }
  dst: { x: number; y: number }
  active: boolean
  // Round-6 focus+context (reference BlueprintEdge): when SOMETHING is selected,
  // every edge NOT in the selection's neighborhood is `dimmed` to near-invisible
  // (opacity ~0.06) so only the handful of edges you care about remain. This — not
  // routing — is what makes the reference canvas read clean; ours used to keep all
  // joins at full opacity at rest AND on select, which is the "hodge podge".
  dimmed: boolean
  // Neighborhood emphasis (`active`) is wide; the label reveal (`verbose`) is the
  // one edge the reader pointed at. Keeping them separate is what stops a single
  // selection from spraying full ON predicates across the canvas.
  verbose: boolean
  // One of a large fan on a shared side: draw the curve, drop the at-rest label
  // (the group carries one count instead). Hover still reveals the predicate.
  bundled: boolean
  onHover: (on: boolean) => void
}) {
  // Column-aware cubic curve: control points offset horizontally so edges bend
  // in the gutter (the §5 "orthogonal elbows or column-aware curves" call).
  //
  // 12f — a BUNDLED edge switches to the §5 orthogonal elbow instead. Thirty
  // curves spanning 2400px of dim column inside a 40px gutter is a ribbon no
  // matter how thin each stroke is; routed as elbows they share one vertical
  // trunk (`cx` is identical for every member of a fan, since they share both
  // the column x and the root port x) and peel off with a short stub per row. One
  // line plus stubs, not thirty near-parallel strands — and each edge stays its
  // own path, so hovering one still reveals its predicate.
  const cx = (src.x + dst.x) / 2
  const path = bundled
    ? `M ${src.x} ${src.y} L ${cx} ${src.y} L ${cx} ${dst.y} L ${dst.x} ${dst.y}`
    : `M ${src.x} ${src.y} C ${cx} ${src.y} ${cx} ${dst.y} ${dst.x} ${dst.y}`
  const midX = (src.x + dst.x) / 2
  const midY = (src.y + dst.y) / 2

  if (edge.kind === "replaces") {
    return (
      <g>
        <path d={path} fill="none" stroke="var(--color-danger)" strokeWidth="1.5" strokeDasharray="4 3" />
        <g pointerEvents="none">
          <rect x={midX - 24} y={midY - 15} width="48" height="15" rx="4" fill="var(--bg-sunken)" stroke="var(--color-danger)" strokeWidth="0.75" />
          <text x={midX} y={midY - 4} textAnchor="middle" className="fill-[var(--color-danger)]" fontSize="9">replaces</text>
        </g>
      </g>
    )
  }
  if (edge.kind === "membership") {
    return <path d={path} fill="none" stroke="var(--color-accent)" strokeWidth="1.5" />
  }

  // governs (round-6 overlay): a proposed metric view → the loose measure it
  // would govern. A dashed accent link that points from the ghost card toward the
  // Space-config box, with one small "would govern" tag at its midpoint. Overlay
  // -only, so it is always drawn when present (never dimmed).
  if (edge.kind === "governs") {
    return (
      <g pointerEvents="none">
        <path d={path} fill="none" stroke="var(--color-accent)" strokeWidth="1.5"
          strokeDasharray="5 3" strokeLinecap="round" opacity="0.8"
          markerEnd="url(#mv-arrow-on)" />
        <g>
          <rect x={midX - 34} y={midY - 15} width="68" height="15" rx="4"
            fill="var(--bg-sunken)" stroke="var(--color-accent)" strokeWidth="0.75" opacity="0.95" />
          <text x={midX} y={midY - 4} textAnchor="middle" className="fill-[var(--color-accent)]" fontSize="8.5" fontWeight="600">would govern</text>
        </g>
      </g>
    )
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

  // derives (round-7): a Space-config (loose) measure → a table its expression
  // is built from. The render loop only emits it when the measure is selected,
  // so it is the on-select lineage reveal for measures that belong to no metric
  // view. Dashed accent like `uses`, but rooted at a measure chip; the arrow
  // points at the source table.
  if (edge.kind === "derives") {
    return (
      <path
        d={path}
        fill="none"
        stroke="var(--color-accent)"
        strokeWidth={1.75}
        strokeDasharray="1 4"
        strokeLinecap="round"
        opacity={0.9}
        markerEnd="url(#mv-uses-arrow)"
      />
    )
  }

  // join — the relationship GLYPH (12d finding 3), reserved for the on-demand
  // reveal. A relationship with no known glyph is omitted rather than shown
  // truncated.
  const glyph = relationshipGlyph(edge.relationship)
  // Round-6 labels-on-demand: a join carries a label ONLY when the reader points
  // at it (`verbose` = hovered, or an endpoint IS the selection and it is not
  // inside a bundle). At rest — and for the merely-in-neighborhood `active` edges
  // — no label at all, so the resting canvas has zero floating glyphs. This drops
  // the scattered "N:1"/"ON …" plates that made the fit read busy even before the
  // curves crossed. (Before round-6 a compact glyph rode at rest; the reference
  // canvas the reviewer approved shows nothing until you point at an edge.)
  const label = verbose
    ? [edge.on ? `ON ${abbreviate(edge.on, 34)}` : null, glyph, edge.scd2 ? "SCD2" : null].filter(Boolean).join(" · ")
    : ""
  // Focus+context strokes (reference BlueprintEdge). Three states:
  //   active  → accent, thick, full opacity, glow underlay (the selection story)
  //   dimmed  → near-invisible grey (something else is selected; this is context)
  //   rest    → a quiet, thin, low-opacity grey skeleton (nothing selected)
  const strokeColor = active ? "var(--color-accent)" : "var(--border-color-strong)"
  const strokeW = active ? 2.25 : dimmed ? 0.75 : bundled ? 1 : 1.25
  const strokeOpacity = active ? 1 : dimmed ? 0.06 : bundled ? 0.32 : 0.4
  return (
    <g onMouseEnter={() => onHover(true)} onMouseLeave={() => onHover(false)} style={{ cursor: "default" }}>
      {/* Selection/hover "lights up" the line (v3 §4; reference BlueprintEdge): a
          soft accent glow underlay + the stroke itself switches from neutral grey
          to the accent hue with an accent arrowhead. At rest it stays a quiet grey
          join so the canvas reads calm until the reader points at something. */}
      {active && (
        <path d={path} fill="none" stroke="var(--color-accent)" strokeWidth={6} strokeLinecap="round" opacity={0.18} />
      )}
      <path
        d={path}
        fill="none"
        stroke={strokeColor}
        strokeWidth={strokeW}
        opacity={strokeOpacity}
        markerEnd={active ? "url(#mv-arrow-on)" : dimmed ? undefined : "url(#mv-arrow)"}
      />
      <EdgeLabel x={midX} y={midY - 6} text={label} emphasis={verbose} />
    </g>
  )
}

// The per-box chip the v7 contract puts on every measure box: "N measures ·
// <dominant rung>", a tinted pill with a rung dot. The dominant rung is the
// highest present on the ladder, so the pill never claims governance the card
// does not have. Absent measures render no pill (nothing to count).
function MeasurePill({ x, y, w, card }: { x: number; y: number; w: number; card: GraphCard }) {
  const rungs = rollup(card)
  const total = card.measures.length + card.unnamedMeasures.length
  if (total === 0) return null
  const noun = card.kind === "concepts" ? "measure" : "measure"
  const dominant = rungs[0]?.rung
  const color = dominant ? GOVERNANCE[dominant].color : "var(--border-color-strong)"
  const text = `${total} ${noun}${total === 1 ? "" : "s"}${dominant ? ` · ${GOVERNANCE[dominant].label.toLowerCase()}` : ""}`
  // Width tracks the label so a long rung name never overflows the card.
  const pw = Math.min(w - 24, 22 + text.length * 5.4)
  const px = x + w - pw - 10
  return (
    <g aria-label={text}>
      <rect x={px} y={y} width={pw} height="18" rx="9" fill={color} opacity="0.16" />
      <circle cx={px + 11} cy={y + 9} r="3.5" fill={color} />
      <text x={px + 19} y={y + 13} className="fill-[var(--text-secondary)]" fontSize="9" fontWeight="600">{text}</text>
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
  showRung,
  onSelect,
}: {
  m: SemanticGraphNode
  x: number
  y: number
  w: number
  selected: boolean
  dim: boolean
  // 12f: the per-chip rung letter is a NON-COLOR discriminator, so it earns its
  // place only when the card's measures actually differ in rung. Inside a metric
  // view they never do — every measure it exposes is governed, and the card's own
  // "N measures · governed" pill already says so — and 24 copies of the same "G"
  // is the visual noise that separated reality from the v7 frame's clean rows.
  showRung: boolean
  onSelect: (n: SemanticGraphNode) => void
}) {
  const g = m.governance ? GOVERNANCE[m.governance] : null
  const color = g?.color ?? "var(--border-color-strong)"
  const tag = m.governance ? m.governance[0].toUpperCase() : "•"
  const label = g ? `${m.label} — ${g.label}` : m.label
  // A loose measure whose NAME a metric view already exposes under a different
  // expression (server-computed `overlaps`) is two answers to one question — the
  // v7 contract puts a warning on that chip; here it is a small amber caret so the
  // gap is visible on the canvas, not only in the inset.
  const wx = x + w - (g && showRung ? 18 : 8)
  return (
    <g opacity={dim ? 0.3 : 1} onClick={() => onSelect(m)} style={{ cursor: "pointer" }} aria-label={m.overlaps ? `${label} — name also in ${leafName(m.overlaps)}` : label}>
      <title>{m.origin ? `${label} · ${m.origin}` : label}</title>
      <rect x={x} y={y} width={w} height={CHIP_H} rx="4" fill={color} opacity="0.14"
        stroke={selected ? "var(--color-accent)" : m.overlaps ? "var(--color-warning)" : color} strokeWidth={selected ? 2 : 1} />
      <text x={x + 6} y={y + 13} className="fill-[var(--text-primary)]" fontSize="9.5" fontWeight="600">{abbreviate(m.label, m.overlaps ? 18 : 22)}</text>
      {m.overlaps && (
        <g aria-label={`also in ${leafName(m.overlaps)}`}>
          <title>{`name also exposed by ${leafName(m.overlaps)} — two definitions, one name`}</title>
          <path d={`M ${wx} ${y + 4} l 5 9 l -10 0 Z`} fill="var(--color-warning)" />
          <text x={wx} y={y + 13} textAnchor="middle" fontSize="7" fontWeight="800" className="fill-[var(--bg-surface)]">!</text>
        </g>
      )}
      {g && showRung && !m.overlaps && (
        <text x={x + w - 6} y={y + 13} textAnchor="end" fill={color} fontSize="8" fontWeight="700">{tag}</text>
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
  // 12f: 0.3 pushed out-of-focus cards past readable — a de-emphasis, not an
  // erasure, and at 0.3 the card's own fill went translucent enough for edges to
  // print through its label. Dimmed still means "not the story", just legible.
  const opacity = dim ? 0.45 : 1
  // A selected card is draggable (§7) — spread the canvas to create room. The
  // grab cursor advertises it; the actual offset is session-only.
  const pointerDown = (e: React.PointerEvent) => onCardPointerDown(card.id, e)
  const dragCursor = draggable ? "grab" : undefined

  if (card.kind === "table") {
    const clickable = card.node
    // Round-5: the caption asserts FACT/DIM only when the data PROVES the role
    // (node.role — fact = a metric view's declared source, dim = a join target).
    // When nothing proves it we say a neutral "TABLE" instead of guessing from
    // column position, which is what mislabelled dim_* tables as FACT.
    const role = card.node?.role
    const typeCaption = role === "fact" ? "FACT" : role === "dim" ? "DIM" : "TABLE"
    return (
      <g opacity={opacity} onClick={() => clickable && onSelect(clickable)} onPointerDown={pointerDown} style={{ cursor: dragCursor ?? (clickable ? "pointer" : "default") }}>
        <title>{unmodeled ? `${card.label} — in no metric view` : card.label}</title>
        <rect x={x} y={y} width={w} height={h} rx="8" fill="var(--bg-surface)" stroke={stroke ?? "var(--border-color-strong)"} strokeWidth={selWidth} strokeDasharray={card.coverage === 0 ? "4 3" : undefined} />
        <text x={x + 14} y={y + 20} className="fill-[var(--text-muted)]" fontSize="8.5" fontWeight="700" letterSpacing="0.06em">{typeCaption}</text>
        <text x={x + 14} y={y + 39} className="fill-[var(--text-primary)]" fontSize="12.5" fontWeight="600" fontFamily="monospace">{abbreviate(card.label, 20)}</text>
        {/* The per-card "no metric view" footnote is gone (12f): the labeled
            UNMODELED region now says it once for the whole group, as the v7
            contract does, instead of repeating it on every card in 7.5px. */}
        <CoverageBadge x={x} y={y} w={w} coverage={card.coverage} inset />
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
  // 12f: the count moved OUT of the subtitle and into the frame's pill on the
  // right (MeasurePill), so the subtitle carries only the card's STATE. Doubling
  // the count in both places is what made the shipped header read cramped and
  // noisy next to the frame.
  const subtitle = isMv
    ? card.proposed
      ? "proposed metric view"
      : defUnavailable
        ? "definition unavailable"
        : "metric view"
    : "not in any MV"
  // The frame names the concepts card for what it is: measures the space config
  // declares that no metric view governs. It wears the warning treatment
  // (dashed, amber) because that gap is the thing the curator must act on. The
  // Round-5: the title gets the FULL card width on its own row and the "N measures
  // · governed" pill drops to a second row, so a long MV name (e.g.
  // customer_analytics_metrics) can no longer collide with the pill (the reported
  // text overlap). The concepts box reads "Space config" — the column header
  // already carries "measures".
  const displayLabel = isMv ? abbreviate(card.label, 30) : "Space config"
  const rungCount = rollup(card).length
  const clickableHeader = card.node
  const hidden = collapsed
  // Round-5 palette: an MV box is a calm, near-neutral tint at rest and only wears
  // the accent when selected — the accent is reserved for selection/lineage so the
  // resting canvas is quiet. The concepts box keeps the amber "gap" treatment
  // because a loose-measure gap is the thing a curator must act on.
  const boxFill = isMv ? "var(--color-accent)" : "var(--color-warning)"
  const boxFillOpacity = isMv ? (card.proposed ? 0.05 : 0.06) : 0.08
  const boxStroke = stroke ?? (isMv ? "var(--border-color-default)" : "var(--color-warning)")
  return (
    <g opacity={opacity}>
      <g onClick={() => clickableHeader && onSelect(clickableHeader)} onPointerDown={pointerDown} style={{ cursor: dragCursor ?? (clickableHeader ? "pointer" : "default") }}>
        <title>{card.proposed ? `${card.label} — proposed metric view` : defUnavailable ? `${card.label} — definition unavailable` : isMv ? card.label : `${conceptCount} measure${conceptCount === 1 ? "" : "s"} governed by no metric view`}</title>
        <rect x={x} y={y} width={w} height={h} rx="10"
          fill={boxFill} opacity={boxFillOpacity}
          stroke={boxStroke} strokeWidth={selWidth}
          strokeDasharray={card.proposed || !isMv ? "5 3" : undefined} />
        <text x={x + 12} y={y + 21} className="fill-[var(--text-primary)]" fontSize="12.5" fontWeight="700">{displayLabel}</text>
        <text x={x + 12} y={y + 40} className="fill-[var(--text-muted)]" fontSize="9">{subtitle}{hidden ? " · collapsed" : ""}</text>
        {/* Pill on its own row (top-right of row 2), clear of the full-width title. */}
        <MeasurePill x={x} y={y + 31} w={w} card={card} />
        {/* The rung breakdown only earns a line when the pill's dominant rung
            doesn't already tell the whole story; it sits under the subtitle. */}
        {rungCount > 1 && <RollUp x={x + 12} y={y + 52} card={card} />}
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
              showRung={rungCount > 1}
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
  // `null` is an explicit deselect (click empty canvas / re-click the selection),
  // so the parent can drive toggle-off and clear-on-background like the reference.
  onSelectNode?: (node: SemanticGraphNode | null) => void
  label?: string
}

function SemanticGraphInner({ nodes, edges, selectedId, onSelectNode, label = "Semantic model" }: SemanticGraphProps) {
  // 12f round 4 (v3 §3): the loose-measures ("Space config") card is a peer of the
  // MV boxes ON THE CANVAS, in the metric-view column below them — NOT a full-width
  // panel below the canvas (the round-3 regression). buildCards places it at
  // (CONCEPTS_COL, CONCEPTS_ROW) so it stacks last in that column, and it renders
  // through the same CardView as every other card.
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
  // Reference parity (BlueprintCanvas): a press that begins on the empty
  // background and releases without a pan is a "click empty → clear selection".
  // We remember whether the press started on the background and whether the pan
  // actually moved, so a genuine pan-drag never clears.
  const bgPress = useRef(false)
  const panMoved = useRef(false)
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

  const { placed: placedBase, width, height, columns } = useMemo(() => layoutCards(cards, collapsed), [cards, collapsed])

  // Session-only drag offsets (§7) applied on top of the deterministic home
  // layout. With no offsets this is byte-identical to the layout, so the home
  // layout and the diff seed are untouched — drag is honest interaction state.
  const placed = useMemo(() => {
    if (Object.keys(offsets).length === 0) return placedBase
    const m = new Map<string, PlacedCard>()
    for (const [id, pc] of placedBase) m.set(id, { card: pc.card, box: applyDragOffset(pc.box, offsets[id]) })
    return m
  }, [placedBase, offsets])

  // Round-5: Impact (downstream blast radius) is only MEANINGFUL for a selected
  // SOURCE TABLE — that is the thing whose change breaks MVs and measures. For an
  // MV or measure selection, or no selection, impact and lineage collapse to the
  // same picture, which is why toggling it looked like it did nothing. So the
  // toggle is disabled unless a table is selected, and the effective mode falls
  // back to Lineage otherwise — the button never lies about having an effect.
  const impactApplicable = useMemo(
    () => !!selectedId && cards.some((c) => c.id === selectedId && c.kind === "table"),
    [selectedId, cards],
  )
  const effectiveMode = mode === "impact" && impactApplicable ? "impact" : "lineage"

  // Focus set depends on the mode: Lineage (both directions) vs Impact
  // (downstream only). Both are pure functions of the selection.
  const highlight = useMemo(
    () => (effectiveMode === "impact" ? impactSet(cards, edges, selectedId) : focusSet(cards, edges, selectedId)),
    [effectiveMode, cards, edges, selectedId],
  )

  // Tables in no metric view — the unmodeled region cue (MV-D33).
  const unmodeled = useMemo(() => unmodeledTableIds(nodes, edges), [nodes, edges])

  // 12f: the v7 contract draws the unmodeled tables inside a LABELED region, not
  // just a caption on each card. Per-member outlines (never a bounding box) so
  // the region can't swallow a governed table that happens to sit between two
  // unmodeled ones. Members are `uses`-edge non-targets and the select-time
  // boundary's members are `uses`-edge targets, so the two regions are disjoint
  // by construction and cannot enclose the same card.
  const unmodeledRects = useMemo(() => {
    const rects: CardBox[] = []
    for (const id of unmodeled) {
      const p = placed.get(id)
      if (!p) continue
      rects.push({ x: p.box.x - HULL_PAD / 2, y: p.box.y - HULL_PAD / 2, w: p.box.w + HULL_PAD, h: p.box.h + HULL_PAD })
    }
    return rects
  }, [unmodeled, placed])

  // The anchor MV of the current selection: the selected MV card itself, or —
  // when a MEASURE is selected — the MV that owns it (v3 §4). Drives BOTH the
  // select-time boundary and which definition (`uses`) edges are drawn.
  const anchorMvId = useMemo<string | null>(() => {
    if (!selectedId) return null
    if (cards.some((c) => c.id === selectedId && c.kind === "metric_view")) return selectedId
    const owner = edges.find((e) => e.kind === "membership" && e.from === selectedId)?.to
    if (owner && cards.some((c) => c.id === owner && c.kind === "metric_view")) return owner
    return null
  }, [selectedId, cards, edges])

  // The select-time boundary: wrap the tables a metric view uses (its `uses`-edge
  // members). Hull is the norm, rect the lucky case.
  const boundary = useMemo(() => {
    if (!anchorMvId) return null
    const sel = cards.find((c) => c.id === anchorMvId && c.kind === "metric_view")
    if (!sel) return null
    const memberIds = memberTableIds(edges, anchorMvId)
    const memberBoxes = memberIds.map((id) => placed.get(id)?.box).filter((b): b is CardBox => !!b)
    if (memberBoxes.length === 0) return null
    const memberSet = new Set(memberIds)
    const others = [...placed.values()].filter((p) => p.card.id !== anchorMvId && !memberSet.has(p.card.id)).map((p) => p.box)
    return { ...memberBoundary(memberBoxes, others), mvLabel: sel.label }
  }, [anchorMvId, cards, edges, placed])

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
      // Round-5: definition (`uses`, dotted) edges are drawn ONLY for the selected
      // metric view — at rest the canvas shows just the join skeleton, so the
      // dozens of MV→table dotted lines that made it "hodge podge" are gone until
      // you point at an MV. The data still carries every `uses` edge (the boundary
      // and the unmodeled region read it); we simply do not RENDER the rest.
      if (edge.kind === "uses" && edge.from !== anchorMvId) return
      // Round-7: a measure→table `derives` edge is the ON-SELECT lineage for a
      // Space-config (loose) measure — the tables its expression is built from.
      // At rest the canvas shows nothing for it (like `uses`); it appears only
      // when its measure is the selection, so clicking a loose measure lights up
      // its source tables + the dashed link, and nothing else.
      if (edge.kind === "derives" && edge.from !== selectedId) return
      const fromCardId = cardIdOfNode.get(edge.from)
      const toCardId = cardIdOfNode.get(edge.to)
      if (!fromCardId || !toCardId || fromCardId === toCardId) return
      const from = placed.get(fromCardId)
      const to = placed.get(toCardId)
      if (!from || !to) return
      items.push({ index, edge, fromCardId, toCardId, fromBox: from.box, toBox: to.box, kind: edge.kind })
    })
    return collapsePairJoins(items)
  }, [edges, cardIdOfNode, placed, anchorMvId, selectedId])

  const edgePorts = useMemo(() => distributeEdgePorts(renderedEdges), [renderedEdges])
  // 12f: which edges belong to a large fan, and where each fan's summary count
  // goes. A 30-table star used to stack 29 label plates down one gutter.
  const edgeGroups = useMemo(() => edgeGroupSizes(renderedEdges), [renderedEdges])
  const bundleAnchors = useMemo(() => edgeBundleAnchors(renderedEdges), [renderedEdges])

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
    // onCardPointerDown, which stops propagation). Record whether this press
    // landed on the empty background (target IS the svg, not a card child) so a
    // release without a pan can clear the selection like the reference does.
    bgPress.current = e.target === svgRef.current
    panMoved.current = false
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
    // A few pixels of jitter is not a pan; past that, mark it so the release
    // does not clear the selection.
    if (Math.abs(delta.dx) > 3 || Math.abs(delta.dy) > 3) panMoved.current = true
    setView((v) => ({ ...v, tx: d.tx + delta.dx, ty: d.ty + delta.dy }))
  }
  const onPointerUp = () => {
    // Click on empty canvas (background press, no pan) clears the selection —
    // the reference's "click empty canvas: reset" (BlueprintCanvas).
    if (bgPress.current && !panMoved.current && selectedId) onSelectNode?.(null)
    bgPress.current = false
    panMoved.current = false
    drag.current = null
    cardDrag.current = null
    setDragging(false)
  }
  const resetLayout = () => setOffsets({})

  const select = (n: SemanticGraphNode) => onSelectNode?.(n)
  const selectedCardDim = (cardId: string) => (highlight ? !highlight.has(cardId) : false)

  // The frame's selected-context chip: the canvas dims and boundaries change
  // meaning entirely with selection, so the control row names what is selected
  // instead of leaving the reader to infer it from the highlight.
  const selectedLabel = useMemo(() => {
    if (!selectedId) return null
    for (const c of cards) {
      if (c.id === selectedId) return c.kind === "concepts" ? "Space config" : c.label
      for (const m of [...c.measures, ...c.unnamedMeasures]) if (m.id === selectedId) return m.label
    }
    return null
  }, [cards, selectedId])

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
  // Before measurement (SSR / first paint) there is no viewport to fit against,
  // so the viewBox IS the frame. Capping its height to the canvas's widest
  // aspect crops a tall model to its top at a legible scale, rather than
  // meet-shrinking the whole 2400-unit stack into a microscopic band — the same
  // choice computeFit makes once a measurement exists.
  const vbH = viewportHeight ?? Math.min(height, width / CANVAS_MAX_ASPECT)

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
            the downstream blast radius of a selected TABLE. Round-5: Impact is
            disabled (and visibly greyed) unless a table is selected, so it never
            appears to do nothing — the effect it has needs a table to act on. */}
        <div className="inline-flex items-center overflow-hidden rounded-md border border-default text-xs" role="group" aria-label="Focus mode">
          <button type="button" aria-pressed={effectiveMode === "lineage"} onClick={() => setMode("lineage")} className={`px-2.5 py-1 ${effectiveMode === "lineage" ? "bg-[var(--color-accent)] font-medium text-white" : "text-secondary hover:text-primary"}`}>Lineage</button>
          <button
            type="button"
            aria-pressed={effectiveMode === "impact"}
            disabled={!impactApplicable}
            onClick={() => setMode("impact")}
            title={impactApplicable ? "Downstream blast radius of the selected table" : "Select a source table to see its downstream impact"}
            className={`px-2.5 py-1 ${effectiveMode === "impact" ? "bg-[var(--color-accent)] font-medium text-white" : impactApplicable ? "text-secondary hover:text-primary" : "cursor-not-allowed text-muted opacity-50"}`}
          >
            Impact
          </button>
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
        {/* Always present (v7 contract 9e), disabled until there is a drag to
            undo — a control that appears and disappears makes the row jump and
            leaves the reader unsure the affordance exists at all. */}
        <button
          type="button"
          onClick={resetLayout}
          disabled={Object.keys(offsets).length === 0}
          className="inline-flex items-center gap-1 rounded-md border border-default bg-surface px-2 py-1 text-xs text-muted enabled:hover:text-secondary disabled:opacity-40"
          title={Object.keys(offsets).length === 0 ? "Nothing to reset — no box has been dragged" : "Restore the default layout (clears your dragging)"}
        >
          <RotateCcw className="h-3.5 w-3.5" /> Reset
        </button>
        {selectedLabel && (
          <span className="ml-auto inline-flex items-center gap-1.5 rounded-md border border-[var(--color-accent)] px-2 py-1 text-xs font-medium text-[var(--color-accent)]">
            <Crosshair className="h-3.5 w-3.5" />
            {abbreviate(selectedLabel, 30)} selected
          </span>
        )}
      </div>

      {/* The canvas takes the CONTENT's aspect ratio (bounded), instead of a fixed
          height. A wide-and-short model — the common shape, and what four
          occupied columns always produces — was rendering as a thin band adrift
          in a tall box, which reads as the tiny-blob defect even after the fit
          math is correct. Bounds keep it interactive (never a sliver) and keep a
          tall model from taking the whole page. */}
      <div
        className="relative overflow-hidden rounded-lg border border-default bg-sunken"
        // width MUST stay explicit: with only aspect-ratio set, a binding
        // min-height makes the browser derive the WIDTH from the height, and the
        // canvas blows out past its panel.
        style={{ width: "100%", aspectRatio: `${width} / ${height}`, minHeight: 280, maxHeight: 680 }}
      >
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
          {/* Accent arrowhead for a lit (active) join — the reference's highlighted
              edge terminates in the accent hue, not the resting grey. */}
          <marker id="mv-arrow-on" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
            <path d="M0,0 L6,3 L0,6 Z" className="fill-[var(--color-accent)]" />
          </marker>
          <marker id="mv-uses-arrow" markerWidth="7" markerHeight="7" refX="5" refY="2.5" orient="auto">
            <path d="M0,0 L5,2.5 L0,5 Z" className="fill-[var(--color-accent)]" />
          </marker>
        </defs>
        <g transform={`translate(${view.tx} ${view.ty}) scale(${view.scale})`}>
          {/* Headers follow the COMPACTED column positions (12f) — an empty
              column reserves neither space nor a caption. */}
          {columns.map((c) => (
            <text key={c.col} x={c.x + c.w / 2} y={22} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="10" fontWeight="700" letterSpacing="0.04em">{COL_HEADERS[c.col]}</text>
          ))}
          {/* The unmodeled region (v7 contract 9e): the governance gap drawn as a
              place, not just a per-card footnote. Rendered before the boundary so
              a selection's accent outline reads on top of it. */}
          {unmodeledRects.length > 0 && (
            <g aria-label="unmodeled tables — in no metric view" pointerEvents="none">
              {unmodeledRects.map((r, i) => (
                <rect key={i} x={r.x} y={r.y} width={r.w} height={r.h} rx="10"
                  fill="var(--color-danger)" fillOpacity="0.05"
                  stroke="var(--color-danger)" strokeWidth="1.25" strokeDasharray="5 4" />
              ))}
              {(() => {
                const top = unmodeledRects.reduce((a, b) => (b.y < a.y ? b : a), unmodeledRects[0])
                const text = "UNMODELED · in no metric view"
                const w = text.length * 5.1 + 14
                const y = Math.max(top.y - 8, CARD_TOP - 18)
                return (
                  <>
                    <rect x={top.x} y={y - 12} width={w} height="16" rx="4"
                      fill="var(--bg-sunken)" stroke="var(--color-danger)" strokeWidth="0.75" />
                    <text x={top.x + 7} y={y} className="fill-[var(--color-danger)]" fontSize="9" fontWeight="700">{text}</text>
                  </>
                )
              })()}
            </g>
          )}
          {/* Select-time MV boundary (MV-D33): wraps the tables the selected MV
              uses. Hull (per-member outlines) is the norm; a single rect is the
              lucky case where no foreign table sits inside the bounding box. */}
          {boundary && (
            <g aria-label={`tables used by ${boundary.mvLabel}`} pointerEvents="none">
              {/* Round-5: a clearer container — a solid accent border (dashed only
                  for the disjoint HULL case, where the dashes signal "these outlined
                  members, not the space between them") over a light accent wash, so
                  the boundary reads as one obvious region rather than a faint dotted
                  rectangle. */}
              {boundary.rects.map((r, i) => (
                <rect key={i} x={r.x} y={r.y} width={r.w} height={r.h} rx="12"
                  fill="var(--color-accent)" fillOpacity="0.10"
                  stroke="var(--color-accent)" strokeWidth="2"
                  strokeDasharray={boundary.kind === "hull" ? "7 4" : undefined} />
              ))}
              {/* The caption is a FILLED accent chip anchored to the TOPMOST member
                  rect, clamped below the column headers, so the label reads as a
                  strong title on the region rather than faint text on a plate. */}
              {(() => {
                const top = boundary.rects.reduce((a, b) => (b.y < a.y ? b : a), boundary.rects[0])
                if (!top) return null
                const text = `Tables used by ${abbreviate(boundary.mvLabel, 22)}`
                const w = text.length * 5.3 + 16
                const y = Math.max(top.y - 8, CARD_TOP - 18)
                return (
                  <g pointerEvents="none">
                    <rect x={top.x} y={y - 13} width={w} height="18" rx="5"
                      fill="var(--color-accent)" />
                    <text x={top.x + 8} y={y} className="fill-white" fontSize="9.5" fontWeight="700">{text}</text>
                  </g>
                )
              })()}
            </g>
          )}
          {renderedEdges.map(({ index, edge, fromCardId, toCardId }) => {
            const ports = edgePorts.get(index)
            if (!ports) return null
            const endpointSelected = fromCardId === selectedId || toCardId === selectedId
            const bundled = (edgeGroups.get(index) ?? 1) >= BUNDLE_MIN
            // Inside a bundle only a direct hover is verbose: selecting the fact
            // card of a star would otherwise print 29 ON predicates at once.
            const verbose = hoverEdge === index || (endpointSelected && !bundled)
            const active =
              // A definition (`uses`) edge only renders for the selected MV now, so
              // it is always "active" (bright) when shown — it IS the selection's
              // story. Join edges keep the neighborhood/hover emphasis rules.
              edge.kind === "uses" ||
              // A `derives` edge only renders when its loose measure is selected,
              // so it too is always the selection's story when shown.
              edge.kind === "derives" ||
              hoverEdge === index ||
              endpointSelected ||
              (highlight != null && highlight.has(fromCardId) && highlight.has(toCardId))
            // Round-6 focus+context: once there IS a focus (a selection sets
            // `highlight`), any edge outside the neighborhood is dimmed to near
            // -invisible — so a click removes clutter instead of adding it. A
            // hovered edge is never dimmed. Overlay links (governs) ignore this.
            const dimmed = highlight != null && !active && hoverEdge !== index
            return (
              <EdgeView
                key={index}
                edge={edge}
                src={ports.src}
                dst={ports.dst}
                active={active}
                dimmed={dimmed}
                verbose={verbose}
                bundled={bundled}
                onHover={(on) => setHoverEdge(on ? index : null)}
              />
            )
          })}
          {/* One count per bundled fan, under the hub card the trunk converges
              on — the replacement for the 29 stacked per-edge plates. */}
          {bundleAnchors.map((b, i) => {
            const text = `${b.count} declared joins`
            const w = text.length * 4.9 + 14
            return (
              <g key={`bundle-${i}`} pointerEvents="none">
                <rect x={b.x} y={b.y - 11} width={w} height="15" rx="4"
                  fill="var(--bg-sunken)" stroke="var(--border-color-default)" strokeWidth="0.75" />
                <text x={b.x + 7} y={b.y} fontSize="8.5" className="fill-[var(--text-muted)]">{text}</text>
              </g>
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
          reading needs no caption hunting. The Space-config (loose measures) box
          now lives ON the canvas in the metric-view column (v3 §3), so there is
          no separate panel below. */}
      <Legend proposalLink={edges.some((e) => e.kind === "governs")} />

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
//
// Round-5: the two LINE kinds are now spelled out with real line swatches — a
// SOLID grey line is a declared join, a DASHED accent line is a metric view's
// definition (its MV→table edges, drawn only when that MV is selected). This is
// the "dotted vs solid" disambiguation the reviewer asked for. Governance rides a
// compact three-dot cluster rather than one-dot-at-a-time entries.
function LineSwatch({ dashed, color }: { dashed?: boolean; color: string }) {
  return (
    <svg width="22" height="8" viewBox="0 0 22 8" aria-hidden="true">
      <line x1="1" y1="4" x2="17" y2="4" stroke={color} strokeWidth="1.75"
        strokeDasharray={dashed ? "2 3" : undefined} strokeLinecap="round" />
      <path d="M16 1 L21 4 L16 7 Z" fill={color} />
    </svg>
  )
}

function Legend({ proposalLink }: { proposalLink?: boolean }) {
  const items: { swatch: ReactNode; label: string }[] = [
    { swatch: <span className="inline-block h-2.5 w-3.5 rounded-sm border border-[var(--border-color-strong)] bg-[var(--bg-surface)]" />, label: "table" },
    { swatch: <span className="inline-block h-2.5 w-3.5 rounded-sm border border-[var(--border-color-default)] bg-[color-mix(in_srgb,var(--color-accent)_8%,transparent)]" />, label: "metric view" },
    { swatch: <LineSwatch color="var(--border-color-strong)" />, label: "declared join (N:1)" },
    { swatch: <LineSwatch dashed color="var(--color-accent)" />, label: "MV definition (on select)" },
    // Only when the proposal overlay is on: the dashed accent link a proposed MV
    // draws to the loose measure it would govern.
    ...(proposalLink
      ? [{ swatch: <LineSwatch dashed color="var(--color-accent)" />, label: "would govern (proposal)" }]
      : []),
    {
      swatch: (
        <span className="inline-flex items-center gap-1">
          <span className="inline-block h-2 w-2 rounded-full bg-[var(--color-success)]" />
          <span className="inline-block h-2 w-2 rounded-full bg-[var(--color-warning)]" />
          <span className="inline-block h-2 w-2 rounded-full bg-[var(--color-danger)]" />
        </span>
      ),
      label: "governed · curated · ungoverned",
    },
    { swatch: <span className="inline-block h-2.5 w-3.5 rounded-sm border border-dashed border-[var(--color-danger)]" />, label: "unmodeled (no MV)" },
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
