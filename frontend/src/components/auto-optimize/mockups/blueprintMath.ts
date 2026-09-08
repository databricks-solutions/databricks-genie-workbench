/**
 * MV-advisor mockups — Semantic Blueprint (v4) P1 pure math, REVIEW SCAFFOLD.
 *
 * Faithful TypeScript port of the north-star prototype's reference
 * implementation (docs/design/mockups/10-blueprint-prototype.html): fact-center
 * re-ranking (§5.12), adaptive layered layout (§5.1), orthogonal routing with
 * rounded elbows + crossing hop arcs (§5.2–§5.3), orientation-aware crow's-foot
 * geometry (§5.4), semantic-zoom heights (§5.5), the single COL_TOP/COL_H/
 * COL_PAD column band (§6), gutter/lane lineage routing (§5.10), and the
 * headline counts (§5.7).
 *
 * Every function here is PURE — a function of the fixture model + a zoom band —
 * and mutates nothing (fixtures keep their authored source-left rank/order).
 * This is the reference the production modules (layout.ts / routing.ts /
 * cardinality.ts / annotate.ts beside SemanticGraph.tsx) extract from once the
 * fidelity frame is approved. Disposed with the rest of the mockup scaffold.
 */
import type {
  BlueprintJoinFixture,
  BlueprintMeasureFixture,
  BlueprintNodeFixture,
  BlueprintScenarioFixture,
  BlueprintTableFixture,
} from "./mvMockData"

export type BlueprintZoom = "far" | "mid" | "near"
export type BlueprintLayoutMode = "fact" | "source"

// ── Geometry constants (single source of truth, §6) ──────────────────────────
export const CARD_W = { table: 188, mv: 252, config: 252 } as const
export const TOP = 46
export const VGAP = 20
export const RANK_GUTTER = 150
/** Columns-LOD band: first-row offset below the header / row height / bottom pad. */
export const COL_TOP = 46
export const COL_H = 18
export const COL_PAD = 8

export interface Box {
  x: number
  y: number
  w: number
  h: number
}

export interface Placement {
  /** node id → rank (column). */
  rank: Record<string, number>
  /** node id → order within its rank. */
  order: Record<string, number>
  /** rank → semantic band (fact-center mode only). */
  band: Record<number, "dim" | "fact" | "mv">
  rolesKnown: boolean
  mode: BlueprintLayoutMode
}

export interface BlueprintMeasure extends BlueprintMeasureFixture {
  id: string
  parent: string
  parentKind: "mv" | "config"
}

export function measureIndex(s: BlueprintScenarioFixture): Record<string, BlueprintMeasure> {
  const out: Record<string, BlueprintMeasure> = {}
  for (const n of s.nodes) {
    if (n.kind === "table") continue
    for (const m of n.measures) {
      out[`${n.id}::${m.name}`] = { ...m, id: `${n.id}::${m.name}`, parent: n.id, parentKind: n.kind }
    }
  }
  return out
}

export function nodeById(s: BlueprintScenarioFixture): Record<string, BlueprintNodeFixture> {
  return Object.fromEntries(s.nodes.map((n) => [n.id, n]))
}

// ── Fact-center re-ranking (§5.12) — never asserts a role it can't prove ─────
export function derivePlacement(s: BlueprintScenarioFixture, mode: BlueprintLayoutMode): Placement {
  const rolesKnown = s.nodes.some((n) => n.kind === "table" && n.role)
  if (mode === "source") {
    const rank: Record<string, number> = {}
    const order: Record<string, number> = {}
    for (const n of s.nodes) {
      rank[n.id] = n.rank
      order[n.id] = n.order
    }
    return { rank, order, band: {}, rolesKnown, mode }
  }

  const tables = s.nodes.filter((n): n is BlueprintTableFixture => n.kind === "table")
  const mvs = s.nodes.filter((n) => n.kind !== "table")

  // Undirected join adjacency + measure-reference counts (the MV-source signal).
  const adj: Record<string, string[]> = {}
  tables.forEach((t) => (adj[t.id] = []))
  s.joins.forEach((j) => {
    if (adj[j.from] && adj[j.to]) {
      adj[j.from].push(j.to)
      adj[j.to].push(j.from)
    }
  })
  const refCount: Record<string, number> = {}
  for (const n of s.nodes) {
    if (n.kind === "table") continue
    n.measures.forEach((m) => m.src.forEach((t) => (refCount[t] = (refCount[t] ?? 0) + 1)))
  }

  // Fact anchors by evidence strength: proven FACT role → most measure-referenced
  // → highest-degree hub → every table its own center (single-table model).
  let anchors: string[]
  const roleFacts = tables.filter((t) => (t.role ?? "").toUpperCase() === "FACT")
  if (roleFacts.length) anchors = roleFacts.map((t) => t.id)
  else if (s.joins.length) {
    const ranked = [...tables].sort(
      (a, b) => (refCount[b.id] ?? 0) - (refCount[a.id] ?? 0) || adj[b.id].length - adj[a.id].length,
    )
    const topRef = refCount[ranked[0].id] ?? 0
    anchors = topRef > 0 ? tables.filter((t) => (refCount[t.id] ?? 0) === topRef).map((t) => t.id) : [ranked[0].id]
  } else anchors = tables.map((t) => t.id)
  const anchorSet = new Set(anchors)

  // BFS join-distance from the anchors; islands sit in the outermost dim band.
  const dist: Record<string, number> = {}
  anchors.forEach((a) => (dist[a] = 0))
  const q = [...anchors]
  while (q.length) {
    const cur = q.shift() as string
    for (const nb of adj[cur] ?? []) {
      if (dist[nb] === undefined) {
        dist[nb] = dist[cur] + 1
        q.push(nb)
      }
    }
  }
  const finite = tables.map((t) => dist[t.id]).filter((d) => d !== undefined)
  const maxDist = finite.length ? Math.max(...finite) : 0
  tables.forEach((t) => {
    if (dist[t.id] === undefined) dist[t.id] = maxDist
  })

  // rank = maxDist − dist → the fact lands center-right, dims fan left; the
  // metric view + config sit one column to the right of the fact.
  const rank: Record<string, number> = {}
  tables.forEach((t) => (rank[t.id] = maxDist - dist[t.id]))
  mvs.forEach((m) => (rank[m.id] = maxDist + 1))

  // Stable order within each rank (authored rank, then order, then id).
  const order: Record<string, number> = {}
  const byRank: Record<number, BlueprintNodeFixture[]> = {}
  s.nodes.forEach((n) => {
    ;(byRank[rank[n.id]] ??= []).push(n)
  })
  Object.values(byRank).forEach((list) => {
    list.sort((a, b) => a.rank - b.rank || a.order - b.order || a.id.localeCompare(b.id))
    list.forEach((n, i) => (order[n.id] = i))
  })

  // Semantic band per rank, for headers.
  const band: Record<number, "dim" | "fact" | "mv"> = {}
  for (const r of new Set(Object.values(rank))) {
    const inR = s.nodes.filter((n) => rank[n.id] === r)
    if (inR.every((n) => n.kind !== "table")) band[r] = "mv"
    else if (inR.some((n) => anchorSet.has(n.id))) band[r] = "fact"
    else band[r] = "dim"
  }
  return { rank, order, band, rolesKnown, mode }
}

// ── Semantic-zoom node heights (§5.5, one COL band §6) ───────────────────────
export function nodeHeight(n: BlueprintNodeFixture, zoom: BlueprintZoom): number {
  if (n.kind === "table") {
    if (zoom === "near") return COL_TOP + n.cols.length * COL_H + COL_PAD
    if (zoom === "far") return 34
    return 52
  }
  if (zoom === "far") return 52
  return 44 + n.measures.length * 22 + 10
}

export function nodeWidth(n: BlueprintNodeFixture): number {
  return CARD_W[n.kind]
}

// ── Adaptive layered layout (§5.1): rank-x from the ranks actually present ───
export function layoutBoxes(
  s: BlueprintScenarioFixture,
  placement: Placement,
  zoom: BlueprintZoom,
): Record<string, Box> {
  const ranks: Record<number, BlueprintNodeFixture[]> = {}
  s.nodes.forEach((n) => {
    ;(ranks[placement.rank[n.id]] ??= []).push(n)
  })
  Object.values(ranks).forEach((list) => list.sort((a, b) => placement.order[a.id] - placement.order[b.id]))
  const rankKeys = Object.keys(ranks).map(Number).sort((a, b) => a - b)
  const xByRank: Record<number, number> = {}
  let x = 40
  rankKeys.forEach((r) => {
    xByRank[r] = x
    x += Math.max(...ranks[r].map(nodeWidth)) + RANK_GUTTER
  })
  const box: Record<string, Box> = {}
  rankKeys.forEach((r) => {
    let y = TOP
    for (const n of ranks[r]) {
      box[n.id] = { x: xByRank[r], y, w: nodeWidth(n), h: nodeHeight(n, zoom) }
      y += box[n.id].h + VGAP
    }
  })
  return box
}

// ── Rank headers — semantic only when a role is proven (§5.11 / §5.12) ───────
export function rankLabel(s: BlueprintScenarioFixture, placement: Placement, r: number): string {
  if (placement.mode === "source") {
    const inRank = s.nodes.filter((n) => placement.rank[n.id] === r)
    if (inRank.every((n) => n.kind !== "table")) return "Metric views · config"
    const tableRanks = [...new Set(s.nodes.filter((n) => n.kind === "table").map((n) => placement.rank[n.id]))].sort(
      (a, b) => a - b,
    )
    return r === tableRanks[0] ? "Tables" : "Joined tables"
  }
  const band = placement.band[r]
  if (band === "mv") return "Metric view · measures"
  if (!placement.rolesKnown) return band === "fact" ? "Tables" : "Related tables"
  return band === "fact" ? "Fact · source" : "Dimensions"
}

// ── Column-accurate attach y (Columns LOD reads the single COL band, §6) ─────
export function colY(n: BlueprintNodeFixture, box: Box, colName: string, zoom: BlueprintZoom): number {
  if (zoom !== "near" || n.kind !== "table") return box.y + box.h / 2
  const i = n.cols.indexOf(colName)
  if (i < 0) return box.y + box.h / 2
  return box.y + COL_TOP + i * COL_H + COL_H / 2
}

// ── Resolved, routed edges (§5.2–§5.4) ───────────────────────────────────────
export interface ResolvedEdge extends BlueprintJoinFixture {
  leftId: string
  rightId: string
  /** True when the authored `from` (the MANY side) is the left node. */
  manyOnLeft: boolean
  sx: number
  sy: number
  dx: number
  dy: number
  midX: number
  hops: number[]
}

/**
 * Orient every join left→right by rank, fan ports (non-Columns zoom),
 * channelize each gutter's verticals, and compute deterministic crossing hops
 * (index order — the lower-index edge's horizontal hops over the other's trunk).
 */
export function resolveEdges(
  s: BlueprintScenarioFixture,
  placement: Placement,
  box: Record<string, Box>,
  zoom: BlueprintZoom,
): ResolvedEdge[] {
  const byId = nodeById(s)
  const resolved: ResolvedEdge[] = s.joins.map((j) => {
    const fromLeft = placement.rank[j.from] <= placement.rank[j.to]
    const leftId = fromLeft ? j.from : j.to
    const rightId = fromLeft ? j.to : j.from
    const leftCol = fromLeft ? j.fromCol : j.toCol
    const rightCol = fromLeft ? j.toCol : j.fromCol
    const lb = box[leftId]
    const rb = box[rightId]
    const sx = lb.x + lb.w
    const sy = colY(byId[leftId], lb, leftCol, zoom)
    const dx = rb.x
    const dy = colY(byId[rightId], rb, rightCol, zoom)
    return { ...j, leftId, rightId, manyOnLeft: fromLeft, sx, sy, dx, dy, midX: Math.round((sx + dx) / 2), hops: [] }
  })

  // Port fanning (§5.2) — spread attach points, ordered by the opposite endpoint.
  if (zoom !== "near") {
    const out: Record<string, ResolvedEdge[]> = {}
    const inc: Record<string, ResolvedEdge[]> = {}
    resolved.forEach((e) => {
      ;(out[e.leftId] ??= []).push(e)
      ;(inc[e.rightId] ??= []).push(e)
    })
    for (const id in out) {
      const b = box[id]
      const list = out[id].sort((a, c) => box[a.rightId].y - box[c.rightId].y)
      list.forEach((e, i) => {
        e.sy = Math.round(b.y + b.h * ((i + 1) / (list.length + 1)))
        e.sx = b.x + b.w
      })
    }
    for (const id in inc) {
      const b = box[id]
      const list = inc[id].sort((a, c) => box[a.leftId].y - box[c.leftId].y)
      list.forEach((e, i) => {
        e.dy = Math.round(b.y + b.h * ((i + 1) / (list.length + 1)))
        e.dx = b.x
      })
    }
  }

  // Channelize: one vertical lane per edge per gutter, ordered by source y.
  const gutters: Record<string, ResolvedEdge[]> = {}
  resolved.forEach((e) => {
    ;(gutters[`${placement.rank[e.leftId]}->${placement.rank[e.rightId]}`] ??= []).push(e)
  })
  Object.values(gutters).forEach((list) => {
    list.sort((a, b) => a.sy - b.sy || a.dy - b.dy)
    list.forEach((e, i) => {
      e.midX = Math.round(e.sx + ((i + 1) / (list.length + 1)) * (e.dx - e.sx))
    })
  })

  computeHops(resolved)
  return resolved
}

/** Crossing hops (§5.3): where another edge's vertical trunk crosses a horizontal leg. */
export function computeHops(edges: ResolvedEdge[]): void {
  edges.forEach((e) => (e.hops = []))
  for (let i = 0; i < edges.length; i++) {
    for (let j = 0; j < edges.length; j++) {
      if (i === j) continue
      const a = edges[i]
      const b = edges[j]
      const vx = b.midX
      const vy0 = Math.min(b.sy, b.dy)
      const vy1 = Math.max(b.sy, b.dy)
      const legs = [
        { y: a.sy, x0: Math.min(a.sx, a.midX), x1: Math.max(a.sx, a.midX) },
        { y: a.dy, x0: Math.min(a.midX, a.dx), x1: Math.max(a.midX, a.dx) },
      ]
      for (const leg of legs) {
        if (vx > leg.x0 + 3 && vx < leg.x1 - 3 && leg.y > vy0 + 3 && leg.y < vy1 - 3) a.hops.push(vx)
      }
    }
  }
}

/** Orthogonal path with rounded elbows; hop arcs on the two horizontal legs. */
export function routePath(sx: number, sy: number, dx: number, dy: number, midX: number, hops: number[]): string {
  const r = Math.min(8, Math.abs(dy - sy) / 2 || 8)
  const sgn = dy >= sy ? 1 : -1
  const HR = 5
  const leftHops = hops.filter((h) => h > sx + 2 && h < midX - r - 2).sort((a, b) => a - b)
  const rightHops = hops.filter((h) => h > midX + r + 2 && h < dx - 2).sort((a, b) => a - b)
  let d = `M ${sx} ${sy}`
  for (const hx of leftHops) d += ` L ${hx - HR} ${sy} A ${HR} ${HR} 0 0 1 ${hx + HR} ${sy}`
  if (Math.abs(dy - sy) < 1) {
    d += ` L ${dx} ${dy}`
    return d
  }
  d += ` L ${midX - r} ${sy}`
  d += ` Q ${midX} ${sy} ${midX} ${sy + sgn * r}`
  d += ` L ${midX} ${dy - sgn * r}`
  d += ` Q ${midX} ${dy} ${midX + r} ${dy}`
  for (const hx of rightHops) d += ` L ${hx - HR} ${dy} A ${HR} ${HR} 0 0 1 ${hx + HR} ${dy}`
  d += ` L ${dx} ${dy}`
  return d
}

// ── Lineage routing (§5.10): gutters + a lane above the cards + bridges ──────
export interface GutterInfo {
  ranks: number[]
  /** rank r → x-center of the gutter to its right. */
  centerBetween: Record<number, number>
}

export function gutterInfo(s: BlueprintScenarioFixture, placement: Placement, box: Record<string, Box>): GutterInfo {
  const ranks = [...new Set(s.nodes.map((n) => placement.rank[n.id]))].sort((a, b) => a - b)
  const right: Record<number, number> = {}
  const left: Record<number, number> = {}
  ranks.forEach((r) => {
    const bs = s.nodes.filter((n) => placement.rank[n.id] === r).map((n) => box[n.id])
    left[r] = Math.min(...bs.map((b) => b.x))
    right[r] = Math.max(...bs.map((b) => b.x + b.w))
  })
  const centerBetween: Record<number, number> = {}
  for (let i = 0; i < ranks.length - 1; i++) centerBetween[ranks[i]] = Math.round((right[ranks[i]] + left[ranks[i + 1]]) / 2)
  return { ranks, centerBetween }
}

export interface HLeg {
  y: number
  x0: number
  x1: number
}

/** Vertical run with a small bridge arc each time it crosses a horizontal leg. */
export function vSegPath(x: number, ya: number, yb: number, legs: HLeg[]): string {
  const dir = yb >= ya ? 1 : -1
  const HR = 5
  const cross = legs
    .filter((l) => l.x0 + 2 < x && x < l.x1 - 2 && Math.min(ya, yb) + HR < l.y && l.y < Math.max(ya, yb) - HR)
    .map((l) => l.y)
    .sort((a, b) => (dir > 0 ? a - b : b - a))
  let d = ""
  for (const hy of cross) d += ` L ${x} ${hy - dir * HR} A ${HR} ${HR} 0 0 1 ${x} ${hy + dir * HR}`
  d += ` L ${x} ${yb}`
  return d
}

export interface LineagePath {
  d: string
  /** "measure" → dashed; "mv" → dotted. */
  mode: "measure" | "mv"
  srcId: string
  sx: number
  sy: number
}

/**
 * Lineage-on-select paths (§5.10): a measure draws dashed paths from each source
 * table to its chip (or its parent card edge in Overview); an MV / Space-config
 * card draws from each origin table to its card. Verticals ride the inter-rank
 * gutters; non-adjacent hops ride a channelized lane above the cards; crossings
 * over join legs are bridged with the vSegPath idiom.
 */
export function lineagePaths(
  s: BlueprintScenarioFixture,
  placement: Placement,
  box: Record<string, Box>,
  resolved: ResolvedEdge[],
  selected: string,
  chipPos: Record<string, { x: number; y: number }>,
): LineagePath[] {
  const byId = nodeById(s)
  const measures = measureIndex(s)
  let mode: "measure" | "mv"
  let destBox: Box | undefined
  let dRank: number
  let srcs: string[] = []
  let destYs: number[] = []

  const m = measures[selected]
  if (m) {
    mode = "measure"
    destBox = box[m.parent]
    dRank = placement.rank[m.parent]
    srcs = m.src.filter((t) => box[t])
    const cp = chipPos[selected]
    const dy = cp ? cp.y : destBox ? destBox.y + destBox.h / 2 : 0
    destYs = srcs.map(() => dy)
  } else {
    const node = byId[selected]
    if (!node || node.kind === "table") return []
    mode = node.kind === "mv" ? "mv" : "measure"
    destBox = box[selected]
    dRank = placement.rank[selected]
    const from = node.kind === "mv" ? (s.uses[selected] ?? []) : [...new Set(node.measures.flatMap((mm) => mm.src))]
    srcs = from.filter((t) => box[t])
    destYs = srcs.map((_, i) => (destBox as Box).y + (destBox as Box).h * ((i + 1) / (srcs.length + 1)))
  }
  if (!srcs.length || !destBox) return []

  const gi = gutterInfo(s, placement, box)
  const rankIdx = (r: number) => gi.ranks.indexOf(r)
  const legs: HLeg[] = []
  resolved.forEach((e) => {
    legs.push({ y: e.sy, x0: Math.min(e.sx, e.midX), x1: Math.max(e.sx, e.midX) })
    legs.push({ y: e.dy, x0: Math.min(e.midX, e.dx), x1: Math.max(e.midX, e.dx) })
  })

  const laneTop = 24
  const laneStep = 7
  return srcs.map((tid, i) => {
    const tb = box[tid]
    const sRank = placement.rank[tid]
    const sx = tb.x + tb.w
    const sy = tb.y + tb.h / 2
    const dx = (destBox as Box).x
    const dy = destYs[i]
    const upX = gi.centerBetween[sRank]
    const prevOfDest = gi.ranks[rankIdx(dRank) - 1]
    const downX0 = gi.centerBetween[prevOfDest]
    const adjacent = rankIdx(dRank) - rankIdx(sRank) === 1
    let d: string
    if (adjacent || upX === downX0 || downX0 == null) {
      d = `M ${sx} ${sy} L ${upX} ${sy}` + vSegPath(upX, sy, dy, legs) + ` L ${dx} ${dy}`
    } else {
      const laneY = laneTop + (srcs.length - 1 - i) * laneStep
      const downX = Math.round(downX0 + (i - (srcs.length - 1) / 2) * 6)
      d =
        `M ${sx} ${sy} L ${upX} ${sy}` +
        vSegPath(upX, sy, laneY, legs) +
        ` L ${downX} ${laneY}` +
        vSegPath(downX, laneY, dy, legs) +
        ` L ${dx} ${dy}`
    }
    return { d, mode, srcId: tid, sx, sy }
  })
}

// ── Selection neighbourhood (focus + context) ────────────────────────────────
export function neighbourhood(s: BlueprintScenarioFixture, selected: string | null): Set<string> | null {
  if (!selected) return null
  const byId = nodeById(s)
  const measures = measureIndex(s)
  const m = measures[selected]
  if (m) return new Set([m.parent, ...m.src])
  const keep = new Set([selected])
  for (const j of s.joins) {
    if (j.from === selected) keep.add(j.to)
    if (j.to === selected) keep.add(j.from)
  }
  if (byId[selected]?.kind === "mv") (s.uses[selected] ?? []).forEach((t) => keep.add(t))
  for (const mv in s.uses) if (s.uses[mv].includes(selected)) keep.add(mv)
  return keep
}

// ── Health headline counts (§5.7) ────────────────────────────────────────────
export interface HeadlineCounts {
  governed: number
  curated: number
  ungoverned: number
  unmodeled: number
  cold: number
}

export function headlineCounts(s: BlueprintScenarioFixture): HeadlineCounts {
  const all = s.nodes.flatMap((n) => (n.kind === "table" ? [] : n.measures))
  return {
    governed: all.filter((m) => m.gov === "governed").length,
    curated: all.filter((m) => m.gov === "curated").length,
    ungoverned: all.filter((m) => m.gov === "ungoverned").length,
    unmodeled: s.nodes.filter((n) => n.kind === "table" && n.unmodeled).length,
    cold: s.nodes.filter((n) => n.kind === "table" && n.cold).length,
  }
}

export function govColor(g: BlueprintMeasureFixture["gov"]): string {
  return g === "governed" ? "var(--color-success)" : g === "curated" ? "var(--color-warning)" : "var(--color-danger)"
}

export function onStr(j: BlueprintJoinFixture): string {
  return `${j.to}.${j.toCol} = ${j.from}.${j.fromCol}`
}
