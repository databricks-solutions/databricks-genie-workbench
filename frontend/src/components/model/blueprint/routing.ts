/**
 * Semantic Blueprint (v4) — orthogonal routing + lineage (§5.2–§5.4, §5.10).
 *
 * Orient every join left→right by rank, fan ports, channelize each gutter's
 * verticals, and bridge crossings with index-stable hop arcs; the same
 * gutter/lane/bridge discipline routes measure/MV lineage on select. Pure
 * geometry of the placed boxes — deterministic, no mutation of the model (§8).
 */
import type { BlueprintJoin, BlueprintModel } from "./model"
import { colY, measureIndex, nodeById, type Box, type BlueprintZoom, type Placement } from "./layout"

export interface ResolvedEdge extends BlueprintJoin {
  leftId: string
  rightId: string
  /** True when the MANY end is the `(sx,sy)` endpoint (the `leftId` node). */
  manyOnLeft: boolean
  sx: number
  sy: number
  dx: number
  dy: number
  midX: number
  hops: number[]
  /** Same-rank (same-column) join — routed as a side bracket, not a gutter leg. */
  intra?: boolean
}

/** Which endpoint is the proven many side (falls back to author `from`). */
function manyIdOf(j: BlueprintJoin): string {
  return j.manyEnd === "to" ? j.to : j.from
}

export function resolveEdges(
  m: BlueprintModel,
  placement: Placement,
  box: Record<string, Box>,
  zoom: BlueprintZoom,
): ResolvedEdge[] {
  const byId = nodeById(m)
  const inter: ResolvedEdge[] = []
  const intra: ResolvedEdge[] = []

  m.joins.forEach((j) => {
    const rf = placement.rank[j.from]
    const rt = placement.rank[j.to]
    if (rf === rt) {
      // Intra-rank: both cards share a column (e.g. fact↔fact after fact-center
      // re-ranking). A left→right gutter leg is degenerate here (sx>dx, foot in
      // empty space), so route a side bracket on their facing edges and land the
      // crow's-foot on the proven many end. Geometry finalized below.
      const upperId = box[j.from].y <= box[j.to].y ? j.from : j.to
      const lowerId = upperId === j.from ? j.to : j.from
      intra.push({
        ...j,
        leftId: upperId,
        rightId: lowerId,
        manyOnLeft: manyIdOf(j) === upperId,
        sx: 0,
        sy: 0,
        dx: 0,
        dy: 0,
        midX: 0,
        hops: [],
        intra: true,
      })
      return
    }
    const fromLeft = rf < rt
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
    inter.push({
      ...j,
      leftId,
      rightId,
      manyOnLeft: manyIdOf(j) === leftId,
      sx,
      sy,
      dx,
      dy,
      midX: Math.round((sx + dx) / 2),
      hops: [],
    })
  })

  // Port fanning (§5.2) — spread attach points, ordered by the opposite endpoint.
  if (zoom !== "near") {
    const out: Record<string, ResolvedEdge[]> = {}
    const inc: Record<string, ResolvedEdge[]> = {}
    inter.forEach((e) => {
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
  inter.forEach((e) => {
    ;(gutters[`${placement.rank[e.leftId]}->${placement.rank[e.rightId]}`] ??= []).push(e)
  })
  Object.values(gutters).forEach((list) => {
    list.sort((a, b) => a.sy - b.sy || a.dy - b.dy)
    list.forEach((e, i) => {
      e.midX = Math.round(e.sx + ((i + 1) / (list.length + 1)) * (e.dx - e.sx))
    })
  })

  computeHops(inter)
  resolveIntra(intra, placement, box)
  return [...inter, ...intra]
}

// Intra-rank bracket geometry (§5.3, ERD): both ends attach to the SAME facing
// edge of the column and bow into the adjacent gutter, so the connector reads as
// one clean bracket and the crow's-foot sits ON the line. Leftmost column bows
// right (into its right gutter); every other column bows left (into the wide
// rank gutter to its left). Multiple brackets in a column stagger so they never
// overlap. Deterministic — pure geometry of the placed boxes.
function resolveIntra(intra: ResolvedEdge[], placement: Placement, box: Record<string, Box>): void {
  if (!intra.length) return
  const ranks = [...new Set(Object.values(placement.rank))].sort((a, b) => a - b)
  const minRank = ranks[0]
  const byRank: Record<number, ResolvedEdge[]> = {}
  intra.forEach((e) => {
    ;(byRank[placement.rank[e.leftId]] ??= []).push(e)
  })
  for (const r of Object.keys(byRank).map(Number)) {
    const list = byRank[r].sort(
      (a, b) => Math.min(box[a.leftId].y, box[a.rightId].y) - Math.min(box[b.leftId].y, box[b.rightId].y),
    )
    const bowRight = r === minRank
    list.forEach((e, i) => {
      const ub = box[e.leftId] // upper (leftId is the upper card, by construction)
      const lb = box[e.rightId] // lower
      const gap = 24 + i * 16
      if (bowRight) {
        e.sx = ub.x + ub.w
        e.dx = lb.x + lb.w
        e.midX = ub.x + ub.w + gap
      } else {
        e.sx = ub.x
        e.dx = lb.x
        e.midX = ub.x - gap
      }
      e.sy = Math.round(ub.y + ub.h / 2)
      e.dy = Math.round(lb.y + lb.h / 2)
    })
  }
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
  centerBetween: Record<number, number>
}

export function gutterInfo(m: BlueprintModel, placement: Placement, box: Record<string, Box>): GutterInfo {
  const ranks = [...new Set(m.nodes.map((n) => placement.rank[n.id]))].sort((a, b) => a - b)
  const right: Record<number, number> = {}
  const left: Record<number, number> = {}
  ranks.forEach((r) => {
    const bs = m.nodes.filter((n) => placement.rank[n.id] === r).map((n) => box[n.id])
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

export function lineagePaths(
  m: BlueprintModel,
  placement: Placement,
  box: Record<string, Box>,
  resolved: ResolvedEdge[],
  selected: string,
  chipPos: Record<string, { x: number; y: number }>,
): LineagePath[] {
  const byId = nodeById(m)
  const measures = measureIndex(m)
  let mode: "measure" | "mv"
  let destBox: Box | undefined
  let dRank: number
  let srcs: string[] = []
  let destYs: number[] = []

  const ms = measures[selected]
  if (ms) {
    mode = "measure"
    destBox = box[ms.parent]
    dRank = placement.rank[ms.parent]
    srcs = ms.src.filter((t) => box[t])
    const cp = chipPos[selected]
    const dy = cp ? cp.y : destBox ? destBox.y + destBox.h / 2 : 0
    destYs = srcs.map(() => dy)
  } else {
    const node = byId[selected]
    if (!node || node.kind === "table") return []
    mode = node.kind === "mv" ? "mv" : "measure"
    destBox = box[selected]
    dRank = placement.rank[selected]
    const from = node.kind === "mv" ? (m.uses[selected] ?? []) : [...new Set(node.measures.flatMap((mm) => mm.src))]
    srcs = from.filter((t) => box[t])
    destYs = srcs.map((_, i) => (destBox as Box).y + (destBox as Box).h * ((i + 1) / (srcs.length + 1)))
  }
  if (!srcs.length || !destBox) return []

  const gi = gutterInfo(m, placement, box)
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
