/**
 * Semantic Blueprint (v4) — deterministic layout (§5.1, §5.5, §5.12, §6).
 *
 * Fact-center re-ranking, adaptive layered placement (rank-x from the ranks
 * actually present + widest card, no fixed COL_X), semantic-zoom heights, the
 * single COL_TOP/COL_H/COL_PAD column band, and honest rank/band headers.
 * Every function is a PURE function of the model (+ a zoom band / layout mode)
 * and mutates nothing — the byte-stable render contract (§8). Production
 * extraction of the gated reference in mockups/blueprintMath.ts.
 */
import type { BlueprintModel, BlueprintNode, BlueprintTable } from "./model"

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
  rank: Record<string, number>
  order: Record<string, number>
  band: Record<number, "dim" | "fact" | "mv">
  rolesKnown: boolean
  mode: BlueprintLayoutMode
}

export interface IndexedMeasure {
  id: string
  name: string
  gov: "governed" | "curated" | "ungoverned"
  expr: string
  src: string[]
  overlaps?: string
  parent: string
  parentKind: "mv" | "config"
}

export function measureIndex(m: BlueprintModel): Record<string, IndexedMeasure> {
  const out: Record<string, IndexedMeasure> = {}
  for (const n of m.nodes) {
    if (n.kind === "table") continue
    for (const ms of n.measures) {
      out[`${n.id}::${ms.name}`] = { ...ms, id: `${n.id}::${ms.name}`, parent: n.id, parentKind: n.kind }
    }
  }
  return out
}

export function nodeById(m: BlueprintModel): Record<string, BlueprintNode> {
  return Object.fromEntries(m.nodes.map((n) => [n.id, n]))
}

// ── Fact-center re-ranking (§5.12) — never asserts a role it can't prove ─────
export function derivePlacement(m: BlueprintModel, mode: BlueprintLayoutMode): Placement {
  const rolesKnown = m.nodes.some((n) => n.kind === "table" && n.role)
  if (mode === "source") {
    const rank: Record<string, number> = {}
    const order: Record<string, number> = {}
    for (const n of m.nodes) {
      rank[n.id] = n.rank
      order[n.id] = n.order
    }
    return { rank, order, band: {}, rolesKnown, mode }
  }

  const tables = m.nodes.filter((n): n is BlueprintTable => n.kind === "table")
  const mvs = m.nodes.filter((n) => n.kind !== "table")

  const adj: Record<string, string[]> = {}
  tables.forEach((t) => (adj[t.id] = []))
  m.joins.forEach((j) => {
    if (adj[j.from] && adj[j.to]) {
      adj[j.from].push(j.to)
      adj[j.to].push(j.from)
    }
  })
  const refCount: Record<string, number> = {}
  for (const n of m.nodes) {
    if (n.kind === "table") continue
    n.measures.forEach((ms) => ms.src.forEach((t) => (refCount[t] = (refCount[t] ?? 0) + 1)))
  }

  let anchors: string[]
  const roleFacts = tables.filter((t) => (t.role ?? "").toUpperCase() === "FACT")
  if (roleFacts.length) anchors = roleFacts.map((t) => t.id)
  else if (m.joins.length) {
    const ranked = [...tables].sort(
      (a, b) => (refCount[b.id] ?? 0) - (refCount[a.id] ?? 0) || adj[b.id].length - adj[a.id].length,
    )
    const topRef = refCount[ranked[0].id] ?? 0
    anchors = topRef > 0 ? tables.filter((t) => (refCount[t.id] ?? 0) === topRef).map((t) => t.id) : [ranked[0].id]
  } else anchors = tables.map((t) => t.id)
  const anchorSet = new Set(anchors)

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

  const rank: Record<string, number> = {}
  tables.forEach((t) => (rank[t.id] = maxDist - dist[t.id]))
  mvs.forEach((mv) => (rank[mv.id] = maxDist + 1))

  const order: Record<string, number> = {}
  const byRank: Record<number, BlueprintNode[]> = {}
  m.nodes.forEach((n) => {
    ;(byRank[rank[n.id]] ??= []).push(n)
  })
  Object.values(byRank).forEach((list) => {
    list.sort((a, b) => a.rank - b.rank || a.order - b.order || a.id.localeCompare(b.id))
    list.forEach((n, i) => (order[n.id] = i))
  })

  const band: Record<number, "dim" | "fact" | "mv"> = {}
  for (const r of new Set(Object.values(rank))) {
    const inR = m.nodes.filter((n) => rank[n.id] === r)
    if (inR.every((n) => n.kind !== "table")) band[r] = "mv"
    else if (inR.some((n) => anchorSet.has(n.id))) band[r] = "fact"
    else band[r] = "dim"
  }
  return { rank, order, band, rolesKnown, mode }
}

// ── Semantic-zoom node heights (§5.5, one COL band §6) ───────────────────────
export function nodeHeight(n: BlueprintNode, zoom: BlueprintZoom): number {
  if (n.kind === "table") {
    if (zoom === "near") return COL_TOP + n.cols.length * COL_H + COL_PAD
    if (zoom === "far") return 34
    return 52
  }
  if (zoom === "far") return 52
  return 44 + n.measures.length * 22 + 10
}

export function nodeWidth(n: BlueprintNode): number {
  return CARD_W[n.kind]
}

// ── Adaptive layered layout (§5.1): rank-x from the ranks actually present ───
export function layoutBoxes(m: BlueprintModel, placement: Placement, zoom: BlueprintZoom): Record<string, Box> {
  const ranks: Record<number, BlueprintNode[]> = {}
  m.nodes.forEach((n) => {
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
export function rankLabel(m: BlueprintModel, placement: Placement, r: number): string {
  if (placement.mode === "source") {
    const inRank = m.nodes.filter((n) => placement.rank[n.id] === r)
    if (inRank.every((n) => n.kind !== "table")) return "Metric views · config"
    const tableRanks = [...new Set(m.nodes.filter((n) => n.kind === "table").map((n) => placement.rank[n.id]))].sort(
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
export function colY(n: BlueprintNode, box: Box, colName: string, zoom: BlueprintZoom): number {
  if (zoom !== "near" || n.kind !== "table") return box.y + box.h / 2
  const i = n.cols.indexOf(colName)
  if (i < 0) return box.y + box.h / 2
  return box.y + COL_TOP + i * COL_H + COL_H / 2
}
