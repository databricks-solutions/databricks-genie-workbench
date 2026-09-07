/**
 * Estate-graph view model (Phase 3e / Ontology Map, MV-D72). Pure, side-effect-free
 * transforms that turn the backend `OntologyGraph` into cytoscape elements for one of
 * three levels of detail (LOD): Domains (aggregate), Sub-domains (compound containers +
 * sub-domain nodes), and Assets (compound containers + capped asset nodes). Kept out of
 * `EstateGraph.tsx` so the component file only exports the component (react-refresh) and
 * so the element builder is unit-testable without mounting cytoscape.
 */
import type { OntologyGraph, OntologyGraphNode } from "@/ontology/types"

export type Lod = "domains" | "subdomains" | "assets"

export interface CyEl {
  group: "nodes" | "edges"
  data: Record<string, unknown>
  // Deterministic seed coordinate (leaf nodes only). Feeds fcose `randomize:false`
  // so the map is stable across renders AND page loads (mental-map preservation),
  // while physics still refines within/between compound boxes (Group-in-a-Box).
  position?: { x: number; y: number }
}

// Stable dark-friendly palette; a top-domain always hashes to the same hue so colour
// reads consistently across LODs and re-renders. Ungrouped gets a neutral slate.
export const PALETTE = [
  "#818CF8", "#6EE7B7", "#FCD34D", "#F472B6", "#22D3EE", "#A78BFA",
  "#FB923C", "#34D399", "#60A5FA", "#F87171", "#C084FC", "#2DD4BF",
]
export const UNGROUPED_COLOR = "#64748B"

export function colorForTop(topId: string): string {
  let h = 0
  for (let i = 0; i < topId.length; i++) h = (h * 31 + topId.charCodeAt(i)) >>> 0
  return PALETTE[h % PALETTE.length]
}

/** Asset fill = its domain colour; metric views / agents keep a recognizable accent. */
export function assetColor(kind: string, domainColor: string): string {
  if (kind === "metric_view") return "#22D3EE"
  if (kind === "agent") return "#A78BFA"
  return domainColor
}

function edgeType(kind: string): "lineage" | "coquery" | "other" {
  if (/quer|co/i.test(kind)) return "coquery"
  if (/line/i.test(kind)) return "lineage"
  return "other"
}

export interface TopDomain {
  id: string
  name: string
  color: string
  memberCount: number
  subIds: string[]
  ungrouped: boolean
}

/**
 * Fold the domain rollup nodes into their top-level domains. A rollup node with a
 * `parent_id` is a Sub-Domain (its top is the parent); one without is itself a top.
 * `member_count` accumulates so a top's size reflects its whole subtree (MV-D71).
 */
export function groupTops(nodes: OntologyGraphNode[]): Map<string, TopDomain> {
  const tops = new Map<string, TopDomain>()
  for (const d of nodes) {
    const ungrouped = d.kind === "ungrouped" || d.id === "ungrouped"
    const topId = d.parent_id || d.id
    const topName = d.parent_id ? d.parent_name || d.parent_id : d.label
    let t = tops.get(topId)
    if (!t) {
      t = {
        id: topId,
        name: ungrouped ? "Ungrouped" : topName,
        color: ungrouped ? UNGROUPED_COLOR : colorForTop(topId),
        memberCount: 0,
        subIds: [],
        ungrouped,
      }
      tops.set(topId, t)
    }
    t.memberCount += d.member_count ?? 0
    if (d.parent_id) t.subIds.push(d.id)
  }
  return tops
}

export interface BuildOpts {
  perContainerCap?: number
}

/** Round-sized px from a member/asset weight, clamped for legibility. */
function sizePx(weight: number, base: number, mult: number, max: number): number {
  return base + Math.min(Math.sqrt(Math.max(weight, 0)), max) * mult
}

/** Deterministic 32-bit hash of an id → stable seed offsets (no RNG, MV-D72). */
function hashId(id: string): number {
  let h = 0
  for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) >>> 0
  return h
}

/** Point on a circle of radius `r` around (cx,cy) at slot i/n. */
function onRing(cx: number, cy: number, r: number, n: number, i: number): { x: number; y: number } {
  const a = (2 * Math.PI * i) / Math.max(n, 1)
  return { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) }
}

/**
 * Deterministic seed point near a center, keyed by id. Gives fcose a stable,
 * group-clustered initial draft so `randomize:false` yields the same map every
 * load, while physics still refines within/between compound boxes.
 */
function seedNear(center: { x: number; y: number }, id: string, spread: number): { x: number; y: number } {
  const h = hashId(id)
  const a = (2 * Math.PI * (h % 997)) / 997
  const r = spread * (0.35 + (0.65 * ((h >>> 10) % 1000)) / 1000)
  return { x: center.x + r * Math.cos(a), y: center.y + r * Math.sin(a) }
}

/**
 * Build the cytoscape element list for one LOD, with a deterministic seed
 * `position` on every leaf node (Group-in-a-Box seed for fcose `randomize:false`).
 *
 * - `domains`: one hub node per top-level domain, sized by subtree member count;
 *   domain-rollup edges aggregated to the top level (self-loops dropped, deduped).
 *   Ignores `focusTop` — Domains is the aggregate overview.
 * - `subdomains`: a compound container per top domain (`top:<id>`) with its
 *   Sub-Domain child nodes nested (`data.parent`); a top with no subs gets one
 *   self-node so its container is never empty.
 * - `assets`: a THREE-level compound hierarchy — top container (`top:<id>`) →
 *   sub-domain container (`sub:<id>`) → asset — so the sub-domain grouping is
 *   preserved when you drill in. Capped per group (top-N by size) with a "+N more"
 *   chip. Edges are emitted only between nodes actually present (no dangling edge).
 *
 * `focusTop`, when set, scopes Sub-domains/Assets to that one top domain (drill-down).
 */
export function buildElements(
  graph: OntologyGraph,
  lod: Lod,
  focusTop: string | null,
  opts: BuildOpts = {},
): CyEl[] {
  const cap = opts.perContainerCap ?? 60
  const domainNodes = graph.domains.nodes
  const tops = groupTops(domainNodes)
  const domainById = new Map(domainNodes.map((d) => [d.id, d]))
  const topOfDomainId = (id?: string | null): string | null => {
    if (!id) return null
    const d = domainById.get(id)
    if (!d) return null
    return d.parent_id || d.id
  }
  const inFocus = (topId: string) => !focusTop || topId === focusTop

  // Deterministic top centers (stable order by id) for the Group-in-a-Box seed.
  const topOrder = [...tops.keys()].sort()
  const topIndex = new Map(topOrder.map((id, i) => [id, i]))
  const rTop = 240 + topOrder.length * 22
  const topCenter = (topId: string) => onRing(0, 0, rTop, topOrder.length, topIndex.get(topId) ?? 0)

  const els: CyEl[] = []
  // Track exactly which node ids were emitted so edges can never reference a node
  // that was filtered out (the "nonexistent source" crash guard).
  const emitted = new Set<string>()
  const pushNode = (data: Record<string, unknown>, position?: { x: number; y: number }) => {
    emitted.add(String(data.id))
    els.push(position ? { group: "nodes", data, position } : { group: "nodes", data })
  }

  // -------- Domains: aggregate hubs, no nesting. Ignores focusTop (overview). --------
  if (lod === "domains") {
    for (const t of tops.values()) {
      pushNode(
        {
          id: t.id, label: t.name, ntype: "domain", color: t.color,
          count: t.memberCount, px: sizePx(t.memberCount, 30, 9, 8),
        },
        topCenter(t.id),
      )
    }
    const seen = new Set<string>()
    for (const e of graph.domains.edges) {
      const s = topOfDomainId(e.src) ?? e.src
      const d = topOfDomainId(e.dst) ?? e.dst
      if (s === d || !emitted.has(s) || !emitted.has(d)) continue
      const key = s < d ? `${s}|${d}` : `${d}|${s}`
      if (seen.has(key)) continue
      seen.add(key)
      els.push({ group: "edges", data: { id: `de_${key}`, source: s, target: d, etype: edgeType(e.kind) } })
    }
    return els
  }

  // -------- Sub-domains + Assets: compound containers --------
  const usedTops = new Set<string>()

  if (lod === "subdomains") {
    for (const d of domainNodes) {
      const top = d.parent_id || d.id
      if (!inFocus(top) || !tops.has(top)) continue
      if (!d.parent_id) continue // tops handled below as self-nodes
      usedTops.add(top)
      pushNode(
        {
          id: d.id, parent: `top:${top}`, label: d.label, ntype: "subdomain",
          color: tops.get(top)!.color, count: d.member_count ?? 0,
          px: sizePx(d.member_count ?? 1, 16, 6, 6),
        },
        seedNear(topCenter(top), d.id, 90),
      )
    }
    // A top with no sub-domains still shows one leaf so its container renders.
    for (const t of tops.values()) {
      if (!inFocus(t.id) || t.subIds.length > 0) continue
      usedTops.add(t.id)
      pushNode(
        {
          id: `self:${t.id}`, parent: `top:${t.id}`, label: t.name, ntype: "subdomain",
          color: t.color, count: t.memberCount, px: sizePx(t.memberCount || 1, 16, 6, 6),
        },
        seedNear(topCenter(t.id), `self:${t.id}`, 40),
      )
    }
    for (const e of graph.domains.edges) {
      if (!emitted.has(e.src) || !emitted.has(e.dst)) continue
      els.push({ group: "edges", data: { id: `se_${e.src}__${e.dst}`, source: e.src, target: e.dst, etype: edgeType(e.kind) } })
    }
    prependContainers(els, tops, usedTops)
    return els
  }

  // -------- Assets: three-level compound hierarchy (top → sub → asset) --------
  const perGroup = new Map<string, OntologyGraphNode[]>()
  const groupMeta = new Map<string, { topId: string; subId: string | null }>()
  for (const a of graph.assets.nodes) {
    const d = a.domain_id ? domainById.get(a.domain_id) : undefined
    let topId: string
    let subId: string | null
    if (d?.parent_id) {
      subId = d.id
      topId = d.parent_id
    } else if (d) {
      subId = null
      topId = d.id
    } else {
      subId = null
      topId = "ungrouped"
    }
    if (!inFocus(topId) || !tops.has(topId)) continue
    const gkey = subId ? `sub:${subId}` : `top:${topId}`
    const list = perGroup.get(gkey)
    if (list) list.push(a)
    else perGroup.set(gkey, [a])
    groupMeta.set(gkey, { topId, subId })
  }

  const usedSubs = new Set<string>()
  for (const [gkey, list] of perGroup) {
    const { topId, subId } = groupMeta.get(gkey)!
    usedTops.add(topId)
    if (subId) usedSubs.add(subId)
    const color = tops.get(topId)!.color
    const topName = tops.get(topId)!.name
    // Each sub gets a deterministic center within its top; direct-to-top assets
    // seed around the top center itself.
    const groupCenter = subId ? seedNear(topCenter(topId), subId, 130) : topCenter(topId)
    const sorted = [...list].sort((x, y) => (y.size ?? 0) - (x.size ?? 0))
    const shown = sorted.slice(0, cap)
    shown.forEach((a, i) => {
      pushNode(
        {
          id: a.id, parent: gkey, label: a.label, ntype: "asset", kind: a.kind,
          color: assetColor(a.kind, color), px: 8 + Math.min(a.size ?? 1, 3) * 5,
          cost: a.cost ?? null, domainName: topName,
        },
        onRing(groupCenter.x, groupCenter.y, 22 + shown.length * 1.4, shown.length, i),
      )
    })
    if (sorted.length > cap) {
      pushNode(
        { id: `more:${gkey}`, parent: gkey, label: `+${sorted.length - cap} more`, ntype: "more", color, px: 12 },
        seedNear(groupCenter, `more:${gkey}`, 18),
      )
    }
  }
  // Sub-domain containers (nested inside their top container) — this is what
  // preserves the sub-domain grouping at the Assets LOD.
  const subContainers: CyEl[] = []
  for (const subId of usedSubs) {
    const sub = domainById.get(subId)
    const topId = sub?.parent_id ?? "ungrouped"
    subContainers.push({
      group: "nodes",
      data: {
        id: `sub:${subId}`, parent: `top:${topId}`, label: sub?.label ?? subId,
        ntype: "subcontainer", color: tops.get(topId)?.color ?? UNGROUPED_COLOR,
        count: sub?.member_count ?? 0,
      },
    })
  }
  // Asset edges among visible assets only (emitted-only guard = no dangling edge).
  for (const e of graph.assets.edges) {
    if (!emitted.has(e.src) || !emitted.has(e.dst)) continue
    els.push({
      group: "edges",
      data: { id: `ae_${e.src}__${e.dst}`, source: e.src, target: e.dst, etype: edgeType(e.kind), w: 1 + Math.min(e.weight ?? 0, 4) },
    })
  }
  els.unshift(...subContainers)
  prependContainers(els, tops, usedTops)
  return els
}

/** Insert compound container parent nodes for exactly the tops that have children. */
function prependContainers(els: CyEl[], tops: Map<string, TopDomain>, usedTops: Set<string>) {
  const containers: CyEl[] = []
  for (const topId of usedTops) {
    const t = tops.get(topId)
    if (!t) continue
    containers.push({
      group: "nodes",
      data: { id: `top:${t.id}`, label: t.name, ntype: "container", color: t.color, count: t.memberCount },
    })
  }
  els.unshift(...containers)
}

export interface NodeFacts {
  title: string
  chip: string
  lines: string[]
  drillTopId: string | null
}

/**
 * Plain-language facts for the detail popover. Zero jargon (MV-D23) — no ids, no SQL.
 * Returns a `drillTopId` when the node is a domain/container the user can open into.
 */
export function nodeFacts(data: Record<string, unknown>): NodeFacts {
  const ntype = String(data.ntype ?? "")
  const label = String(data.label ?? "")
  const count = typeof data.count === "number" ? data.count : null
  const cost = typeof data.cost === "number" ? data.cost : null
  const lines: string[] = []

  if (ntype === "domain" || ntype === "container") {
    if (count != null) lines.push(`${count} ${count === 1 ? "asset" : "assets"} in this business area`)
    const id = ntype === "container" ? String(data.id).replace(/^top:/, "") : String(data.id)
    return { title: label, chip: "Business area", lines, drillTopId: id }
  }
  if (ntype === "subdomain" || ntype === "subcontainer") {
    if (count != null) lines.push(`${count} ${count === 1 ? "asset" : "assets"} grouped here`)
    return { title: label, chip: "Sub-area", lines, drillTopId: null }
  }
  if (ntype === "more") {
    return { title: label, chip: "More assets", lines: ["Zoom in or open this business area to see the rest."], drillTopId: null }
  }
  // asset
  const kind = String(data.kind ?? "table")
  const kindLabel = kind === "metric_view" ? "Metric view" : kind === "agent" ? "Genie agent" : kind === "view" ? "View" : "Table"
  const domainName = data.domainName ? String(data.domainName) : null
  if (domainName) lines.push(`In ${domainName}`)
  if (cost != null && cost > 0) lines.push(`About $${cost >= 1000 ? `${(cost / 1000).toFixed(1)}k` : cost.toFixed(0)} / month`)
  return { title: label, chip: kindLabel, lines, drillTopId: null }
}
