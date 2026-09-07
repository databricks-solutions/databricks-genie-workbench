/**
 * Estate-graph view model (Phase 3e / Ontology Map, MV-D72). Pure, side-effect-free
 * transforms that turn the backend `OntologyGraph` into cytoscape elements for one of
 * three levels of detail (LOD): Domains (aggregate), Sub-domains (compound containers +
 * sub-domain nodes), and Assets (compound containers + capped asset nodes). Kept out of
 * `EstateGraph.tsx` so the component file only exports the component (react-refresh) and
 * so the element builder is unit-testable without mounting cytoscape.
 */
import type { OntologyGraph, OntologyGraphExpand, OntologyGraphNode } from "@/ontology/types"

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
  // Provenance (MV-D74): "applied" (governed-tag current state) vs "proposed" (engine
  // cluster → rendered dashed + "Suggested"). Taken from the top-level node; a top seen
  // only through its children inherits the first child's origin until the top appears.
  origin: string | null
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
        origin: d.origin ?? null,
      }
      tops.set(topId, t)
    }
    // The actual top-level node (no parent_id) is authoritative for the top's origin.
    if (!d.parent_id && d.origin != null) t.origin = d.origin
    t.memberCount += d.member_count ?? 0
    if (d.parent_id) t.subIds.push(d.id)
  }
  return tops
}

export interface BuildOpts {
  perContainerCap?: number
}

/**
 * Trim the word-prefix every top-level domain name shares ("Alaska Airlines …")
 * from the CANVAS captions only — the estate reads "Commercial / Operations /
 * Maintenance And Engineering" instead of four labels that all start with the
 * company name. Presentation-only: `label` (inspector, breadcrumb, search) keeps
 * the full name. Trims only when 2+ names share a full leading word sequence and
 * every trimmed remainder is non-empty.
 */
export function trimCommonPrefix(names: string[]): Map<string, string> {
  const out = new Map(names.map((n) => [n, n]))
  if (names.length < 2) return out
  const split = names.map((n) => n.split(" "))
  const first = split[0]
  let prefixLen = 0
  for (let i = 0; i < first.length; i++) {
    const word = first[i]
    if (!split.every((w) => i < w.length && w[i] === word)) break
    prefixLen = i + 1
  }
  // Trim only when every name keeps at least one word of its own — if any name IS
  // the shared prefix, trimming would erase its identity, so leave all untouched.
  if (prefixLen === 0 || !split.every((w) => w.length > prefixLen)) return out
  for (let k = 0; k < names.length; k++) {
    out.set(names[k], split[k].slice(prefixLen).join(" "))
  }
  return out
}

/** 1,911-style count for labels (Map v3 §1D — captions read at a glance). */
export function fmtCount(n: number): string {
  return n.toLocaleString("en-US")
}

/**
 * Two-line hub caption (Map v3 §1D): the business-area name plus a plain-language
 * count line, rendered by cytoscape as a wrapped multi-line label. Kept separate from
 * `label` so search and the inspector keep the clean name.
 */
export function domainDisplay(name: string, memberCount: number, ungrouped: boolean): string {
  const noun = ungrouped ? (memberCount === 1 ? "table" : "tables") : memberCount === 1 ? "asset" : "assets"
  if (memberCount <= 0) return name
  return `${name}\n${fmtCount(memberCount)} ${noun}`
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

  // Canvas display names: shared word-prefix trimmed across the real tops
  // ("Alaska Airlines Commercial" → "Commercial"); Ungrouped stays as-is.
  const realTopNames = [...tops.values()].filter((t) => !t.ungrouped).map((t) => t.name)
  const shortNames = trimCommonPrefix(realTopNames)
  const shortName = (t: TopDomain) => (t.ungrouped ? t.name : shortNames.get(t.name) ?? t.name)

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
          origin: t.origin,
          display: domainDisplay(shortName(t), t.memberCount, t.ungrouped),
          isUngrouped: t.ungrouped,
          subNames: t.subIds.map((id) => domainById.get(id)?.label).filter((n): n is string => !!n),
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
          origin: d.origin ?? tops.get(top)!.origin,
          isUngrouped: tops.get(top)!.ungrouped,
          domainShort: shortName(tops.get(top)!),
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
          origin: t.origin,
          isUngrouped: t.ungrouped,
        },
        seedNear(topCenter(t.id), `self:${t.id}`, 40),
      )
    }
    for (const e of graph.domains.edges) {
      if (!emitted.has(e.src) || !emitted.has(e.dst)) continue
      els.push({ group: "edges", data: { id: `se_${e.src}__${e.dst}`, source: e.src, target: e.dst, etype: edgeType(e.kind) } })
    }
    prependContainers(els, tops, usedTops, shortName, (id) => domainById.get(id)?.label ?? null)
    return els
  }

  // -------- Assets: three-level compound hierarchy (top → sub → asset) --------
  // Connectivity facts for the inspector (Map v3 §1D — data already in the snapshot,
  // revealed not invented): each asset's degree across the WHOLE estate + its top
  // linked assets by edge weight. Deterministic (weight desc, then id asc).
  const labelByAsset = new Map(graph.assets.nodes.map((a) => [a.id, a.label]))
  const neighbors = new Map<string, { id: string; w: number }[]>()
  const addNeighbor = (a: string, b: string, w: number) => {
    const list = neighbors.get(a)
    if (list) list.push({ id: b, w })
    else neighbors.set(a, [{ id: b, w }])
  }
  for (const e of graph.assets.edges) {
    const w = e.weight ?? 0
    addNeighbor(e.src, e.dst, w)
    addNeighbor(e.dst, e.src, w)
  }
  const topLinks = (id: string): string[] => {
    const list = neighbors.get(id)
    if (!list) return []
    return [...list]
      .sort((x, y) => y.w - x.w || (x.id < y.id ? -1 : 1))
      .slice(0, 3)
      .map((n) => labelByAsset.get(n.id))
      .filter((l): l is string => !!l)
  }

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
    const subName = subId ? domainById.get(subId)?.label ?? null : null
    shown.forEach((a, i) => {
      pushNode(
        {
          id: a.id, parent: gkey, label: a.label, ntype: "asset", kind: a.kind,
          color: assetColor(a.kind, color), px: 8 + Math.min(a.size ?? 1, 3) * 5,
          cost: a.cost ?? null, domainName: topName,
          domainShort: shortName(tops.get(topId)!), subName,
          deg: neighbors.get(a.id)?.length ?? 0, links: topLinks(a.id),
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
        origin: sub?.origin ?? tops.get(topId)?.origin ?? null,
        isUngrouped: tops.get(topId)?.ungrouped ?? false,
        domainShort: tops.has(topId) ? shortName(tops.get(topId)!) : null,
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
  prependContainers(els, tops, usedTops, shortName, (id) => domainById.get(id)?.label ?? null)
  return els
}

/**
 * The LOD gate the Map v2 renderer applies (MV-D75): the **Assets LOD requires a focused
 * domain**, so without one it yields NO elements — the "pick a business area" state — rather
 * than the all-domains-at-once asset mush. Every other case delegates to `buildElements`.
 * Kept pure + exported so the gate is unit-testable without mounting cytoscape.
 */
export function viewElements(
  graph: OntologyGraph,
  lod: Lod,
  focusTop: string | null,
  opts: BuildOpts = {},
): CyEl[] {
  if (lod === "assets" && !focusTop) return []
  return buildElements(graph, lod, focusTop, opts)
}

// Satellite "snippet" accents (MV-D75): measures read as one hue, Pages another, so the
// expand-on-demand layer is visually distinct from the domain palette.
export const MEASURE_COLOR = "#38BDF8"
export const PAGE_COLOR = "#FBBF24"

/** ntype for an expand-layer child: measure | page | (fallback) snippet. */
function satelliteNtype(kind: string): "measure" | "page" | "snippet" {
  if (kind === "measure") return "measure"
  if (kind === "page") return "page"
  return "snippet"
}

/**
 * Merge an expand-on-demand payload (§2.3, MV-D73) into an existing element list, PURELY.
 *
 * Appends each measure/Page child as a satellite node parented into `parentId` (the tapped
 * node's container, so satellites nest with their MV/sub-domain) and each `mv_measure` /
 * `page_source` edge — with two invariants the renderer relies on:
 *  - **dedup:** a node/edge already present (by id) is never re-added, so re-tapping or
 *    re-expanding the same parent is idempotent.
 *  - **emitted-only guard:** an edge is dropped unless BOTH endpoints exist in the merged
 *    node set, so a satellite edge can never dangle (the "nonexistent source" crash guard).
 *
 * Side-effect-free: returns a new array; the inputs are not mutated. Deterministic seed
 * positions keep the map stable across re-renders (mental-map preservation, MV-D72).
 */
export function mergeExpand(
  elements: CyEl[],
  expand: OntologyGraphExpand,
  parentId: string,
): CyEl[] {
  const nodeIds = new Set<string>()
  const edgeIds = new Set<string>()
  for (const el of elements) {
    if (el.group === "nodes") nodeIds.add(String(el.data.id))
    else edgeIds.add(String(el.data.id))
  }

  const out = [...elements]
  // Deterministic center for this parent's satellites (id-hashed → stable, no RNG).
  const center = seedNear({ x: 0, y: 0 }, parentId, 400)
  for (const n of expand.nodes) {
    if (nodeIds.has(n.id)) continue
    nodeIds.add(n.id)
    const ntype = satelliteNtype(n.kind)
    out.push({
      group: "nodes",
      data: {
        id: n.id,
        parent: parentId,
        label: n.label,
        ntype,
        kind: n.kind,
        color: ntype === "page" ? PAGE_COLOR : MEASURE_COLOR,
        px: ntype === "page" ? 12 : 9,
        parentNodeId: parentId,
      },
      position: seedNear(center, n.id, 60),
    })
  }
  for (const e of expand.edges) {
    const id = `xe_${e.src}__${e.dst}`
    if (edgeIds.has(id)) continue
    // Emitted-only guard: never reference a node that isn't in the merged set.
    if (!nodeIds.has(e.src) || !nodeIds.has(e.dst)) continue
    edgeIds.add(id)
    out.push({
      group: "edges",
      data: { id, source: e.src, target: e.dst, etype: "snippet", w: 1 },
    })
  }
  return out
}

/** Insert compound container parent nodes for exactly the tops that have children. */
function prependContainers(
  els: CyEl[],
  tops: Map<string, TopDomain>,
  usedTops: Set<string>,
  shortName: (t: TopDomain) => string,
  subNameOf?: (subId: string) => string | null,
) {
  const containers: CyEl[] = []
  for (const topId of usedTops) {
    const t = tops.get(topId)
    if (!t) continue
    const subNames = subNameOf
      ? t.subIds.map((id) => subNameOf(id)).filter((n): n is string => !!n)
      : []
    containers.push({
      group: "nodes",
      data: {
        id: `top:${t.id}`, label: t.name, ntype: "container", color: t.color,
        count: t.memberCount, origin: t.origin, isUngrouped: t.ungrouped,
        // Container title: trimmed name + at-a-glance size (§1D one-line caption).
        display: t.memberCount > 0 ? `${shortName(t)} · ${fmtCount(t.memberCount)}` : shortName(t),
        subNames,
      },
    })
  }
  els.unshift(...containers)
}

// ── Annotation layer (Map v3 §1D): the one-line story of the current view ──
export interface ViewCaption {
  headline: string
  sub: string | null
}

/**
 * Plain-language caption for the current view — what an infographic would print in
 * its corner so the graphic reads at a glance (MV-D23: no jargon, no ids). Pure:
 * derived entirely from the graph + view state.
 */
export function viewCaption(
  graph: OntologyGraph,
  lod: Lod,
  focusTop: string | null,
  origin: "applied" | "proposed",
): ViewCaption {
  const tops = groupTops(graph.domains.nodes)
  const real = [...tops.values()].filter((t) => !t.ungrouped)
  const ungrouped = [...tops.values()].find((t) => t.ungrouped)
  const shortNames = trimCommonPrefix(real.map((t) => t.name))
  const assetTotal = real.reduce((s, t) => s + t.memberCount, 0)

  if (origin === "proposed") {
    if (real.length === 0) {
      return {
        headline: "No new grouping to suggest",
        sub: "Everything the engine can group is already applied — only ungrouped tables remain.",
      }
    }
    return {
      headline: `${real.length} suggested business ${real.length === 1 ? "area" : "areas"}`,
      sub: "Dashed shapes are suggestions — nothing here is applied yet.",
    }
  }

  if (lod === "assets" && focusTop) {
    const t = tops.get(focusTop)
    if (t) {
      const name = t.ungrouped ? t.name : shortNames.get(t.name) ?? t.name
      const subCount = t.subIds.length
      return {
        headline: name,
        sub: t.ungrouped
          ? `${fmtCount(t.memberCount)} tables waiting to be organised.`
          : `${fmtCount(t.memberCount)} assets${subCount > 0 ? ` across ${subCount} sub-areas` : ""}. Tap anything to see what it is.`,
      }
    }
  }

  if (lod === "subdomains") {
    const subTotal = real.reduce((s, t) => s + t.subIds.length, 0)
    return {
      headline: `${subTotal} sub-areas across ${real.length} business ${real.length === 1 ? "area" : "areas"}`,
      sub: "Open a box to see the tables and metrics inside it.",
    }
  }

  // Domains overview.
  return {
    headline: `${real.length} business ${real.length === 1 ? "area" : "areas"} · ${fmtCount(assetTotal)} assets organised`,
    sub: ungrouped && ungrouped.memberCount > 0
      ? `${fmtCount(ungrouped.memberCount)} tables are not grouped yet.`
      : "Every table in scope belongs to a business area.",
  }
}

export interface NodeFacts {
  title: string
  chip: string
  lines: string[]
  drillTopId: string | null
}

/** Natural-language list: "a", "a and b", "a, b and c". */
function joinNames(names: string[]): string {
  if (names.length <= 1) return names[0] ?? ""
  return `${names.slice(0, -1).join(", ")} and ${names[names.length - 1]}`
}

// Page archetypes ("[Guardrail] fare rules" labels) → what that note DOES, in plain
// language (MV-D23). These describe the product's page kinds, not invented data.
const PAGE_ARCHETYPES: Record<string, string> = {
  Routing: "Points questions at the right data.",
  Disambiguation: "Clears up a term that could mean two things.",
  Guardrail: "A rule answers in this area must respect.",
  Taxonomy: "How this area's terms relate to each other.",
}

/**
 * Plain-language facts for the inspector rail. Zero jargon (MV-D23) — no ids, no SQL.
 * Returns a `drillTopId` when the node is a domain/container the user can open into.
 * Everything shown is already in the snapshot/expand contract (Map v3 §3): counts,
 * grouping, connectivity (degree + strongest links), cost when present.
 */
export function nodeFacts(data: Record<string, unknown>): NodeFacts {
  const ntype = String(data.ntype ?? "")
  const label = String(data.label ?? "")
  const count = typeof data.count === "number" ? data.count : null
  const cost = typeof data.cost === "number" ? data.cost : null
  const subNames = Array.isArray(data.subNames) ? (data.subNames as string[]) : []
  const lines: string[] = []

  if (ntype === "domain" || ntype === "container") {
    const ungrouped = data.isUngrouped === true
    if (ungrouped) {
      if (count != null) lines.push(`${fmtCount(count)} ${count === 1 ? "table" : "tables"} not organised into any business area yet`)
      lines.push("Grouping them makes questions in this area easier to answer.")
      const id = ntype === "container" ? String(data.id).replace(/^top:/, "") : String(data.id)
      return { title: label, chip: "Not yet grouped", lines, drillTopId: id }
    }
    lines.push("A business area of the estate.")
    if (count != null) lines.push(`${fmtCount(count)} ${count === 1 ? "asset" : "assets"} in this area`)
    if (subNames.length > 0) {
      const shownSubs = subNames.slice(0, 4)
      const rest = subNames.length - shownSubs.length
      lines.push(`Sub-areas: ${joinNames(shownSubs)}${rest > 0 ? ` and ${rest} more` : ""}`)
    }
    if (cost != null && cost > 0) lines.push(`About ${fmtMoney(cost)} / month`)
    const id = ntype === "container" ? String(data.id).replace(/^top:/, "") : String(data.id)
    return { title: label, chip: "Business area", lines, drillTopId: id }
  }
  if (ntype === "subdomain" || ntype === "subcontainer") {
    const domainShort = data.domainShort ? String(data.domainShort) : null
    if (domainShort && domainShort !== label) lines.push(`Part of ${domainShort}`)
    if (count != null) lines.push(`${fmtCount(count)} ${count === 1 ? "asset" : "assets"} grouped here`)
    return { title: label, chip: "Sub-area", lines, drillTopId: null }
  }
  if (ntype === "more") {
    return { title: label, chip: "More assets", lines: ["Zoom in or open this business area to see the rest."], drillTopId: null }
  }
  // Expand-on-demand satellites (MV-D73/D75) — plain language, zero jargon (MV-D23).
  if (ntype === "measure") {
    return { title: label, chip: "Measure", lines: ["A number this metric view reports."], drillTopId: null }
  }
  if (ntype === "page") {
    const m = label.match(/^\[([A-Za-z]+)\]\s*(.*)$/)
    const archetype = m?.[1] ?? null
    const clean = m?.[2] || label
    const what = (archetype && PAGE_ARCHETYPES[archetype]) || "A guidance note attached to this area."
    return {
      title: clean,
      chip: archetype ? `${archetype} note` : "Page",
      lines: [what],
      drillTopId: null,
    }
  }
  // asset
  const kind = String(data.kind ?? "table")
  const kindLabel = kind === "metric_view" ? "Metric view" : kind === "agent" || kind === "genie_agent" ? "Genie agent" : kind === "view" ? "View" : kind === "dashboard" ? "Dashboard" : "Table"
  const what =
    kind === "metric_view"
      ? "A curated set of business measures."
      : kind === "agent" || kind === "genie_agent"
        ? "Answers questions about this area in plain language."
        : kind === "dashboard"
          ? "A chart page built on this area's data."
          : "A data table."
  lines.push(what)
  const domainShort = data.domainShort ? String(data.domainShort) : data.domainName ? String(data.domainName) : null
  const subName = data.subName ? String(data.subName) : null
  if (domainShort) lines.push(subName ? `In ${domainShort} › ${subName}` : `In ${domainShort}`)
  const deg = typeof data.deg === "number" ? data.deg : 0
  const links = Array.isArray(data.links) ? (data.links as string[]) : []
  if (deg > 0 && links.length > 0) {
    const rest = deg - links.length
    lines.push(
      rest > 0
        ? `Works with ${links.join(", ")} and ${fmtCount(rest)} more`
        : `Works with ${joinNames(links)}`,
    )
  }
  if (cost != null && cost > 0) lines.push(`About ${fmtMoney(cost)} / month`)
  return { title: label, chip: kindLabel, lines, drillTopId: null }
}

/** $2.4k-style money for fact lines. */
function fmtMoney(cost: number): string {
  return `$${cost >= 1000 ? `${(cost / 1000).toFixed(1)}k` : cost.toFixed(0)}`
}
