/**
 * Estate-graph view model (Ontology Map north-star — MV-D81/D82/D83). PURE,
 * side-effect-free transforms that turn the backend `OntologyGraph` (+ optional
 * `OntologyDrafts`/`OntologyTaxonomy`) into a normalized *containment tree blob* the
 * deterministic d3 layout (`ontologyTreeLayout.ts`) lays out and `EstateGraph.tsx`
 * renders to SVG.
 *
 * Replaces the LOD/Cytoscape element builder (MV-D84, §9-A). Output is a single
 * hierarchy — `org → domain → subdomain → asset → {measure, table}` — plus:
 *  - typed **cross-edges** (verb + within/cross-domain class) overlaid on the tree,
 *  - an off-tree **Ungrouped tray** (§3.5) of assets with no applied group,
 *  - dashed **proposals** (§3.5) that would group tray assets, with a confidence band.
 *
 * DEGRADES gracefully (MV-D43, §9-E): when the Lane-D fields are absent (`root`,
 * `parent_id` asset containment, `attach_level`, edge `verb`/`rel_class`) it falls back
 * to a synthesized org root over `domain → subdomain → asset` with verbs derived from
 * edge `kind` and classes derived from the endpoints' domain ancestors.
 */
import type {
  OntologyDrafts,
  OntologyGraph,
  OntologyGraphEdge,
  OntologyGraphExpand,
  OntologyGraphNode,
  OntologyTaxonomy,
} from "@/ontology/types"

// ── The tree taxonomy (§3.1) ────────────────────────────────────────────────
export type NodeType =
  | "org"
  | "domain"
  | "subdomain"
  | "agent"
  | "dashboard"
  | "metric_view"
  | "measure"
  | "table"

export type RelClass = "shared" | "xdom"
export type Origin = "applied" | "proposed"
export type Band = "High" | "Medium" | "Low"

/** One node in the containment tree (org excluded from the tray). */
export interface EstateNode {
  id: string
  parentId: string | null
  type: NodeType
  /** Full name — inspector / breadcrumb / search read this. */
  label: string
  /** Canvas caption — shared word-prefix trimmed across sibling domains. */
  displayName: string
  origin: Origin
  /** Top-domain ancestor id — drives domain tint + cross-link classing. */
  domainId: string | null
  attachLevel: "asset" | "subdomain" | "domain" | null
  /** Raw backend kind, for assets (table / metric_view / …). */
  kind: string
  memberCount: number | null
  cost: number | null
  /** Total descendants in the full tree (the collapsed `+N` badge count). */
  descendantCount: number
  /**
   * Plain-language description from the snapshot (MV-D86, Lane D2). The inspector +
   * hover-snippet prefer this over the generic `describe()` copy; null/absent on a pre-MV-D86
   * blob (Lane P degrades to the generic strings). Never fabricated (R13). Optional so
   * synthetic nodes (the `+N more` chip) need not carry it.
   */
  description?: string | null
  /** Compact key/value bag (rows/format/freshness/expression/…) for the snippet + inspector. */
  meta?: Record<string, string> | null
}

/** A typed relational edge overlaid on the tree (§3.3). */
export interface EstateCrossEdge {
  id: string
  src: string
  dst: string
  verb: string
  relClass: RelClass
  kind: string
  /**
   * Compact optional evidence bag for the hover edge-tooltip (MV-D88, Lane E → MV-D87,
   * Lane P2): e.g. { "Shares": "customer_id, flight_id", "Co-queried": "42 sessions" }.
   * Null on a pre-MV-D88 blob — the tooltip then degrades to verb + endpoints + class (R25).
   */
  detail?: Record<string, string> | null
}

/** An asset with no applied group — lives in the off-tree tray (§3.5). */
export interface TrayItem {
  id: string
  label: string
  type: NodeType
  kind: string
}

/** A dashed "Suggested" grouping over tray assets (§3.5). */
export interface EstateProposal {
  id: string
  name: string
  band: Band | null
  memberIds: string[]
  kind: "domain" | "subdomain"
}

export interface EstateModel {
  root: EstateNode | null
  /** All tree nodes incl. root; NOT tray items. */
  nodes: EstateNode[]
  /** parentId → children, each list sorted by the stable key. */
  childrenByParent: Map<string, EstateNode[]>
  crossEdges: EstateCrossEdge[]
  trayItems: TrayItem[]
  proposals: EstateProposal[]
  /** True when the Lane-D contract fields were absent and we synthesized structure. */
  degraded: boolean
}

export interface BuildModelOpts {
  /** Proposals with real confidence bands (§3.4 data source). */
  drafts?: OntologyDrafts | null
  /** Ungrouped tray membership (§3.4 data source). */
  taxonomy?: OntologyTaxonomy | null
  /** Company/estate name for a synthesized org root (degrade path). */
  estateName?: string | null
}

const SYNTH_ROOT_ID = "__org__"

// ── Small pure helpers ──────────────────────────────────────────────────────

/** 1,911-style count for labels. */
export function fmtCount(n: number): string {
  return n.toLocaleString("en-US")
}

/** $2.4k-style money for fact lines. */
export function fmtMoney(cost: number): string {
  return `$${cost >= 1000 ? `${(cost / 1000).toFixed(1)}k` : cost.toFixed(0)}`
}

/**
 * Trim the leading word sequence every sibling domain name shares
 * ("Alaska Airlines Commercial" → "Commercial") from CANVAS captions only.
 * Presentation-only; `label` keeps the full name. Trims only when 2+ names share a
 * full leading word run and every trimmed remainder is non-empty.
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
  if (prefixLen === 0 || !split.every((w) => w.length > prefixLen)) return out
  for (let k = 0; k < names.length; k++) out.set(names[k], split[k].slice(prefixLen).join(" "))
  return out
}

/** Map a raw backend asset `kind` to a tree node type (§3.1). */
export function typeForKind(kind: string): NodeType {
  switch (kind) {
    case "metric_view":
      return "metric_view"
    case "agent":
    case "genie_agent":
      return "agent"
    case "dashboard":
      return "dashboard"
    case "measure":
      return "measure"
    case "view":
    case "table":
    default:
      return "table"
  }
}

/**
 * Plain-language verb for an edge (§3.3 / §3.4 table). Honors an explicit Lane-D
 * `verb`; otherwise derives one from the raw signal `kind` (reveal-don't-invent —
 * never fabricates a relationship, only names one that exists).
 */
export function verbForEdge(edge: OntologyGraphEdge): string {
  if (edge.verb && edge.verb.trim()) return edge.verb.trim()
  const k = edge.kind ?? ""
  if (/mv_member|member/i.test(k)) return "reads"
  if (/agent|scope/i.test(k)) return "uses"
  if (/line/i.test(k)) return "feeds"
  if (/join|fk|shared/i.test(k)) return "shares key with"
  if (/co.?quer/i.test(k)) return "also queried with"
  if (/sem|sim/i.test(k)) return "similar to"
  return "relates to"
}

/**
 * Classify a cross-link as within-domain (`shared`) or cross-domain (`xdom`).
 * Honors an explicit Lane-D `rel_class`; otherwise compares the endpoints' domain
 * ancestors (§3.3).
 */
export function classForEdge(
  edge: OntologyGraphEdge,
  domainOf: (id: string) => string | null,
): RelClass {
  if (edge.rel_class === "shared" || edge.rel_class === "xdom") return edge.rel_class
  const a = domainOf(edge.src)
  const b = domainOf(edge.dst)
  if (a && b && a !== b) return "xdom"
  return "shared"
}

function isUngroupedDomain(n: OntologyGraphNode): boolean {
  return n.kind === "ungrouped" || n.id === "ungrouped"
}

function bandFromTier(tier: string | null | undefined): Band | null {
  if (tier === "high") return "High"
  if (tier === "medium") return "Medium"
  if (tier === "low") return "Low"
  return null
}

/**
 * Build the normalized estate model from the backend graph (+ optional drafts /
 * taxonomy). Pure: no DOM, no layout, no side effects.
 */
export function buildEstateModel(graph: OntologyGraph, opts: BuildModelOpts = {}): EstateModel {
  const domainNodes = graph.domains.nodes
  const domainById = new Map(domainNodes.map((d) => [d.id, d]))
  const ungroupedIds = new Set(domainNodes.filter(isUngroupedDomain).map((d) => d.id))

  // Top-domain ancestor of any domain/subdomain id (self if a top).
  const topOfDomain = (id?: string | null): string | null => {
    if (!id) return null
    const d = domainById.get(id)
    if (!d) return null
    return d.parent_id || d.id
  }

  const degraded = !graph.root

  // ── Root ────────────────────────────────────────────────────────────────
  const rootId = graph.root?.id ?? SYNTH_ROOT_ID
  const rootLabel = graph.root?.label ?? opts.estateName ?? "Estate"
  const root: EstateNode = {
    id: rootId,
    parentId: null,
    type: "org",
    label: rootLabel,
    displayName: rootLabel,
    origin: "applied",
    domainId: null,
    attachLevel: null,
    kind: "org",
    memberCount: graph.root?.member_count ?? null,
    cost: null,
    descendantCount: 0,
    description: graph.root?.description ?? null,
    meta: graph.root?.meta ?? null,
  }

  const nodes: EstateNode[] = [root]
  const push = (n: EstateNode) => nodes.push(n)

  // ── Domains + sub-domains (applied, non-ungrouped) ───────────────────────
  const realTops = domainNodes.filter((d) => !d.parent_id && !isUngroupedDomain(d))
  const shortNames = trimCommonPrefix(realTops.map((d) => d.label))
  const treeDomainIds = new Set<string>()

  for (const d of domainNodes) {
    if (isUngroupedDomain(d)) continue
    if (d.origin === "proposed") continue // proposals render off-tree (§3.5)
    const isSub = !!d.parent_id
    const top = d.parent_id || d.id
    treeDomainIds.add(d.id)
    push({
      id: d.id,
      parentId: isSub ? d.parent_id! : rootId,
      type: isSub ? "subdomain" : "domain",
      label: d.label,
      displayName: isSub ? d.label : shortNames.get(d.label) ?? d.label,
      origin: "applied",
      domainId: top,
      attachLevel: null,
      kind: d.kind,
      memberCount: d.member_count ?? null,
      cost: d.cost ?? null,
      descendantCount: 0,
      description: d.description ?? null,
      meta: d.meta ?? null,
    })
  }

  // ── Assets (+ measures/tables under MVs when Lane D present) ─────────────
  const assetById = new Map(graph.assets.nodes.map((a) => [a.id, a]))
  const trayItems: TrayItem[] = []
  const assetDomain = new Map<string, string | null>() // asset id → top-domain ancestor

  for (const a of graph.assets.nodes) {
    const dom = a.domain_id ? domainById.get(a.domain_id) : undefined
    // Tray = no applied group: missing domain, the ungrouped bucket, OR a domain that is
    // only *proposed* (its grouping is not applied yet, so the asset is still loose — the
    // proposal hull draws over it in the tray, §3.5).
    const ungrouped =
      !dom ||
      isUngroupedDomain(dom) ||
      (a.domain_id ? ungroupedIds.has(a.domain_id) : true) ||
      dom.origin === "proposed"
    if (ungrouped) {
      trayItems.push({ id: a.id, label: a.label, type: typeForKind(a.kind), kind: a.kind })
      assetDomain.set(a.id, null)
      continue
    }
    // Canonical parent: Lane-D asset→asset containment (parent_id) wins; else the
    // asset attaches to its domain/subdomain (attach_level, or derived).
    const hasContainer = a.parent_id && (assetById.has(a.parent_id) || domainById.has(a.parent_id))
    const parentId = hasContainer ? a.parent_id! : a.domain_id!
    const attach: EstateNode["attachLevel"] =
      (a.attach_level as EstateNode["attachLevel"]) ??
      (hasContainer ? "asset" : dom!.parent_id ? "subdomain" : "domain")
    const top = topOfDomain(a.domain_id)
    assetDomain.set(a.id, top)
    push({
      id: a.id,
      parentId,
      type: typeForKind(a.kind),
      label: a.label,
      displayName: a.label,
      origin: "applied",
      domainId: top,
      attachLevel: attach,
      kind: a.kind,
      memberCount: a.member_count ?? null,
      cost: a.cost ?? null,
      descendantCount: 0,
      description: a.description ?? null,
      meta: a.meta ?? null,
    })
  }

  // ── Index children, prune dangling parents, compute descendant counts ────
  const nodeById = new Map(nodes.map((n) => [n.id, n]))
  // Re-root any node whose parent was dropped (e.g. asset under an MV that fell to
  // the tray) onto the org root so the tree never loses a subtree (degrade-not-hang).
  for (const n of nodes) {
    if (n.parentId && !nodeById.has(n.parentId)) n.parentId = rootId
  }
  const childrenByParent = indexChildren(nodes)
  computeDescendantCounts(root, childrenByParent)

  // ── Typed cross-edges (overlay) ──────────────────────────────────────────
  const domainOfNode = (id: string): string | null => {
    const n = nodeById.get(id)
    if (n) return n.domainId ?? (n.type === "domain" ? n.id : null)
    return assetDomain.get(id) ?? null
  }
  const crossEdges = buildCrossEdges(graph, nodeById, domainOfNode)

  // ── Proposals + tray (§3.5) ──────────────────────────────────────────────
  const proposals = buildProposals(graph, opts.drafts, ungroupedIds)
  const trayFromTaxonomy = trayFromTaxonomyBucket(opts.taxonomy)
  const mergedTray = trayFromTaxonomy.length ? trayFromTaxonomy : trayItems

  return {
    root,
    nodes,
    childrenByParent,
    crossEdges,
    trayItems: mergedTray,
    proposals,
    degraded,
  }
}

/** Stable child ordering key: (attach_level rank, name, id) → byte-stable layout. */
const ATTACH_RANK: Record<string, number> = { domain: 0, subdomain: 1, asset: 2 }
const TYPE_RANK: Record<NodeType, number> = {
  org: 0,
  domain: 1,
  subdomain: 2,
  agent: 3,
  dashboard: 4,
  metric_view: 5,
  table: 6,
  measure: 7,
}

function stableChildCompare(a: EstateNode, b: EstateNode): number {
  const ar = ATTACH_RANK[a.attachLevel ?? ""] ?? 9
  const br = ATTACH_RANK[b.attachLevel ?? ""] ?? 9
  if (ar !== br) return ar - br
  const at = TYPE_RANK[a.type]
  const bt = TYPE_RANK[b.type]
  if (at !== bt) return at - bt
  if (a.label !== b.label) return a.label < b.label ? -1 : 1
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0
}

function indexChildren(nodes: EstateNode[]): Map<string, EstateNode[]> {
  const byParent = new Map<string, EstateNode[]>()
  for (const n of nodes) {
    if (!n.parentId) continue
    const list = byParent.get(n.parentId)
    if (list) list.push(n)
    else byParent.set(n.parentId, [n])
  }
  for (const list of byParent.values()) list.sort(stableChildCompare)
  return byParent
}

function computeDescendantCounts(
  root: EstateNode,
  childrenByParent: Map<string, EstateNode[]>,
): void {
  const visit = (n: EstateNode): number => {
    const kids = childrenByParent.get(n.id) ?? []
    let total = kids.length
    for (const k of kids) total += visit(k)
    n.descendantCount = total
    return total
  }
  visit(root)
}

function buildCrossEdges(
  graph: OntologyGraph,
  nodeById: Map<string, EstateNode>,
  domainOfNode: (id: string) => string | null,
): EstateCrossEdge[] {
  const out: EstateCrossEdge[] = []
  const seen = new Set<string>()
  const consider = (edge: OntologyGraphEdge) => {
    if (edge.src === edge.dst) return
    if (!nodeById.has(edge.src) || !nodeById.has(edge.dst)) return
    // Skip pure hierarchy edges (a parent↔child pair is already the spine).
    const s = nodeById.get(edge.src)!
    const d = nodeById.get(edge.dst)!
    if (s.parentId === d.id || d.parentId === s.id) return
    const key = edge.src < edge.dst ? `${edge.src}|${edge.dst}` : `${edge.dst}|${edge.src}`
    if (seen.has(key)) return
    seen.add(key)
    out.push({
      id: `x_${key}`,
      src: edge.src,
      dst: edge.dst,
      verb: verbForEdge(edge),
      relClass: classForEdge(edge, domainOfNode),
      kind: edge.kind,
      detail: edge.detail ?? null,
    })
  }
  for (const e of graph.assets.edges) consider(e)
  for (const e of graph.domains.edges) consider(e)
  // Deterministic order (byte-stable overlay).
  out.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
  return out
}

function buildProposals(
  graph: OntologyGraph,
  drafts: OntologyDrafts | null | undefined,
  ungroupedIds: Set<string>,
): EstateProposal[] {
  // Preferred source (§3.4): OntologyDrafts — carries a real confidence band.
  if (drafts && drafts.domains.length) {
    return drafts.domains
      .filter((d) => d.kind === "domain" || d.kind === "subdomain")
      .map((d) => ({
        id: d.proposal_id,
        name: d.name,
        band: d.confidence?.band ?? bandFromTier(d.tier),
        memberIds: d.members.map((m) => m.fqn),
        kind: d.kind as "domain" | "subdomain",
      }))
      .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
  }
  // Degrade: proposed-origin domain nodes grouping their assets.
  const proposed = graph.domains.nodes.filter(
    (d) => d.origin === "proposed" && !ungroupedIds.has(d.id),
  )
  if (!proposed.length) return []
  const membersByDomain = new Map<string, string[]>()
  for (const a of graph.assets.nodes) {
    if (!a.domain_id) continue
    const list = membersByDomain.get(a.domain_id)
    if (list) list.push(a.id)
    else membersByDomain.set(a.domain_id, [a.id])
  }
  return proposed
    .map((d) => ({
      id: d.id,
      name: d.label,
      band: null,
      memberIds: membersByDomain.get(d.id) ?? [],
      kind: (d.parent_id ? "subdomain" : "domain") as "domain" | "subdomain",
    }))
    .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
}

function trayFromTaxonomyBucket(taxonomy: OntologyTaxonomy | null | undefined): TrayItem[] {
  if (!taxonomy) return []
  const out: TrayItem[] = []
  for (const m of [...taxonomy.ungrouped.metric_views, ...taxonomy.ungrouped.genie_agents]) {
    out.push({ id: m.fqn, label: m.fqn.split(".").pop() ?? m.fqn, type: typeForKind(m.asset_type), kind: m.asset_type })
  }
  return out
}

// ── Expand-on-demand hydration (MV-D73, §2.3) ────────────────────────────────
// A metric view's measures + a subdomain's Pages are fetched on click and folded in.
// Measures PROMOTE into the tree as amber leaf children of the MV — the missing middle
// tier that makes the estate deep. Pages are "attached" (surfaced in the inspector via
// {@link pagesFromExpansions}) rather than promoted, so the containment tree stays the
// north-star taxonomy (org→domain→subdomain→asset→{measure,table}). Pure; reveal-don't-
// invent (R13) — only merges nodes/edges the expand payload actually returned.

/** True for a hydrated node that promotes into the tree as an MV child (a measure). */
function isMeasureNode(n: OntologyGraphNode): boolean {
  return n.kind === "measure"
}

/**
 * Fold expand payloads (keyed by the expanded node id) into the base graph: measure
 * children re-parented onto their metric view (the payload's `parent_id`), carrying the
 * MV's domain so classing + tint still work. Dedupes against ids already present and across
 * payloads, so re-expands are idempotent (the caller also caches per id). Returns the graph
 * unchanged when nothing new merges — a stable identity so downstream memos don't churn.
 */
export function mergeHydration(
  graph: OntologyGraph,
  expansions: Iterable<OntologyGraphExpand>,
): OntologyGraph {
  const domainOfAsset = new Map(graph.assets.nodes.map((a) => [a.id, a.domain_id ?? null]))
  const seen = new Set(graph.assets.nodes.map((a) => a.id))
  const extraNodes: OntologyGraphNode[] = []
  const extraEdges: OntologyGraphEdge[] = []
  for (const exp of expansions) {
    const parentDomain = domainOfAsset.get(exp.parent_id) ?? null
    for (const n of exp.nodes) {
      if (!isMeasureNode(n) || seen.has(n.id)) continue
      seen.add(n.id)
      extraNodes.push({
        ...n,
        parent_id: n.parent_id ?? exp.parent_id,
        domain_id: n.domain_id ?? parentDomain,
        attach_level: n.attach_level ?? "asset",
      })
    }
    for (const e of exp.edges) {
      if (e.kind === "mv_measure") extraEdges.push(e)
    }
  }
  if (!extraNodes.length && !extraEdges.length) return graph
  return {
    ...graph,
    assets: {
      ...graph.assets,
      nodes: [...graph.assets.nodes, ...extraNodes],
      edges: [...graph.assets.edges, ...extraEdges],
    },
  }
}

/** A hydrated Page attached to a node (surfaced in the inspector, not the tree). */
export interface AttachedPage {
  id: string
  label: string
}

/** The `kind="page"` children a node's expand payload returned (empty when none). */
export function pagesFromExpansions(
  expansion: OntologyGraphExpand | null | undefined,
): AttachedPage[] {
  if (!expansion) return []
  return expansion.nodes
    .filter((n) => n.kind === "page")
    .map((n) => ({ id: n.id, label: n.label }))
}

// ── Domain show/hide (MV-D87 P1-b, §5) ───────────────────────────────────────
// Pure, VIEW-ONLY: derive a filtered model that omits every node whose domain (or
// sub-domain) the curator has unchecked, plus its whole subtree and any cross-link that
// touches a removed node. Never mutates the snapshot (R13) — it returns a fresh model, and
// with nothing hidden it returns the input UNCHANGED (byte-stable identity so the default
// paint's layout hash never churns, R18).

/** True for a top-domain / sub-domain container that can be toggled in the panel. */
function isVisibilityContainer(n: EstateNode): boolean {
  return n.type === "domain" || n.type === "subdomain"
}

/** The top-domain + sub-domain containers, top-first then stable order (panel listing). */
export function visibilityDomains(model: EstateModel): EstateNode[] {
  return model.nodes.filter(isVisibilityContainer)
}

/**
 * Filter the model to hide the subtrees of the container ids in `hidden` (top domains or
 * sub-domains). A node is hidden when itself or any ancestor is in `hidden`; hidden nodes
 * drop from `nodes`/`childrenByParent`, cross-edges with a hidden endpoint drop, and the
 * remaining collapse-badge descendant counts are recomputed. Tray + proposals are untouched
 * (they are off-tree / ungrouped, not domain-scoped). Returns the input model unchanged when
 * `hidden` is empty.
 */
export function filterModelByVisibility(
  model: EstateModel,
  hidden: ReadonlySet<string>,
): EstateModel {
  if (hidden.size === 0 || !model.root) return model
  const byId = new Map(model.nodes.map((n) => [n.id, n]))
  const removed = new Set<string>()
  const isHidden = (n: EstateNode): boolean => {
    // Walk to the root; hidden if any ancestor (or self) is unchecked.
    let cur: EstateNode | null = n
    const guard = new Set<string>()
    while (cur && !guard.has(cur.id)) {
      guard.add(cur.id)
      if (hidden.has(cur.id)) return true
      cur = cur.parentId ? byId.get(cur.parentId) ?? null : null
    }
    return false
  }
  for (const n of model.nodes) if (isHidden(n)) removed.add(n.id)
  if (removed.size === 0) return model
  const nodes = model.nodes.filter((n) => !removed.has(n.id))
  const crossEdges = model.crossEdges.filter((e) => !removed.has(e.src) && !removed.has(e.dst))
  const childrenByParent = indexChildren(nodes)
  computeDescendantCounts(model.root, childrenByParent)
  return { ...model, nodes, childrenByParent, crossEdges }
}

/** One row of the relationship-type legend (§5, Bloom-idiom): a verb + its live count. */
export interface RelTypeLegendRow {
  verb: string
  count: number
  /** How many of this verb's edges are cross-domain (drives the swatch class). */
  xdom: number
}

/**
 * Group cross-edges by their plain verb into legend rows with live counts, so the legend
 * lists each relationship type present (with a number, click-to-highlight). Deterministic
 * order: count desc, then verb A→Z. Pure — recomputes from whatever edges are currently
 * visible, so domain show/hide changes the counts (R27).
 */
export function relTypeLegend(edges: EstateCrossEdge[]): RelTypeLegendRow[] {
  const by = new Map<string, RelTypeLegendRow>()
  for (const e of edges) {
    const row = by.get(e.verb) ?? { verb: e.verb, count: 0, xdom: 0 }
    row.count += 1
    if (e.relClass === "xdom") row.xdom += 1
    by.set(e.verb, row)
  }
  return [...by.values()].sort((a, b) => (b.count - a.count) || (a.verb < b.verb ? -1 : a.verb > b.verb ? 1 : 0))
}
