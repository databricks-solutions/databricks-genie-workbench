/**
 * Ontology Map — pure deterministic tree layout (MV-D84, §4.1). NO DOM, NO
 * d3-selection: given the estate model + which nodes are expanded + manual drag
 * offsets + a config, returns positioned nodes, spine links, bowed typed cross-links,
 * the off-tree Ungrouped tray, dashed proposal hulls, and the overall bounds.
 *
 * Determinism (R18/G6): children are pre-sorted by a stable key in the model, and
 * `d3.tree().nodeSize(...)` is analytic (Reingold–Tilford) — physics-free — so the same
 * estate + expansion yields byte-identical geometry across renders and reloads. Manual
 * drag offsets are applied AFTER layout as `{dx,dy}` deltas, so tailoring survives
 * expand/collapse and refresh (R17).
 *
 * Unit-testable without mounting React (the whole point of keeping it pure).
 */
import { hierarchy, tree, type HierarchyNode } from "d3-hierarchy"
import type {
  EstateModel,
  EstateNode,
  EstateProposal,
  NodeType,
  RelClass,
  TrayItem,
} from "@/ontology/estateGraphModel"

export interface Point {
  x: number
  y: number
}

export interface LayoutConfig {
  /** Sibling gap (x) fed to d3.tree().nodeSize. */
  dx: number
  /** Row height (y) fed to d3.tree().nodeSize. */
  dy: number
  /** Radius per node type (also used for tray + hull padding). */
  radius: Record<NodeType, number>
  /** Gap between the tree's right edge and the tray column. */
  trayGap: number
  /** Tray grid geometry. */
  trayCols: number
  trayCellW: number
  trayCellH: number
  /** Cap of tray items rendered before a "+N more" chip. */
  trayCap: number
  /** Bow factor for cross-link beziers; xdom is multiplied by `xdomBow`. */
  bow: number
  xdomBow: number
}

export const DEFAULT_LAYOUT: LayoutConfig = {
  dx: 44,
  dy: 120,
  radius: {
    org: 26,
    domain: 20,
    subdomain: 15,
    agent: 13,
    dashboard: 12,
    metric_view: 13,
    table: 10,
    measure: 8,
  },
  trayGap: 160,
  trayCols: 4,
  trayCellW: 128,
  trayCellH: 38,
  trayCap: 48,
  bow: 0.28,
  xdomBow: 1.4,
}

export interface LaidNode {
  id: string
  x: number
  y: number
  depth: number
  type: NodeType
  label: string
  displayName: string
  origin: EstateNode["origin"]
  domainId: string | null
  kind: string
  radius: number
  /** True when this node has children that are currently collapsed. */
  collapsed: boolean
  /** Descendant count for the `+N` badge (0 when expanded / leaf). */
  badge: number
  memberCount: number | null
  cost: number | null
}

export interface SpineLink {
  id: string
  sourceId: string
  targetId: string
  source: Point
  target: Point
  /** SVG path (cubic bezier, vertical tidy-tree link). */
  path: string
  domainId: string | null
}

export interface CrossLink {
  id: string
  sourceId: string
  targetId: string
  verb: string
  relClass: RelClass
  path: string
  /** Midpoint of the arc — where the verb plate sits. */
  labelAt: Point
}

export interface TrayLaidItem extends TrayItem {
  x: number
  y: number
  radius: number
}

export interface ProposalHull {
  id: string
  name: string
  band: EstateProposal["band"]
  kind: EstateProposal["kind"]
  /** Rounded-rect hull over the member tray cells. */
  x: number
  y: number
  width: number
  height: number
  memberIds: string[]
}

export interface Bounds {
  minX: number
  minY: number
  maxX: number
  maxY: number
  width: number
  height: number
}

export interface Layout {
  nodes: LaidNode[]
  spineLinks: SpineLink[]
  crossLinks: CrossLink[]
  trayItems: TrayLaidItem[]
  trayOverflow: number
  trayBounds: Bounds | null
  proposalHulls: ProposalHull[]
  bounds: Bounds
}

/** Internal d3 hierarchy datum. */
interface Datum {
  node: EstateNode
}

/**
 * Build the visible hierarchy: start at root, descend only through expanded nodes.
 * A node is "expanded" if it is in `expandedSet`; its children are included only then.
 * The root is always expanded.
 */
function buildVisibleHierarchy(
  model: EstateModel,
  expandedSet: Set<string>,
): { rootDatum: Datum; hasHiddenChildren: Set<string> } | null {
  if (!model.root) return null
  const hasHiddenChildren = new Set<string>()
  const make = (node: EstateNode, isRoot: boolean): Datum & { children?: Datum[] } => {
    const kids = model.childrenByParent.get(node.id) ?? []
    const expanded = isRoot || expandedSet.has(node.id)
    if (kids.length && !expanded) hasHiddenChildren.add(node.id)
    const datum: Datum & { children?: Datum[] } = { node }
    if (expanded && kids.length) datum.children = kids.map((k) => make(k, false))
    return datum
  }
  return { rootDatum: make(model.root, true), hasHiddenChildren }
}

function emptyBounds(): Bounds {
  return { minX: 0, minY: 0, maxX: 0, maxY: 0, width: 0, height: 0 }
}

function boundsOf(points: Point[], pad: number): Bounds {
  if (!points.length) return emptyBounds()
  let minX = Infinity
  let minY = Infinity
  let maxX = -Infinity
  let maxY = -Infinity
  for (const p of points) {
    if (p.x < minX) minX = p.x
    if (p.x > maxX) maxX = p.x
    if (p.y < minY) minY = p.y
    if (p.y > maxY) maxY = p.y
  }
  return {
    minX: minX - pad,
    minY: minY - pad,
    maxX: maxX + pad,
    maxY: maxY + pad,
    width: maxX - minX + pad * 2,
    height: maxY - minY + pad * 2,
  }
}

/** Vertical tidy-tree cubic bezier between parent and child (linkVertical shape). */
function spinePath(s: Point, t: Point): string {
  const my = (s.y + t.y) / 2
  return `M${s.x},${s.y}C${s.x},${my} ${t.x},${my} ${t.x},${t.y}`
}

/**
 * Quadratic bezier bowed off the straight line between two points; xdom bows wider
 * (§4.1). Returns the path plus the arc midpoint (for the verb plate).
 */
function crossPath(a: Point, b: Point, bow: number): { path: string; mid: Point } {
  const mx = (a.x + b.x) / 2
  const my = (a.y + b.y) / 2
  const dx = b.x - a.x
  const dy = b.y - a.y
  const len = Math.hypot(dx, dy) || 1
  // Perpendicular unit vector, deterministic sign (bow toward +normal).
  const nx = -dy / len
  const ny = dx / len
  const off = len * bow
  const cx = mx + nx * off
  const cy = my + ny * off
  // Midpoint of a quadratic bezier at t=0.5.
  const midX = 0.25 * a.x + 0.5 * cx + 0.25 * b.x
  const midY = 0.25 * a.y + 0.5 * cy + 0.25 * b.y
  return { path: `M${a.x},${a.y}Q${cx},${cy} ${b.x},${b.y}`, mid: { x: midX, y: midY } }
}

/**
 * Lay out the estate model deterministically.
 */
export function layoutTree(
  model: EstateModel,
  expandedSet: Set<string>,
  dragOffsets: Map<string, Point>,
  cfg: LayoutConfig = DEFAULT_LAYOUT,
): Layout {
  const built = buildVisibleHierarchy(model, expandedSet)
  if (!built) {
    return {
      nodes: [],
      spineLinks: [],
      crossLinks: [],
      trayItems: [],
      trayOverflow: 0,
      trayBounds: null,
      proposalHulls: [],
      bounds: emptyBounds(),
    }
  }

  const h = hierarchy<Datum>(built.rootDatum, (d) => (d as { children?: Datum[] }).children)
  tree<Datum>().nodeSize([cfg.dx, cfg.dy])(h)

  // Position (with drag offset applied post-layout).
  const posById = new Map<string, Point>()
  const laid: LaidNode[] = []
  h.each((hn: HierarchyNode<Datum>) => {
    const node = hn.data.node
    const off = dragOffsets.get(node.id)
    const x = (hn.x ?? 0) + (off?.x ?? 0)
    const y = (hn.y ?? 0) + (off?.y ?? 0)
    posById.set(node.id, { x, y })
    const collapsed = built.hasHiddenChildren.has(node.id)
    laid.push({
      id: node.id,
      x,
      y,
      depth: hn.depth,
      type: node.type,
      label: node.label,
      displayName: node.displayName,
      origin: node.origin,
      domainId: node.domainId,
      kind: node.kind,
      radius: cfg.radius[node.type] ?? 10,
      collapsed,
      badge: collapsed ? node.descendantCount : 0,
      memberCount: node.memberCount,
      cost: node.cost,
    })
  })

  // Spine links (visible parent→child only).
  const spineLinks: SpineLink[] = []
  h.links().forEach((lnk) => {
    const s = posById.get(lnk.source.data.node.id)
    const t = posById.get(lnk.target.data.node.id)
    if (!s || !t) return
    spineLinks.push({
      id: `s_${lnk.source.data.node.id}__${lnk.target.data.node.id}`,
      sourceId: lnk.source.data.node.id,
      targetId: lnk.target.data.node.id,
      source: s,
      target: t,
      path: spinePath(s, t),
      domainId: lnk.target.data.node.domainId,
    })
  })

  // Cross-links — only between two currently-visible nodes (keeps the overlay from
  // becoming a hairball, §3.3 / R4).
  const crossLinks: CrossLink[] = []
  for (const e of model.crossEdges) {
    const a = posById.get(e.src)
    const b = posById.get(e.dst)
    if (!a || !b) continue
    const bow = cfg.bow * (e.relClass === "xdom" ? cfg.xdomBow : 1)
    const { path, mid } = crossPath(a, b, bow)
    crossLinks.push({
      id: e.id,
      sourceId: e.src,
      targetId: e.dst,
      verb: e.verb,
      relClass: e.relClass,
      path,
      labelAt: mid,
    })
  }

  const treeBounds = boundsOf([...posById.values()], 60)

  // ── Off-tree Ungrouped tray — a fixed column right of the tree (§3.5/§4.7) ──
  const shown = model.trayItems.slice(0, cfg.trayCap)
  const trayOverflow = Math.max(0, model.trayItems.length - shown.length)
  const trayX0 = treeBounds.maxX + cfg.trayGap
  const trayY0 = treeBounds.minY + 40
  const trayItems: TrayLaidItem[] = shown.map((item, i) => {
    const col = i % cfg.trayCols
    const row = Math.floor(i / cfg.trayCols)
    return {
      ...item,
      x: trayX0 + col * cfg.trayCellW,
      y: trayY0 + row * cfg.trayCellH,
      radius: cfg.radius[item.type] ?? 9,
    }
  })
  const trayBounds = trayItems.length
    ? boundsOf(
        trayItems.map((t) => ({ x: t.x, y: t.y })),
        cfg.trayCellW / 2,
      )
    : null

  // Proposal hulls over the tray members they would group.
  const trayPos = new Map(trayItems.map((t) => [t.id, t]))
  const proposalHulls: ProposalHull[] = []
  for (const p of model.proposals) {
    const pts = p.memberIds.map((id) => trayPos.get(id)).filter((t): t is TrayLaidItem => !!t)
    if (!pts.length) continue
    const b = boundsOf(
      pts.map((t) => ({ x: t.x, y: t.y })),
      cfg.trayCellW / 2 + 6,
    )
    proposalHulls.push({
      id: p.id,
      name: p.name,
      band: p.band,
      kind: p.kind,
      x: b.minX,
      y: b.minY,
      width: b.width,
      height: b.height,
      memberIds: pts.map((t) => t.id),
    })
  }

  // Overall bounds cover the tree + tray.
  const allPts: Point[] = [
    { x: treeBounds.minX, y: treeBounds.minY },
    { x: treeBounds.maxX, y: treeBounds.maxY },
  ]
  if (trayBounds) {
    allPts.push({ x: trayBounds.minX, y: trayBounds.minY }, { x: trayBounds.maxX, y: trayBounds.maxY })
  }
  const bounds = boundsOf(allPts, 0)

  return {
    nodes: laid,
    spineLinks,
    crossLinks,
    trayItems,
    trayOverflow,
    trayBounds,
    proposalHulls,
    bounds,
  }
}

/**
 * A byte-stable hash of the laid-out node positions (G6 determinism check). Rounds to
 * 2 dp so sub-pixel FP noise never trips the regression guard.
 */
export function layoutHash(layout: Layout): string {
  const parts = layout.nodes
    .map((n) => `${n.id}:${n.x.toFixed(2)},${n.y.toFixed(2)}`)
    .sort()
  let h = 0
  const s = parts.join("|")
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0
  return h.toString(16)
}

/** Initial expansion (§5): org + domains + sub-domains open; assets visible, deeper collapsed. */
export function initialExpanded(model: EstateModel): Set<string> {
  const set = new Set<string>()
  if (model.root) set.add(model.root.id)
  for (const n of model.nodes) {
    if (n.type === "org" || n.type === "domain" || n.type === "subdomain") set.add(n.id)
  }
  return set
}

/** Ancestor path (root → node) for breadcrumb + search-to-reveal. */
export function ancestorPath(model: EstateModel, id: string): EstateNode[] {
  const byId = new Map(model.nodes.map((n) => [n.id, n]))
  const path: EstateNode[] = []
  let cur = byId.get(id) ?? null
  const guard = new Set<string>()
  while (cur && !guard.has(cur.id)) {
    guard.add(cur.id)
    path.unshift(cur)
    cur = cur.parentId ? byId.get(cur.parentId) ?? null : null
  }
  return path
}
