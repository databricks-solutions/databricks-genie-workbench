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
  /**
   * Sibling gap (x) fed to d3.tree().nodeSize. Sized to clear a plated, ellipsized
   * label so sibling names never collide/hard-clip at the fit zoom (R12d).
   */
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
  /**
   * Per-parent visible-child cap (§6). A parent with more expanded children than this
   * renders the first `childCap` (stable sort) plus a synthetic `+N more` sentinel that
   * expands on click — so no parent ever dumps a nameless band of children (R3).
   */
  childCap: number
  /**
   * Cross-link overlay cap when NOTHING is focused (§6/R4). Above this many
   * simultaneously-visible typed arcs the unfocused overlay is suppressed (a "+N links"
   * affordance is surfaced instead) so realistic scale never paints a hairball; a
   * selection re-reveals just that node's arcs (with verb labels).
   */
  crossLinkCap: number
  /** Bow factor for cross-link beziers; xdom is multiplied by `xdomBow`. */
  bow: number
  xdomBow: number
}

export const DEFAULT_LAYOUT: LayoutConfig = {
  dx: 118,
  dy: 128,
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
  childCap: 10,
  crossLinkCap: 8,
  bow: 0.16,
  xdomBow: 1.35,
}

/** Sentinel id for a parent's "+N more" truncation chip (§6/R3). */
export function moreSentinelId(parentId: string): string {
  return `${parentId}::more`
}

const EMPTY_SET: ReadonlySet<string> = new Set<string>()

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
  /** Hierarchy parent id (null for the root). Drives the `+N more` expand target. */
  parentId: string | null
  kind: string
  radius: number
  /** True when this node has children that are currently collapsed. */
  collapsed: boolean
  /** Descendant count for the `+N` badge (0 when expanded / leaf). */
  badge: number
  memberCount: number | null
  cost: number | null
  /** Synthetic "+N more" truncation chip (§6/R3) — rendered as a pill, not a disc. */
  isMore: boolean
  /** Count of children this chip stands in for (0 for real nodes). */
  moreCount: number
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
  /**
   * Whether to draw the verb plate. Only the focused node's arcs are labelled (R4) so
   * realistic scale never paints a red verb-label cloud.
   */
  showLabel: boolean
  /** Optional pre-seed evidence bag (MV-D88) for the hover edge-tooltip (R25); null when absent. */
  detail?: Record<string, string> | null
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
  /** The cross-links actually drawn after visible-only + focus/cap gating (R4). */
  crossLinks: CrossLink[]
  /**
   * Count of visible typed arcs suppressed because nothing is focused and the density is
   * over `crossLinkCap` — surfaced as a "+N links" affordance (R4). 0 when focused or below
   * the cap.
   */
  crossLinkOverflow: number
  trayItems: TrayLaidItem[]
  trayOverflow: number
  trayBounds: Bounds | null
  proposalHulls: ProposalHull[]
  bounds: Bounds
}

/** Gating options for {@link layoutTree} (all optional; defaults keep prior behaviour). */
export interface LayoutOpts {
  /** Selected/focused node id — reveals just its typed arcs, with verb labels (R4). */
  focusId?: string | null
  /** Parent ids whose per-parent child cap is lifted (a `+N more` chip was clicked). */
  uncapped?: Set<string>
  /**
   * Highlight a single relationship VERB across the whole visible tree (legend click,
   * Bloom-idiom, R25): draws every visible arc of that verb — labelled — and suppresses the
   * rest, so a rel-type reads as structure even past `crossLinkCap`. Node `focusId` wins when
   * both are set (a selection is the more specific navigation).
   */
  verbFocus?: string | null
}

/** Internal d3 hierarchy datum. */
interface Datum {
  node: EstateNode
  /** When set, this datum is a synthetic "+N more" truncation chip for `parentId`. */
  more?: { parentId: string; count: number }
}

/** Synthetic EstateNode-shaped datum for a parent's "+N more" truncation chip (§6/R3). */
function makeMoreDatum(parentId: string, count: number, domainId: string | null): Datum {
  return {
    node: {
      id: moreSentinelId(parentId),
      parentId,
      type: "table", // radius fallback only; rendered as a pill, never a disc
      label: `+${count} more`,
      displayName: `+${count} more`,
      origin: "applied",
      domainId,
      attachLevel: null,
      kind: "__more__",
      memberCount: null,
      cost: null,
      descendantCount: 0,
    },
    more: { parentId, count },
  }
}

/**
 * Build the visible hierarchy: start at root, descend only through expanded nodes.
 * A node is "expanded" if it is in `expandedSet`; its children are included only then.
 * The root is always expanded.
 *
 * Per-parent cap (§6/R3): when an expanded parent has more children than `childCap` (and
 * it is not in `uncapped`), only the first `childCap` (stable-sorted in the model) are laid
 * out and a synthetic `+N more` sentinel is appended — deterministic, so no parent ever
 * renders a nameless band. The chip lifts the cap for that parent when clicked.
 */
function buildVisibleHierarchy(
  model: EstateModel,
  expandedSet: Set<string>,
  childCap: number,
  uncapped: ReadonlySet<string>,
): { rootDatum: Datum; hasHiddenChildren: Set<string> } | null {
  if (!model.root) return null
  const hasHiddenChildren = new Set<string>()
  const make = (node: EstateNode, isRoot: boolean): Datum & { children?: Datum[] } => {
    const kids = model.childrenByParent.get(node.id) ?? []
    const expanded = isRoot || expandedSet.has(node.id)
    if (kids.length && !expanded) hasHiddenChildren.add(node.id)
    const datum: Datum & { children?: Datum[] } = { node }
    if (expanded && kids.length) {
      const capped = childCap > 0 && kids.length > childCap && !uncapped.has(node.id)
      const shown = capped ? kids.slice(0, childCap) : kids
      const children = shown.map((k) => make(k, false))
      if (capped) {
        children.push(makeMoreDatum(node.id, kids.length - shown.length, node.domainId))
      }
      datum.children = children
    }
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
  opts: LayoutOpts = {},
): Layout {
  const focusId = opts.focusId ?? null
  const verbFocus = opts.verbFocus ?? null
  const uncapped = opts.uncapped ?? EMPTY_SET
  const built = buildVisibleHierarchy(model, expandedSet, cfg.childCap, uncapped)
  if (!built) {
    return {
      nodes: [],
      spineLinks: [],
      crossLinks: [],
      crossLinkOverflow: 0,
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
    const more = hn.data.more
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
      parentId: node.parentId,
      kind: node.kind,
      radius: cfg.radius[node.type] ?? 10,
      collapsed,
      badge: collapsed ? node.descendantCount : 0,
      memberCount: node.memberCount,
      cost: node.cost,
      isMore: !!more,
      moreCount: more?.count ?? 0,
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

  // Cross-links (§3.3 / §6 / R4). Two-stage gate so the typed overlay reads as structure,
  // never a hairball:
  //   1. VISIBLE-ONLY — both endpoints must currently be on the tree (expanded).
  //   2. FOCUS/CAP — with a selection, draw ONLY that node's arcs, each with its verb
  //      label; with no selection, draw every arc unlabelled up to `crossLinkCap`, and
  //      above the cap suppress the mat entirely (surface a "+N links" affordance instead).
  const candidates: CrossLink[] = []
  for (const e of model.crossEdges) {
    const a = posById.get(e.src)
    const b = posById.get(e.dst)
    if (!a || !b) continue
    const bow = cfg.bow * (e.relClass === "xdom" ? cfg.xdomBow : 1)
    const { path, mid } = crossPath(a, b, bow)
    candidates.push({
      id: e.id,
      sourceId: e.src,
      targetId: e.dst,
      verb: e.verb,
      relClass: e.relClass,
      path,
      labelAt: mid,
      showLabel: false,
      detail: e.detail ?? null,
    })
  }
  const focusVisible = focusId != null && posById.has(focusId)
  let crossLinks: CrossLink[]
  let crossLinkOverflow = 0
  if (focusVisible) {
    crossLinks = candidates
      .filter((c) => c.sourceId === focusId || c.targetId === focusId)
      .map((c) => ({ ...c, showLabel: true }))
  } else if (verbFocus) {
    // Legend rel-type highlight (R25): reveal every visible arc of this verb, labelled.
    crossLinks = candidates.filter((c) => c.verb === verbFocus).map((c) => ({ ...c, showLabel: true }))
  } else if (candidates.length <= cfg.crossLinkCap) {
    crossLinks = candidates
  } else {
    crossLinks = []
    crossLinkOverflow = candidates.length
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
    crossLinkOverflow,
    trayItems,
    trayOverflow,
    trayBounds,
    proposalHulls,
    bounds,
  }
}

/**
 * Bounding box of the content the camera should frame (R12a fit-to-bounds). Unions the
 * laid-out node discs (incl. radius) with any visible cross-link arc extents (their bowed
 * label midpoints — so a selected node's verb arcs are always kept on-canvas, R12b) and,
 * when requested, the off-tree tray. Pure + unit-testable so the fit math is covered
 * without mounting the camera.
 */
export function contentBounds(
  layout: Layout,
  opts: { tree?: boolean; tray?: boolean } = { tree: true },
): Bounds {
  const pts: Point[] = []
  if (opts.tree) {
    for (const n of layout.nodes) {
      const r = n.radius + 22 // disc + label plate headroom
      pts.push({ x: n.x - r, y: n.y - r }, { x: n.x + r, y: n.y + r })
    }
    for (const c of layout.crossLinks) {
      pts.push({ x: c.labelAt.x, y: c.labelAt.y })
    }
  }
  if (opts.tray && layout.trayBounds) {
    pts.push(
      { x: layout.trayBounds.minX, y: layout.trayBounds.minY },
      { x: layout.trayBounds.maxX, y: layout.trayBounds.maxY },
    )
  }
  return boundsOf(pts, 0)
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

/**
 * Collapse-all target (§5 / MV-D87 P0-a): only the org root is expanded, so the tree reads
 * down to the DOMAIN tier — every domain a collapsed container with its `+N` badge, nothing
 * below on screen. The counterpart to `Expand all`; the map re-fits after applying it.
 */
export function collapsedToDomainTier(model: EstateModel): Set<string> {
  const set = new Set<string>()
  if (model.root) set.add(model.root.id)
  return set
}

/**
 * The current viewport rectangle expressed in CONTENT (pre-transform) coordinates, for the
 * minimap "you-are-here" box (MV-D87 P1-a). Given the live `d3.zoom` transform (screen =
 * content·k + translate) and the on-screen canvas size, inverts to the content-space rect the
 * camera currently frames. Pure — the minimap stays presentational.
 */
export function viewportContentRect(
  transform: { x: number; y: number; k: number },
  size: { width: number; height: number },
): { x1: number; y1: number; x2: number; y2: number } {
  const k = transform.k || 1
  return {
    x1: (0 - transform.x) / k,
    y1: (0 - transform.y) / k,
    x2: (size.width - transform.x) / k,
    y2: (size.height - transform.y) / k,
  }
}

/**
 * The `d3.zoom` transform that recenters the camera on a CONTENT-space point at the current
 * scale (MV-D87 P1-a — minimap click/drag pan). Keeps the zoom level; only translates so the
 * point lands at the viewport centre. Pure + unit-testable so the nav math is covered without
 * a live camera.
 */
export function centerOnTransform(
  point: Point,
  size: { width: number; height: number },
  k: number,
): { x: number; y: number; k: number } {
  return { x: size.width / 2 - point.x * k, y: size.height / 2 - point.y * k, k }
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
