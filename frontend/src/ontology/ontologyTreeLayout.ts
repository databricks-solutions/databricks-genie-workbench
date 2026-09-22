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
   * Cap of Proposed-tray suggestion CARDS rendered before a "+N more suggested areas" note
   * (MV-D108). The long tail past this is summarised, not painted, so Proposed never renders
   * a wall of overlapping hulls. Only used when `groupTrayByProposal` is on.
   */
  proposalCap: number
  /** Column count for the balanced grid of Proposed suggestion cards (MV-D108). */
  proposalBlockCols: number
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
  proposalCap: 24,
  proposalBlockCols: 3,
  childCap: 10,
  crossLinkCap: 8,
  bow: 0.16,
  xdomBow: 1.35,
}

/** Sentinel id for a parent's "+N more" truncation chip (§6/R3). */
export function moreSentinelId(parentId: string): string {
  return `${parentId}::more`
}

/**
 * Popularity → radius (MV-D97, §6). Scales a base per-type radius by a BOUNDED log of the
 * node's `size` (the L6 rank score's usage/cost popularity, clamped [0.5, 2.0] server-side)
 * so a popular hub reads bigger WITHOUT blowing up into a hairball. Deterministic (fixed
 * `k`), monotonic in `size`, and clamped to `RADIUS_SIZE_MAX`×. Degrade-clean (MV-D43): a
 * node with no `size` (a synthetic chip, a pre-enrichment blob) keeps the base radius
 * EXACTLY. A non-positive size is floored at 0 so `ln(1+size)` never goes negative/NaN.
 */
export const RADIUS_SIZE_K = 0.5
export const RADIUS_SIZE_MAX = 1.6
export function scaleRadius(base: number, size?: number | null): number {
  if (size == null) return base
  const s = Math.max(0, size)
  const factor = Math.min(RADIUS_SIZE_MAX, 1 + RADIUS_SIZE_K * Math.log(1 + s))
  return base * factor
}

/**
 * Join strength → arc thickness (MV-D97, §6). Scales a cross-link's base per-class stroke by
 * a BOUNDED log of its `weight` (co-query count / similarity), so a strongly-joined pair reads
 * thicker without a thick tangle. Degrade-clean (MV-D43): a missing/non-positive weight ⇒
 * today's fixed base stroke EXACTLY. Deterministic (fixed `k`, clamped `CROSS_WEIGHT_MAX`×).
 * Lives here (not in the renderer) so it stays pure + unit-testable.
 */
export const CROSS_WEIGHT_K = 0.35
export const CROSS_WEIGHT_MAX = 2.4
export function crossStrokeWidth(base: number, weight?: number | null): number {
  if (weight == null || weight <= 0) return base
  return base * Math.min(CROSS_WEIGHT_MAX, 1 + CROSS_WEIGHT_K * Math.log(1 + weight))
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
  /**
   * Compact key/value bag from the snapshot (MV-D86 + MV-D97) — carries the additive
   * `certified`/`deprecated` authority flags the renderer reads. Optional/absent on a
   * synthetic node (the `+N more` chip) or a pre-enrichment blob (degrade → plain node).
   */
  meta?: Record<string, string> | null
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
   * Join/co-query strength (MV-D97, §6) — the renderer scales the arc stroke by a bounded
   * log of it; null/absent ⇒ the fixed per-class stroke (MV-D43).
   */
  weight?: number | null
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
  /** Suggestion cards summarised past `proposalCap` in the grouped Proposed tray (MV-D108). */
  proposalOverflow: number
  bounds: Bounds
}

/** Gating options for {@link layoutTree} (all optional; defaults keep prior behaviour). */
export interface LayoutOpts {
  /** Selected/focused node id — reveals just its typed arcs, with verb labels (R4). */
  focusId?: string | null
  /** Parent ids whose per-parent child cap is lifted (a `+N more` chip was clicked). */
  uncapped?: Set<string>
  /**
   * Tier-aware asset gating (owner directive). When PRESENT (even empty), expanding a
   * DOMAIN/ORG reveals only its container children (sub-domains) — its directly-attached
   * assets stay hidden until that domain id is in this set (an explicit "drill"), so the
   * default view reads a clean `Estate → Domain → Sub-domain`. Sub-domains and asset nodes
   * (e.g. a metric view → its measures) reveal their children on expand, unchanged. When
   * ABSENT (undefined) gating is OFF and every expanded node reveals all children (the legacy
   * pure-layout behaviour), so existing callers/tests are unaffected.
   */
  assetsExpanded?: Set<string>
  /**
   * Highlight a single relationship VERB across the whole visible tree (legend click,
   * Bloom-idiom, R25): draws every visible arc of that verb — labelled — and suppresses the
   * rest, so a rel-type reads as structure even past `crossLinkCap`. Node `focusId` wins when
   * both are set (a selection is the more specific navigation).
   */
  verbFocus?: string | null
  /**
   * Group the Ungrouped tray BY PROPOSAL (MV-D108): each suggestion becomes a tidy,
   * non-overlapping card in a balanced column grid, capped at `proposalCap`. ON under
   * Proposed/Both (where the tray + hulls render); OFF (default) keeps the flat grid, so
   * Applied and every existing caller/test are byte-identical.
   */
  groupTrayByProposal?: boolean
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
/** Container tiers (the always-navigable skeleton); everything else is a leaf/asset. */
function isContainerType(t: NodeType): boolean {
  return t === "org" || t === "domain" || t === "subdomain"
}

function buildVisibleHierarchy(
  model: EstateModel,
  expandedSet: Set<string>,
  childCap: number,
  uncapped: ReadonlySet<string>,
  assetGate: { on: boolean; drilled: ReadonlySet<string> },
): { rootDatum: Datum; hiddenBadge: Map<string, number> } | null {
  if (!model.root) return null
  // node id → collapse-badge count. A fully-hidden subtree reports its deep descendant count
  // (as before); a partially-open node (domain showing sub-domains but hiding its gated assets)
  // reports the honest count of hidden DIRECT children.
  const hiddenBadge = new Map<string, number>()
  const make = (node: EstateNode, isRoot: boolean): Datum & { children?: Datum[] } => {
    const kids = model.childrenByParent.get(node.id) ?? []
    const expanded = isRoot || expandedSet.has(node.id)
    // Tier-aware gating (opt-in): a DOMAIN/ORG reveals its container children on expand but
    // gates its directly-attached assets behind an explicit drill (`drilled`), so the default
    // reads Estate→Domain→Sub-domain. Sub-domains/assets reveal children on expand as before.
    const gateAssets = assetGate.on && (node.type === "domain" || node.type === "org")
    const showAssets = expanded && (!gateAssets || assetGate.drilled.has(node.id))
    const visible = kids.filter((k) => (isContainerType(k.type) ? expanded : showAssets))
    const hiddenCount = kids.length - visible.length
    if (hiddenCount > 0) {
      hiddenBadge.set(node.id, visible.length === 0 ? node.descendantCount : hiddenCount)
    }
    const datum: Datum & { children?: Datum[] } = { node }
    if (visible.length) {
      const capped = childCap > 0 && visible.length > childCap && !uncapped.has(node.id)
      const shown = capped ? visible.slice(0, childCap) : visible
      const children = shown.map((k) => make(k, false))
      if (capped) {
        children.push(makeMoreDatum(node.id, visible.length - shown.length, node.domainId))
      }
      datum.children = children
    }
    return datum
  }
  return { rootDatum: make(model.root, true), hiddenBadge }
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

/**
 * Vertical tidy-tree cubic bezier between parent and child (linkVertical shape). Scalar
 * endpoints so the SAME builder can recompute a spine `d` from live-dragged / tweened
 * endpoint positions imperatively (EstateGraph buttery interactions) — not just at layout
 * time. Pure + unit-tested.
 */
export function spinePath(sx: number, sy: number, tx: number, ty: number): string {
  const my = (sy + ty) / 2
  return `M${sx},${sy}C${sx},${my} ${tx},${my} ${tx},${ty}`
}

/**
 * Quadratic bezier bowed off the straight line between two points; xdom bows wider
 * (§4.1). Returns the path plus the arc midpoint (for the verb plate). Exported so an edge
 * can be redrawn from moved endpoints during drag / relayout glide with byte-identical
 * geometry to the layout pass. Pure + unit-tested.
 */
export function crossPath(
  a: Point,
  b: Point,
  bow: number,
  startTrim = 0,
  endTrim = 0,
): { path: string; mid: Point } {
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
  // Trim the DRAWN endpoints back to the node perimeter along the bezier's end tangents
  // (tangent at t=0 points a→c, at t=1 points c→b), so the `marker-end` arrowhead sits just
  // OUTSIDE the target disc instead of under it — the R25 fix. The control point (hence the
  // bow + arc shape) is unchanged, so the overlay stays deterministic.
  const trim = (from: Point, toward: Point, d: number): Point => {
    if (d <= 0) return from
    const vx = toward.x - from.x
    const vy = toward.y - from.y
    const vlen = Math.hypot(vx, vy) || 1
    return { x: from.x + (vx / vlen) * d, y: from.y + (vy / vlen) * d }
  }
  const ctrl = { x: cx, y: cy }
  const a2 = trim(a, ctrl, startTrim)
  const b2 = trim(b, ctrl, endTrim)
  // Midpoint of the (trimmed) quadratic bezier at t=0.5.
  const midX = 0.25 * a2.x + 0.5 * cx + 0.25 * b2.x
  const midY = 0.25 * a2.y + 0.5 * cy + 0.25 * b2.y
  return { path: `M${a2.x},${a2.y}Q${cx},${cy} ${b2.x},${b2.y}`, mid: { x: midX, y: midY } }
}

/**
 * Lay out the estate model deterministically.
 */
/** Result of {@link groupedTrayLayout} — the Proposed tray laid out as suggestion cards. */
interface GroupedTray {
  trayItems: TrayLaidItem[]
  trayOverflow: number
  trayBounds: Bounds | null
  proposalHulls: ProposalHull[]
  proposalOverflow: number
}

/**
 * MV-D108 — lay the Ungrouped tray GROUPED BY PROPOSAL so each suggestion reads as a tidy,
 * non-overlapping card instead of a wall of dashed hulls scattered over a flat grid (the
 * "Proposed is cluttered" report). Deterministic + pure:
 *  1. rank proposals by tray-member count (desc, then name, then id);
 *  2. greedily claim each tray asset for the highest-ranked proposal that wants it (an asset
 *     belongs to exactly one card, so cards never overlap by sharing a cell);
 *  3. keep the top `proposalCap` cards, summarising the tail as `proposalOverflow`;
 *  4. pack cards into `proposalBlockCols` balanced columns (shortest-column-first — blocks are
 *     size-desc so the packing stays even), each card a `trayCols`-wide mini-grid with a clean
 *     hull; column pitch clears the hull padding so adjacent cards never touch;
 *  5. lay the loose (unclaimed) assets in the existing flat grid below the cards, capped by
 *     `trayCap` with the usual `trayOverflow`.
 */
function groupedTrayLayout(model: EstateModel, cfg: LayoutConfig, treeBounds: Bounds): GroupedTray {
  // Dedupe tray items by id, preserving first-seen order (loose fallback order).
  const itemById = new Map<string, TrayItem>()
  for (const it of model.trayItems) if (!itemById.has(it.id)) itemById.set(it.id, it)

  const ranked = model.proposals
    .map((p) => ({ p, n: p.memberIds.reduce((c, id) => (itemById.has(id) ? c + 1 : c), 0) }))
    .filter((x) => x.n > 0)
    .sort((a, b) => b.n - a.n || (a.p.name < b.p.name ? -1 : a.p.name > b.p.name ? 1 : a.p.id < b.p.id ? -1 : 1))

  const claimed = new Set<string>()
  const allBlocks: { p: EstateProposal; members: string[] }[] = []
  for (const { p } of ranked) {
    const members = p.memberIds.filter((id) => itemById.has(id) && !claimed.has(id))
    if (!members.length) continue
    for (const id of members) claimed.add(id)
    allBlocks.push({ p, members })
  }

  const kept = allBlocks.slice(0, Math.max(0, cfg.proposalCap))
  const proposalOverflow = allBlocks.length - kept.length
  const keptIds = new Set<string>(kept.flatMap((b) => b.members))

  const trayX0 = treeBounds.maxX + cfg.trayGap
  const trayY0 = treeBounds.minY + 40
  const headerH = cfg.trayCellH
  const blockGap = Math.round(cfg.trayCellH * 0.7)
  const blockColW = cfg.trayCols * cfg.trayCellW + cfg.trayCellW // clears the hull padding gap
  const cols = Math.max(1, cfg.proposalBlockCols)
  const colBottom = new Array<number>(cols).fill(trayY0)
  // Asymmetric hull padding: WIDE in x (clear the disc's ellipsized label) but TIGHT in y, so
  // single-row cards stacked in a column stay separated by `blockGap` instead of overlapping.
  const padX = cfg.trayCellW / 2 + 6
  const padY = Math.round(cfg.trayCellH / 2) + 8

  const laid: TrayLaidItem[] = []
  const proposalHulls: ProposalHull[] = []
  for (const b of kept) {
    let c = 0
    for (let i = 1; i < cols; i++) if (colBottom[i] < colBottom[c]) c = i
    const colX0 = trayX0 + c * blockColW
    const cellTop = colBottom[c] + headerH
    let minX = Infinity
    let minY = Infinity
    let maxX = -Infinity
    let maxY = -Infinity
    b.members.forEach((id, i) => {
      const x = colX0 + (i % cfg.trayCols) * cfg.trayCellW
      const y = cellTop + Math.floor(i / cfg.trayCols) * cfg.trayCellH
      const it = itemById.get(id)!
      laid.push({ ...it, x, y, radius: cfg.radius[it.type] ?? 9 })
      if (x < minX) minX = x
      if (y < minY) minY = y
      if (x > maxX) maxX = x
      if (y > maxY) maxY = y
    })
    proposalHulls.push({
      id: b.p.id,
      name: b.p.name,
      band: b.p.band,
      kind: b.p.kind,
      x: minX - padX,
      y: minY - padY,
      width: maxX - minX + 2 * padX,
      height: maxY - minY + 2 * padY,
      memberIds: b.members,
    })
    const rows = Math.ceil(b.members.length / cfg.trayCols)
    colBottom[c] = cellTop + rows * cfg.trayCellH + blockGap
  }

  // Loose (unclaimed / past-cap) assets → flat grid below the tallest card column.
  const loose = [...itemById.keys()].filter((id) => !keptIds.has(id))
  const looseCap = Math.max(0, cfg.trayCap - laid.length)
  const looseShown = loose.slice(0, looseCap)
  const trayOverflow = loose.length - looseShown.length
  const looseTop = Math.max(trayY0, ...colBottom) + (kept.length ? headerH : 0)
  looseShown.forEach((id, i) => {
    const it = itemById.get(id)!
    laid.push({
      ...it,
      x: trayX0 + (i % cfg.trayCols) * cfg.trayCellW,
      y: looseTop + Math.floor(i / cfg.trayCols) * cfg.trayCellH,
      radius: cfg.radius[it.type] ?? 9,
    })
  })

  const trayBounds = laid.length ? boundsOf(laid.map((t) => ({ x: t.x, y: t.y })), cfg.trayCellW / 2) : null
  return { trayItems: laid, trayOverflow, trayBounds, proposalHulls, proposalOverflow }
}

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
  const built = buildVisibleHierarchy(model, expandedSet, cfg.childCap, uncapped, {
    on: opts.assetsExpanded != null,
    drilled: opts.assetsExpanded ?? EMPTY_SET,
  })
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
      proposalOverflow: 0,
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
    const badge = built.hiddenBadge.get(node.id) ?? 0
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
      radius: scaleRadius(cfg.radius[node.type] ?? 10, node.size),
      meta: node.meta ?? null,
      collapsed: badge > 0,
      badge,
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
      path: spinePath(s.x, s.y, t.x, t.y),
      domainId: lnk.target.data.node.domainId,
    })
  })

  // Cross-links (§3.3 / §6 / R4). Two-stage gate so the typed overlay reads as structure,
  // never a hairball:
  //   1. VISIBLE-ONLY — both endpoints must currently be on the tree (expanded).
  //   2. FOCUS — relationships are OFF at rest (owner directive: appear on click, disappear
  //      on click). A node selection draws ONLY that node's arcs, each with its verb label; a
  //      legend verb-focus draws every visible arc of that verb. With NO focus, no arcs draw —
  //      `crossLinkOverflow` reports how many are available so the UI can offer a
  //      "click a node to trace" affordance. (`crossLinkCap` no longer gates the at-rest mat.)
  const radiusById = new Map(laid.map((n) => [n.id, n.radius]))
  const candidates: CrossLink[] = []
  for (const e of model.crossEdges) {
    const a = posById.get(e.src)
    const b = posById.get(e.dst)
    if (!a || !b) continue
    const bow = cfg.bow * (e.relClass === "xdom" ? cfg.xdomBow : 1)
    // Pull the tail off the source disc and leave arrowhead headroom past the target disc
    // so the marker-end is visible (R25).
    const startTrim = (radiusById.get(e.src) ?? 0) + 2
    const endTrim = (radiusById.get(e.dst) ?? 0) + 7
    const { path, mid } = crossPath(a, b, bow, startTrim, endTrim)
    candidates.push({
      id: e.id,
      sourceId: e.src,
      targetId: e.dst,
      verb: e.verb,
      relClass: e.relClass,
      path,
      labelAt: mid,
      showLabel: false,
      weight: e.weight ?? null,
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
  } else {
    // At rest: no arcs (edges-on-focus, not edges-at-rest) — report the count instead.
    crossLinks = []
    crossLinkOverflow = candidates.length
  }

  const treeBounds = boundsOf([...posById.values()], 60)

  // ── Off-tree Ungrouped tray — a fixed column right of the tree (§3.5/§4.7) ──
  // Under Proposed/Both (`groupTrayByProposal`) the tray is laid out grouped by proposal so
  // each suggestion is a tidy, non-overlapping card (MV-D108); Applied keeps the flat grid
  // (the tray isn't rendered there anyway), so it stays byte-identical.
  let trayItems: TrayLaidItem[]
  let trayOverflow: number
  let trayBounds: Bounds | null
  let proposalHulls: ProposalHull[]
  let proposalOverflow = 0
  if (opts.groupTrayByProposal && model.proposals.length > 0) {
    const g = groupedTrayLayout(model, cfg, treeBounds)
    trayItems = g.trayItems
    trayOverflow = g.trayOverflow
    trayBounds = g.trayBounds
    proposalHulls = g.proposalHulls
    proposalOverflow = g.proposalOverflow
  } else {
    const shown = model.trayItems.slice(0, cfg.trayCap)
    trayOverflow = Math.max(0, model.trayItems.length - shown.length)
    const trayX0 = treeBounds.maxX + cfg.trayGap
    const trayY0 = treeBounds.minY + 40
    trayItems = shown.map((item, i) => {
      const col = i % cfg.trayCols
      const row = Math.floor(i / cfg.trayCols)
      return {
        ...item,
        x: trayX0 + col * cfg.trayCellW,
        y: trayY0 + row * cfg.trayCellH,
        radius: cfg.radius[item.type] ?? 9,
      }
    })
    trayBounds = trayItems.length
      ? boundsOf(
          trayItems.map((t) => ({ x: t.x, y: t.y })),
          cfg.trayCellW / 2,
        )
      : null

    // Proposal hulls over the tray members they would group.
    const trayPos = new Map(trayItems.map((t) => [t.id, t]))
    proposalHulls = []
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
    proposalOverflow,
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

/**
 * Initial / reset expansion (§5, owner directive): open org + domains ONLY, so the default
 * view reads exactly three tiers — `Estate → Domain → Sub-domain`. Sub-domains are VISIBLE
 * (their parent domain is expanded) but stay COLLAPSED containers (a `+N` badge), so their
 * assets are NOT revealed by default. With `LayoutOpts.assetsExpanded` gating on (the app),
 * a domain's directly-attached assets are ALSO gated — a domain shows only its sub-domains
 * until the curator drills in — so the default view carries no leaf assets at all.
 */
export function initialExpanded(model: EstateModel): Set<string> {
  const set = new Set<string>()
  if (model.root) set.add(model.root.id)
  for (const n of model.nodes) {
    if (n.type === "org" || n.type === "domain") set.add(n.id)
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
 * A viewport rectangle in CONTENT (pre-transform) coordinates — exactly the shape
 * {@link viewportContentRect} produces and the minimap consumes. Kept structural (no import
 * from the minimap) so the culler stays a leaf of this pure module.
 */
export interface ViewportRect {
  x1: number
  y1: number
  x2: number
  y2: number
}

/** Options for {@link cullToViewport} (all optional; the defaults keep the whole scene). */
export interface CullOptions {
  /**
   * Node ids that must ALWAYS survive regardless of viewport — the selected / searched /
   * hovered / dragged node and its breadcrumb ancestors — so navigation never culls the very
   * node the curator is acting on, even when it is off-screen.
   */
  keep?: ReadonlySet<string>
  /**
   * Padding around the viewport, as a fraction of its own width/height applied on EACH side
   * (default 1.0 → a full viewport-span of headroom in every direction). The margin means a
   * pan reveals already-mounted nodes before the next zoom-end re-render, so scrolling never
   * flashes empty.
   */
  pad?: number
  /**
   * Node-count floor (default 600): at or below it, culling is skipped entirely and the inputs
   * are returned unchanged (referential passthrough). Small estates stay byte-identical.
   */
  threshold?: number
}

export const CULL_THRESHOLD = 600
export const CULL_PAD = 1.0

export interface CullResult {
  nodes: LaidNode[]
  spineLinks: SpineLink[]
  crossLinks: CrossLink[]
}

/**
 * Viewport culling (MV-D106, Phase 1) — the DETERMINISTIC, PURE narrowing of a laid-out scene to
 * just what the camera can see (plus a keep-set), so a huge estate mounts a bounded number of SVG
 * nodes instead of the whole tree at once.
 *
 * DEFAULT-SAFE passthrough (the byte-identical path): with no viewport (`viewportRect == null`, e.g.
 * the pre-measure first paint) OR a scene at/under `threshold` nodes, the inputs are returned
 * UNCHANGED — the SAME array references — so small estates and SSR render exactly as before and no
 * downstream memo/effect sees a new identity.
 *
 * Above the threshold with a viewport: a node survives iff its disc — its (x,y) ± its radius —
 * intersects the viewport PADDED by `pad`× its width/height on each side, OR its id is in `keep`
 * (so the selected / searched / hovered / dragged node and breadcrumb ancestors are ALWAYS mounted,
 * even off-screen). An edge survives iff BOTH endpoints survived. Input order is preserved (stable
 * React keys) and no node is ever invented. Pure + unit-testable — the camera never enters here.
 */
export function cullToViewport(
  nodes: LaidNode[],
  spineLinks: SpineLink[],
  crossLinks: CrossLink[],
  viewportRect: ViewportRect | null,
  opts: CullOptions = {},
): CullResult {
  const threshold = opts.threshold ?? CULL_THRESHOLD
  // Default-safe: no viewport, or a small-enough scene ⇒ hand back the inputs unchanged.
  if (viewportRect == null || nodes.length <= threshold) {
    return { nodes, spineLinks, crossLinks }
  }
  const keep = opts.keep ?? EMPTY_SET
  const pad = opts.pad ?? CULL_PAD
  // Normalize (robust to an inverted rect) then pad by a fraction of the viewport's own span.
  const vMinX = Math.min(viewportRect.x1, viewportRect.x2)
  const vMaxX = Math.max(viewportRect.x1, viewportRect.x2)
  const vMinY = Math.min(viewportRect.y1, viewportRect.y2)
  const vMaxY = Math.max(viewportRect.y1, viewportRect.y2)
  const padX = (vMaxX - vMinX) * pad
  const padY = (vMaxY - vMinY) * pad
  const minX = vMinX - padX
  const maxX = vMaxX + padX
  const minY = vMinY - padY
  const maxY = vMaxY + padY

  const keptIds = new Set<string>()
  const keptNodes: LaidNode[] = []
  for (const n of nodes) {
    const r = n.radius
    const inView = n.x + r >= minX && n.x - r <= maxX && n.y + r >= minY && n.y - r <= maxY
    if (inView || keep.has(n.id)) {
      keptIds.add(n.id)
      keptNodes.push(n)
    }
  }
  const keptSpine = spineLinks.filter((l) => keptIds.has(l.sourceId) && keptIds.has(l.targetId))
  const keptCross = crossLinks.filter((c) => keptIds.has(c.sourceId) && keptIds.has(c.targetId))
  return { nodes: keptNodes, spineLinks: keptSpine, crossLinks: keptCross }
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
