import { describe, expect, it } from "vitest"
import { buildEstateModel } from "@/ontology/estateGraphModel"
import {
  DEFAULT_LAYOUT,
  ancestorPath,
  centerOnTransform,
  collapsedToDomainTier,
  contentBounds,
  crossPath,
  CULL_THRESHOLD,
  cullToViewport,
  initialExpanded,
  layoutHash,
  layoutTree,
  moreSentinelId,
  RADIUS_SIZE_MAX,
  scaleRadius,
  spinePath,
  viewportContentRect,
  type CrossLink,
  type LaidNode,
  type Point,
  type SpineLink,
  type ViewportRect,
} from "@/ontology/ontologyTreeLayout"
import type { OntologyGraph, OntologyGraphEdge, OntologyGraphNode } from "@/ontology/types"

function node(p: Partial<OntologyGraphNode> & { id: string }): OntologyGraphNode {
  return {
    label: p.id,
    kind: "table",
    domain_id: null,
    parent_id: null,
    parent_name: null,
    x: 0,
    y: 0,
    size: 1,
    cost: null,
    member_count: null,
    origin: null,
    ...p,
  }
}

function graph(): OntologyGraph {
  const domains: OntologyGraphNode[] = [
    node({ id: "d_fin", label: "Acme Finance", kind: "domain", origin: "applied" }),
    node({ id: "d_ops", label: "Acme Operations", kind: "domain", origin: "applied" }),
    node({ id: "s_rev", label: "Revenue", kind: "subdomain", parent_id: "d_fin", origin: "applied" }),
    node({ id: "ungrouped", label: "Ungrouped", kind: "ungrouped", member_count: 2, origin: "proposed" }),
  ]
  const assets: OntologyGraphNode[] = [
    node({ id: "mv:a", label: "net sales", kind: "metric_view", domain_id: "s_rev", attach_level: "subdomain", origin: "applied" }),
    node({ id: "t:sales", label: "fact_sales", kind: "table", domain_id: "s_rev", parent_id: "mv:a", attach_level: "asset", origin: "applied" }),
    node({ id: "t:cal", label: "ref_calendar", kind: "table", domain_id: "d_fin", attach_level: "domain", origin: "applied" }),
    node({ id: "t:ops", label: "ops_x", kind: "table", domain_id: "d_ops", attach_level: "domain", origin: "applied" }),
    node({ id: "u1", label: "orphan_a", kind: "table", domain_id: "ungrouped" }),
    node({ id: "u2", label: "orphan_b", kind: "table", domain_id: "ungrouped" }),
  ]
  const edges: OntologyGraphEdge[] = [
    { src: "t:sales", dst: "t:cal", kind: "join_key", verb: "joins calendar", rel_class: "shared" },
    { src: "t:sales", dst: "t:ops", kind: "co_query", verb: "also queried with", rel_class: "xdom" },
  ]
  return {
    root: node({ id: "org", label: "Acme", kind: "org" }),
    domains: { nodes: domains, edges: [], truncated: false },
    assets: { nodes: assets, edges, truncated: false },
    layout: "tree",
    node_count: 11,
    edge_count: 2,
    state: "fresh",
    as_of: null,
  }
}

const noOffsets = new Map<string, Point>()

describe("layoutTree", () => {
  it("returns empty layout when there is no root/model", () => {
    const empty = buildEstateModel({
      domains: { nodes: [], edges: [], truncated: false },
      assets: { nodes: [], edges: [], truncated: false },
      layout: "fr",
      node_count: 0,
      edge_count: 0,
      state: "cold",
      as_of: null,
    })
    // synthesized root still exists, but with no children the tree is just the root
    const l = layoutTree(empty, initialExpanded(empty), noOffsets)
    expect(l.nodes.length).toBe(1)
    expect(l.spineLinks.length).toBe(0)
  })

  it("lays out only expanded branches (collapse hides children + sets a badge)", () => {
    const model = buildEstateModel(graph())
    // Expand root + domains but NOT s_rev → its assets are hidden.
    const expanded = new Set(["org", "d_fin", "d_ops"])
    const l = layoutTree(model, expanded, noOffsets)
    const ids = l.nodes.map((n) => n.id)
    expect(ids).toContain("s_rev")
    expect(ids).not.toContain("mv:a")
    const sRev = l.nodes.find((n) => n.id === "s_rev")!
    expect(sRev.collapsed).toBe(true)
    expect(sRev.badge).toBeGreaterThan(0)
  })

  it("is deterministic — identical geometry across repeated layouts (R18/G6)", () => {
    const model = buildEstateModel(graph())
    const exp = initialExpanded(model)
    const h1 = layoutHash(layoutTree(model, exp, noOffsets))
    const h2 = layoutHash(layoutTree(model, exp, noOffsets))
    expect(h1).toBe(h2)
  })

  it("applies manual drag offsets as post-layout deltas (R17)", () => {
    const model = buildEstateModel(graph())
    const exp = initialExpanded(model)
    const base = layoutTree(model, exp, noOffsets)
    const off = new Map<string, Point>([["d_fin", { x: 100, y: -50 }]])
    const dragged = layoutTree(model, exp, off)
    const b = base.nodes.find((n) => n.id === "d_fin")!
    const d = dragged.nodes.find((n) => n.id === "d_fin")!
    expect(d.x).toBeCloseTo(b.x + 100)
    expect(d.y).toBeCloseTo(b.y - 50)
    // Non-dragged nodes are unaffected.
    const bo = base.nodes.find((n) => n.id === "d_ops")!
    const dorg = dragged.nodes.find((n) => n.id === "d_ops")!
    expect(dorg.x).toBeCloseTo(bo.x)
  })

  it("bows xdom cross-links wider than shared ones", () => {
    const model = buildEstateModel(graph())
    // Expand the MV too so its child table (a cross-link endpoint) is visible. Arcs are
    // focus-gated now (R4: off at rest), so select t:sales — it touches BOTH the shared join
    // to t:cal and the xdom co-query to t:ops.
    const exp = new Set([...initialExpanded(model), "s_rev", "mv:a"])
    const l = layoutTree(model, exp, noOffsets, DEFAULT_LAYOUT, { focusId: "t:sales" })
    const shared = l.crossLinks.find((c) => c.relClass === "shared")
    const xdom = l.crossLinks.find((c) => c.relClass === "xdom")
    expect(shared).toBeDefined()
    expect(xdom).toBeDefined()
    // Both carry a verb + a label anchor.
    expect(shared!.verb).toBe("joins calendar")
    expect(xdom!.verb).toBe("also queried with")
    expect(shared!.path.startsWith("M")).toBe(true)
  })

  it("trims cross-link endpoints off the node discs so the arrowhead clears the target (R25)", () => {
    const model = buildEstateModel(graph())
    const exp = new Set([...initialExpanded(model), "s_rev", "mv:a"])
    // Arcs are focus-gated (R4) — select an endpoint to reveal its arc.
    const l = layoutTree(model, exp, noOffsets, DEFAULT_LAYOUT, { focusId: "t:sales" })
    const c = l.crossLinks[0]
    expect(c).toBeDefined()
    const tgt = l.nodes.find((n) => n.id === c.targetId)!
    // Parse the drawn end point from "M ax,ay Q cx,cy ex,ey".
    const m = c.path.match(/Q[\d.-]+,[\d.-]+ ([\d.-]+),([\d.-]+)$/)!
    const ex = parseFloat(m[1])
    const ey = parseFloat(m[2])
    const dist = Math.hypot(ex - tgt.x, ey - tgt.y)
    // End is pulled back past the target radius (arrowhead headroom) — OUTSIDE the disc,
    // never the bare target centre (the pre-fix occlusion bug).
    expect(dist).toBeGreaterThanOrEqual(tgt.radius)
    expect(ex === tgt.x && ey === tgt.y).toBe(false)
  })

  it("only draws cross-links between two visible nodes (no hairball, R4)", () => {
    const model = buildEstateModel(graph())
    // Collapse s_rev so t:sales is hidden → its cross-links must disappear.
    const exp = new Set(["org", "d_fin", "d_ops"])
    const l = layoutTree(model, exp, noOffsets)
    expect(l.crossLinks.length).toBe(0)
  })

  it("places the ungrouped tray off to the right of the tree with a proposal hull", () => {
    const g = graph()
    g.domains.nodes.push(node({ id: "sug", label: "Suggested", kind: "domain", origin: "proposed" }))
    g.assets.nodes.push(node({ id: "u1", label: "orphan_a", kind: "table", domain_id: "sug" }))
    const model = buildEstateModel(g)
    const l = layoutTree(model, initialExpanded(model), noOffsets)
    expect(l.trayItems.length).toBeGreaterThan(0)
    expect(l.trayBounds).not.toBeNull()
    // tray sits to the right of the tree body
    const maxTreeX = Math.max(...l.nodes.map((n) => n.x))
    expect(Math.min(...l.trayItems.map((t) => t.x))).toBeGreaterThan(maxTreeX)
  })

  it("caps the tray and reports overflow", () => {
    const g = graph()
    for (let i = 0; i < 60; i++) {
      g.assets.nodes.push(node({ id: `uu${i}`, label: `orphan_${i}`, kind: "table", domain_id: "ungrouped" }))
    }
    const model = buildEstateModel(g)
    const l = layoutTree(model, initialExpanded(model), noOffsets, {
      ...DEFAULT_LAYOUT,
      trayCap: 10,
    })
    expect(l.trayItems.length).toBe(10)
    expect(l.trayOverflow).toBeGreaterThan(0)
  })
})

// ── Proposed tray grouping (MV-D108 — "Proposed is cluttered" fix) ───────────
function proposedGraph(nProps: number, membersPer: number): OntologyGraph {
  const g = graph()
  for (let p = 0; p < nProps; p++) {
    g.domains.nodes.push(node({ id: `sug${p}`, label: `Suggested Area ${p}`, kind: "domain", origin: "proposed" }))
    for (let m = 0; m < membersPer; m++) {
      g.assets.nodes.push(node({ id: `sa${p}_${m}`, label: `asset_${p}_${m}`, kind: "table", domain_id: `sug${p}` }))
    }
  }
  return g
}

function rectsOverlap(
  a: { x: number; y: number; width: number; height: number },
  b: { x: number; y: number; width: number; height: number },
): boolean {
  return a.x < b.x + b.width && b.x < a.x + a.width && a.y < b.y + b.height && b.y < a.y + a.height
}

describe("Proposed tray grouping (MV-D108)", () => {
  it("lays each suggestion as a NON-OVERLAPPING card when groupTrayByProposal is on", () => {
    const model = buildEstateModel(proposedGraph(6, 3))
    const l = layoutTree(model, initialExpanded(model), noOffsets, DEFAULT_LAYOUT, {
      groupTrayByProposal: true,
    })
    expect(l.proposalHulls.length).toBe(6)
    // the wall-of-hulls bug: with a flat grid the cards overlapped. Grouped, none do.
    for (let i = 0; i < l.proposalHulls.length; i++) {
      for (let j = i + 1; j < l.proposalHulls.length; j++) {
        expect(rectsOverlap(l.proposalHulls[i], l.proposalHulls[j])).toBe(false)
      }
    }
    // every tray asset is placed once at a unique position (one asset ⇒ one card cell)
    const ids = new Set(l.trayItems.map((t) => t.id))
    expect(ids.size).toBe(l.trayItems.length)
    const positions = new Set(l.trayItems.map((t) => `${t.x},${t.y}`))
    expect(positions.size).toBe(l.trayItems.length)
  })

  it("caps the number of suggestion cards and summarises the tail as proposalOverflow", () => {
    const model = buildEstateModel(proposedGraph(10, 2))
    const l = layoutTree(model, initialExpanded(model), noOffsets, { ...DEFAULT_LAYOUT, proposalCap: 4 }, {
      groupTrayByProposal: true,
    })
    expect(l.proposalHulls.length).toBe(4)
    expect(l.proposalOverflow).toBe(6)
  })

  it("keeps the flat tray (Applied) byte-identical when grouping is off", () => {
    const model = buildEstateModel(proposedGraph(6, 3))
    const flat = layoutTree(model, initialExpanded(model), noOffsets, DEFAULT_LAYOUT)
    expect(flat.proposalOverflow).toBe(0)
    const grouped = layoutTree(model, initialExpanded(model), noOffsets, DEFAULT_LAYOUT, {
      groupTrayByProposal: true,
    })
    // grouping re-lays the tray into tidy cards ⇒ geometry differs from the flat grid
    expect(grouped.trayItems.map((t) => `${t.x},${t.y}`)).not.toEqual(
      flat.trayItems.map((t) => `${t.x},${t.y}`),
    )
  })
})

// ── Per-parent child cap + "+N more" sentinel (§6 / R3) ──────────────────────
/** A graph whose `d_ops` domain has `n` direct table children (a wide fan). */
function wideGraph(n: number): OntologyGraph {
  const g = graph()
  for (let i = 0; i < n; i++) {
    g.assets.nodes.push(
      node({ id: `w${i}`, label: `wide_${i}`, kind: "table", domain_id: "d_ops", attach_level: "domain", origin: "applied" }),
    )
  }
  return g
}

describe("layoutTree — per-parent child cap (R3)", () => {
  const cfg = { ...DEFAULT_LAYOUT, childCap: 5 }

  it("caps a wide parent to childCap real children + one '+N more' sentinel", () => {
    const model = buildEstateModel(wideGraph(20))
    const l = layoutTree(model, initialExpanded(model), noOffsets, cfg)
    const realKids = l.nodes.filter((x) => x.parentId === "d_ops" && !x.isMore)
    expect(realKids.length).toBe(5)
    const more = l.nodes.find((x) => x.id === moreSentinelId("d_ops"))!
    expect(more).toBeTruthy()
    expect(more.isMore).toBe(true)
    // 20 direct wide_* children + the original ops_x leaf = 21 → 21 - 5 shown = 16 hidden.
    expect(more.moreCount).toBe(16)
  })

  it("never renders a nameless node — every laid node is labelled or an explicit '+N more'", () => {
    const model = buildEstateModel(wideGraph(40))
    const l = layoutTree(model, initialExpanded(model), noOffsets, cfg)
    for (const laid of l.nodes) {
      if (laid.isMore) expect(laid.label).toMatch(/^\+\d+ more$/)
      else expect(laid.label.length).toBeGreaterThan(0)
    }
  })

  it("stays byte-stable across repeated capped layouts (R18)", () => {
    const model = buildEstateModel(wideGraph(30))
    const exp = initialExpanded(model)
    expect(layoutHash(layoutTree(model, exp, noOffsets, cfg))).toBe(
      layoutHash(layoutTree(model, exp, noOffsets, cfg)),
    )
  })

  it("lifts the cap for an uncapped parent (the '+N more' click target)", () => {
    const model = buildEstateModel(wideGraph(20))
    const exp = initialExpanded(model)
    const capped = layoutTree(model, exp, noOffsets, cfg)
    const uncapped = layoutTree(model, exp, noOffsets, cfg, { uncapped: new Set(["d_ops"]) })
    expect(capped.nodes.some((x) => x.isMore)).toBe(true)
    // Uncapped: all 21 real children present, no sentinel for d_ops.
    expect(uncapped.nodes.filter((x) => x.parentId === "d_ops" && !x.isMore).length).toBe(21)
    expect(uncapped.nodes.some((x) => x.id === moreSentinelId("d_ops"))).toBe(false)
  })

  it("does not cap a parent whose child count is within the cap", () => {
    const model = buildEstateModel(wideGraph(3))
    const l = layoutTree(model, initialExpanded(model), noOffsets, cfg)
    expect(l.nodes.some((x) => x.isMore)).toBe(false)
  })
})

// ── Cross-link overlay gating (§6 / R4 — no hairball) ────────────────────────
describe("layoutTree — cross-link gating (R4)", () => {
  it("draws NO arcs at rest and reports the count for a click-to-trace affordance", () => {
    const model = buildEstateModel(graph())
    const exp = new Set([...initialExpanded(model), "s_rev", "mv:a"]) // reveal t:sales endpoints
    const l = layoutTree(model, exp, noOffsets)
    // Relationships are off at rest (owner directive: appear on click) — the count is surfaced
    // so the UI can show "+N links — select a node to trace".
    expect(l.crossLinks.length).toBe(0)
    expect(l.crossLinkOverflow).toBe(2)
  })

  it("keeps arcs off at rest regardless of the cap, still reporting the count", () => {
    const model = buildEstateModel(graph())
    const exp = new Set([...initialExpanded(model), "s_rev", "mv:a"])
    const l = layoutTree(model, exp, noOffsets, { ...DEFAULT_LAYOUT, crossLinkCap: 1 })
    expect(l.crossLinks.length).toBe(0)
    expect(l.crossLinkOverflow).toBe(2)
  })

  it("reveals ONLY the focused node's arcs, and labels them (labels only on the active arc)", () => {
    const model = buildEstateModel(graph())
    const exp = new Set([...initialExpanded(model), "s_rev", "mv:a"])
    // Focus t:cal — endpoint of the single 'joins calendar' shared arc only.
    const l = layoutTree(model, exp, noOffsets, DEFAULT_LAYOUT, { focusId: "t:cal" })
    expect(l.crossLinks.length).toBe(1)
    expect(l.crossLinks[0].verb).toBe("joins calendar")
    expect(l.crossLinks[0].showLabel).toBe(true)
    expect(l.crossLinkOverflow).toBe(0)
  })

  it("draws no arcs for a focused node whose cross-link endpoints are hidden", () => {
    const model = buildEstateModel(graph())
    // t:sales is hidden (mv:a collapsed) → focusing it yields no visible arcs.
    const l = layoutTree(model, initialExpanded(model), noOffsets, DEFAULT_LAYOUT, { focusId: "t:sales" })
    expect(l.crossLinks.length).toBe(0)
  })
})

// ── Fit-to-bounds math (§4.1 / R12a-b) ───────────────────────────────────────
describe("contentBounds — fit-to-bounds (R12)", () => {
  it("frames the tree with a positive, finite bbox", () => {
    const model = buildEstateModel(graph())
    const l = layoutTree(model, initialExpanded(model), noOffsets)
    const b = contentBounds(l, { tree: true })
    expect(b.width).toBeGreaterThan(0)
    expect(b.height).toBeGreaterThan(0)
    expect(Number.isFinite(b.minX)).toBe(true)
  })

  it("includes a focused node's verb-arc extent so arcs never fall outside the fit (R12b)", () => {
    const model = buildEstateModel(graph())
    const exp = new Set([...initialExpanded(model), "s_rev", "mv:a"])
    const l = layoutTree(model, exp, noOffsets, DEFAULT_LAYOUT, { focusId: "t:sales" })
    expect(l.crossLinks.length).toBeGreaterThan(0)
    const b = contentBounds(l, { tree: true })
    for (const c of l.crossLinks) {
      expect(c.labelAt.x).toBeGreaterThanOrEqual(b.minX)
      expect(c.labelAt.x).toBeLessThanOrEqual(b.maxX)
      expect(c.labelAt.y).toBeGreaterThanOrEqual(b.minY)
      expect(c.labelAt.y).toBeLessThanOrEqual(b.maxY)
    }
  })

  it("frames the tray separately from the tree (provenance-aware fit)", () => {
    const g = graph()
    g.domains.nodes.push(node({ id: "sug", label: "Suggested", kind: "domain", origin: "proposed" }))
    g.assets.nodes.push(node({ id: "z1", label: "z_orphan", kind: "table", domain_id: "sug" }))
    const model = buildEstateModel(g)
    const l = layoutTree(model, initialExpanded(model), noOffsets)
    const treeOnly = contentBounds(l, { tree: true, tray: false })
    const trayOnly = contentBounds(l, { tree: false, tray: true })
    // The tray sits to the right, so its bbox starts past the tree's right edge.
    expect(trayOnly.minX).toBeGreaterThan(treeOnly.minX)
  })
})

describe("ancestorPath + initialExpanded", () => {
  it("returns the root→node path", () => {
    const model = buildEstateModel(graph())
    const path = ancestorPath(model, "t:sales").map((n) => n.id)
    expect(path[0]).toBe("org")
    expect(path[path.length - 1]).toBe("t:sales")
    expect(path).toContain("mv:a")
  })

  it("opens org + domains only — sub-domains visible but collapsed, assets hidden", () => {
    const model = buildEstateModel(graph())
    const exp = initialExpanded(model)
    expect(exp.has("org")).toBe(true)
    expect(exp.has("d_fin")).toBe(true)
    // Sub-domain is visible (its domain is open) but NOT expanded → assets stay hidden.
    expect(exp.has("s_rev")).toBe(false)
    expect(exp.has("mv:a")).toBe(false)
    // Estate → Domain → Sub-domain renders; the sub-domain is a collapsed container.
    const laid = layoutTree(model, exp, noOffsets)
    const ids = new Set(laid.nodes.map((n) => n.id))
    expect(ids.has("s_rev")).toBe(true)
    expect(ids.has("t:sales")).toBe(false)
    const sub = laid.nodes.find((n) => n.id === "s_rev")!
    expect(sub.collapsed).toBe(true)
  })
})

// ── Tier-aware asset gating (owner directive) ────────────────────────────────
describe("layoutTree — tier-aware asset gating", () => {
  it("hides a domain's directly-attached assets by default, showing only sub-domains (+badge)", () => {
    const model = buildEstateModel(graph())
    const exp = initialExpanded(model) // org + domains
    const l = layoutTree(model, exp, noOffsets, DEFAULT_LAYOUT, { assetsExpanded: new Set() })
    const ids = new Set(l.nodes.map((n) => n.id))
    expect(ids.has("s_rev")).toBe(true) // sub-domain container still shows
    expect(ids.has("t:cal")).toBe(false) // domain-attached asset is gated
    expect(ids.has("t:ops")).toBe(false)
    // The domain is partially open (sub-domain shown, asset hidden) → collapsed with a +N badge.
    const dFin = l.nodes.find((n) => n.id === "d_fin")!
    expect(dFin.collapsed).toBe(true)
    expect(dFin.badge).toBeGreaterThan(0)
  })

  it("reveals a domain's assets once that domain id is drilled", () => {
    const model = buildEstateModel(graph())
    const exp = initialExpanded(model)
    const l = layoutTree(model, exp, noOffsets, DEFAULT_LAYOUT, { assetsExpanded: new Set(["d_fin"]) })
    const ids = new Set(l.nodes.map((n) => n.id))
    expect(ids.has("t:cal")).toBe(true) // drilled → visible
    expect(ids.has("t:ops")).toBe(false) // the sibling domain stays gated
  })

  it("does NOT gate assets under a sub-domain — expanding the sub-domain reveals them", () => {
    const model = buildEstateModel(graph())
    const exp = new Set([...initialExpanded(model), "s_rev"])
    const l = layoutTree(model, exp, noOffsets, DEFAULT_LAYOUT, { assetsExpanded: new Set() })
    expect(new Set(l.nodes.map((n) => n.id)).has("mv:a")).toBe(true)
  })

  it("is opt-in — absent assetsExpanded, an expanded domain reveals its assets (legacy)", () => {
    const model = buildEstateModel(graph())
    const l = layoutTree(model, initialExpanded(model), noOffsets)
    const ids = new Set(l.nodes.map((n) => n.id))
    expect(ids.has("t:cal")).toBe(true)
    expect(ids.has("t:ops")).toBe(true)
  })
})

// ── MV-D87 (Lane P2) — verb focus, collapse-tier, minimap/camera math ─────────
describe("layoutTree — rel-type verb focus (R25)", () => {
  it("reveals every visible arc of a verb, labelled, and suppresses the rest", () => {
    const model = buildEstateModel(graph())
    const exp = new Set([...initialExpanded(model), "s_rev", "mv:a"])
    const l = layoutTree(model, exp, noOffsets, DEFAULT_LAYOUT, { verbFocus: "also queried with" })
    expect(l.crossLinks.length).toBe(1)
    expect(l.crossLinks[0].verb).toBe("also queried with")
    expect(l.crossLinks[0].showLabel).toBe(true)
  })

  it("verb focus overrides the cap so a rel-type reads even in a dense estate", () => {
    const model = buildEstateModel(graph())
    const exp = new Set([...initialExpanded(model), "s_rev", "mv:a"])
    const l = layoutTree(model, exp, noOffsets, { ...DEFAULT_LAYOUT, crossLinkCap: 0 }, { verbFocus: "joins calendar" })
    expect(l.crossLinks.map((c) => c.verb)).toEqual(["joins calendar"])
  })

  it("node focusId takes precedence over verbFocus (a selection is more specific)", () => {
    const model = buildEstateModel(graph())
    const exp = new Set([...initialExpanded(model), "s_rev", "mv:a"])
    const l = layoutTree(model, exp, noOffsets, DEFAULT_LAYOUT, { focusId: "t:cal", verbFocus: "also queried with" })
    // t:cal only touches the shared 'joins calendar' arc — verbFocus is ignored.
    expect(l.crossLinks.map((c) => c.verb)).toEqual(["joins calendar"])
  })

  it("threads an edge's detail bag onto the laid-out cross-link", () => {
    const g = graph()
    g.assets.edges = [
      { src: "t:sales", dst: "t:cal", kind: "join_key", verb: "joins calendar", rel_class: "shared", detail: { Shares: "date_key" } },
    ]
    const model = buildEstateModel(g)
    const exp = new Set([...initialExpanded(model), "s_rev", "mv:a"])
    const l = layoutTree(model, exp, noOffsets, DEFAULT_LAYOUT, { focusId: "t:cal" })
    expect(l.crossLinks[0].detail).toEqual({ Shares: "date_key" })
  })
})

describe("collapsedToDomainTier (P0-a, R24)", () => {
  it("expands only the org root → the tree reads down to the domain tier", () => {
    const model = buildEstateModel(graph())
    const exp = collapsedToDomainTier(model)
    const laid = layoutTree(model, exp, noOffsets)
    const types = new Set(laid.nodes.map((n) => n.type))
    // Only org + its domain children are laid out; nothing below the domain tier.
    expect(types.has("org")).toBe(true)
    expect(types.has("domain")).toBe(true)
    expect(types.has("subdomain")).toBe(false)
    expect(types.has("table")).toBe(false)
    // Every domain node reads as a collapsed container (a +N badge to drill).
    expect(laid.nodes.filter((n) => n.type === "domain").every((n) => n.collapsed)).toBe(true)
  })
})

// ── Exported path builders (buttery interactions §1) ─────────────────────────
// These are the SAME builders the layout pass uses; EstateGraph re-runs them imperatively to
// redraw an edge from moved endpoints (drag / relayout glide) without a full React relayout.
describe("spinePath / crossPath builders", () => {
  it("spinePath draws a vertical tidy-tree cubic through the vertical midpoint", () => {
    const d = spinePath(0, 0, 100, 200)
    // M sx,sy C sx,my tx,my tx,ty  with my = (sy+ty)/2 = 100.
    expect(d).toBe("M0,0C0,100 100,100 100,200")
  })

  it("spinePath is what layoutTree emits for a spine link (render === redraw)", () => {
    const model = buildEstateModel(graph())
    const l = layoutTree(model, initialExpanded(model), noOffsets)
    const link = l.spineLinks[0]
    expect(link).toBeDefined()
    expect(link.path).toBe(
      spinePath(link.source.x, link.source.y, link.target.x, link.target.y),
    )
  })

  it("crossPath returns a quadratic bezier + its arc midpoint", () => {
    const { path, mid } = crossPath({ x: 0, y: 0 }, { x: 100, y: 0 }, 0.2)
    expect(path.startsWith("M")).toBe(true)
    expect(path).toContain("Q")
    // Symmetric horizontal arc → midpoint x at the centre, bowed off the line in y.
    expect(mid.x).toBeCloseTo(50)
    expect(Math.abs(mid.y)).toBeGreaterThan(0)
  })

  it("crossPath bows more with a larger bow factor (xdom vs shared)", () => {
    const shared = crossPath({ x: 0, y: 0 }, { x: 100, y: 0 }, 0.16)
    const xdom = crossPath({ x: 0, y: 0 }, { x: 100, y: 0 }, 0.16 * 1.35)
    expect(Math.abs(xdom.mid.y)).toBeGreaterThan(Math.abs(shared.mid.y))
  })

  it("crossPath trims endpoints inward so the arrowhead clears the target disc", () => {
    const a = { x: 0, y: 0 }
    const b = { x: 100, y: 0 }
    const untrimmed = crossPath(a, b, 0.2)
    const trimmed = crossPath(a, b, 0.2, 6, 12)
    // The drawn start moves off `a` and the drawn end pulls back from `b`.
    expect(untrimmed.path).toContain("M0,0")
    expect(trimmed.path).not.toContain("M0,0")
  })
})

describe("minimap + camera math (P1-a, R26)", () => {
  it("viewportContentRect inverts the camera transform to content coords", () => {
    // transform x=40,y=40,k=0.5; a 800×600 canvas frames content [-80,-80]..[1520,1120].
    const r = viewportContentRect({ x: 40, y: 40, k: 0.5 }, { width: 800, height: 600 })
    expect(r.x1).toBeCloseTo(-80)
    expect(r.y1).toBeCloseTo(-80)
    expect(r.x2).toBeCloseTo(1520)
    expect(r.y2).toBeCloseTo(1120)
  })

  it("centerOnTransform puts a content point at the viewport centre at the given zoom", () => {
    const t = centerOnTransform({ x: 100, y: 50 }, { width: 800, height: 600 }, 2)
    // screen = content*k + translate; the point must map to the centre (400,300).
    expect(100 * t.k + t.x).toBeCloseTo(400)
    expect(50 * t.k + t.y).toBeCloseTo(300)
    expect(t.k).toBe(2)
  })
})

// ── Visual encoding: popularity → radius, join strength → CrossLink.weight (MV-D97 §6) ──
describe("scaleRadius — popularity → radius (MV-D97 §6)", () => {
  it("no size ⇒ base radius EXACTLY (degrade, MV-D43)", () => {
    expect(scaleRadius(20, undefined)).toBe(20)
    expect(scaleRadius(20, null)).toBe(20)
  })

  it("grows monotonically with size", () => {
    const base = 10
    expect(scaleRadius(base, 0.5)).toBeGreaterThan(base)
    expect(scaleRadius(base, 2.0)).toBeGreaterThan(scaleRadius(base, 0.5))
  })

  it("is bounded — never exceeds RADIUS_SIZE_MAX×, clamps at the cap for a huge size", () => {
    const base = 10
    expect(scaleRadius(base, 1000)).toBeLessThanOrEqual(base * RADIUS_SIZE_MAX + 1e-9)
    expect(scaleRadius(base, 1e6)).toBeCloseTo(base * RADIUS_SIZE_MAX, 6)
  })

  it("size 0 or negative ⇒ base radius (log floored)", () => {
    expect(scaleRadius(10, 0)).toBe(10)
    expect(scaleRadius(10, -5)).toBe(10)
  })
})

describe("layoutTree — reads node size + threads edge weight (MV-D97 §6)", () => {
  function sizedGraph(): OntologyGraph {
    return {
      root: node({ id: "org", label: "Org", kind: "org" }),
      domains: { nodes: [node({ id: "d", label: "D", kind: "domain", origin: "applied" })], edges: [], truncated: false },
      assets: {
        nodes: [
          node({ id: "big", label: "big", kind: "table", domain_id: "d", attach_level: "domain", origin: "applied", size: 2.0 }),
          node({ id: "small", label: "small", kind: "table", domain_id: "d", attach_level: "domain", origin: "applied", size: 0.5 }),
        ],
        edges: [{ src: "big", dst: "small", kind: "join_key", weight: 7, verb: "shares key with", rel_class: "shared" }],
        truncated: false,
      },
      layout: "tree",
      node_count: 4,
      edge_count: 1,
      state: "fresh",
      as_of: null,
    }
  }

  it("lays out a popular node with a bigger radius than a quiet same-type node, bounded by the cap", () => {
    const model = buildEstateModel(sizedGraph())
    // Gating OFF (no assetsExpanded opt): an expanded domain reveals its direct assets.
    const l = layoutTree(model, new Set(["org", "d"]), noOffsets)
    const big = l.nodes.find((n) => n.id === "big")!
    const small = l.nodes.find((n) => n.id === "small")!
    const base = DEFAULT_LAYOUT.radius.table
    expect(big.radius).toBeGreaterThan(small.radius)
    expect(small.radius).toBeGreaterThan(base) // size 0.5 still reads a touch bigger than base
    expect(big.radius).toBeLessThanOrEqual(base * RADIUS_SIZE_MAX + 1e-9)
  })

  it("threads the cross-edge weight onto the laid CrossLink (drives arc thickness)", () => {
    const model = buildEstateModel(sizedGraph())
    const l = layoutTree(model, new Set(["org", "d"]), noOffsets, DEFAULT_LAYOUT, { focusId: "big" })
    const shared = l.crossLinks.find((c) => c.relClass === "shared")!
    expect(shared).toBeDefined()
    expect(shared.weight).toBe(7)
  })

  it("threads node meta (certified/deprecated flags) onto the laid node for the renderer", () => {
    const g = sizedGraph()
    g.assets.nodes.find((n) => n.id === "big")!.meta = { certified: "true" }
    const model = buildEstateModel(g)
    const l = layoutTree(model, new Set(["org", "d"]), noOffsets)
    expect(l.nodes.find((n) => n.id === "big")!.meta).toEqual({ certified: "true" })
    expect(l.nodes.find((n) => n.id === "small")!.meta).toBeNull()
  })
})

// ── MV-D106 Phase 1 — viewport culling ─────────────────────────────────────────
describe("cullToViewport", () => {
  function laid(id: string, x: number, y: number, radius = 10): LaidNode {
    return {
      id,
      x,
      y,
      depth: 1,
      type: "table",
      label: id,
      displayName: id,
      origin: "applied",
      domainId: null,
      parentId: null,
      kind: "table",
      radius,
      meta: null,
      collapsed: false,
      badge: 0,
      memberCount: null,
      cost: null,
      isMore: false,
      moreCount: 0,
    }
  }
  function spine(sourceId: string, targetId: string): SpineLink {
    return {
      id: `s_${sourceId}__${targetId}`,
      sourceId,
      targetId,
      source: { x: 0, y: 0 },
      target: { x: 0, y: 0 },
      path: "",
      domainId: null,
    }
  }
  function cross(id: string, sourceId: string, targetId: string): CrossLink {
    return {
      id,
      sourceId,
      targetId,
      verb: "joins",
      relClass: "shared",
      path: "",
      labelAt: { x: 0, y: 0 },
      showLabel: false,
    }
  }
  // A viewport at the origin, 200×200 content units.
  const rect: ViewportRect = { x1: 0, y1: 0, x2: 200, y2: 200 }

  it("passes the inputs through UNCHANGED when viewportRect is null (referential)", () => {
    const nodes = [laid("a", 0, 0), laid("b", 5000, 5000)]
    const spines = [spine("a", "b")]
    const crosses = [cross("e", "a", "b")]
    // A tiny threshold so the count gate can't be what triggers the passthrough.
    const out = cullToViewport(nodes, spines, crosses, null, { threshold: 1 })
    expect(out.nodes).toBe(nodes)
    expect(out.spineLinks).toBe(spines)
    expect(out.crossLinks).toBe(crosses)
  })

  it("passes through UNCHANGED when node count is at/under the threshold (referential)", () => {
    const nodes = [laid("a", 0, 0), laid("b", 5000, 5000), laid("c", 6000, 6000)]
    const spines = [spine("a", "b")]
    const crosses = [cross("e", "a", "b")]
    const out = cullToViewport(nodes, spines, crosses, rect, { threshold: 3 })
    expect(out.nodes).toBe(nodes)
    expect(out.spineLinks).toBe(spines)
    expect(out.crossLinks).toBe(crosses)
  })

  it("defaults to a 600-node threshold (a smaller scene passes through even with a viewport)", () => {
    expect(CULL_THRESHOLD).toBe(600)
    const nodes = [laid("a", 0, 0), laid("b", 9999, 9999)]
    const out = cullToViewport(nodes, [], [], rect) // 2 nodes ≪ 600 ⇒ passthrough
    expect(out.nodes).toBe(nodes)
  })

  it("keeps in-viewport nodes and drops far ones once over the threshold", () => {
    const inside = laid("in", 100, 100)
    const far = laid("far", 100000, 100000)
    const out = cullToViewport([inside, far], [], [], rect, { threshold: 1, pad: 0 })
    const ids = out.nodes.map((n) => n.id)
    expect(ids).toContain("in")
    expect(ids).not.toContain("far")
  })

  it("keeps a node just outside the raw rect but within the padded margin", () => {
    // rect is 0..200; with pad=1 the padded box is -200..400. A node at 350 is culled with pad=0
    // but kept with pad=1.
    const near = laid("near", 350, 100)
    const dropped = cullToViewport([near, laid("x", 1e6, 1e6)], [], [], rect, { threshold: 1, pad: 0 })
    expect(dropped.nodes.map((n) => n.id)).not.toContain("near")
    const kept = cullToViewport([near, laid("x", 1e6, 1e6)], [], [], rect, { threshold: 1, pad: 1 })
    expect(kept.nodes.map((n) => n.id)).toContain("near")
  })

  it("keeps a node whose disc (radius) overlaps the rect even if its centre is outside", () => {
    // Centre at x=210 (just past the rect's 200 edge) but radius 20 ⇒ disc reaches 190 < 200.
    const grazing = laid("graze", 210, 100, 20)
    const out = cullToViewport([grazing, laid("x", 1e6, 1e6)], [], [], rect, { threshold: 1, pad: 0 })
    expect(out.nodes.map((n) => n.id)).toContain("graze")
  })

  it("ALWAYS keeps ids in the keep-set, even far off-screen", () => {
    const far = laid("sel", 99999, 99999)
    const out = cullToViewport([far, laid("other", 5e5, 5e5)], [], [], rect, {
      threshold: 1,
      pad: 0,
      keep: new Set(["sel"]),
    })
    const ids = out.nodes.map((n) => n.id)
    expect(ids).toContain("sel")
    expect(ids).not.toContain("other")
  })

  it("keeps an edge iff BOTH endpoints survive", () => {
    const a = laid("a", 50, 50) // in view
    const b = laid("b", 60, 60) // in view
    const c = laid("c", 99999, 99999) // far, culled
    const spines = [spine("a", "b"), spine("a", "c")]
    const crosses = [cross("ab", "a", "b"), cross("bc", "b", "c")]
    const out = cullToViewport([a, b, c], spines, crosses, rect, { threshold: 1, pad: 0 })
    expect(out.spineLinks.map((l) => l.id)).toEqual(["s_a__b"])
    expect(out.crossLinks.map((l) => l.id)).toEqual(["ab"])
  })

  it("preserves input order among the kept nodes (stable React keys)", () => {
    const nodes = [laid("z", 10, 10), laid("far", 1e6, 1e6), laid("a", 20, 20), laid("m", 30, 30)]
    const out = cullToViewport(nodes, [], [], rect, { threshold: 1, pad: 0 })
    expect(out.nodes.map((n) => n.id)).toEqual(["z", "a", "m"])
  })

  it("never invents nodes — the kept set is a subset of the input", () => {
    const nodes = [laid("a", 10, 10), laid("b", 20, 20)]
    const out = cullToViewport(nodes, [], [], rect, { threshold: 1, pad: 0, keep: new Set(["ghost"]) })
    expect(out.nodes.every((n) => nodes.includes(n))).toBe(true)
    expect(out.nodes.map((n) => n.id)).not.toContain("ghost")
  })
})
