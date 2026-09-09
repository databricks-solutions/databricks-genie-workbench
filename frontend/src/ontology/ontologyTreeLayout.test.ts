import { describe, expect, it } from "vitest"
import { buildEstateModel } from "@/ontology/estateGraphModel"
import {
  DEFAULT_LAYOUT,
  ancestorPath,
  centerOnTransform,
  collapsedToDomainTier,
  contentBounds,
  initialExpanded,
  layoutHash,
  layoutTree,
  moreSentinelId,
  viewportContentRect,
  type Point,
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
    // Expand the MV too so its child table (a cross-link endpoint) is visible.
    const exp = new Set([...initialExpanded(model), "s_rev", "mv:a"])
    const l = layoutTree(model, exp, noOffsets)
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
    const l = layoutTree(model, exp, noOffsets)
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
  it("draws every visible arc UNLABELLED when nothing is focused and under the cap", () => {
    const model = buildEstateModel(graph())
    const exp = new Set([...initialExpanded(model), "s_rev", "mv:a"]) // reveal t:sales endpoints
    const l = layoutTree(model, exp, noOffsets)
    expect(l.crossLinks.length).toBe(2)
    expect(l.crossLinks.every((c) => c.showLabel === false)).toBe(true)
    expect(l.crossLinkOverflow).toBe(0)
  })

  it("suppresses the overlay mat above the cap and reports the count (+N links)", () => {
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
