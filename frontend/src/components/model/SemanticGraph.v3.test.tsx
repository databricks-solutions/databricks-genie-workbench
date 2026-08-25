/**
 * SemanticGraph — Prompt 12e / MV-D33 coverage (semantic-graph-v3-note.md).
 *
 * v3 draws the metric view AS a semantic model: a deduplicated table canvas,
 * measures boxed by owner (already the 12c grouping), MV → table `uses` arrows
 * that require proof, an on-demand boundary that wraps the tables an MV uses,
 * an unmodeled region, Lineage vs Impact focus, and session-only drag. These
 * tests pin the pure helpers and the render paths, per the note's §10 test plan.
 *
 * Reviewer watch-items folded in:
 *  - HULL is the norm, the clean RECT is the lucky case — a shared dim forces the
 *    per-member hull; only isolated members earn a single enclosing rect.
 *  - UNREADABLE YAML = unproven = no arrows — a definition_available:false MV
 *    draws no `uses` edges and renders "definition unavailable".
 */
import { describe, expect, it } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import type { SemanticGraphEdge, SemanticGraphNode } from "@/types"
import {
  SemanticGraph,
  applyDragOffset,
  buildCards,
  focusSet,
  impactSet,
  memberBoundary,
  memberTableIds,
  unmodeledTableIds,
} from "./SemanticGraph"

const render = (el: React.ReactElement) => renderToStaticMarkup(el)

// A star with one MV that USES its fact + one joined dim (the proof arrows), a
// governed measure, and a second table in no MV (the unmodeled region).
function mvModel(): { nodes: SemanticGraphNode[]; edges: SemanticGraphEdge[] } {
  const nodes: SemanticGraphNode[] = [
    { id: "c.s.orders", kind: "table", label: "orders", col: 0, row: 0 },
    { id: "c.s.customer", kind: "table", label: "customer", col: 1, row: 0 },
    { id: "c.s.audit", kind: "table", label: "audit", col: 0, row: 1 }, // in no MV
    { id: "c.s.orders_metrics", kind: "metric_view", label: "orders_metrics", col: 2, row: 0, definition_available: true },
    { id: "measure:order_count", kind: "measure", label: "order_count", col: 3, row: 0, governance: "governed" },
  ]
  const edges: SemanticGraphEdge[] = [
    { from: "c.s.orders", to: "c.s.customer", kind: "join", on: "orders.cid = customer.id", relationship: "many-to-one" },
    { from: "c.s.orders_metrics", to: "c.s.orders", kind: "uses" },
    { from: "c.s.orders_metrics", to: "c.s.customer", kind: "uses" },
    { from: "measure:order_count", to: "c.s.orders_metrics", kind: "membership" },
  ]
  return { nodes, edges }
}

describe("memberTableIds — the tables an MV uses (proof arrows / boundary set)", () => {
  it("returns the uses-edge targets in order, deduped", () => {
    const { edges } = mvModel()
    expect(memberTableIds(edges, "c.s.orders_metrics")).toEqual(["c.s.orders", "c.s.customer"])
  })
  it("an MV with no uses edges (unreadable) has no members", () => {
    expect(memberTableIds(mvModel().edges, "c.s.nope")).toEqual([])
  })
})

describe("unmodeledTableIds — the governance gap, but only when contrastable", () => {
  it("flags a table in no metric view when membership is known", () => {
    const { nodes, edges } = mvModel()
    const un = unmodeledTableIds(nodes, edges)
    expect(un.has("c.s.audit")).toBe(true)
    expect(un.has("c.s.orders")).toBe(false)
    expect(un.has("c.s.customer")).toBe(false)
  })
  it("claims NOTHING when there are no uses edges (membership unknown)", () => {
    const nodes: SemanticGraphNode[] = [{ id: "t", kind: "table", label: "t", col: 0, row: 0 }]
    expect(unmodeledTableIds(nodes, []).size).toBe(0)
  })
})

describe("memberBoundary — HULL is the norm, RECT is the lucky case", () => {
  it("falls back to a per-member HULL when a bounding rect would swallow a foreigner", () => {
    // Two members with a FOREIGN table sitting between them — a single rect
    // enclosing both members would contain the foreigner. This is the common
    // case on real schemas (a shared dim cannot sit adjacent to every owner).
    const members = [
      { x: 0, y: 0, w: 50, h: 40 },
      { x: 0, y: 200, w: 50, h: 40 },
    ]
    const foreignerBetween = [{ x: 0, y: 100, w: 50, h: 40 }]
    const b = memberBoundary(members, foreignerBetween)
    expect(b.kind).toBe("hull")
    // One outline per member — never a single rect around the foreigner.
    expect(b.rects).toHaveLength(2)
  })

  it("uses a single RECT only when no foreign box sits inside the bounding box", () => {
    const members = [
      { x: 0, y: 0, w: 50, h: 40 },
      { x: 0, y: 120, w: 50, h: 40 },
    ]
    const farAway = [{ x: 400, y: 0, w: 50, h: 40 }]
    const b = memberBoundary(members, farAway)
    expect(b.kind).toBe("rect")
    expect(b.rects).toHaveLength(1)
  })

  it("empty members produce an empty hull (no boundary)", () => {
    expect(memberBoundary([], [{ x: 0, y: 0, w: 1, h: 1 }])).toEqual({ kind: "hull", rects: [] })
  })
})

describe("impactSet vs focusSet — downstream blast radius vs both-direction lineage", () => {
  it("impact of a table follows uses downstream (its MVs) but NOT its joins", () => {
    const { nodes, edges } = mvModel()
    const cards = buildCards(nodes, edges)
    const impact = impactSet(cards, edges, "c.s.orders")!
    expect(impact.has("c.s.orders")).toBe(true)
    expect(impact.has("c.s.orders_metrics")).toBe(true) // downstream via uses
    expect(impact.has("c.s.customer")).toBe(false) // a join is not downstream impact
  })

  it("lineage (focusSet) of the same table DOES include its join neighbor", () => {
    const { nodes, edges } = mvModel()
    const cards = buildCards(nodes, edges)
    const lineage = focusSet(cards, edges, "c.s.orders")!
    expect(lineage.has("c.s.customer")).toBe(true) // join neighbor is 1-hop lineage
    expect(lineage.has("c.s.orders_metrics")).toBe(true) // uses neighbor too
  })

  it("returns null when nothing is selected", () => {
    const { nodes, edges } = mvModel()
    expect(impactSet(buildCards(nodes, edges), edges, null)).toBeNull()
  })
})

describe("applyDragOffset — session-only spread, additive on the home layout", () => {
  it("adds the offset; a missing offset is the identity (home layout untouched)", () => {
    const box = { x: 10, y: 20, w: 100, h: 40 }
    expect(applyDragOffset(box, { dx: 5, dy: -7 })).toEqual({ x: 15, y: 13, w: 100, h: 40 })
    expect(applyDragOffset(box, undefined)).toBe(box)
  })
})

describe("SemanticGraph render — v3 paths", () => {
  it("draws proof `uses` arrows and, when an MV is selected, its boundary label", () => {
    const { nodes, edges } = mvModel()
    const html = render(<SemanticGraph nodes={nodes} edges={edges} selectedId="c.s.orders_metrics" />)
    expect(html).toContain("mv-uses-arrow") // the proof-arrow marker is wired
    expect(html).toContain("tables used by orders_metrics") // select-time boundary
  })

  it("an unreadable MV renders 'definition unavailable' and draws NO uses arrows", () => {
    const nodes: SemanticGraphNode[] = [
      { id: "c.s.orders", kind: "table", label: "orders", col: 0, row: 0 },
      { id: "c.s.mv", kind: "metric_view", label: "broken_metrics", col: 2, row: 0, definition_available: false },
    ]
    // Constraint 2: an unreadable MV emitted no uses edges server-side.
    const edges: SemanticGraphEdge[] = []
    const html = render(<SemanticGraph nodes={nodes} edges={edges} selectedId="c.s.mv" />)
    expect(html).toContain("definition unavailable")
    // No boundary (no members) and no uses arrows exist to draw.
    expect(html).not.toContain("tables used by")
  })

  it("tags the unmodeled table 'no metric view' when membership is known", () => {
    const { nodes, edges } = mvModel()
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    expect(html).toContain("no metric view")
  })
})
