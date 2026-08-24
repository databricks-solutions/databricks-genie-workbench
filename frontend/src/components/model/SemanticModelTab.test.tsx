/**
 * SemanticModelTab — render-level tests (Prompt 12, MV-D23). The coverage of
 * record for the production Model tab: the mockup frames (9a–9d) stay in the
 * emitter as the design record but were NOT wired into the emitter registry,
 * because that scaffold is disposed at Prompt 13.5 (see the Prompt 12 body).
 *
 * Node env + renderToStaticMarkup — the repo's frontend test pattern. State
 * (overlay toggle, node selection) is exercised through the pure helpers
 * (withOverlay, NodeDetail) and by rendering SemanticGraph with an explicit
 * selection, since static markup cannot fire a click.
 */
import { describe, expect, it } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import type { MvProposal, SemanticGraphResponse } from "@/types"
import { NodeDetail, SemanticModelView, withOverlay } from "./SemanticModelTab"
import { SemanticGraph } from "./SemanticGraph"

const render = (el: React.ReactElement) => renderToStaticMarkup(el)

function proposal(over: Partial<MvProposal> = {}): MvProposal {
  return {
    suggestion_id: "sug1",
    dedup_fingerprint: "fp1",
    target_space_id: "space-1",
    run_id: null,
    candidate_type: "PROPOSE",
    confidence_score: 88,
    tier: "HIGH",
    proposed_object: "finance.sales.order_revenue",
    score_components: null,
    evidence: { recurrence_count: 14, benchmark_question_ids: ["bq_1", "bq_2"], source_tables: ["finance.sales.orders"] },
    provenance: null,
    alternatives: null,
    conflicts: null,
    requested_mode: null,
    effective_mode: null,
    decision: null,
    decided_by: null,
    decided_at: null,
    suppressed_until: null,
    approved_for_rerun: false,
    created_at: null,
    updated_at: null,
    ...over,
  }
}

const EMPTY: SemanticGraphResponse = { space_id: "space-1", nodes: [], edges: [], proposals: [] }

const JOINS_AND_SNIPPETS: SemanticGraphResponse = {
  space_id: "space-1",
  nodes: [
    { id: "finance.sales.orders", kind: "table", label: "orders", col: 0, row: 0 },
    { id: "finance.ref.customer", kind: "table", label: "customer", col: 1, row: 0 },
    { id: "measure:gross_margin", kind: "measure", label: "gross_margin", col: 3, row: 0, governance: "curated", origin: "sql_snippets.measures" },
  ],
  edges: [
    { from: "finance.sales.orders", to: "finance.ref.customer", kind: "join", on: "orders.customer_id = customer.id AND customer.is_current = true", relationship: "many-to-one", scd2: true },
  ],
  proposals: [],
}

const ATTACHED_MV: SemanticGraphResponse = {
  space_id: "space-1",
  nodes: [
    { id: "finance.sales.orders", kind: "table", label: "orders", col: 0, row: 0 },
    { id: "finance.sales.orders_metrics", kind: "metric_view", label: "orders_metrics", col: 2, row: 0 },
    { id: "measure:order_count", kind: "measure", label: "order_count", col: 3, row: 0, governance: "governed", origin: "orders_metrics (attached MV)" },
  ],
  edges: [{ from: "measure:order_count", to: "finance.sales.orders_metrics", kind: "membership" }],
  proposals: [],
}

const WITH_PROPOSAL: SemanticGraphResponse = {
  ...ATTACHED_MV,
  nodes: [
    ...ATTACHED_MV.nodes,
    { id: "measure:order_revenue", kind: "measure", label: "order_revenue", col: 3, row: 1, governance: "ungoverned", origin: "proposal evidence · 14×" },
  ],
  proposals: [proposal()],
}

const MULTI_JOIN: SemanticGraphResponse = {
  space_id: "space-1",
  nodes: [
    { id: "finance.sales.orders", kind: "table", label: "orders", col: 0, row: 0 },
    { id: "finance.sales.order_items", kind: "table", label: "order_items", col: 0, row: 1 },
    { id: "finance.ref.customer", kind: "table", label: "customer", col: 1, row: 0 },
  ],
  edges: [
    { from: "finance.sales.order_items", to: "finance.sales.orders", kind: "join", on: "order_items.order_id = orders.order_id", relationship: "many-to-one", scd2: false },
    { from: "finance.sales.orders", to: "finance.ref.customer", kind: "join", on: "orders.customer_id = customer.id", relationship: "one-to-one", scd2: false },
  ],
  proposals: [],
}

describe("SemanticModelView — states", () => {
  it("never-optimized/empty: config-scoped copy, empty ladder, no overlay toggle", () => {
    const html = render(<SemanticModelView graph={EMPTY} isLoading={false} error={null} onRefresh={() => {}} />)
    expect(html).toContain("configuration defines no joins, SQL snippets, or metric views yet")
    expect(html).toContain("Metric view suggestions")
    expect(html).toContain("No measure concepts yet")
    // Nothing green and nothing red for a space that has surfaced nothing.
    expect(html).not.toContain("Governed")
    expect(html).not.toContain("Ungoverned")
    // No proposals → no overlay to overlay.
    expect(html).not.toContain("Show proposal overlay")
  })

  it("loading with no data yet: spinner copy, no graph", () => {
    const html = render(<SemanticModelView graph={null} isLoading={true} error={null} onRefresh={() => {}} />)
    expect(html).toContain("Loading semantic model")
  })

  it("error: message and a retry affordance", () => {
    const html = render(<SemanticModelView graph={null} isLoading={false} error={"no access"} onRefresh={() => {}} />)
    expect(html).toContain("no access")
    expect(html).toContain("Try again")
  })

  it("joins + snippets: curated rung, join labeled with relationship + SCD2 at rest", () => {
    const html = render(<SemanticModelView graph={JOINS_AND_SNIPPETS} isLoading={false} error={null} onRefresh={() => {}} />)
    expect(html).toContain("Curated")
    expect(html).toContain("many-to-one")
    expect(html).toContain("SCD2")
    expect(html).toContain("gross_margin")
    // No proposals here either.
    expect(html).not.toContain("Show proposal overlay")
  })

  it("attached MV: governed rung and a metric view node", () => {
    const html = render(<SemanticModelView graph={ATTACHED_MV} isLoading={false} error={null} onRefresh={() => {}} />)
    expect(html).toContain("Governed")
    expect(html).toContain("metric view")
    expect(html).toContain("order_count")
  })

  it("with proposals: default-off overlay toggle offered, ungoverned rung present", () => {
    const html = render(<SemanticModelView graph={WITH_PROPOSAL} isLoading={false} error={null} onRefresh={() => {}} />)
    expect(html).toContain("Show proposal overlay")
    expect(html).toContain("default off")
    expect(html).toContain("Ungoverned")
    // Overlay is OFF by default, so the ghost/replaces are not yet drawn.
    expect(html).not.toContain("proposed metric view")
    expect(html).not.toContain("replaces")
  })

  it("multi-join: both join edges render their relationships", () => {
    const html = render(<SemanticModelView graph={MULTI_JOIN} isLoading={false} error={null} onRefresh={() => {}} />)
    expect(html).toContain("many-to-one")
    expect(html).toContain("one-to-one")
  })
})

describe("proposal overlay — synthesized client-side", () => {
  it("withOverlay adds a ghosted proposed MV and dashed replaces edges", () => {
    const { nodes, edges } = withOverlay(WITH_PROPOSAL)
    const ghost = nodes.find((n) => n.proposed)
    expect(ghost).toBeTruthy()
    expect(ghost!.label).toBe("order_revenue")
    expect(edges.some((e) => e.kind === "replaces" && e.to === "finance.sales.orders")).toBe(true)
  })

  it("rendering the overlaid graph shows the ghost MV and replaces edge", () => {
    const { nodes, edges } = withOverlay(WITH_PROPOSAL)
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    expect(html).toContain("proposed metric view")
    expect(html).toContain("replaces")
  })
})

describe("SemanticGraph — selection reveals the full ON predicate", () => {
  it("shows the ON predicate when an edge endpoint is selected (declutter)", () => {
    const html = render(
      <SemanticGraph nodes={JOINS_AND_SNIPPETS.nodes} edges={JOINS_AND_SNIPPETS.edges} selectedId="finance.ref.customer" />,
    )
    expect(html).toContain("ON orders.customer_id = customer.id")
  })
})

describe("NodeDetail — measure evidence and conflict", () => {
  it("surfaces evidence for a measure backed by a proposal", () => {
    const node = WITH_PROPOSAL.nodes.find((n) => n.id === "measure:order_revenue")!
    const html = render(<NodeDetail node={node} proposals={WITH_PROPOSAL.proposals} />)
    expect(html).toContain("Ungoverned")
    expect(html).toContain("14 occurrences")
    expect(html).toContain("bq_1")
    expect(html).toContain("finance.sales.orders")
  })

  it("flags a conflict when the backing proposal carries one", () => {
    const node = WITH_PROPOSAL.nodes.find((n) => n.id === "measure:order_revenue")!
    const conflicted = [proposal({ conflicts: [{ with: "gross_margin" }] })]
    const html = render(<NodeDetail node={node} proposals={conflicted} />)
    expect(html).toContain("conflict")
  })
})
