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
    uncapped_tier: "HIGH",
    tier_capped_by_coverage: false,
    proposed_object: "finance.sales.order_revenue",
    score_components: null,
    evidence: { recurrence_count: 14, benchmark_question_ids: ["bq_1", "bq_2"], source_tables: ["finance.sales.orders"] },
    provenance_labels: null,
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

  it("joins + snippets: curated rung; the join's glyph + SCD2 ride selection, not rest", () => {
    const html = render(<SemanticModelView graph={JOINS_AND_SNIPPETS} isLoading={false} error={null} onRefresh={() => {}} />)
    expect(html).toContain("Curated")
    // Round-6 labels-on-demand: at rest nothing floats — no predicate, no SCD2.
    expect(html).not.toContain("many-to-one")
    expect(html).not.toContain("SCD2")
    expect(html).toContain("gross_margin")
    // Selecting an endpoint reveals the full predicate with its glyph + SCD2.
    const selected = render(<SemanticGraph nodes={JOINS_AND_SNIPPETS.nodes} edges={JOINS_AND_SNIPPETS.edges} selectedId="finance.ref.customer" />)
    expect(selected).toContain("N:1")
    expect(selected).toContain("SCD2")
    // No proposals here either.
    expect(html).not.toContain("Show proposal overlay")
  })

  it("attached MV: governed rung and a metric view node", () => {
    const html = render(<SemanticModelView graph={ATTACHED_MV} isLoading={false} error={null} onRefresh={() => {}} />)
    expect(html).toContain("Governed")
    expect(html).toContain("metric view")
    expect(html).toContain("order_count")
  })

  it("blueprint-only: proposals are NOT ghosted onto the canvas (arrows require proof)", () => {
    // Classic's proposal overlay was removed with the classic canvas — proposals
    // now live only in the advisory list below. The blueprint stays grounded: it
    // draws the ungoverned loose measure it can prove, but no ghost proposed MV
    // and no overlay toggle.
    const html = render(<SemanticModelView graph={WITH_PROPOSAL} isLoading={false} error={null} onRefresh={() => {}} />)
    expect(html).toContain("Ungoverned")
    expect(html).toContain("order_revenue")
    expect(html).not.toContain("Show proposal overlay")
    expect(html).not.toContain("proposed metric view")
    expect(html).not.toContain("would govern")
  })

  it("multi-join: cardinality glyphs appear on demand, not at rest (round-6)", () => {
    const idle = render(<SemanticModelView graph={MULTI_JOIN} isLoading={false} error={null} onRefresh={() => {}} />)
    // Round-6 labels-on-demand: no per-edge predicate floats on the resting canvas.
    expect(idle).not.toContain("ON orders.customer_id")
    // Selecting an endpoint reveals that join's predicate + glyph (1:1 here).
    const selected = render(<SemanticGraph nodes={MULTI_JOIN.nodes} edges={MULTI_JOIN.edges} selectedId="finance.ref.customer" />)
    expect(selected).toContain("1:1")
  })
})

describe("proposal overlay — synthesized client-side", () => {
  it("withOverlay adds a ghost MV and a 'governs' link, WITHOUT moving the measure", () => {
    const { nodes, edges } = withOverlay(WITH_PROPOSAL)
    const ghost = nodes.find((n) => n.proposed)
    expect(ghost).toBeTruthy()
    expect(ghost!.label).toBe("order_revenue")
    // Round-6: link, don't move — a "governs" edge from the ghost to the loose
    // measure, and NO membership edge that would pull the measure out of Space
    // config into an off-screen ghost card.
    expect(edges.some((e) => e.kind === "governs" && e.to === "measure:order_revenue")).toBe(true)
    expect(edges.some((e) => e.kind === "membership" && e.to === ghost!.id)).toBe(false)
    // The loose measure node is still present and untouched.
    expect(nodes.some((n) => n.id === "measure:order_revenue")).toBe(true)
  })

  it("rendering the overlaid graph shows the ghost MV and the 'would govern' link", () => {
    const { nodes, edges } = withOverlay(WITH_PROPOSAL)
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    expect(html).toContain("proposed metric view")
    expect(html).toContain("would govern")
    // The loose measure it would govern is still chipped in the Space-config box.
    expect(html).toContain("order_revenue")
  })

  it("withOverlay accepts an explicit proposals list (the Model tab's live set)", () => {
    // The graph carries no proposals of its own; the explicit argument (the
    // advisory's live-scanned set) is what synthesizes the ghost node.
    const ghostFromArg = withOverlay(ATTACHED_MV, [proposal()]).nodes.find((n) => n.proposed)
    expect(ghostFromArg?.label).toBe("order_revenue")
    // With neither argument nor graph proposals, nothing is added.
    expect(withOverlay(ATTACHED_MV).nodes.some((n) => n.proposed)).toBe(false)
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

// Prompt 12b SQL-coverage lens fixtures.
const WITH_COVERAGE: SemanticGraphResponse = {
  space_id: "space-1",
  nodes: [
    { id: "finance.sales.orders", kind: "table", label: "orders", col: 0, row: 0, coverage: 2 },
    { id: "finance.ref.unused", kind: "table", label: "unused", col: 1, row: 0, coverage: 0 },
  ],
  edges: [],
  proposals: [],
  coverage_status: "COMPUTED",
  coverage_reason: null,
}

describe("SQL-coverage lens (Prompt 12b) — additive", () => {
  it("COMPUTED: renders the coverage note and marks the cold spot on the blueprint", () => {
    const html = render(<SemanticModelView graph={WITH_COVERAGE} isLoading={false} error={null} onRefresh={() => {}} />)
    expect(html).toContain("Query coverage")
    // The blueprint calls out the zero-coverage table as a cold spot.
    expect(html).toContain("Cold spot")
    expect(html).toContain("no curated SQL touches it")
  })

  it("EMPTY: says coverage is not measured (frame-7b honesty), no invented zero", () => {
    const empty = { ...JOINS_AND_SNIPPETS, coverage_status: "EMPTY" }
    const html = render(<SemanticModelView graph={empty} isLoading={false} error={null} onRefresh={() => {}} />)
    expect(html).toContain("query coverage is not measured")
  })

  it("UNAVAILABLE: names the reason, never silently zero", () => {
    const bad = { ...JOINS_AND_SNIPPETS, coverage_status: "UNAVAILABLE", coverage_reason: "all 2 curated statement(s) failed to parse" }
    const html = render(<SemanticModelView graph={bad} isLoading={false} error={null} onRefresh={() => {}} />)
    expect(html).toContain("Query coverage unavailable")
    expect(html).toContain("failed to parse")
  })

  it("lens-free response (no coverage_status) renders no coverage note — Prompt 12 compatibility", () => {
    const html = render(<SemanticModelView graph={ATTACHED_MV} isLoading={false} error={null} onRefresh={() => {}} />)
    expect(html).not.toContain("Query coverage")
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

  it("names the metric view a loose measure collides with", () => {
    const node = {
      ...WITH_PROPOSAL.nodes.find((n) => n.id === "measure:order_revenue")!,
      overlaps: "finance.sales.orders_metrics",
    }
    const html = render(<NodeDetail node={node} proposals={[]} />)
    expect(html).toContain("orders_metrics")
    expect(html).toContain("two definitions, one name")
  })

  it("round-5: shows a selected measure's expression and description", () => {
    const node = {
      ...WITH_PROPOSAL.nodes.find((n) => n.id === "measure:order_revenue")!,
      expr: "sum(o.quantity * o.unit_price)",
      description: "Total revenue across order lines",
    }
    const html = render(<NodeDetail node={node} proposals={[]} />)
    expect(html).toContain("expression")
    expect(html).toContain("sum(o.quantity * o.unit_price)")
    expect(html).toContain("Total revenue across order lines")
  })

  it("round-5: a measure with no expression prints no Definition section", () => {
    const node = WITH_PROPOSAL.nodes.find((n) => n.id === "measure:order_revenue")!
    const html = render(<NodeDetail node={node} proposals={[]} />)
    // The ungoverned proposal exposes a name only — no invented expression.
    expect(html).not.toContain("expression")
  })
})

// ── 12f: the curator inset reads the parsed definition ───────────────────────
describe("NodeDetail — a metric view's definition", () => {
  const MV_WITH_YAML: SemanticGraphResponse = {
    space_id: "space-1",
    nodes: [
      { id: "finance.sales.orders", kind: "table", label: "orders", col: 0, row: 0 },
      { id: "finance.ref.customer", kind: "table", label: "customer", col: 1, row: 0 },
      {
        id: "finance.sales.orders_metrics", kind: "metric_view", label: "orders_metrics", col: 2, row: 0,
        definition_available: true,
        mv_source: "finance.sales.orders",
        mv_filter: "order_status != 'CANCELLED'",
        materialization: "2 materializations · EVERY 1 DAY",
        dimensions: [
          { name: "region", expr: "customer.region", binding: "finance.ref.customer" },
          { name: "order_day", expr: "date_trunc('DAY', order_ts)", binding: "finance.sales.orders" },
        ],
      },
    ],
    edges: [
      { from: "finance.sales.orders_metrics", to: "finance.sales.orders", kind: "uses" },
      { from: "finance.sales.orders_metrics", to: "finance.ref.customer", kind: "uses" },
      { from: "finance.sales.orders", to: "finance.ref.customer", kind: "join", on: "orders.cid = customer.id", relationship: "many-to-one" },
    ],
    proposals: [],
  }
  const mvNode = MV_WITH_YAML.nodes.find((n) => n.kind === "metric_view")!
  const detail = () =>
    render(<NodeDetail node={mvNode} proposals={[]} nodes={MV_WITH_YAML.nodes} edges={MV_WITH_YAML.edges} />)

  it("draws the join tree rooted at the source, with the ON predicate", () => {
    const html = detail()
    expect(html).toContain("Join tree")
    expect(html).toContain("ON orders.cid = customer.id")
    expect(html).toContain("N:1")
    // The tree replaces the flat chip list for a metric view.
    expect(html).not.toContain("Source tables")
  })

  it("roots the tree at mv_source even when the join points AT it", () => {
    // A fact→fact join (detail rows pointing at their header) is the shape a
    // topological root guess gets wrong: order_items→orders would make
    // order_items the root even though the view reads FROM orders.
    const factToFact: SemanticGraphResponse = {
      ...MV_WITH_YAML,
      nodes: [...MV_WITH_YAML.nodes, { id: "finance.sales.order_items", kind: "table", label: "order_items", col: 0, row: 1 }],
      edges: [
        ...MV_WITH_YAML.edges,
        { from: "finance.sales.orders_metrics", to: "finance.sales.order_items", kind: "uses" },
        { from: "finance.sales.order_items", to: "finance.sales.orders", kind: "join", on: "order_items.order_id = orders.order_id", relationship: "many-to-one" },
      ],
    }
    const html = render(<NodeDetail node={mvNode} proposals={[]} nodes={factToFact.nodes} edges={factToFact.edges} />)
    // orders is the root (labelled `source`); order_items hangs off it.
    expect(html).toMatch(/orders<\/span><span class="ml-1\.5 text-\[10px\] text-muted">source/)
    expect(html).toContain("order_items")
  })

  it("nests a snowflake's second level under its dimension", () => {
    const snowflake: SemanticGraphResponse = {
      ...MV_WITH_YAML,
      nodes: [...MV_WITH_YAML.nodes, { id: "finance.ref.nation", kind: "table", label: "nation", col: 1, row: 1 }],
      edges: [
        ...MV_WITH_YAML.edges,
        { from: "finance.sales.orders_metrics", to: "finance.ref.nation", kind: "uses" },
        { from: "finance.ref.customer", to: "finance.ref.nation", kind: "join", on: "customer.nation_id = nation.id", relationship: "many-to-one" },
      ],
    }
    const html = render(<NodeDetail node={mvNode} proposals={[]} nodes={snowflake.nodes} edges={snowflake.edges} />)
    expect(html).toContain("ON customer.nation_id = nation.id")
    // nation sits in a nested list, not beside customer at the first level.
    expect(html).toContain('<ul class="pl-4">')
  })

  it("groups dimensions by the relation they bind to", () => {
    const html = detail()
    expect(html).toContain("Dimensions (2)")
    expect(html).toContain("region")
    expect(html).toContain("order_day")
    // Grouped under the SHORT binding name, so "which came through the join" is
    // answerable at a glance.
    expect(html).toContain(">customer</p>")
  })

  it("reports the filter verbatim and the materialization posture", () => {
    const html = detail()
    expect(html).toContain("order_status != &#x27;CANCELLED&#x27;")
    expect(html).toContain("2 materializations · EVERY 1 DAY")
  })

  it("omits what the definition does not declare rather than printing unknown", () => {
    const bare = { ...mvNode, mv_filter: null, materialization: null, dimensions: null }
    const html = render(<NodeDetail node={bare} proposals={[]} nodes={MV_WITH_YAML.nodes} edges={MV_WITH_YAML.edges} />)
    expect(html).not.toContain("Dimensions")
    expect(html).not.toContain("unknown")
    expect(html).not.toContain("served")
  })

  it("counts source-table reuse from the uses edges of OTHER metric views", () => {
    const shared: SemanticGraphResponse = {
      ...MV_WITH_YAML,
      edges: [
        ...MV_WITH_YAML.edges,
        { from: "finance.sales.other_metrics", to: "finance.ref.customer", kind: "uses" },
      ],
    }
    const html = render(<NodeDetail node={mvNode} proposals={[]} nodes={shared.nodes} edges={shared.edges} />)
    expect(html).toContain("1 of 2 source tables shared with other metric views")
  })

  it("a single-source view says so instead of drawing an empty tree", () => {
    const lone: SemanticGraphResponse = {
      ...MV_WITH_YAML,
      edges: [{ from: "finance.sales.orders_metrics", to: "finance.sales.orders", kind: "uses" }],
    }
    const html = render(<NodeDetail node={mvNode} proposals={[]} nodes={lone.nodes} edges={lone.edges} />)
    expect(html).toContain("no joins — single-source view")
  })
})
