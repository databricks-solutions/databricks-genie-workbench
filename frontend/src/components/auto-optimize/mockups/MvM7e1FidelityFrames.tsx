/**
 * PR #332 M7e-1 fidelity frames (MV-D122) — the REAL shared components, with fixtures:
 *   m7e1-a — IQ scan: the list after a reshape, one card where the reshaped pair was
 *            listed (reference: m6b-a). A layout pin for the post-drop list only: its
 *            fixture is what the server returns, so it cannot catch a regressed drop.
 *            The drop is guarded by backend/tests/test_mv_create.py::
 *            test_the_older_undecided_row_of_a_view_leaves_the_list and
 *            ::test_the_newest_undecided_drop_applies_at_every_list_site.
 *   m7e1-b — run output: one stale and one current proposal; the stale card has no
 *            config preview and no Lift label, and the summary counts the current
 *            proposal and states the re-scan (reference: m6b-b)
 *   m7e1-c — Model tab: one current and one stale proposal; only the current ghost
 *            is drawn (reference: 9j, RealModelOverlayFrame). The deployed Model tab
 *            does not mount this path today: SemanticBlueprint folds the graph with
 *            `proposals: []` (SemanticBlueprint.tsx:1026), and no live surface passes
 *            `onLocateInGraph`. This frame guards the contract for when it is mounted.
 *
 * Disposed with the rest of the scaffold (see docs/design/mockups/README.md).
 */
import { ScanProposalCard } from "../MvIqScanAdvisorySection"
import { MvProposalsSummary } from "../MvProposalsSummary"
import { MvSuggestOnlyPanel } from "../MvSuggestOnlyPanel"
import { orthogonalityCallout, rankProposals, recommendedIndex, recommendedReason } from "../mvFormat"
import { SemanticGraph } from "@/components/model/SemanticGraph"
import { withOverlay } from "@/components/model/SemanticModelTab"
import type { MvDdlArtifact, MvProposal, SemanticGraphEdge, SemanticGraphNode, SemanticGraphResponse } from "@/types"

const ordersMetrics: MvProposal = {
  suggestion_id: "sug_orders_b2",
  dedup_fingerprint: "b2c4e6a8d0f13579",
  target_space_id: "01ef9a2b3c4d5e6f",
  run_id: null,
  candidate_type: "NEW_METRIC_VIEW",
  confidence_score: 84,
  tier: "HIGH",
  uncapped_tier: "HIGH",
  tier_capped_by_coverage: false,
  proposed_object: "finance.sales.orders_metrics",
  measures: [
    { display_name: "total_revenue", expr: "SUM(orders.amount)", dedup_fingerprint: "m_rev", recurrence: 14, provenance_count: 14, benchmark_question_ids: ["bq_0007", "bq_0019", "bq_0022"] },
    { display_name: "order_count", expr: "COUNT(DISTINCT orders.order_id)", dedup_fingerprint: "m_cnt", recurrence: 9, provenance_count: 9, benchmark_question_ids: ["bq_0007", "bq_0022"] },
    { display_name: "avg_order_value", expr: "SUM(orders.amount) / COUNT(DISTINCT orders.order_id)", dedup_fingerprint: "m_aov", recurrence: 5, provenance_count: 5, benchmark_question_ids: ["bq_0031"] },
  ],
  checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" },
  score_components: { statuses: { L: "COMPUTED", Y: "COMPUTED", S: "COMPUTED", D: "COMPUTED" }, L: 0.4, Y: 0.6, S: 0.5, D: 0.3 },
  evidence: { recurrence_count: 14, source_tables: ["finance.sales.orders"], benchmark_question_ids: ["bq_0007", "bq_0019", "bq_0022", "bq_0031"] },
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
  created_at: "2026-10-01T15:20:00Z",
  updated_at: "2026-10-01T15:20:00Z",
}

const refundsMetrics: MvProposal = {
  ...ordersMetrics,
  suggestion_id: "sug_refunds_4d",
  dedup_fingerprint: "4d6f8a0c2e4b6d8f",
  confidence_score: 66,
  tier: "MEDIUM",
  uncapped_tier: "MEDIUM",
  proposed_object: "finance.sales.refunds_metrics",
  measures: [
    { display_name: "refund_count", expr: "COUNT(DISTINCT refunds.refund_id)", dedup_fingerprint: "m_ref", recurrence: 6, provenance_count: 6, benchmark_question_ids: ["bq_0052", "bq_0057"] },
    { display_name: "refund_amount", expr: "SUM(refunds.amount)", dedup_fingerprint: "m_ramt", recurrence: 4, provenance_count: 4, benchmark_question_ids: ["bq_0052"] },
  ],
  evidence: { recurrence_count: 6, source_tables: ["finance.sales.refunds"], benchmark_question_ids: ["bq_0052", "bq_0057"] },
}

// Found before the M3 render (MV-D117); a different view name, so the server's
// stale-beside-its-successor drop leaves it listed.
const staleMargin: MvProposal = {
  ...ordersMetrics,
  suggestion_id: "sug_margin_3b",
  dedup_fingerprint: "3b77e0aa91d2f5c1",
  confidence_score: 71,
  tier: "MEDIUM",
  uncapped_tier: "MEDIUM",
  proposed_object: "finance.sales.gross_margin",
  measures: [
    { display_name: "gross_margin", expr: "SUM(orders.revenue - cost.amount)", dedup_fingerprint: "m_gm", recurrence: 6, provenance_count: 6, benchmark_question_ids: ["bq_0011", "bq_0033"] },
  ],
  checks: { no_overlap: "PASS" },
  evidence: { recurrence_count: 6, source_tables: ["finance.sales.orders", "finance.ref.product_cost"], benchmark_question_ids: ["bq_0011", "bq_0033"] },
  created_at: "2026-08-12T09:00:00Z",
  updated_at: "2026-08-12T09:00:00Z",
  stale_body: true,
}

function ddlFor(proposal: MvProposal, yaml: string): MvDdlArtifact {
  const [catalog, schema, name] = (proposal.proposed_object ?? "").split(".")
  const quoted = `\`${catalog}\`.\`${schema}\`.\`${name}\``
  return {
    suggestion_id: proposal.suggestion_id,
    dedup_fingerprint: proposal.dedup_fingerprint,
    proposed_object: proposal.proposed_object,
    join_strategy: null,
    source_tables: proposal.evidence?.source_tables as string[],
    yaml_text: yaml,
    ddl: `CREATE VIEW ${quoted}\nWITH METRICS\nLANGUAGE YAML\nAS $$\n${yaml}\n$$;`,
    validation: null,
    grant_sql: `GRANT SELECT ON VIEW ${quoted} TO \`sales-analysts\`;`,
  }
}

const ORDERS_YAML = `version: "1.1"
source: finance.sales.orders
measures:
  - name: total_revenue
    expr: SUM(amount)
  - name: order_count
    expr: COUNT(DISTINCT order_id)
  - name: avg_order_value
    expr: SUM(amount) / COUNT(DISTINCT order_id)`

const REFUNDS_YAML = `version: "1.1"
source: finance.sales.refunds
measures:
  - name: refund_count
    expr: COUNT(DISTINCT refund_id)
  - name: refund_amount
    expr: SUM(amount)`

const ddlById: Record<string, MvDdlArtifact> = {
  [ordersMetrics.suggestion_id]: ddlFor(ordersMetrics, ORDERS_YAML),
  [refundsMetrics.suggestion_id]: ddlFor(refundsMetrics, REFUNDS_YAML),
}

// The re-scan reshaped the orders bundle (it gained avg_order_value), which wrote a
// new row beside the older undecided one for the same view. The drop of the older
// sibling is server-side (`_live_proposal_rows` in backend/routers/auto_optimize.py,
// Task 3), so this list fixture holds only what the backend returns: one
// orders_metrics proposal. This frame pins the layout of that list, not the drop. MvIqScanAdvisorySection renders its list only after a
// scan fetch, so this mirrors its summary and list loop (MvIqScanAdvisorySection.tsx:470-526).
export function IqScanReshapedListFrame() {
  const primary = [ordersMetrics, refundsMetrics]
  const ranked = rankProposals(primary)
  const callout = orthogonalityCallout(ranked)
  const recommendedAt = recommendedIndex(ranked, callout)
  return (
    <div className="space-y-4">
      <MvProposalsSummary proposals={primary} />
      {callout && <p className="text-xs text-secondary">{callout}</p>}
      {ranked.map((proposal, i) => (
        <ScanProposalCard
          key={proposal.suggestion_id}
          proposal={proposal}
          ddl={ddlById[proposal.suggestion_id]}
          onReviewCreate={() => {}}
          onClaim={() => {}}
          onLocate={() => {}}
          recommended={i === recommendedAt}
          recommendedReason={i === recommendedAt ? recommendedReason(proposal) : undefined}
          defaultExpanded={i === 0}
        />
      ))}
    </div>
  )
}

export function RunOutputStaleNoPreviewFrame() {
  return (
    <MvSuggestOnlyPanel
      runId="run_7e1b"
      proposals={[staleMargin, ordersMetrics]}
      ddlBySuggestion={{ [ordersMetrics.suggestion_id]: ddlById[ordersMetrics.suggestion_id] }}
      currentIdentifiers={[]}
      onRerun={() => {}}
    />
  )
}

// One governed view, and two loose measures in Space config — each named like the
// view a proposal would create, so withOverlay can draw its "would govern" link.
const MODEL_NODES: SemanticGraphNode[] = [
  { id: "finance.sales.orders", kind: "table", label: "orders", col: 0, row: 0, role: "fact" },
  { id: "finance.sales.refunds", kind: "table", label: "refunds", col: 0, row: 1 },
  { id: "finance.ref.product_cost", kind: "table", label: "product_cost", col: 1, row: 0 },
  { id: "finance.sales.order_revenue", kind: "metric_view", label: "order_revenue", col: 2, row: 0, definition_available: true, mv_source: "finance.sales.orders" },
  { id: "measure:total_revenue", kind: "measure", label: "total_revenue", col: 3, row: 0, governance: "governed", origin: "order_revenue (attached)" },
  { id: "measure:order_count", kind: "measure", label: "order_count", col: 3, row: 1, governance: "governed", origin: "order_revenue (attached)" },
  { id: "measure:refunds_metrics", kind: "measure", label: "refunds_metrics", col: 3, row: 2, governance: "ungoverned", origin: "proposal evidence · 6×" },
  { id: "measure:gross_margin", kind: "measure", label: "gross_margin", col: 3, row: 3, governance: "ungoverned", origin: "proposal evidence · 6×" },
]

const MODEL_EDGES: SemanticGraphEdge[] = [
  { from: "finance.sales.refunds", to: "finance.sales.orders", kind: "join", on: "refunds.order_id = orders.order_id", relationship: "many-to-one", scd2: false },
  { from: "finance.sales.order_revenue", to: "finance.sales.orders", kind: "uses" },
  { from: "measure:total_revenue", to: "finance.sales.order_revenue", kind: "membership" },
  { from: "measure:order_count", to: "finance.sales.order_revenue", kind: "membership" },
]

// Not mounted by the deployed Model tab today (see the header); guards withOverlay's
// contract for when it is.
export function ModelTabStaleGhostFrame() {
  const graph: SemanticGraphResponse = {
    space_id: "01ef9a2b3c4d5e6f",
    nodes: MODEL_NODES,
    edges: MODEL_EDGES,
    proposals: [refundsMetrics, staleMargin],
    coverage_status: "ok",
    coverage_reason: null,
  }
  const { nodes, edges } = withOverlay(graph)
  return (
    <div className="space-y-3 rounded-xl border border-default bg-surface p-4">
      <h3 className="text-sm font-semibold uppercase tracking-wide text-secondary">Semantic model · proposal overlay ON</h3>
      <SemanticGraph nodes={nodes} edges={edges} label="Semantic model — proposal overlay ON, one stale proposal" />
    </div>
  )
}
