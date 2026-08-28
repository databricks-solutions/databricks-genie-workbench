/**
 * MV-advisor mockups — Prompt 15.8 FIDELITY-GATE EXPORT (review scaffold).
 *
 * The fidelity gate (both rules copies) requires that a prompt changing a
 * user-facing surface export the changed surface states through this emitter, in
 * both themes, next to the approved reference frames. Prompt 15.8 replaced the
 * proposal card's face (facts row, no "%"/"confidence") and added one shared
 * accept flow ([Create this metric view]) to BOTH suggestion surfaces. So these
 * two frames render the REAL production components — not scaffold copies — from
 * production `MvProposal` payloads, so the reviewer compares the deployed
 * experience, not a hand-drawn stand-in:
 *   15.8a — IQ-scan proposal card (the recommended card + a collapsed sibling)
 *   15.8b — run-output suggest-only panel (count truth, ranked, one Recommended)
 *
 * Disposed with the rest of the scaffold (see docs/design/mockups/README.md).
 * MV-D23: run_id is presentational only here.
 */
import { ScanProposalCard } from "../MvIqScanAdvisorySection"
import { MvSuggestOnlyPanel } from "../MvSuggestOnlyPanel"
import type { MvDdlArtifact, MvProposal } from "@/types"

// A strong, fully-proven proposal: validated + executable + no-overlap all ran,
// so the facts row shows all three. HIGH tier drives ranking, never a percent.
const proposalRevenue: MvProposal = {
  suggestion_id: "sug_9f2a1c7d4e0b",
  dedup_fingerprint: "9f2a1c7d4e0b6a83",
  target_space_id: "01ef9a2b3c4d5e6f",
  run_id: "run_5c1e",
  candidate_type: "NEW_METRIC_VIEW",
  confidence_score: 88,
  tier: "HIGH",
  uncapped_tier: "HIGH",
  tier_capped_by_coverage: false,
  proposed_object: "finance.sales.order_revenue",
  measures: [
    { display_name: "total_revenue", expr: "SUM(items.quantity * items.unit_price)", dedup_fingerprint: "m_rev", recurrence: 14, provenance_count: 14, benchmark_question_ids: ["bq_0007", "bq_0019", "bq_0022", "bq_0041"] },
    { display_name: "order_count", expr: "COUNT(DISTINCT orders.order_id)", dedup_fingerprint: "m_cnt", recurrence: 9, provenance_count: 9, benchmark_question_ids: ["bq_0007", "bq_0022"] },
  ],
  checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" },
  // All four signals ran and contributed → the caption reads "backed by usage
  // history + lineage", never a percent.
  score_components: { statuses: { L: "COMPUTED", Y: "COMPUTED", S: "COMPUTED", D: "COMPUTED" }, L: 0.42, Y: 0.61, S: 0.5, D: 0.33 },
  evidence: { recurrence_count: 14, source_tables: ["finance.sales.orders", "finance.sales.order_items"], benchmark_question_ids: ["bq_0007", "bq_0019", "bq_0022", "bq_0041"] },
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
}

// A second, orthogonal proposal — disjoint measures, so the two can both be
// created independently. Collapsed by default (only the Recommended expands).
const proposalMargin: MvProposal = {
  ...proposalRevenue,
  suggestion_id: "sug_3b77e0aa91d2",
  dedup_fingerprint: "3b77e0aa91d2f5c1",
  confidence_score: 71,
  tier: "MEDIUM",
  uncapped_tier: "MEDIUM",
  proposed_object: "finance.sales.gross_margin",
  measures: [
    { display_name: "gross_margin", expr: "SUM(orders.revenue - cost.amount)", dedup_fingerprint: "m_gm", recurrence: 6, provenance_count: 6, benchmark_question_ids: ["bq_0011", "bq_0033"] },
  ],
  checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" },
  // COMPUTED≠SUPPORTIVE (fix #4): the usage signal D RAN but contributed 0, so
  // the caption must NOT claim usage backing — it reads "curated SQL only".
  score_components: { statuses: { L: "COMPUTED", Y: "COMPUTED", S: "COMPUTED", D: "COMPUTED" }, L: 0, Y: 0.55, S: 0.3, D: 0 },
  evidence: { recurrence_count: 6, source_tables: ["finance.sales.orders", "finance.ref.product_cost"], benchmark_question_ids: ["bq_0011", "bq_0033"] },
}

const REVENUE_YAML = `version: "1.1"
source: finance.sales.orders
joins:
  - name: items
    source: finance.sales.order_items
    "on": orders.order_id = items.order_id
measures:
  - name: total_revenue
    expr: SUM(items.quantity * items.unit_price)
    format: number`

const ddlRevenue: MvDdlArtifact = {
  suggestion_id: "sug_9f2a1c7d4e0b",
  dedup_fingerprint: "9f2a1c7d4e0b6a83",
  proposed_object: "finance.sales.order_revenue",
  join_strategy: null,
  source_tables: ["finance.sales.orders", "finance.sales.order_items"],
  yaml_text: REVENUE_YAML,
  ddl: `CREATE VIEW \`finance\`.\`sales\`.\`order_revenue\`\nWITH METRICS\nLANGUAGE YAML\nAS $$\n${REVENUE_YAML}\n$$;`,
  validation: null,
  grant_sql: "GRANT SELECT ON VIEW `finance`.`sales`.`order_revenue` TO `sales-analysts`;",
}

export function Iq158CardFrame() {
  return (
    <div className="space-y-4">
      <ScanProposalCard
        proposal={proposalRevenue}
        ddl={ddlRevenue}
        onReviewCreate={() => {}}
        onClaim={() => {}}
        recommended
        recommendedReason="Bundles 2 measures seen across 4 example questions."
        defaultExpanded
      />
      <ScanProposalCard
        proposal={proposalMargin}
        ddl={undefined}
        onReviewCreate={() => {}}
        onClaim={() => {}}
      />
    </div>
  )
}

export function RunOutput158Frame() {
  return (
    <MvSuggestOnlyPanel
      runId="run_5c1e"
      proposals={[proposalRevenue, proposalMargin]}
      ddl={ddlRevenue}
      currentIdentifiers={[]}
      onRerun={() => {}}
    />
  )
}
