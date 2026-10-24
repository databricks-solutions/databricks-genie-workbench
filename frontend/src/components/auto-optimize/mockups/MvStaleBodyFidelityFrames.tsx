/**
 * M6b fidelity frames — a proposal whose body predates the M3 render (MV-D117).
 *
 * Renders the REAL ScanProposalCard and MvSuggestOnlyPanel, ranked by the
 * production rankProposals / recommendedIndex, so the reviewer compares the
 * shipped stale card against 15.8a / 15.8b / 15.10:
 *   m6b-a — IQ scan: the fresh card is Recommended; the stale one ranks last
 *   m6b-b — run output: the same two proposals in the suggest-only panel
 *   m6b-c — IQ scan: a stale card the user approved before M3
 *   m6b-d — run output: two disjoint current proposals + one stale — the
 *           callout names the current proposals only
 *   m6b-e — IQ scan: the LOW disclosure open, one current and one stale card
 *
 * Fixtures are copied from Mv158FidelityFrames (not exported there).
 * Disposed with the rest of the scaffold (see docs/design/mockups/README.md).
 */
import { LowProposalsDisclosure, ScanProposalCard } from "../MvIqScanAdvisorySection"
import { MvSuggestOnlyPanel } from "../MvSuggestOnlyPanel"
import {
  orthogonalityCallout,
  rankProposals,
  recommendedIndex,
  recommendedReason,
  splitProposalsByConfidence,
} from "../mvFormat"
import type { MvDdlArtifact, MvProposal } from "@/types"

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

const fresh: MvProposal = { ...proposalRevenue, tier: "MEDIUM", uncapped_tier: "MEDIUM" }

const stale: MvProposal = {
  ...proposalMargin,
  tier: "HIGH",
  uncapped_tier: "HIGH",
  checks: { no_overlap: "PASS" },
  stale_body: true,
}

const approvedStale: MvProposal = { ...stale, decision: "approved" }

// A third proposal, stale, disjoint from both fresh ones (m6b-d).
const staleAov: MvProposal = {
  ...stale,
  suggestion_id: "sug_curated_1a2b",
  dedup_fingerprint: "curated1a2b3c4d",
  proposed_object: "finance.sales.avg_order_value",
  measures: [
    { display_name: "avg_order_value", expr: "SUM(orders.revenue) / COUNT(DISTINCT orders.order_id)", dedup_fingerprint: "m_aov", recurrence: 1, provenance_count: 1, benchmark_question_ids: ["sql_snippet:measures:01f13a"] },
  ],
}

// MvIqScanAdvisorySection renders its list only after a scan fetch, so this
// mirrors its list loop, callout line included (MvIqScanAdvisorySection.tsx:480-509).
export function IqScanStaleFrame() {
  const ranked = rankProposals([stale, fresh])
  const callout = orthogonalityCallout(ranked)
  const recommendedAt = recommendedIndex(ranked, callout)
  return (
    <div className="space-y-4">
      {callout && <p className="text-xs text-secondary">{callout}</p>}
      {ranked.map((proposal, i) => (
        <ScanProposalCard
          key={proposal.suggestion_id}
          proposal={proposal}
          ddl={proposal.stale_body ? undefined : ddlRevenue}
          onReviewCreate={() => {}}
          onClaim={() => {}}
          recommended={i === recommendedAt}
          recommendedReason={i === recommendedAt ? recommendedReason(proposal) : undefined}
          defaultExpanded={i === 0}
        />
      ))}
    </div>
  )
}

export function RunOutputStaleFrame() {
  return (
    <MvSuggestOnlyPanel
      runId="run_5c1e"
      proposals={[stale, fresh]}
      ddlBySuggestion={{ [fresh.suggestion_id]: ddlRevenue }}
      currentIdentifiers={[]}
      onRerun={() => {}}
    />
  )
}

export function RunOutputCurrentCalloutFrame() {
  return (
    <MvSuggestOnlyPanel
      runId="run_5c1e"
      proposals={[staleAov, proposalRevenue, proposalMargin]}
      ddlBySuggestion={{ [proposalRevenue.suggestion_id]: ddlRevenue }}
      currentIdentifiers={[]}
      onRerun={() => {}}
    />
  )
}

// m6b-e: a generated LOW proposal and a curated one rendered before M3 — the
// curated one fails its facts, so both land behind the disclosure.
const lowCurrent: MvProposal = {
  ...proposalMargin,
  suggestion_id: "sug_low_refunds",
  dedup_fingerprint: "low0refunds1a2b",
  confidence_score: 38,
  tier: "LOW",
  uncapped_tier: "LOW",
  proposed_object: "finance.sales.refund_rate",
  measures: [
    { display_name: "refund_count", expr: "COUNT(DISTINCT refunds.refund_id)", dedup_fingerprint: "m_ref", recurrence: 2, provenance_count: 2, benchmark_question_ids: ["bq_0052"] },
  ],
  evidence: { recurrence_count: 2, source_tables: ["finance.sales.refunds"], benchmark_question_ids: ["bq_0052"] },
}

const lowStaleCurated: MvProposal = {
  ...lowCurrent,
  suggestion_id: "sug_low_discounts",
  dedup_fingerprint: "low0discounts3c4d",
  proposed_object: "finance.sales.discount_depth",
  measures: [
    { display_name: "avg_discount", expr: "AVG(orders.discount_pct)", dedup_fingerprint: "m_disc", recurrence: 3, provenance_count: 3, benchmark_question_ids: ["sql_snippet:measures:02a7c1"] },
  ],
  checks: { no_overlap: "PASS" },
  evidence: { recurrence_count: 3, ast_curated_provenance_count: 3, source_tables: ["finance.sales.orders"], benchmark_question_ids: ["sql_snippet:measures:02a7c1"] },
  stale_body: true,
}

export function IqScanLowStaleFrame() {
  const { primary, low } = splitProposalsByConfidence([lowStaleCurated, lowCurrent])
  return (
    <LowProposalsDisclosure
      low={low}
      primaryEmpty={primary.length === 0}
      open
      onToggle={() => {}}
      renderCard={(proposal) => (
        <ScanProposalCard
          key={proposal.suggestion_id}
          proposal={proposal}
          ddl={undefined}
          onReviewCreate={() => {}}
          onClaim={() => {}}
        />
      )}
    />
  )
}

export function IqScanApprovedStaleFrame() {
  return (
    <ScanProposalCard
      proposal={approvedStale}
      ddl={undefined}
      onReviewCreate={() => {}}
      onClaim={() => {}}
      defaultExpanded
    />
  )
}
