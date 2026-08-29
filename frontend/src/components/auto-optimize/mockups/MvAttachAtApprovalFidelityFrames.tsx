/**
 * MV-advisor mockups — attach-at-approval FIDELITY-GATE EXPORT (review scaffold).
 *
 * The fidelity gate (both rules copies) requires that a change to a user-facing
 * surface export the changed surface states through this emitter, in both themes,
 * next to the approved reference frames. This round (MV-D34 attach-at-approval)
 * reshaped "Create this metric view" into create-AND-attach: approving now shelves
 * the view on the Agent config (the source of truth), so the proposal list badges
 * an already-attached proposal instead of re-offering it, and the shared accept
 * flow opens on an "Attached to your Agent" terminal rather than the action state.
 *
 * The frame renders the REAL production components (ScanProposalCard, which embeds
 * MvAcceptFlow) from a production `MvProposal` marked `attached`, so the reviewer
 * compares the deployed experience:
 *   - the header "Attached" badge (scannable, replaces "still N to create")
 *   - the accept-flow "Attached to your Agent" terminal + the SP grant it needs
 *
 * The post-create "Created & attached" terminal is an interaction-only state
 * (reached after clicking create), so it is proven by the component tests, not a
 * static frame. Disposed with the rest of the scaffold (see docs/design/mockups).
 */
import { ScanProposalCard } from "../MvIqScanAdvisorySection"
import type { MvDdlArtifact, MvProposal } from "@/types"

const attachedProposal: MvProposal = {
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
    { display_name: "total_revenue", expr: "SUM(items.quantity * items.unit_price)", dedup_fingerprint: "m_rev", recurrence: 14, provenance_count: 14, benchmark_question_ids: ["bq_0007", "bq_0019"] },
  ],
  checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" },
  score_components: { statuses: { L: "COMPUTED", Y: "COMPUTED", S: "COMPUTED", D: "COMPUTED" }, L: 0.42, Y: 0.61, S: 0.5, D: 0.33 },
  evidence: { recurrence_count: 14, source_tables: ["finance.sales.orders", "finance.sales.order_items"], benchmark_question_ids: ["bq_0007", "bq_0019"] },
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
  // The one field this round adds: the space-scoped list marked it as already on
  // the Agent config, so the card badges "Attached" and the flow opens attached.
  attached: true,
  created_at: null,
  updated_at: null,
}

const ddlAttached: MvDdlArtifact = {
  suggestion_id: "sug_9f2a1c7d4e0b",
  dedup_fingerprint: "9f2a1c7d4e0b6a83",
  proposed_object: "finance.sales.order_revenue",
  join_strategy: null,
  source_tables: ["finance.sales.orders", "finance.sales.order_items"],
  yaml_text: "version: \"1.1\"\nsource: finance.sales.orders",
  ddl: "CREATE VIEW `finance`.`sales`.`order_revenue` WITH METRICS LANGUAGE YAML AS $$ ... $$;",
  validation: null,
  grant_sql: "GRANT SELECT ON VIEW `finance`.`sales`.`order_revenue` TO `1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d`;",
}

export function AttachedProposalCardFrame() {
  return (
    <ScanProposalCard
      proposal={attachedProposal}
      ddl={ddlAttached}
      onReviewCreate={() => {}}
      onClaim={() => {}}
      defaultExpanded
    />
  )
}
