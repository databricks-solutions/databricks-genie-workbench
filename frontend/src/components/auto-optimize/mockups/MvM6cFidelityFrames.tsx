/**
 * M6c fidelity frames (MV-D118) — the run headline after a kept metric-view attach,
 * and the singular gain sentence (Task 12). Renders the REAL ScoreSummary,
 * convergenceReasonText and ScanProposalCard. Disposed with the rest of the
 * scaffold (see docs/design/mockups/README.md).
 */
import { ScoreSummary } from "../ScoreSummary"
import { ScanProposalCard } from "../MvIqScanAdvisorySection"
import { convergenceReasonText } from "@/lib/score-display"
import type { MvDdlArtifact, MvProposal } from "@/types"

function Headline(props: { status: string; bestEvalScope: string; optimizedScore: number }) {
  const reason = convergenceReasonText({
    baselineScore: 86.67,
    optimizedScore: props.optimizedScore,
    bestIteration: 0,
    status: props.status,
    bestEvalScope: props.bestEvalScope,
    convergenceReason: null,
  })
  return (
    <div className="max-w-3xl space-y-2 p-6">
      <ScoreSummary
        baselineScore={86.67}
        optimizedScore={props.optimizedScore}
        bestIteration={0}
        bestEvalScope={props.bestEvalScope}
        status={props.status}
      />
      <p className="text-sm text-muted-foreground">{reason ?? "(no reason line)"}</p>
    </div>
  )
}

export const KeptAttachTerminalFrame = () => (
  <Headline status="CONVERGED" bestEvalScope="metric_view" optimizedScore={90.0} />
)
export const KeptAttachRunningFrame = () => (
  <Headline status="RUNNING" bestEvalScope="metric_view" optimizedScore={90.0} />
)
export const BaselineRetainedFrame = () => (
  <Headline status="CONVERGED" bestEvalScope="full" optimizedScore={86.67} />
)

// Trimmed from MvStaleBodyFidelityFrames' proposalRevenue (not exported there).
const proposalSingleMeasure: MvProposal = {
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
    { display_name: "total_revenue", expr: "SUM(items.quantity * items.unit_price)", dedup_fingerprint: "m_rev", recurrence: 1, provenance_count: 1, benchmark_question_ids: ["bq_0007"] },
  ],
  checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" },
  score_components: { statuses: { L: "COMPUTED", Y: "COMPUTED", S: "COMPUTED", D: "COMPUTED" }, L: 0.42, Y: 0.61, S: 0.5, D: 0.33 },
  evidence: { recurrence_count: 1, source_tables: ["finance.sales.orders", "finance.sales.order_items"], benchmark_question_ids: ["bq_0007"] },
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
  stale_body: false,
}

// Mv158FidelityFrames' ddlRevenue (not exported there), whose body already
// carries the single total_revenue measure.
const SINGLE_MEASURE_YAML = `version: "1.1"
source: finance.sales.orders
joins:
  - name: items
    source: finance.sales.order_items
    "on": orders.order_id = items.order_id
measures:
  - name: total_revenue
    expr: SUM(items.quantity * items.unit_price)
    format: number`

const ddlSingleMeasure: MvDdlArtifact = {
  suggestion_id: "sug_9f2a1c7d4e0b",
  dedup_fingerprint: "9f2a1c7d4e0b6a83",
  proposed_object: "finance.sales.order_revenue",
  join_strategy: null,
  source_tables: ["finance.sales.orders", "finance.sales.order_items"],
  yaml_text: SINGLE_MEASURE_YAML,
  ddl: `CREATE VIEW \`finance\`.\`sales\`.\`order_revenue\`\nWITH METRICS\nLANGUAGE YAML\nAS $$\n${SINGLE_MEASURE_YAML}\n$$;`,
  validation: null,
  grant_sql: "GRANT SELECT ON VIEW `finance`.`sales`.`order_revenue` TO `sales-analysts`;",
}

export const SingularGainCardFrame = () => (
  <ScanProposalCard
    proposal={proposalSingleMeasure}
    ddl={ddlSingleMeasure}
    onReviewCreate={() => {}}
    onClaim={() => {}}
    defaultExpanded
  />
)
