/**
 * PR #332 M7c fidelity frames (MV-D120) — the REAL created terminal on the IQ scan card
 * after attach-at-approval: a view the caller created, with the GRANT it needs to run, and
 * an existing view someone else owns, which names the owner and offers no GRANT. The
 * terminal is reached only by clicking create, so each frame renders `MvCreatedTerminal`
 * directly in the card's actions slot, as `ScanProposalCard` would after the click.
 */
import { MvProposalCard } from "../MvProposalCard"
import { MvCreatedTerminal } from "../MvAcceptFlow"
import type { MvDdlArtifact, MvProposal } from "@/types"

const VIEW = "finance.sales.order_revenue"
const CATALOG_URL = "https://example.cloud.databricks.com/explore/data/finance/sales/order_revenue"

const proposal: MvProposal = {
  suggestion_id: "sug_9f2a1c7d4e0b",
  dedup_fingerprint: "9f2a1c7d4e0b6a83",
  target_space_id: "01ef9a2b3c4d5e6f",
  run_id: "run_5c1e",
  candidate_type: "NEW_METRIC_VIEW",
  confidence_score: 88,
  tier: "HIGH",
  uncapped_tier: "HIGH",
  tier_capped_by_coverage: false,
  proposed_object: VIEW,
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
  attached: true,
  created_at: null,
  updated_at: null,
}

const ddl: MvDdlArtifact = {
  suggestion_id: "sug_9f2a1c7d4e0b",
  dedup_fingerprint: "9f2a1c7d4e0b6a83",
  proposed_object: VIEW,
  join_strategy: null,
  source_tables: ["finance.sales.orders", "finance.sales.order_items"],
  yaml_text: "version: \"1.1\"\nsource: finance.sales.orders",
  ddl: "CREATE VIEW `finance`.`sales`.`order_revenue` WITH METRICS LANGUAGE YAML AS $$ ... $$;",
  validation: null,
  grant_sql: "GRANT SELECT ON VIEW `finance`.`sales`.`order_revenue` TO `1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d`;",
}

const noop = () => {}

export function CreatedTerminalOwnerFrame() {
  return (
    <MvProposalCard
      proposal={proposal}
      ddl={ddl}
      actions={
        <MvCreatedTerminal
          attached
          alreadyExisted={false}
          provenance="OBO_CREATED"
          owner={null}
          grantSql="GRANT SELECT ON VIEW `finance`.`sales`.`order_revenue` TO `00000000-0000-0000-0000-000000000000`;"
          catalogUrl={CATALOG_URL}
          onStartRun={noop}
        />
      }
    />
  )
}

export function AttachedSomeoneElsesViewFrame() {
  return (
    <MvProposalCard
      proposal={proposal}
      ddl={ddl}
      actions={
        <MvCreatedTerminal
          attached
          alreadyExisted
          provenance="USER_CREATED"
          owner="data.owner@example.com"
          grantSql={null}
          catalogUrl={CATALOG_URL}
          onStartRun={noop}
        />
      }
    />
  )
}
