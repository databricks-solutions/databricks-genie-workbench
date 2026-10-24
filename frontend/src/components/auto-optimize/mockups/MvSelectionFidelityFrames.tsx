/**
 * PR #332 M2 fidelity frames — the REAL run-setup metric-view section when the
 * selection decides the target (MV-D112), and the run-output panel for a view the
 * app attached at approval but did not create.
 */
import { MvSuggestSection } from "@/components/auto-optimize/MvSuggestSection"
import { MvCreateAttachPanel } from "@/components/auto-optimize/MvCreateAttachPanel"
import { deriveMvTarget, mvSelectionMessage, selectedMvProposals } from "@/components/auto-optimize/optimizationRequest"
import type { MvCreatedObject, MvProbeResult, MvProposal } from "@/types"

const noop = () => {}

function proposal(suggestion_id: string, proposed_object: string): MvProposal {
  return {
    suggestion_id, dedup_fingerprint: `fp_${suggestion_id}`, target_space_id: "s1", run_id: null,
    candidate_type: "PROPOSE", confidence_score: 88, tier: "HIGH", uncapped_tier: "HIGH",
    tier_capped_by_coverage: false, proposed_object,
    measures: [],
    checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" },
    score_components: null, evidence: null,
    provenance_labels: null, provenance: null, alternatives: null, conflicts: null,
    requested_mode: null, effective_mode: null, decision: "approved", decided_by: "analyst@example.com",
    decided_at: "2026-09-29T09:00:00Z", suppressed_until: null, approved_for_rerun: true,
    created_at: null, updated_at: null,
  }
}

const proposals = [
  proposal("sug_a", "finance.sales.order_revenue"),
  proposal("sug_b", "finance.sales.customer_ltv"),
  proposal("sug_c", "finance.marketing.campaign_roi"),
]

const granted: MvProbeResult = {
  probe_id: "probe_7f21", checked_as: "analyst@example.com", auth_identity: "OBO", target: "finance.sales",
  checked_at: "2026-09-29T09:00:00Z", results: {}, privileges: [], capabilities: [], verdict: "SUFFICIENT",
  missing: [], remediation_sql: null, fallback_mode: "suggest_only", materialize_consented: false,
  consent_recorded: true, errors: [], audience_grantees: [],
}

function Section({
  ids,
  probe,
  mode = "create_and_attach",
}: {
  ids: string[]
  probe: MvProbeResult | null
  mode?: "suggest_only" | "create_and_attach"
}) {
  const selectedIds = new Set(ids)
  const selected = selectedMvProposals(proposals, selectedIds)
  return (
    <MvSuggestSection
      enabled onToggle={noop} proposalsLoading={false} proposals={proposals}
      selectedProposalIds={selectedIds} onToggleProposal={noop}
      mode={mode} onModeChange={noop}
      target={deriveMvTarget(selected)} probe={probe} probeLoading={false} probeError={null}
      onCopyGrant={noop} selectionMessage={mvSelectionMessage(selected, mode)}
    />
  )
}

export function SelectionSubsetFrame() {
  return <Section ids={["sug_a", "sug_b"]} probe={granted} />
}

export function SelectionNoneFrame() {
  return <Section ids={[]} probe={null} />
}

export function SelectionTwoSchemasFrame() {
  return <Section ids={["sug_a", "sug_c"]} probe={null} />
}

export function SelectionTwoSchemasSuggestOnlyFrame() {
  return <Section ids={["sug_a", "sug_b", "sug_c"]} probe={null} mode="suggest_only" />
}

const attachedNotCreated: MvCreatedObject = {
  run_id: "run-obo-2", suggestion_id: "sug_a", full_name: "finance.sales.order_revenue",
  created_by: "analyst@example.com", provenance: "USER_CREATED", status: "ATTACHED",
  attach_patch_id: null, baseline_eval_run_id: null, post_attach_eval_run_id: null,
  on_regression_action: "DETACH_ONLY_NEVER_DROP", created_at: null, lift_report: null,
}

export function AttachedNotCreatedFrame() {
  return <MvCreateAttachPanel obj={attachedNotCreated} ddl={null} catalogUrl={null} />
}
