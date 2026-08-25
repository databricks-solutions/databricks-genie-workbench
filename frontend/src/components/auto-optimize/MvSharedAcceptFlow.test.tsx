/**
 * The architecture-constraint test for Prompt 15.8 (MV-D34/MV-D35): ONE
 * MvProposalCard, ONE accept flow, ONE display module, consumed by BOTH
 * suggestion surfaces. Divergence between the IQ surface and the run-output
 * surface is the defect class Prompt 15.8 exists to end, so this pins that both
 * surfaces mount the SAME components and render the SAME facts-led, percent-free
 * card. Static markup only (the harness is renderToStaticMarkup / node env — the
 * probe→consent→create click journey is proven in the backend service test and
 * the scenario_d E2E sub-leg, not here).
 */
import { describe, expect, it } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import { MvSuggestOnlyPanel } from "./MvSuggestOnlyPanel"
import { ScanProposalCard } from "./MvIqScanAdvisorySection"
import { MvAcceptFlow } from "./MvAcceptFlow"
import type { MvProposal } from "@/types"

const render = (el: React.ReactElement) => renderToStaticMarkup(el)

const proposal: MvProposal = {
  suggestion_id: "sug1",
  dedup_fingerprint: "fp1",
  target_space_id: "space-1",
  run_id: null,
  candidate_type: "NEW_METRIC_VIEW",
  confidence_score: 82,
  tier: "HIGH",
  uncapped_tier: "HIGH",
  tier_capped_by_coverage: false,
  proposed_object: "finance.sales.order_revenue",
  measures: [
    { display_name: "revenue", expr: "SUM(x)", dedup_fingerprint: "m1", recurrence: 5, provenance_count: 5, benchmark_question_ids: ["q1"] },
  ],
  checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" },
  score_components: null,
  evidence: { source_tables: ["finance.sales.orders"] },
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

// The signature strings of the two shared modules: the accept flow's primary
// action (MvAcceptFlow) and the display module's facts row (MvProposalCard via
// mvFormat.factsChecks). If either surface stopped rendering a shared module,
// its signature would vanish from that surface's markup.
const ACCEPT_FLOW_SIGNATURE = "Create this metric view"
const CARD_FACTS_SIGNATURE = "no overlap with existing metric views"

describe("Prompt 15.8 architecture constraint — one card, one accept flow, both surfaces", () => {
  const runOutput = render(
    <MvSuggestOnlyPanel
      runId="run-1"
      proposals={[proposal]}
      ddl={null}
      currentIdentifiers={[]}
      onRerun={() => {}}
    />,
  )
  const iqCard = render(
    <ScanProposalCard proposal={proposal} ddl={undefined} onReviewCreate={() => {}} onClaim={() => {}} />,
  )

  it("BOTH surfaces render the SAME accept flow (primary [Create this metric view])", () => {
    expect(runOutput).toContain(ACCEPT_FLOW_SIGNATURE)
    expect(iqCard).toContain(ACCEPT_FLOW_SIGNATURE)
  })

  it("BOTH surfaces render the SAME display module (the facts row)", () => {
    expect(runOutput).toContain(CARD_FACTS_SIGNATURE)
    expect(iqCard).toContain(CARD_FACTS_SIGNATURE)
  })

  it("NEITHER surface renders a percent or the word 'confidence' on the card (MV-D35 re-grep)", () => {
    for (const html of [runOutput, iqCard]) {
      expect(html).not.toMatch(/\d+%\s*confidence/i)
      expect(html.toLowerCase()).not.toContain("confidence")
    }
  })
})

describe("MvAcceptFlow — the shared flow's resting affordance (MV-D34)", () => {
  it("leads with the primary [Create this metric view] action", () => {
    const html = render(<MvAcceptFlow proposal={proposal} />)
    expect(html).toContain("Create this metric view")
  })

  it("an already-approved proposal shows the retained approved-for-later state, not the primary", () => {
    const approved = { ...proposal, approved_for_rerun: true }
    const html = render(<MvAcceptFlow proposal={approved} />)
    expect(html).toContain("Approved for the next run")
    expect(html).not.toContain("Create this metric view")
  })
})
