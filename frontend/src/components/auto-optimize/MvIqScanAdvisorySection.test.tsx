/**
 * Production tests for the IQ Scan advisory section (Prompt 13.5, MV-D23/D24),
 * pinning the copy and per-state structure the graduated mockup frames 7–8 used
 * to guard. renderToStaticMarkup + node env — the repo's frontend test pattern.
 *
 * The presentational sub-views render directly with props (never the fetching
 * container), which is the MV-D23 contract: the SAME MvProposalCard the run
 * output uses renders here from a space-scoped source with no component change.
 */
import { describe, expect, it } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import {
  MvAdvisoryEmpty,
  MvAdvisoryCouldNotRun,
  MvRegisterInput,
  MvRegisterVerified,
  MvRegisterRefused,
} from "./MvIqScanAdvisorySection"
import { MvProposalCard } from "./MvProposalCard"
import type { MvProposal, MvRegisterResponse } from "@/types"

const render = (el: React.ReactElement) => renderToStaticMarkup(el)

const proposal: MvProposal = {
  suggestion_id: "sug1",
  dedup_fingerprint: "fp1",
  target_space_id: "space-1",
  run_id: null,
  candidate_type: "NEW_METRIC_VIEW",
  confidence_score: 82,
  tier: "HIGH",
  proposed_object: "finance.sales.order_revenue",
  score_components: null,
  evidence: { recurrence_count: 6 },
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

describe("IQ Scan advisory — found (MV-D23 prop-driven payoff)", () => {
  it("the run-output MvProposalCard renders from a space-scoped proposal (no run_id)", () => {
    // The same card the run output uses, with NO liftLabel and NO re-run action
    // — nothing was run to measure. Mounts unchanged from the suggest source.
    const html = render(
      <MvProposalCard proposal={proposal} />,
    )
    expect(html).toContain("finance.sales.order_revenue")
    expect(html).not.toContain("Lift not measured")
  })
})

describe("IQ Scan advisory — EMPTY (MV-D15, authored copy)", () => {
  it("names what the scan read and reads as a clean result, not an error", () => {
    const html = render(<MvAdvisoryEmpty />)
    expect(html).toContain("No recurring measures to propose yet")
    expect(html).toContain("example question SQL")
    expect(html).toContain("clean result")
    // The omission is load-bearing: query history is not in the corpus at all.
    expect(html).not.toContain("query history")
  })
})

describe("IQ Scan advisory — couldn't-run (FAILED is not a silent empty)", () => {
  it("surfaces the failure and a retry, never an empty state", () => {
    const html = render(<MvAdvisoryCouldNotRun reason="corpus read timed out" onRetry={() => {}} />)
    expect(html).toContain("didn")
    expect(html).toContain("corpus read timed out")
    expect(html).toContain("Try again")
  })
})

describe("BYO register (MV-D24)", () => {
  it("8a input prompts for a three-part identifier", () => {
    const html = render(
      <MvRegisterInput value="" onChange={() => {}} busy={false} onRegister={() => {}} claimId={null} />,
    )
    expect(html).toContain("Register an existing metric view")
    expect(html).toContain("catalog.schema.metric_view")
  })

  it("8b verified renders USER_CREATED, the verbatim copy, and NO Drop action (invariant 1)", () => {
    const verified: MvRegisterResponse = {
      registered: true,
      full_name: "finance.sales.net_revenue",
      provenance: "USER_CREATED",
      run_id: "run-x",
      suggestion_id: "user_abc",
      reason: null,
      warnings: [],
    }
    const html = render(<MvRegisterVerified result={verified} onStartRun={() => {}} />)
    expect(html).toContain("USER_CREATED")
    expect(html).toContain("METRIC_VIEW")
    expect(html).toContain("The app never drops views it didn")
    expect(html).toContain("Start an optimization run")
    expect(html).not.toContain("Drop view")
  })

  it("8c refused renders the reason and states nothing was recorded (invariant 2)", () => {
    const refused: MvRegisterResponse = {
      registered: false,
      full_name: "finance.sales.orders",
      provenance: "USER_CREATED",
      run_id: null,
      suggestion_id: null,
      reason: "finance.sales.orders is not a metric view",
      warnings: [],
    }
    const html = render(<MvRegisterRefused result={refused} />)
    expect(html).toContain("is not a metric view")
    expect(html).toContain("Nothing was recorded")
  })
})
