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
import type { MvProposal, MvRegisterResponse, MvDdlArtifact } from "@/types"

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
  measures: [],
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

  it("renders copy-ready DDL + GRANT when the advice-run fallback supplies it (Prompt 15.1)", () => {
    // The container fetches this per proposal from route 7's candidate fallback
    // (a never-optimized space has no artifact); given the ddl prop the card
    // shows the executable CREATE VIEW wrapper and the copy-ready GRANT.
    const ddl: MvDdlArtifact = {
      suggestion_id: "sug1",
      dedup_fingerprint: "fp1",
      proposed_object: "finance.sales.order_revenue",
      join_strategy: "subquery_source",
      yaml_text: "version: 0.1\n",
      ddl: "CREATE VIEW finance.sales.order_revenue\nWITH METRICS\nLANGUAGE YAML\nAS $$\nversion: 0.1\n$$",
      validation: null,
      grant_sql: "GRANT SELECT ON VIEW finance.sales.order_revenue TO `<grantee>`;",
    }
    const html = render(<MvProposalCard proposal={proposal} ddl={ddl} />)
    // Two SqlCodeBlock panels render — the CREATE VIEW wrapper and the GRANT.
    // (prism-react-renderer tokenizes the SQL, so assert on the panel count and
    // the intact keyword tokens rather than the full split identifier string.)
    expect(html.match(/>SQL</g)?.length).toBe(2)
    expect(html).toContain(">CREATE<")
    expect(html).toContain(">GRANT<")
  })
})

describe("IQ Scan advisory — EMPTY (MV-D15/D30, governance ladder)", () => {
  it("nothing-recurring: names what the scan read and reads as a clean result", () => {
    // Default variant (no skip reason, or NO_CANDIDATES with nothing found).
    const html = render(<MvAdvisoryEmpty skipReason="NO_CANDIDATES" measuresFound={0} />)
    expect(html).toContain("No recurring measures to propose yet")
    expect(html).toContain("example question SQL")
    expect(html).toContain("clean result")
    // The omission is load-bearing: query history is not in the corpus at all.
    expect(html).not.toContain("query history")
  })

  it("already-governed: measures were found but all are governed — a confidence empty", () => {
    // NO_CANDIDATES with measures_found > 0 is NOT barren: every recurring
    // measure is already governed. This is the "you're in good shape" statement,
    // not the "nothing recurring" copy — the two must not collapse into one.
    const html = render(<MvAdvisoryEmpty skipReason="NO_CANDIDATES" measuresFound={4} />)
    expect(html).toContain("already governed")
    expect(html).toContain("4 recurring measures")
    expect(html).not.toContain("No recurring measures to propose yet")
  })

  it("no-SQL: NO_PARSEABLE_SQL asks for example questions, not 'nothing recurring'", () => {
    const html = render(<MvAdvisoryEmpty skipReason="NO_PARSEABLE_SQL" measuresFound={0} />)
    expect(html).toContain("No SQL to scan yet")
    expect(html).toContain("example question")
    expect(html).not.toContain("No recurring measures to propose yet")
  })
})

describe("IQ Scan advisory — per-card justification (MV-D30)", () => {
  it("a bundle card names the measures it governs and the gain, from evidence", () => {
    const bundle: MvProposal = {
      ...proposal,
      measures: [
        {
          display_name: "total_booking_value",
          expr: "SUM(booking_value)",
          dedup_fingerprint: "m1",
          recurrence: 5,
          provenance_count: 5,
          benchmark_question_ids: ["q1", "q2"],
        },
        {
          display_name: "booking_count",
          expr: "COUNT(1)",
          dedup_fingerprint: "m2",
          recurrence: 4,
          provenance_count: 4,
          benchmark_question_ids: ["q2", "q3"],
        },
      ],
    }
    const html = render(<MvProposalCard proposal={bundle} />)
    expect(html).toContain("Governs 2 measures")
    expect(html).toContain("total_booking_value")
    expect(html).toContain("booking_count")
    // Gain line: 2 measures across 3 distinct curated queries (q1,q2,q3).
    expect(html).toContain("These 2 measures recur across 3 curated queries")
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
