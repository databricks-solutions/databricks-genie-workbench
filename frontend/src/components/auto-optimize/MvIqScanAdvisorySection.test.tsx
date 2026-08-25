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
  ScanProgress,
} from "./MvIqScanAdvisorySection"
import { MvProposalCard } from "./MvProposalCard"
import type { MvProposal, MvRegisterResponse, MvDdlArtifact } from "@/types"

const render = (el: React.ReactElement) => renderToStaticMarkup(el)

// The four honest scan stages, in order — mirrors SCAN_STAGES in the component.
// Passing all four to ScanProgress means the LAST stage is active (idx 3).
const SCAN_STAGES_FIXTURE = [
  "reading curated SQL",
  "scanning for recurring measures",
  "scoring candidates (embeddings + usage signals)",
  "rendering DDL",
]

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
  measures: [],
  checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" },
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
    const html = render(<MvProposalCard proposal={proposal} ddl={ddl} defaultExpanded />)
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

describe("MvProposalCard — uniform skeleton + explicit expand/collapse (15.6 finding 2)", () => {
  const bundle: MvProposal = {
    ...proposal,
    measures: [
      { display_name: "total_booking_value", expr: "SUM(booking_value)", dedup_fingerprint: "m1", recurrence: 5, provenance_count: 5, benchmark_question_ids: ["sql_snippet:a", "q_bare"] },
    ],
  }

  it("collapsed by default (defaultExpanded=false): skeleton shows, detail hidden", () => {
    const html = render(<MvProposalCard proposal={bundle} defaultExpanded={false} />)
    // Skeleton is always visible.
    expect(html).toContain("finance.sales.order_revenue")
    expect(html).toContain("Governs 1 measure")
    expect(html).toContain("Show detail")
    // Detail (the measure expr) is NOT rendered while collapsed.
    expect(html).not.toContain("SUM(booking_value)")
  })

  it("expanded: renders the detail and the evidence chips, hides raw ids", () => {
    const html = render(<MvProposalCard proposal={bundle} defaultExpanded />)
    expect(html).toContain("Hide detail")
    expect(html).toContain("SUM(booking_value)")
    // Evidence for humans: counts + labels, never the raw prefixed id.
    expect(html).toContain("Evidence")
    expect(html).toContain("curated snippet")
    expect(html).toContain("details") // the disclosure control for raw ids
    // Raw ids live behind the closed disclosure — not in the default markup.
    expect(html).not.toContain("sql_snippet:a")
  })

  it("Recommended badge renders with its one-line reason", () => {
    const html = render(
      <MvProposalCard proposal={bundle} recommended recommendedReason="Strongest candidate — governs 1 measure." />,
    )
    expect(html).toContain("Recommended")
    expect(html).toContain("Strongest candidate")
  })

  it("the facts row leads with the proven gates, and the card shows NO percent and NO 'confidence' (MV-D35)", () => {
    const poor: MvProposal = {
      ...bundle,
      confidence_score: 34,
      tier: "LOW",
      checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" },
      score_components: { statuses: { L: "UNAVAILABLE", Y: "COMPUTED", S: "COMPUTED", D: "UNAVAILABLE" } },
    }
    const html = render(<MvProposalCard proposal={poor} defaultExpanded={false} />)
    // Facts lead.
    expect(html).toContain("validated")
    expect(html).toContain("executable")
    expect(html).toContain("no overlap with existing metric views")
    // Evidence basis as a human sentence — no "confidence" word, no percent.
    expect(html).toContain("Based on curated SQL only")
    expect(html).not.toMatch(/\d+%/)
    expect(html.toLowerCase()).not.toContain("confidence")
    // A cold, evidence-poor proposal shows no cross-surface growth line.
    expect(html).not.toContain("Evidence grew beyond the initial scan")
  })

  it("surfaces cross-surface enrichment when a GSO run added signals (15.7 / MV-D32(3))", () => {
    const enriched: MvProposal = {
      ...bundle,
      score_components: { statuses: { L: "COMPUTED", Y: "COMPUTED", S: "COMPUTED", D: "COMPUTED" } },
      evidence: { query_history_statement_ids: ["s1"] },
    }
    const html = render(<MvProposalCard proposal={enriched} defaultExpanded={false} />)
    expect(html).toContain("Evidence grew beyond the initial scan")
    expect(html).toContain("usage signals")
    expect(html).toContain("lineage")
  })

  it("a coverage-capped-strong proposal shows facts + evidence basis — never a bare LOW, a percent, or 'confidence' (MV-D35 supersedes 15.7b badge)", () => {
    // Fresh-space case: strong Y, L and D UNAVAILABLE, so MV-D15 capped the
    // served tier to LOW while the score-only tier stayed HIGH. Under MV-D35 the
    // "Strong (evidence-limited)" badge is retired from the card face; the facts
    // row and the honest evidence basis carry it instead.
    const cappedStrong: MvProposal = {
      ...bundle,
      confidence_score: 82,
      tier: "LOW",
      uncapped_tier: "HIGH",
      tier_capped_by_coverage: true,
      checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" },
      score_components: { statuses: { L: "UNAVAILABLE", Y: "COMPUTED", S: "COMPUTED", D: "UNAVAILABLE" } },
    }
    const html = render(<MvProposalCard proposal={cappedStrong} defaultExpanded={false} />)
    expect(html).toContain("validated")
    expect(html).toContain("Based on curated SQL only")
    // No percent, no "confidence", and never a bare LOW badge on the card face.
    expect(html).not.toMatch(/\d+%/)
    expect(html.toLowerCase()).not.toContain("confidence")
    expect(html).not.toContain(">LOW<")
    expect(html).not.toContain("Strong (evidence-limited)")
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
    const html = render(<MvProposalCard proposal={bundle} defaultExpanded />)
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

describe("IQ Scan advisory — staged progress (MV-D31, finding 2)", () => {
  it("shows all four honest stages, with SCORING labelled by what it waits on", () => {
    // Not a bare spinner: the four stages render as a checklist, and the long
    // SCORING stage names what it is waiting on (embeddings + usage signals) so
    // an honest label carries the multi-minute wait.
    const html = render(<ScanProgress stages={["reading curated SQL"]} lastDuration={null} />)
    expect(html).toContain("reading curated SQL")
    expect(html).toContain("scanning for recurring measures")
    expect(html).toContain("scoring candidates (embeddings + usage signals)")
    expect(html).toContain("rendering DDL")
    // No fabricated estimate when there's no prior duration — honest and vague.
    expect(html).toContain("a few minutes")
  })

  it("frames the wait with the last scan's REAL duration when known (note 3)", () => {
    // Duration comes from the previous scan's measured wall time, never a made-up
    // "~30–60s". The staged progress is what makes the wait tolerable.
    const html = render(<ScanProgress stages={["scoring candidates (embeddings + usage signals)"]} lastDuration="4m 12s" />)
    expect(html).toContain("The last scan took 4m 12s")
    // Fix #4 (honest-estimate rule): one sample is not a distribution — the
    // projection is dropped until there are >=3 samples to range over.
    expect(html).not.toContain("usually takes about that long")
  })

  it("renders a weighted progress bar that advances with the active stage (finding 8)", () => {
    // On the first stage the bar is a small positive fraction (active-half of one
    // of four equal segments = ~13%); by the last it is well past halfway.
    const early = render(<ScanProgress stages={["reading curated SQL"]} lastDuration={null} />)
    expect(early).toContain('role="progressbar"')
    expect(early).toContain('aria-valuenow="13"')
    const late = render(<ScanProgress stages={SCAN_STAGES_FIXTURE} lastDuration={null} />)
    // Last of four stages: 3 done + half of the fourth = 87.5% → 88.
    expect(late).toContain('aria-valuenow="88"')
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
