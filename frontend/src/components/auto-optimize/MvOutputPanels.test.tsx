/**
 * Production tests for the metric view output panels (Prompt 13), pinning the
 * copy and per-state structure that the graduated mockup frames 4–5 used to
 * guard. renderToStaticMarkup + node env — the repo's frontend test pattern.
 *
 * These render the PRESENTATIONAL panels directly with props (never the fetching
 * container), which is exactly the MV-D23 contract: nothing keys on run_id, so a
 * space-scoped source can feed the same components at Prompt 13.5.
 */
import { describe, expect, it } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import { MvSuggestOnlyPanel } from "./MvSuggestOnlyPanel"
import { MvCreateAttachPanel } from "./MvCreateAttachPanel"
import { MvSpaceConfigDiff } from "./MvSpaceConfigDiff"
import { MvProposalCard } from "./MvProposalCard"
import { joinStrategyLabel, LIFT_NOT_MEASURED } from "./mvFormat"
import type { MvCreatedObject, MvDdlArtifact, MvLiftReport, MvProposal } from "@/types"

const render = (el: React.ReactElement) => renderToStaticMarkup(el)

const proposal: MvProposal = {
  suggestion_id: "sug1",
  dedup_fingerprint: "fp1",
  target_space_id: "space-1",
  run_id: "run-1",
  candidate_type: "NEW_METRIC_VIEW",
  confidence_score: 82,
  tier: "HIGH",
  uncapped_tier: "HIGH",
  tier_capped_by_coverage: false,
  proposed_object: "finance.sales.order_revenue",
  measures: [],
  checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" },
  score_components: null,
  evidence: {
    recurrence_count: 6,
    benchmark_question_ids: ["bq_0007", "bq_0019"],
    source_tables: ["finance.sales.orders"],
  },
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

const ddl: MvDdlArtifact = {
  suggestion_id: "sug1",
  dedup_fingerprint: "fp1",
  proposed_object: "finance.sales.order_revenue",
  join_strategy: "subquery_source",
  yaml_text: "version: 0.1\n",
  ddl: "CREATE VIEW finance.sales.order_revenue WITH METRICS LANGUAGE YAML AS $$ ... $$",
  validation: { ok: true },
  grant_sql: "GRANT SELECT ON VIEW finance.sales.order_revenue TO `analysts`;",
}

const lift: MvLiftReport = {
  delta_affected: -0.07,
  delta_suite: -0.03,
  regressed_question_ids: ["bq_0007"],
  needs_review_count: 3,
  pre_eval_run_id: "eval_a1",
  post_eval_run_id: "eval_b2",
  question_subset: ["bq_0007", "bq_0019"],
  pre_accuracy_affected: 0.78,
  post_accuracy_affected: 0.71,
  pre_accuracy_suite: 0.8,
  post_accuracy_suite: 0.77,
  needs_review_question_ids: ["bq_0022", "bq_0033", "bq_0041"],
  graded_affected_count: 12,
  graded_suite_count: 40,
}

const detached: MvCreatedObject = {
  run_id: "run-1",
  suggestion_id: "sug1",
  full_name: "finance.sales.order_revenue",
  created_by: "analyst@example.com",
  provenance: "OBO_CREATED",
  status: "DETACHED",
  attach_patch_id: null,
  baseline_eval_run_id: "eval_a1",
  post_attach_eval_run_id: "eval_b2",
  on_regression_action: "DETACH_ONLY_NEVER_DROP",
  created_at: null,
  lift_report: lift,
}

describe("suggest-only panel (frame 4)", () => {
  const html = render(
    <MvSuggestOnlyPanel
      runId="run-1"
      proposals={[proposal]}
      ddl={ddl}
      currentIdentifiers={["finance.sales.customer_ltv"]}
      onRerun={() => {}}
    />,
  )
  it("carries the verbatim lift-not-measured label and the shared accept flow's actions (MV-D34)", () => {
    expect(html).toContain(LIFT_NOT_MEASURED)
    expect(html).toContain("was not created or attached during this run")
    // The run-output surface now renders the SAME accept flow the IQ surface
    // does: the primary [Create this metric view] plus [Review in run setup].
    expect(html).toContain("Create this metric view")
    expect(html).toContain("Review in run setup")
  })
  it("renders the proposed object, its DDL, and the space-config diff", () => {
    expect(html).toContain("finance.sales.order_revenue")
    // Prism tokenizes the DDL, so assert single-token keywords rather than a
    // phrase that spans token boundaries ("WITH METRICS").
    expect(html).toContain("CREATE")
    expect(html).toContain("GRANT")
    // The diff synthesizes the proposed side client-side (§7.5).
    expect(html).toContain("With this metric view attached")
    expect(html).toContain("none created")
  })
})

describe("create-and-attach panel (frame 5) — DETACHED regression", () => {
  const html = render(<MvCreateAttachPanel obj={detached} ddl={ddl} catalogUrl={null} />)
  it("shows DETACHED, the drop flow, both accuracies, and needs-review separately", () => {
    expect(html).toContain("DETACHED")
    expect(html).toContain("Drop view")
    expect(html).toContain("Baseline accuracy")
    expect(html).toContain("78%")
    expect(html).toContain("Post-attach accuracy")
    expect(html).toContain("71%")
    expect(html).toContain("Needs review")
    expect(html).toContain("counted separately")
    expect(html).toContain("eval_a1")
    expect(html).toContain("eval_b2")
  })
  it("names provenance OBO_CREATED and shows the downgrade/regression banner", () => {
    expect(html).toContain("OBO_CREATED")
    expect(html).toContain("reverted to the pre-attach snapshot")
  })
  it("CUTS tables_freed and never shows the unreachable nested join badge", () => {
    expect(html).not.toContain("Tables freed")
    expect(html).not.toContain("Nested")
    expect(html).toContain("Subquery source")
  })
})

describe("create-and-attach panel — healthy ATTACHED object", () => {
  const attached: MvCreatedObject = { ...detached, status: "ATTACHED" }
  const html = render(<MvCreateAttachPanel obj={attached} ddl={ddl} catalogUrl={null} />)
  it("offers no drop and no regression banner while attached", () => {
    expect(html).toContain("ATTACHED")
    expect(html).not.toContain("Drop view")
    expect(html).not.toContain("reverted to the pre-attach snapshot")
  })
})

describe("create-and-attach panel — USER_CREATED (bring-your-own, MV-D24 invariant 1)", () => {
  // Same DETACHED status that shows Drop for an OBO_CREATED view: proving the
  // affordance is gated on provenance, not status. The server refuses to drop a
  // USER_CREATED view, so the panel must not render the button (Prompt 14.1).
  const userCreated: MvCreatedObject = {
    ...detached,
    provenance: "USER_CREATED",
    created_by: "prashanth@example.com",
  }
  const html = render(<MvCreateAttachPanel obj={userCreated} ddl={ddl} catalogUrl={null} />)
  it("renders the USER_CREATED badge and the frame-8b vocabulary", () => {
    expect(html).toContain("USER_CREATED")
    expect(html).toContain("dropping this one stays in your hands")
  })
  it("NEVER renders a Drop view action, even while DETACHED", () => {
    expect(html).toContain("DETACHED")
    expect(html).not.toContain("Drop view")
  })
})

describe("create-and-attach panel — Catalog Explorer link", () => {
  it("links the object name when a workspace origin is known", () => {
    const html = render(
      <MvCreateAttachPanel
        obj={detached}
        ddl={ddl}
        catalogUrl="https://ws.example.com/explore/data/finance/sales/order_revenue"
      />,
    )
    expect(html).toContain('href="https://ws.example.com/explore/data/finance/sales/order_revenue"')
    expect(html).toContain("opens in Catalog Explorer")
  })
})

describe("space-config diff", () => {
  it("adds the proposed identifier and omits it when already present", () => {
    const added = render(
      <MvSpaceConfigDiff
        currentIdentifiers={["finance.sales.customer_ltv"]}
        proposedObject="finance.sales.order_revenue"
      />,
    )
    expect(added).toContain("finance.sales.order_revenue")
    expect(added).toContain("Current metric views")

    const noop = render(
      <MvSpaceConfigDiff
        currentIdentifiers={["finance.sales.order_revenue"]}
        proposedObject="finance.sales.order_revenue"
      />,
    )
    // Already present ⇒ nothing to add ⇒ no diff rendered.
    expect(noop).toBe("")
  })
})

describe("proposal card — reachable join strategies only (MV-D14/D15)", () => {
  it("labels denormalized and subquery-source, and renders no badge for nested", () => {
    expect(joinStrategyLabel("denormalized")).toBe("Denormalized")
    expect(joinStrategyLabel("subquery_source")).toBe("Subquery source")
    expect(joinStrategyLabel("nested")).toBeNull()

    const nestedDdl: MvDdlArtifact = { ...ddl, join_strategy: "nested" }
    const html = render(<MvProposalCard proposal={proposal} ddl={nestedDdl} />)
    expect(html).not.toContain("Nested")
  })
})
