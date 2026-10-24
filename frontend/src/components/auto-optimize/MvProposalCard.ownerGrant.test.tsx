// @vitest-environment jsdom
/**
 * MV-D120: once the card's own accept flow returns a create result whose view
 * someone else owns (USER_CREATED), the card's detail offers no GRANT either.
 * Run through BOTH surfaces that pair the card with the flow, so the fix holds
 * on each by construction.
 */
import { act } from "react"
import { createRoot, type Root } from "react-dom/client"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import type { MvCreateAtApprovalResponse, MvDdlArtifact, MvProbeResult, MvProposal } from "@/types"

const api = vi.hoisted(() => ({
  probeMvEntitlement: vi.fn(),
  createMvAtApproval: vi.fn(),
  decideMvProposal: vi.fn(),
}))
vi.mock("@/lib/api", async (importOriginal) => ({ ...(await importOriginal<typeof import("@/lib/api")>()), ...api }))

import { ScanProposalCard } from "./MvIqScanAdvisorySection"
import { MvSuggestOnlyPanel } from "./MvSuggestOnlyPanel"

const PROPOSAL: MvProposal = {
  suggestion_id: "sug1", dedup_fingerprint: "fp1", target_space_id: "space-1", run_id: null,
  candidate_type: "NEW_METRIC_VIEW", confidence_score: 82, tier: "HIGH", uncapped_tier: "HIGH",
  tier_capped_by_coverage: false, proposed_object: "finance.sales.order_revenue",
  measures: [
    { display_name: "revenue", expr: "SUM(x)", dedup_fingerprint: "m1", recurrence: 5, provenance_count: 5, benchmark_question_ids: ["q1"] },
  ],
  checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" }, score_components: null,
  evidence: { source_tables: ["finance.sales.orders"] }, provenance_labels: null, provenance: null,
  alternatives: null, conflicts: null, requested_mode: null, effective_mode: null, decision: null,
  decided_by: null, decided_at: null, suppressed_until: null, approved_for_rerun: false,
  created_at: null, updated_at: null,
}

const CARD_GRANTEE = "card-sp"
const DDL: MvDdlArtifact = {
  suggestion_id: "sug1", dedup_fingerprint: "fp1", proposed_object: "finance.sales.order_revenue",
  join_strategy: null, source_tables: ["finance.sales.orders"], yaml_text: "version: \"1.1\"",
  ddl: "CREATE VIEW finance.sales.order_revenue WITH METRICS LANGUAGE YAML AS $$ x $$",
  validation: null,
  grant_sql: `GRANT SELECT ON VIEW finance.sales.order_revenue TO \`${CARD_GRANTEE}\``,
}

function probe(probe_id: string): MvProbeResult {
  return {
    probe_id, checked_as: "user@example.com", auth_identity: "OBO", target: "finance.sales",
    checked_at: "2026-09-29T09:00:00Z", results: {}, privileges: [], capabilities: [],
    verdict: "SUFFICIENT", missing: [], remediation_sql: null, fallback_mode: "suggest_only",
    materialize_consented: false, consent_recorded: true, errors: [], audience_grantees: [],
  }
}

function created(over: Partial<MvCreateAtApprovalResponse>): MvCreateAtApprovalResponse {
  return {
    created: true, degraded: false, attached: true, already_existed: false,
    full_name: "finance.sales.order_revenue", run_id: null, suggestion_id: "sug1",
    provenance: "OBO_CREATED", owner: null, verdict: "SUFFICIENT", remediation_sql: null,
    grant_sql: "GRANT SELECT ON VIEW finance.sales.order_revenue TO `response-sp`",
    reason: null, workspace_host: null, ...over,
  }
}

let host: HTMLDivElement
let root: Root
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  // SqlCodeBlock's useTheme reads both; the jsdom runtime here exposes neither.
  vi.stubGlobal("localStorage", { getItem: () => null, setItem: () => {}, removeItem: () => {} })
  vi.stubGlobal("matchMedia", () => ({ matches: false, addEventListener: () => {}, removeEventListener: () => {} }))
  vi.clearAllMocks()
  let n = 0
  api.probeMvEntitlement.mockImplementation(async () => probe(`probe_${++n}`))
  host = document.createElement("div")
  root = createRoot(host)
})
afterEach(async () => {
  await act(async () => root.unmount())
  vi.unstubAllGlobals()
})

const button = (label: string) =>
  [...host.querySelectorAll("button")].find((b) => b.textContent?.includes(label)) as HTMLButtonElement
const text = () => host.textContent ?? ""

const SURFACES = {
  "IQ scan (ScanProposalCard)": () => (
    <ScanProposalCard proposal={PROPOSAL} ddl={DDL} onClaim={() => {}} defaultExpanded={false} />
  ),
  "run output (MvSuggestOnlyPanel)": () => (
    <MvSuggestOnlyPanel
      runId="run-1"
      proposals={[PROPOSAL]}
      ddlBySuggestion={{ sug1: DDL }}
      currentIdentifiers={[]}
      onRerun={() => {}}
    />
  ),
}

async function expandDetail() {
  const show = button("Show detail")
  if (show) await act(async () => show.click())
  // The DDL renders only in the open detail: the control that it is expanded.
  expect(text()).toContain("WITH METRICS")
}

async function createAndConfirm() {
  await act(async () => button("Create this metric view").click())
  await act(async () => button("Create and attach").click())
}

describe.each(Object.entries(SURFACES))("the card detail's GRANT on the %s surface", (_name, Surface) => {
  it("is shown before any create", async () => {
    await act(async () => root.render(<Surface />))
    await expandDetail()
    expect(text()).toContain(CARD_GRANTEE)
  })

  it("is hidden once the flow's create returns USER_CREATED", async () => {
    api.createMvAtApproval.mockResolvedValue(
      created({ provenance: "USER_CREATED", already_existed: true, owner: "other@example.com", grant_sql: null }),
    )
    await act(async () => root.render(<Surface />))
    await createAndConfirm()
    expect(text()).toContain("Owned by other@example.com")
    await expandDetail()
    expect(text()).not.toContain(CARD_GRANTEE)
    expect(text()).not.toContain("GRANT")
  })

  it("is still shown once the flow's create returns OBO_CREATED", async () => {
    api.createMvAtApproval.mockResolvedValue(created({}))
    await act(async () => root.render(<Surface />))
    await createAndConfirm()
    expect(text()).toContain("Created & attached to your Agent")
    await expandDetail()
    expect(text()).toContain(CARD_GRANTEE)
  })

  // A degraded result offers the approve-for-later path; a refusal shows its reason.
  it.each([
    ["degraded", { degraded: true, reason: "Your access changed before the create." }, "Approve for later"],
    ["refused", { degraded: false, reason: "The create didn't complete and the view wasn't found." },
      "The create didn't complete and the view wasn't found."],
  ])("is still shown once the flow's create is %s", async (_kind, over, landed) => {
    api.createMvAtApproval.mockResolvedValue(
      created({ created: false, attached: false, provenance: null, grant_sql: null, ...over }),
    )
    await act(async () => root.render(<Surface />))
    await createAndConfirm()
    expect(text()).toContain(landed)
    await expandDetail()
    expect(text()).toContain(CARD_GRANTEE)
  })
})

const SIBLING_GRANTEE = "sibling-sp"
const SIBLING: MvProposal = {
  ...PROPOSAL, suggestion_id: "sug2", dedup_fingerprint: "fp2", proposed_object: "finance.sales.customer_ltv",
}
const SIBLING_DDL: MvDdlArtifact = {
  ...DDL, suggestion_id: "sug2", dedup_fingerprint: "fp2", proposed_object: "finance.sales.customer_ltv",
  ddl: "CREATE VIEW finance.sales.customer_ltv WITH METRICS LANGUAGE YAML AS $$ y $$",
  grant_sql: `GRANT SELECT ON VIEW finance.sales.customer_ltv TO \`${SIBLING_GRANTEE}\``,
}

const TWO_CARD_SURFACES = {
  "IQ scan (ScanProposalCard)": () => (
    <>
      <ScanProposalCard proposal={PROPOSAL} ddl={DDL} onClaim={() => {}} defaultExpanded={false} />
      <ScanProposalCard proposal={SIBLING} ddl={SIBLING_DDL} onClaim={() => {}} defaultExpanded={false} />
    </>
  ),
  "run output (MvSuggestOnlyPanel)": () => (
    <MvSuggestOnlyPanel
      runId="run-1"
      proposals={[PROPOSAL, SIBLING]}
      ddlBySuggestion={{ sug1: DDL, sug2: SIBLING_DDL }}
      currentIdentifiers={[]}
      onRerun={() => {}}
    />
  ),
}

describe.each(Object.entries(TWO_CARD_SURFACES))("a sibling card's GRANT on the %s surface", (_name, Surface) => {
  it("survives when the other card's create returns USER_CREATED", async () => {
    api.createMvAtApproval.mockResolvedValue(
      created({ provenance: "USER_CREATED", already_existed: true, owner: "other@example.com", grant_sql: null }),
    )
    await act(async () => root.render(<Surface />))
    // The first card's flow is the first "Create this metric view" on the page.
    await createAndConfirm()
    expect(api.createMvAtApproval.mock.calls[0][1].suggestion_id).toBe("sug1")
    expect(text()).toContain("Owned by other@example.com")
    for (const show of [...host.querySelectorAll("button")].filter((b) => b.textContent?.includes("Show detail"))) {
      await act(async () => show.click())
    }
    expect(text()).toContain("finance.sales.customer_ltv WITH METRICS")
    expect(text()).toContain(SIBLING_GRANTEE)
    expect(text()).not.toContain(CARD_GRANTEE)
  })
})
