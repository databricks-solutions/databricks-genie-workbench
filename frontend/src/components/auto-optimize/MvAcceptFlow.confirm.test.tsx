// @vitest-environment jsdom
import { act } from "react"
import { createRoot, type Root } from "react-dom/client"
import { afterEach, beforeEach, expect, it, vi } from "vitest"
import type { MvCreateAtApprovalResponse, MvProbeResult, MvProposal } from "@/types"

const api = vi.hoisted(() => ({
  probeMvEntitlement: vi.fn(),
  createMvAtApproval: vi.fn(),
  decideMvProposal: vi.fn(),
}))
vi.mock("@/lib/api", async (importOriginal) => ({ ...(await importOriginal<typeof import("@/lib/api")>()), ...api }))

import { MvAcceptFlow } from "./MvAcceptFlow"

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
    grant_sql: null, reason: null, workspace_host: null, ...over,
  }
}

const CARD_GRANT = "GRANT SELECT ON VIEW finance.sales.order_revenue TO `gso-sp`"

let host: HTMLDivElement
let root: Root
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  // SqlCodeBlock's useTheme reads localStorage and matchMedia, and the jsdom this
  // suite runs on under Node provides neither, so both are stubbed.
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

async function createAndConfirm(grantSql: string | null) {
  await act(async () => root.render(<MvAcceptFlow proposal={PROPOSAL} grantSql={grantSql} />))
  await act(async () => button("Create this metric view").click())
  await act(async () => button("Create and attach").click())
}

it("neither the click probe nor the confirm re-probe sends materialize_consented", async () => {
  api.createMvAtApproval.mockResolvedValue(created({ grant_sql: CARD_GRANT }))
  await createAndConfirm(null)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(2)
  const [first] = api.probeMvEntitlement.mock.calls[0]
  const [second] = api.probeMvEntitlement.mock.calls[1]
  expect(first).not.toHaveProperty("materialize_consented")
  expect(second).not.toHaveProperty("materialize_consented")
  expect(second).toEqual({
    catalog: "finance", schema: "sales", space_id: "space-1", source_tables: ["finance.sales.orders"],
  })
  expect(api.createMvAtApproval).toHaveBeenCalledWith("space-1", { suggestion_id: "sug1", probe_id: "probe_2" })
})

it("a USER_CREATED result names the owner and offers no GRANT, not even the card's", async () => {
  api.createMvAtApproval.mockResolvedValue(
    created({ provenance: "USER_CREATED", already_existed: true, owner: "other@example.com", grant_sql: null }),
  )
  await createAndConfirm(CARD_GRANT)
  expect(text()).toContain("Attached to your Agent (view already existed)")
  expect(text()).toContain("Owned by other@example.com, so only they can change it or grant the optimizer access to it.")
  expect(text()).toContain("ask other@example.com to grant it SELECT")
  expect(text()).not.toContain("GRANT")
  expect(host.querySelector('[title="Copy to clipboard"]')).toBeNull()
})

it("an OBO_CREATED result still falls back to the card's GRANT", async () => {
  api.createMvAtApproval.mockResolvedValue(created({ grant_sql: null }))
  await createAndConfirm(CARD_GRANT)
  expect(text()).toContain("Created & attached to your Agent")
  expect(text()).toContain("One step left:")
  expect(text()).not.toContain("Owned by")
  expect(host.querySelector('[title="Copy to clipboard"]')).not.toBeNull()
  expect(text()).toContain("GRANT")
})
