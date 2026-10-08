// @vitest-environment jsdom
import { act } from "react"
import { createRoot, type Root } from "react-dom/client"
import { afterEach, beforeEach, expect, it, vi } from "vitest"
import type { GSOPermissionCheck, MvProbeResult, MvProposal } from "@/types"

const api = vi.hoisted(() => ({
  fetchSpaceMvProposals: vi.fn(),
  probeMvEntitlement: vi.fn(),
  triggerAutoOptimize: vi.fn(),
  getModels: vi.fn(async () => []),
}))
vi.mock("@/lib/api", async (importOriginal) => ({ ...(await importOriginal<typeof import("@/lib/api")>()), ...api }))

import { MV_PROBE_DEBOUNCE_MS, OptimizationConfig } from "./OptimizationConfig"

function proposal(suggestion_id: string, proposed_object: string, source_tables: string[]): MvProposal {
  return {
    suggestion_id, dedup_fingerprint: `fp_${suggestion_id}`, target_space_id: "space-1", run_id: null,
    candidate_type: "PROPOSE", confidence_score: 88, tier: "HIGH", uncapped_tier: "HIGH",
    tier_capped_by_coverage: false, proposed_object, score_components: null,
    evidence: { source_tables }, provenance_labels: null, provenance: null, alternatives: null,
    conflicts: null, requested_mode: null, effective_mode: null, decision: "approved",
    decided_by: "user@example.com", decided_at: "2026-08-23T09:14:22Z", suppressed_until: null,
    approved_for_rerun: true, created_at: null, updated_at: null,
  }
}

const PROPOSALS = [
  proposal("sug_a", "finance.sales.order_revenue", ["finance.sales.orders", "finance.sales.order_items"]),
  proposal("sug_b", "finance.sales.customer_ltv", ["finance.sales.customers"]),
  proposal("sug_c", "finance.marketing.campaign_roi", ["finance.marketing.campaigns"]),
]

function probe(probe_id: string): MvProbeResult {
  return {
    probe_id, checked_as: "user@example.com", auth_identity: "OBO", target: "finance.sales",
    checked_at: "2026-09-29T09:00:00Z", results: {}, privileges: [], capabilities: [],
    verdict: "SUFFICIENT", missing: [], remediation_sql: null, fallback_mode: "suggest_only",
    materialize_consented: false, consent_recorded: true, errors: [],
  }
}

const PERMISSIONS: GSOPermissionCheck = {
  sp_display_name: "gso-sp", sp_application_id: "app-1", sp_has_manage: true,
  schemas: [], can_start: true, errors: [],
}

let host: HTMLDivElement
let root: Root
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  vi.useFakeTimers()
  vi.clearAllMocks()
  api.fetchSpaceMvProposals.mockResolvedValue({ space_id: "space-1", proposals: PROPOSALS })
  api.triggerAutoOptimize.mockResolvedValue({ runId: "run-1", jobRunId: "j-1", jobUrl: null, status: "PENDING" })
  host = document.createElement("div")
  root = createRoot(host)
})
afterEach(async () => {
  try {
    await act(async () => root.unmount())
  } finally {
    vi.useRealTimers()
  }
})

async function mount() {
  await act(async () => root.render(
    <OptimizationConfig
      spaceId="space-1" onStarted={() => {}} hasActiveRun={false}
      permissions={PERMISSIONS} permsLoading={false} initialMv={{ mode: "create_and_attach" }}
    />,
  ))
}
async function mountSuggestOnly() {
  await act(async () => root.render(
    <OptimizationConfig
      spaceId="space-1" onStarted={() => {}} hasActiveRun={false}
      permissions={PERMISSIONS} permsLoading={false}
    />,
  ))
  const toggle = [...host.querySelectorAll("label")].find((l) => l.textContent?.includes("Suggest metric views"))!
  await act(async () => (toggle.querySelector("button, input") as HTMLElement).click())
}
const createRadioReason = () =>
  [...host.querySelectorAll("label")].find((l) => l.textContent?.includes("Create and attach, then optimize"))?.textContent ?? ""
const advance = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms) })
const text = () => host.textContent ?? ""
const startButton = () =>
  [...host.querySelectorAll("button")].find((b) => b.textContent?.includes("Start Optimization")) as HTMLButtonElement
async function toggle(object: string) {
  const label = [...host.querySelectorAll("label")].find((l) => l.textContent?.includes(object))!
  await act(async () => label.querySelector("input")!.click())
}

it("a selection spanning two schemas is not probed and blocks Start with the reason", async () => {
  await mount()
  await advance(MV_PROBE_DEBOUNCE_MS)
  expect(api.probeMvEntitlement).not.toHaveBeenCalled()
  expect(text()).toContain("2 schemas (finance.sales, finance.marketing)")
  expect(startButton().disabled).toBe(true)
  expect(startButton().title).toContain("2 schemas")
})

it("narrowing the selection re-probes its tables and starts with only its ids", async () => {
  let n = 0
  api.probeMvEntitlement.mockImplementation(async () => probe(`probe_${++n}`))
  await mount()
  await toggle("finance.marketing.campaign_roi")
  await advance(MV_PROBE_DEBOUNCE_MS)
  expect(api.probeMvEntitlement).toHaveBeenLastCalledWith({
    catalog: "finance", schema: "sales", space_id: "space-1",
    source_tables: ["finance.sales.customers", "finance.sales.order_items", "finance.sales.orders"],
  })
  await toggle("finance.sales.customer_ltv")
  await advance(MV_PROBE_DEBOUNCE_MS)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(2)
  expect(api.probeMvEntitlement).toHaveBeenLastCalledWith({
    catalog: "finance", schema: "sales", space_id: "space-1",
    source_tables: ["finance.sales.order_items", "finance.sales.orders"],
  })
  expect(text()).toContain("You can create metric views in finance.sales")
  expect(startButton().disabled).toBe(false)
  await act(async () => startButton().click())
  const req = api.triggerAutoOptimize.mock.calls[0][0]
  expect(req.mv_action_mode).toBe("create_and_attach")
  expect(req.mv_approved_suggestion_ids).toEqual(["sug_a"])
  expect(req.mv_consent.probe_id).toBe("probe_2")
})

it("unticking every view blocks Start and says to select one", async () => {
  api.probeMvEntitlement.mockImplementation(async () => probe("probe_1"))
  await mount()
  for (const object of ["finance.sales.order_revenue", "finance.sales.customer_ltv", "finance.marketing.campaign_roi"]) {
    await toggle(object)
  }
  expect(text()).toContain("Select at least one metric view to create, or choose Suggest only.")
  expect(startButton().disabled).toBe(true)
})

it("suggest-only says the default selection spans two schemas, and the radio gives that reason", async () => {
  await mountSuggestOnly()
  await advance(MV_PROBE_DEBOUNCE_MS)
  expect(api.probeMvEntitlement).not.toHaveBeenCalled()
  expect(text()).toContain("2 schemas (finance.sales, finance.marketing)")
  expect(text()).not.toContain("choose Suggest only")
  expect(createRadioReason()).toContain("Available once the selected metric views are in one schema.")
  expect(createRadioReason()).not.toContain("permission")
  expect(startButton().disabled).toBe(false)
})

it("suggest-only with nothing selected does not ask to select one", async () => {
  await mountSuggestOnly()
  for (const object of ["finance.sales.order_revenue", "finance.sales.customer_ltv", "finance.marketing.campaign_roi"]) {
    await toggle(object)
  }
  expect(text()).not.toContain("Select at least one metric view to create")
  expect(createRadioReason()).toContain("Available once you select a metric view to create.")
  expect(startButton().disabled).toBe(false)
})

it("a denied permission check keeps the permission reason on the radio", async () => {
  api.probeMvEntitlement.mockImplementation(async () => ({
    ...probe("probe_1"), verdict: "INSUFFICIENT", missing: ["CREATE TABLE on finance.sales"],
  }))
  await mountSuggestOnly()
  await toggle("finance.marketing.campaign_roi")
  await advance(MV_PROBE_DEBOUNCE_MS)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(1)
  expect(createRadioReason()).toContain("Available once you have permission to create metric views in the target schema.")
})

it("an answer for an earlier selection never unlocks Start for the current one", async () => {
  const pending: Array<(r: MvProbeResult) => void> = []
  api.probeMvEntitlement.mockImplementation(() => new Promise<MvProbeResult>((resolve) => pending.push(resolve)))
  await mount()
  await toggle("finance.marketing.campaign_roi")
  await advance(MV_PROBE_DEBOUNCE_MS)
  await toggle("finance.sales.customer_ltv")
  await advance(MV_PROBE_DEBOUNCE_MS)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(2)
  await act(async () => pending[0](probe("probe_1")))
  expect(startButton().disabled).toBe(true)
  expect(text()).not.toContain("You can create metric views")
  await act(async () => pending[1](probe("probe_2")))
  expect(startButton().disabled).toBe(false)
  await act(async () => startButton().click())
  expect(api.triggerAutoOptimize.mock.calls[0][0].mv_consent.probe_id).toBe("probe_2")
})

it("an earlier answer for the same selection never replaces the latest request's", async () => {
  const pending: Array<(r: MvProbeResult) => void> = []
  api.probeMvEntitlement.mockImplementation(() => new Promise<MvProbeResult>((resolve) => pending.push(resolve)))
  await mount()
  await toggle("finance.marketing.campaign_roi")
  await advance(MV_PROBE_DEBOUNCE_MS)
  await toggle("finance.sales.customer_ltv")
  await advance(MV_PROBE_DEBOUNCE_MS)
  await toggle("finance.sales.customer_ltv")
  await advance(MV_PROBE_DEBOUNCE_MS)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(3)
  await act(async () => pending[0](probe("probe_1")))
  expect(startButton().disabled).toBe(true)
  await act(async () => pending[2](probe("probe_3")))
  expect(startButton().disabled).toBe(false)
  await act(async () => startButton().click())
  expect(api.triggerAutoOptimize.mock.calls[0][0].mv_consent.probe_id).toBe("probe_3")
})

it("turning the section off and on during a probe keeps that probe's answer", async () => {
  const pending: Array<(r: MvProbeResult) => void> = []
  api.probeMvEntitlement.mockImplementation(() => new Promise<MvProbeResult>((resolve) => pending.push(resolve)))
  await mount()
  await toggle("finance.marketing.campaign_roi")
  await advance(MV_PROBE_DEBOUNCE_MS)
  const section = () =>
    [...host.querySelectorAll("label")].find((l) => l.textContent?.includes("Suggest metric views"))!
      .querySelector("button, input") as HTMLElement
  await act(async () => section().click())
  await act(async () => section().click())
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(1)
  await act(async () => pending[0](probe("probe_1")))
  expect(startButton().disabled).toBe(false)
  await act(async () => startButton().click())
  expect(api.triggerAutoOptimize.mock.calls[0][0].mv_consent.probe_id).toBe("probe_1")
})
