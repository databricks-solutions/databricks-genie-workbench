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

// One schema, so the selection the proposals load with is probeable at once.
const PROPOSALS = [
  proposal("sug_a", "finance.sales.order_revenue", ["finance.sales.orders", "finance.sales.order_items"]),
  proposal("sug_b", "finance.sales.customer_ltv", ["finance.sales.customers"]),
  proposal("sug_c", "finance.sales.region_margin", ["finance.sales.regions"]),
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
let mounted: boolean
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  vi.useFakeTimers()
  vi.clearAllMocks()
  api.fetchSpaceMvProposals.mockResolvedValue({ space_id: "space-1", proposals: PROPOSALS })
  api.triggerAutoOptimize.mockResolvedValue({ runId: "run-1", jobRunId: "j-1", jobUrl: null, status: "PENDING" })
  host = document.createElement("div")
  root = createRoot(host)
  mounted = false
})
afterEach(async () => {
  try {
    if (mounted) await unmount()
  } finally {
    vi.useRealTimers()
  }
})

async function mount(spaceId = "space-1") {
  await act(async () => root.render(
    <OptimizationConfig
      spaceId={spaceId} onStarted={() => {}} hasActiveRun={false}
      permissions={PERMISSIONS} permsLoading={false} initialMv={{ mode: "create_and_attach" }}
    />,
  ))
  mounted = true
}
async function unmount() {
  await act(async () => root.unmount())
  mounted = false
}
const advance = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms) })
const startButton = () =>
  [...host.querySelectorAll("button")].find((b) => b.textContent?.includes("Start Optimization")) as HTMLButtonElement
async function toggle(object: string) {
  const label = [...host.querySelectorAll("label")].find((l) => l.textContent?.includes(object))!
  await act(async () => label.querySelector("input")!.click())
}
async function toggleSection() {
  const label = [...host.querySelectorAll("label")].find((l) => l.textContent?.includes("Suggest metric views"))!
  await act(async () => (label.querySelector("button, input") as HTMLElement).click())
}

it("one selection probes once, after the delay", async () => {
  api.probeMvEntitlement.mockImplementation(async () => probe("probe_1"))
  await mount()
  expect(api.fetchSpaceMvProposals).toHaveBeenCalledTimes(1)
  expect(api.probeMvEntitlement).not.toHaveBeenCalled()
  expect(startButton().disabled).toBe(true)
  await advance(MV_PROBE_DEBOUNCE_MS - 1)
  expect(api.probeMvEntitlement).not.toHaveBeenCalled()
  await advance(1)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(1)
  expect(api.probeMvEntitlement).toHaveBeenLastCalledWith({
    catalog: "finance", schema: "sales", space_id: "space-1",
    source_tables: ["finance.sales.customers", "finance.sales.order_items", "finance.sales.orders", "finance.sales.regions"],
  })
  await advance(MV_PROBE_DEBOUNCE_MS * 5)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(1)
})

it("three quick toggles probe once, for the last selection", async () => {
  api.probeMvEntitlement.mockImplementation(async () => probe("probe_1"))
  await mount()
  await advance(100)
  await toggle("finance.sales.region_margin")
  await advance(100)
  await toggle("finance.sales.customer_ltv")
  await advance(100)
  await toggle("finance.sales.region_margin")
  expect(api.probeMvEntitlement).not.toHaveBeenCalled()
  await advance(MV_PROBE_DEBOUNCE_MS)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(1)
  expect(api.probeMvEntitlement).toHaveBeenLastCalledWith({
    catalog: "finance", schema: "sales", space_id: "space-1",
    source_tables: ["finance.sales.order_items", "finance.sales.orders", "finance.sales.regions"],
  })
  await advance(MV_PROBE_DEBOUNCE_MS * 5)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(1)
})

it("unmounting before the delay never probes", async () => {
  api.probeMvEntitlement.mockImplementation(async () => probe("probe_1"))
  await mount()
  await advance(MV_PROBE_DEBOUNCE_MS / 2)
  await unmount()
  await advance(MV_PROBE_DEBOUNCE_MS * 5)
  expect(api.probeMvEntitlement).not.toHaveBeenCalled()
})

it("turning the section off before the delay cancels the probe, and turning it on probes again", async () => {
  api.probeMvEntitlement.mockImplementation(async () => probe("probe_1"))
  await mount()
  await advance(MV_PROBE_DEBOUNCE_MS / 2)
  await toggleSection()
  await advance(MV_PROBE_DEBOUNCE_MS * 5)
  expect(api.probeMvEntitlement).not.toHaveBeenCalled()
  await toggleSection()
  expect(startButton().disabled).toBe(true)
  await advance(MV_PROBE_DEBOUNCE_MS - 1)
  expect(api.probeMvEntitlement).not.toHaveBeenCalled()
  await advance(1)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(1)
  expect(startButton().disabled).toBe(false)
})

it("unticking every view before the delay cancels the probe", async () => {
  api.probeMvEntitlement.mockImplementation(async () => probe("probe_1"))
  await mount()
  await advance(MV_PROBE_DEBOUNCE_MS / 2)
  for (const object of ["finance.sales.order_revenue", "finance.sales.customer_ltv", "finance.sales.region_margin"]) {
    await toggle(object)
  }
  await advance(MV_PROBE_DEBOUNCE_MS * 5)
  expect(api.probeMvEntitlement).not.toHaveBeenCalled()
  await toggle("finance.sales.customer_ltv")
  await advance(MV_PROBE_DEBOUNCE_MS)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(1)
  expect(api.probeMvEntitlement).toHaveBeenLastCalledWith({
    catalog: "finance", schema: "sales", space_id: "space-1", source_tables: ["finance.sales.customers"],
  })
  expect(startButton().disabled).toBe(false)
})

it("a space change before the delay probes the new space only", async () => {
  api.probeMvEntitlement.mockImplementation(async () => probe("probe_1"))
  await mount("space-1")
  await advance(MV_PROBE_DEBOUNCE_MS / 2)
  await mount("space-2")
  await advance(MV_PROBE_DEBOUNCE_MS * 5)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(1)
  expect(api.probeMvEntitlement.mock.calls[0][0].space_id).toBe("space-2")
})

it("an older probe's answer that lands while a newer timer is pending is discarded", async () => {
  const pending: Array<(r: MvProbeResult) => void> = []
  api.probeMvEntitlement.mockImplementation(() => new Promise<MvProbeResult>((resolve) => pending.push(resolve)))
  await mount()
  await advance(MV_PROBE_DEBOUNCE_MS)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(1)
  // Away and back: the same selection, so a kept old answer would count for it.
  await toggle("finance.sales.region_margin")
  await toggle("finance.sales.region_margin")
  await act(async () => pending[0](probe("probe_old")))
  expect(startButton().disabled).toBe(true)
  await advance(MV_PROBE_DEBOUNCE_MS)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(2)
  expect(startButton().disabled).toBe(true)
  await act(async () => pending[1](probe("probe_new")))
  expect(startButton().disabled).toBe(false)
  await act(async () => startButton().click())
  expect(api.triggerAutoOptimize.mock.calls[0][0].mv_consent.probe_id).toBe("probe_new")
})

it("Start stays disabled until the debounced answer lands", async () => {
  const pending: Array<(r: MvProbeResult) => void> = []
  api.probeMvEntitlement.mockImplementation(() => new Promise<MvProbeResult>((resolve) => pending.push(resolve)))
  await mount()
  expect(startButton().disabled).toBe(true)
  await advance(MV_PROBE_DEBOUNCE_MS - 1)
  expect(api.probeMvEntitlement).not.toHaveBeenCalled()
  expect(startButton().disabled).toBe(true)
  await advance(1)
  expect(api.probeMvEntitlement).toHaveBeenCalledTimes(1)
  expect(startButton().disabled).toBe(true)
  await act(async () => pending[0](probe("probe_1")))
  expect(startButton().disabled).toBe(false)
  await act(async () => startButton().click())
  expect(api.triggerAutoOptimize.mock.calls[0][0].mv_consent.probe_id).toBe("probe_1")
})
