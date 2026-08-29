import { describe, expect, it } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import { OptimizationConfig } from "./OptimizationConfig"
import { MvSuggestSection } from "./MvSuggestSection"
import {
  buildOptimizationTriggerRequest,
  collectMvSourceTables,
  deriveMvTarget,
  parseMaxAttempts,
  parseTargetAccuracy,
} from "./optimizationRequest"
import type { MvProbeResult, MvProposal } from "@/types"

// Minimal MvProposal fixture — only the fields the panel reads are meaningful.
function proposal(overrides: Partial<MvProposal> = {}): MvProposal {
  return {
    suggestion_id: "sug_1",
    dedup_fingerprint: "fp_1",
    target_space_id: "space-1",
    run_id: null,
    candidate_type: "PROPOSE",
    confidence_score: 88,
    tier: "HIGH",
    uncapped_tier: "HIGH",
    tier_capped_by_coverage: false,
    proposed_object: "finance.sales.order_revenue",
    score_components: null,
    evidence: { source_tables: ["finance.sales.orders", "finance.sales.order_items"] },
    provenance_labels: null,
    provenance: null,
    alternatives: null,
    conflicts: null,
    requested_mode: null,
    effective_mode: null,
    decision: "approved",
    decided_by: "user@example.com",
    decided_at: "2026-08-23T09:14:22Z",
    suppressed_until: null,
    approved_for_rerun: true,
    created_at: null,
    updated_at: null,
    ...overrides,
  }
}

function probeResult(overrides: Partial<MvProbeResult> = {}): MvProbeResult {
  return {
    probe_id: "probe_7f21",
    checked_as: "user@example.com",
    auth_identity: "OBO",
    target: "finance.sales",
    checked_at: "2026-08-23T09:14:22Z",
    results: {},
    privileges: [],
    capabilities: [],
    verdict: "SUFFICIENT",
    missing: [],
    remediation_sql: null,
    fallback_mode: "suggest_only",
    materialize_consented: false,
    consent_recorded: true,
    errors: [],
    ...overrides,
  }
}

const noop = () => {}

describe("OptimizationConfig lever scope", () => {
  // The current 4-task flow exposes only the six patch levers (1..6). Retired
  // lever 0 is a legacy data concern, not a selectable control.
  it("renders only the six patch levers", () => {
    const markup = renderToStaticMarkup(
      <OptimizationConfig
        spaceId="space-1"
        onStarted={() => {}}
        hasActiveRun={false}
        permissions={null}
        permsLoading={false}
      />,
    )

    expect(markup).toContain("Optimization Scope")
    expect(markup).toContain("Select which changes the optimizer may make")
    expect(markup).toContain("Optimization Config")
    expect(markup).toContain("Model selection")
    expect(markup).toContain("lg:grid-cols-2")
    expect(markup).not.toContain("lg:grid-cols-3")
    expect(markup).not.toContain("Coverage pass")
    expect(markup).not.toContain("Surgical optimization scope")

    // Six levers + benchmark-repair consent + the "Suggest metric views" toggle.
    const checkboxCount = (markup.match(/type="checkbox"/g) ?? []).length
    expect(checkboxCount).toBe(8)
    expect(markup).toContain("Allow GSO to repair and add benchmarks")
    expect(markup).toContain("Off by default")
    // The MV toggle is present but its body stays collapsed until expanded; with
    // the toggle off there is no materialize control anywhere (gap report 1526).
    expect(markup).toContain("Suggest metric views")
    expect(markup).not.toContain("Also materialize")
  })

  it("surfaces the two stopping-criteria knobs with their default values", () => {
    const markup = renderToStaticMarkup(
      <OptimizationConfig
        spaceId="space-1"
        onStarted={() => {}}
        hasActiveRun={false}
        permissions={null}
        permsLoading={false}
      />,
    )

    expect(markup).toContain("Target accuracy")
    expect(markup).toContain("Max patch attempts")
    expect(markup).toContain("whichever comes first")
    // Defaults reflect the job defaults (0.90 -> 90%, 3 attempts).
    expect(markup).toContain('value="90"')
    expect(markup).toContain('value="3"')
  })

  it("explains the non-blocking fallback when query usage is unavailable", () => {
    const markup = renderToStaticMarkup(
      <OptimizationConfig
        spaceId="space-1"
        onStarted={() => {}}
        hasActiveRun={false}
        permissions={{
          sp_display_name: "gso-service-principal",
          sp_application_id: "application-id",
          sp_has_manage: true,
          schemas: [],
          can_start: true,
          errors: [],
          query_usage_signal: {
            status: "unavailable",
            system_table_available: false,
            warehouse_api_available: false,
            warehouses: [],
            inaccessible_warehouses: [],
            system_grant_sql: null,
          },
        }}
        permsLoading={false}
      />,
    )

    expect(markup).toContain("Query usage signal")
    expect(markup).toContain("Optimization will still run")
    expect(markup).toContain("local profiling evidence")
  })
})

describe("stopping-criteria parsing", () => {
  it("converts the target-accuracy percentage to the 0–1 scale", () => {
    expect(parseTargetAccuracy("90")).toBeCloseTo(0.9)
    expect(parseTargetAccuracy("100")).toBe(1)
    expect(parseTargetAccuracy("80")).toBeCloseTo(0.8) // lower boundary (80% floor)
  })

  it("rejects out-of-range or non-numeric target accuracy", () => {
    // The 80% floor: a lower optimization target isn't useful, so <80 is invalid.
    expect(parseTargetAccuracy("79")).toBeNull() // just below the 80% floor
    expect(parseTargetAccuracy("1")).toBeNull()
    expect(parseTargetAccuracy("0")).toBeNull()
    expect(parseTargetAccuracy("0.5")).toBeNull()
    expect(parseTargetAccuracy("150")).toBeNull()
    expect(parseTargetAccuracy("")).toBeNull()
    expect(parseTargetAccuracy("abc")).toBeNull()
  })

  it("accepts only positive integers for max attempts", () => {
    expect(parseMaxAttempts("3")).toBe(3)
    expect(parseMaxAttempts("1")).toBe(1)
    expect(parseMaxAttempts("0")).toBeNull()
    expect(parseMaxAttempts("-1")).toBeNull()
    expect(parseMaxAttempts("2.5")).toBeNull()
    expect(parseMaxAttempts("")).toBeNull()
  })
})

describe("buildOptimizationTriggerRequest (trigger payload)", () => {
  // (b) target_accuracy (0–1) and max_attempts must be passed through. The
  // component sends exactly this object to triggerAutoOptimize in handleStart;
  // api.test.ts independently verifies triggerAutoOptimize forwards both fields.
  it("forwards target_accuracy on the 0–1 scale and max_attempts as an integer", () => {
    const req = buildOptimizationTriggerRequest({
      spaceId: "space-1",
      applyMode: "genie_config",
      selectedLevers: new Set([1, 2, 3, 4, 5, 6]),
      selectedModel: "claude-opus",
      targetAccuracy: parseTargetAccuracy("90")!,
      maxAttempts: parseMaxAttempts("3")!,
      benchmarkPolicy: "review_only",
    })

    expect(req).toEqual({
      space_id: "space-1",
      apply_mode: "genie_config",
      levers: [1, 2, 3, 4, 5, 6],
      llm_model: "claude-opus",
      target_accuracy: 0.9,
      max_attempts: 3,
      workload_warehouse_ids: [],
      benchmark_policy: "review_only",
    })
    expect(req.target_accuracy).toBeLessThanOrEqual(1)
  })

  it("forwards workload warehouses for optional query-history evidence", () => {
    const req = buildOptimizationTriggerRequest({
      spaceId: "space-1",
      applyMode: "genie_config",
      selectedLevers: new Set([1]),
      selectedModel: null,
      targetAccuracy: 0.9,
      maxAttempts: 3,
      benchmarkPolicy: "review_only",
      workloadWarehouseIds: ["warehouse-b", "warehouse-a"],
    })

    expect(req.workload_warehouse_ids).toEqual(["warehouse-b", "warehouse-a"])
  })

  it("never includes retired lever 0 in the levers payload", () => {
    const req = buildOptimizationTriggerRequest({
      spaceId: "space-1",
      applyMode: "genie_config",
      selectedLevers: new Set([3, 1]),
      selectedModel: null,
      targetAccuracy: 0.9,
      maxAttempts: 3,
      benchmarkPolicy: "review_only",
    })

    expect(req.levers).toEqual([1, 3]) // sorted subset of {1..6}
    expect(req.levers).not.toContain(0)
  })

  it("filters out-of-range lever ids (retired 0, 7+) before sending", () => {
    const req = buildOptimizationTriggerRequest({
      spaceId: "space-1",
      applyMode: "genie_config",
      selectedLevers: new Set([0, 1, 7]),
      selectedModel: null,
      targetAccuracy: 0.9,
      maxAttempts: 3,
      benchmarkPolicy: "review_only",
    })

    expect(req.levers).toEqual([1]) // 0 (retired) and 7 (out of range) dropped
  })
})

describe("buildOptimizationTriggerRequest — metric view payload", () => {
  const base = {
    spaceId: "space-1",
    applyMode: "genie_config" as const,
    selectedLevers: new Set([1, 2]),
    selectedModel: null,
    targetAccuracy: 0.9,
    maxAttempts: 3,
    benchmarkPolicy: "review_only" as const,
  }

  const MV_KEYS = [
    "enable_metric_view_suggestions",
    "mv_action_mode",
    "mv_min_confidence",
    "mv_approved_suggestion_ids",
    "mv_consent",
    "mv_materialize",
  ] as const

  it("emits NO mv_* fields when the toggle is off (mv_materialize included)", () => {
    // Toggling off must clear every mv_* field — mv_materialize too, even though
    // nothing sets it yet (its control is unbuilt, gap report 1526).
    const off = buildOptimizationTriggerRequest(base)
    for (const key of MV_KEYS) expect(off).not.toHaveProperty(key)

    const disabled = buildOptimizationTriggerRequest({
      ...base,
      mv: {
        enabled: false,
        mode: "create_and_attach",
        approvedSuggestionIds: ["sug_1"],
        consent: { granted_by: "u", granted_at: "t", probe_id: "p" },
        materialize: true,
      },
    })
    for (const key of MV_KEYS) expect(disabled).not.toHaveProperty(key)
  })

  it("carries approved ids + consent on a create_and_attach run, materialize plumbed off", () => {
    const req = buildOptimizationTriggerRequest({
      ...base,
      mv: {
        enabled: true,
        mode: "create_and_attach",
        approvedSuggestionIds: ["sug_b", "sug_a"],
        consent: { granted_by: "user@example.com", granted_at: "2026-08-23T09:14:22Z", probe_id: "probe_7f21" },
      },
    })

    expect(req.enable_metric_view_suggestions).toBe(true)
    expect(req.mv_action_mode).toBe("create_and_attach")
    expect(req.mv_approved_suggestion_ids).toEqual(["sug_b", "sug_a"])
    expect(req.mv_consent).toEqual({
      granted_by: "user@example.com",
      granted_at: "2026-08-23T09:14:22Z",
      probe_id: "probe_7f21",
    })
    expect(req.mv_materialize).toBe(false) // plumbed, never set true (no control)
  })

  it("suggest_only sends no consent and no approved ids", () => {
    const req = buildOptimizationTriggerRequest({
      ...base,
      mv: {
        enabled: true,
        mode: "suggest_only",
        approvedSuggestionIds: ["sug_a"],
        consent: { granted_by: "u", granted_at: "t", probe_id: "p" },
      },
    })

    expect(req.enable_metric_view_suggestions).toBe(true)
    expect(req.mv_action_mode).toBe("suggest_only")
    expect(req.mv_consent).toBeNull()
    expect(req.mv_approved_suggestion_ids).toEqual([])
  })
})

describe("deriveMvTarget / collectMvSourceTables", () => {
  it("derives catalog.schema from the first three-part proposed_object", () => {
    expect(deriveMvTarget([proposal()])).toEqual({ catalog: "finance", schema: "sales" })
  })

  it("returns null when no proposal carries a three-part object (first-run)", () => {
    expect(deriveMvTarget([])).toBeNull()
    expect(deriveMvTarget([proposal({ proposed_object: null })])).toBeNull()
  })

  it("collects distinct three-part source tables, sorted and deduped", () => {
    const tables = collectMvSourceTables([
      proposal(),
      proposal({ suggestion_id: "sug_2", evidence: { source_tables: ["finance.sales.orders", "bad", 5] } }),
    ])
    expect(tables).toEqual(["finance.sales.order_items", "finance.sales.orders"])
  })
})

describe("MvSuggestSection states", () => {
  const commonProps = {
    onToggle: noop,
    onToggleProposal: noop,
    onModeChange: noop,
    onCopyGrant: noop,
    selectedProposalIds: new Set<string>(),
    mode: "suggest_only" as const,
  }

  it("collapsed when disabled toggle is off — no proposals, probe, or materialize", () => {
    const html = renderToStaticMarkup(
      <MvSuggestSection
        {...commonProps}
        enabled={false}
        proposalsLoading={false}
        proposals={[]}
        target={null}
        probe={null}
        probeLoading={false}
        probeError={null}
      />,
    )
    expect(html).toContain("Suggest metric views")
    expect(html).not.toContain("Approved for this Agent")
    expect(html).not.toContain("Also materialize")
  })

  it("loading is NON-BLOCKING: Suggest only is usable, check runs as a caption (15.6)", () => {
    const html = renderToStaticMarkup(
      <MvSuggestSection
        {...commonProps}
        enabled
        proposalsLoading
        proposals={[]}
        target={null}
        probe={null}
        probeLoading={false}
        probeError={null}
      />,
    )
    // The check runs as a subtle caption, not a blocking replacement…
    expect(html).toContain("Checking this Agent for approved proposals")
    expect(html).toContain("you can start with Suggest")
    // …and the mode UI (Suggest only) is present the moment the section opens.
    expect(html).toContain("Suggest only")
  })

  it("proposals check failed: surfaces the reason, keeps Suggest only usable (15.6)", () => {
    const html = renderToStaticMarkup(
      <MvSuggestSection
        {...commonProps}
        enabled
        proposalsLoading={false}
        proposalsError="request timed out"
        proposals={[]}
        target={null}
        probe={null}
        probeLoading={false}
        probeError={null}
      />,
    )
    expect(html).toContain("Couldn")
    expect(html).toContain("request timed out")
    expect(html).toContain("Suggest only")
    // A failed check must NOT masquerade as "no approved proposals".
    expect(html).not.toContain("Approved for this Agent")
  })

  it("first-run: disables Create and attach with the MV-D1 rationale", () => {
    const html = renderToStaticMarkup(
      <MvSuggestSection
        {...commonProps}
        enabled
        proposalsLoading={false}
        proposals={[]}
        target={null}
        probe={null}
        probeLoading={false}
        probeError={null}
      />,
    )
    expect(html).toContain("Available after this run produces proposals you approve")
    expect(html).toContain("disabled")
    expect(html).not.toContain("Approved for this Agent")
  })

  it("probing: shows the checking-permissions line for the derived target", () => {
    const html = renderToStaticMarkup(
      <MvSuggestSection
        {...commonProps}
        enabled
        proposalsLoading={false}
        proposals={[proposal()]}
        target={{ catalog: "finance", schema: "sales" }}
        probe={null}
        probeLoading
        probeError={null}
      />,
    )
    expect(html).toContain("Checking your permissions")
    expect(html).toContain("finance.sales")
  })

  it("re-run granted: lists the approved object and enables Create and attach", () => {
    const html = renderToStaticMarkup(
      <MvSuggestSection
        {...commonProps}
        enabled
        proposalsLoading={false}
        proposals={[proposal()]}
        selectedProposalIds={new Set(["sug_1"])}
        target={{ catalog: "finance", schema: "sales" }}
        probe={probeResult()}
        probeLoading={false}
        probeError={null}
      />,
    )
    expect(html).toContain("Approved for this Agent")
    expect(html).toContain("finance.sales.order_revenue")
    expect(html).toContain("You can create metric views in")
    expect(html).toContain("Create and attach, then optimize")
    // Never names the run as the data source (space-scoped, MV-D23).
    expect(html).not.toContain("from run")
  })

  it("re-run denied: shows the denial banner, Copy grant request, and remediation SQL", () => {
    const html = renderToStaticMarkup(
      <MvSuggestSection
        {...commonProps}
        enabled
        proposalsLoading={false}
        proposals={[proposal()]}
        selectedProposalIds={new Set(["sug_1"])}
        target={{ catalog: "finance", schema: "sales" }}
        probe={probeResult({
          verdict: "INSUFFICIENT",
          missing: ["CREATE TABLE on finance.sales"],
          remediation_sql: "GRANT CREATE TABLE ON SCHEMA finance.sales TO `user@example.com`;",
        })}
        probeLoading={false}
        probeError={null}
      />,
    )
    expect(html).toContain("permission to create metric views")
    expect(html).toContain("Copy grant request")
    expect(html).toContain("GRANT CREATE TABLE ON SCHEMA finance.sales")
  })
})
