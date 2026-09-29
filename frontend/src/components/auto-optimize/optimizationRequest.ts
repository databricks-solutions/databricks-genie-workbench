import type { GSOTriggerRequest, MvConsentPayload, MvProposal } from "@/types"

// Pure helpers for the optimization config surface. Kept out of
// OptimizationConfig.tsx so that component file only exports a component
// (react-refresh/only-export-components).

// Parse the target-accuracy percentage field into the 0–1 scale the backend
// expects. Returns null when the input is outside [80, 100] or not a number so
// the guard matches the input's min={80} attribute and the "between 80–100%"
// copy. The 80% floor is intentional: a lower optimization target isn't useful.
export function parseTargetAccuracy(percentInput: string): number | null {
  const pct = Number(percentInput)
  if (!Number.isFinite(pct) || pct < 80 || pct > 100) return null
  return pct / 100
}

// Parse the max-attempts field into a positive integer. Returns null for
// non-integers or values < 1.
export function parseMaxAttempts(input: string): number | null {
  const n = Number(input)
  if (!Number.isInteger(n) || n < 1) return null
  return n
}

// Metric view advisor knobs the run-config panel folds into the trigger payload.
// `enabled` mirrors the "Suggest metric views" toggle; when it is off,
// `buildOptimizationTriggerRequest` emits NO `mv_*` fields at all (the caller's
// "toggling off clears every mv_* field" contract). `materialize` is plumbed but
// has no control today — a later prompt adds it (mv-advisor-gap-report.md:1526).
export interface MvTriggerOptions {
  enabled: boolean
  mode: "suggest_only" | "create_and_attach"
  minConfidence?: number | null
  approvedSuggestionIds?: string[]
  consent?: MvConsentPayload | null
  materialize?: boolean
}

// Assemble the trigger payload. `levers` is the selected subset of {1..6};
// lever 0 is not part of the 4-task runner's user-selectable contract.
// `target_accuracy` is sent on the 0–1 scale; `max_attempts` bounds patch attempts.
export function buildOptimizationTriggerRequest(args: {
  spaceId: string
  applyMode: "genie_config" | "both"
  selectedLevers: Set<number>
  selectedModel: string | null
  targetAccuracy: number
  maxAttempts: number
  workloadWarehouseIds?: string[]
  benchmarkPolicy: "review_only" | "repair_allowed"
  mv?: MvTriggerOptions
  operatorGuidance?: string
}): GSOTriggerRequest {
  const request: GSOTriggerRequest = {
    space_id: args.spaceId,
    apply_mode: args.applyMode,
    levers: Array.from(args.selectedLevers)
      .filter((id) => id >= 1 && id <= 6)
      .sort((a, b) => a - b),
    llm_model: args.selectedModel,
    target_accuracy: args.targetAccuracy,
    max_attempts: args.maxAttempts,
    workload_warehouse_ids: args.workloadWarehouseIds ?? [],
    benchmark_policy: args.benchmarkPolicy,
  }

  // Per-run free-text guidance: only travels when non-blank, matching the panel's
  // "empty field sends nothing" convention. Trimmed so whitespace-only stays out.
  const guidance = args.operatorGuidance?.trim()
  if (guidance) {
    request.operator_guidance = guidance
  }

  // Only when the toggle is on. Otherwise every mv_* field stays absent, so
  // flipping the toggle off truly clears the request (mv_materialize included,
  // even though nothing sets it yet).
  if (args.mv?.enabled) {
    const createAndAttach = args.mv.mode === "create_and_attach"
    request.enable_metric_view_suggestions = true
    request.mv_action_mode = args.mv.mode
    request.mv_min_confidence = args.mv.minConfidence ?? null
    // Approved ids and a consent object only travel with a create_and_attach run;
    // "Suggest only" sends neither.
    request.mv_approved_suggestion_ids = createAndAttach
      ? args.mv.approvedSuggestionIds ?? []
      : []
    request.mv_consent = createAndAttach ? args.mv.consent ?? null : null
    request.mv_materialize = args.mv.materialize ?? false
  }

  return request
}

export type MvTarget = { catalog: string; schema: string }

// The proposals the user has ticked, in list order.
export function selectedMvProposals(proposals: MvProposal[], selectedIds: Set<string>): MvProposal[] {
  return proposals.filter((p) => selectedIds.has(p.suggestion_id))
}

// Every distinct catalog.schema the proposals would be created in, first seen
// first. A proposal without a three-part `proposed_object` adds none.
export function deriveMvTargets(proposals: MvProposal[]): MvTarget[] {
  const targets = new Map<string, MvTarget>()
  for (const proposal of proposals) {
    const parts = (proposal.proposed_object ?? "").split(".")
    if (parts.length === 3 && parts[0] && parts[1]) {
      const key = `${parts[0]}.${parts[1]}`
      if (!targets.has(key)) targets.set(key, { catalog: parts[0], schema: parts[1] })
    }
  }
  return Array.from(targets.values())
}

// The create target for the selected proposals. A consent covers one schema, so
// a selection spanning two has none; null also covers first-run (MV-D112).
export function deriveMvTarget(proposals: MvProposal[]): MvTarget | null {
  const targets = deriveMvTargets(proposals)
  return targets.length === 1 ? targets[0] : null
}

// Why create-and-attach cannot use this selection, or null when it can.
export function mvSelectionMessage(
  selected: MvProposal[],
  mode: "suggest_only" | "create_and_attach" = "create_and_attach",
): string | null {
  const creating = mode === "create_and_attach"
  if (selected.length === 0) {
    return creating ? "Select at least one metric view to create, or choose Suggest only." : null
  }
  const targets = deriveMvTargets(selected)
  if (targets.length > 1) {
    const names = targets.map((t) => `${t.catalog}.${t.schema}`).join(", ")
    const next = creating ? "select views from one schema, or choose Suggest only." : "select views from one schema to use it."
    return `The selected metric views are in ${targets.length} schemas (${names}). Create and attach uses one schema, so ${next}`
  }
  return null
}

// Why the create radio is unavailable because of the selection itself, or null
// when the selection names one schema and only the permission check decides.
export function mvCreateSelectionReason(selected: MvProposal[]): string | null {
  if (selected.length === 0) return "Available once you select a metric view to create."
  if (deriveMvTargets(selected).length > 1) return "Available once the selected metric views are in one schema."
  return null
}

// Why Start is blocked by the metric-view section, or null. Only a re-run in
// create-and-attach can block: Start waits for a creatable selection and for the
// permission check that covers exactly that selection.
export function mvStartBlockReason(args: {
  enabled: boolean
  mode: "suggest_only" | "create_and_attach"
  proposals: MvProposal[]
  selected: MvProposal[]
  probeLoading: boolean
}): string | null {
  if (!args.enabled || args.mode !== "create_and_attach" || args.proposals.length === 0) return null
  return (
    mvSelectionMessage(args.selected) ??
    (args.probeLoading ? "Checking your permissions for the selected metric views…" : null)
  )
}

// Collect the distinct three-part source tables across proposals' evidence, for
// the entitlement probe's SELECT checks. Deduped and sorted so the probe body is
// stable across renders. Evidence is a decoded JSON blob (Record); source_tables
// is read defensively and non-string / non-three-part entries are dropped.
export function collectMvSourceTables(proposals: MvProposal[]): string[] {
  const tables = new Set<string>()
  for (const proposal of proposals) {
    const raw = proposal.evidence?.source_tables
    if (!Array.isArray(raw)) continue
    for (const entry of raw) {
      if (typeof entry === "string" && entry.split(".").length === 3) {
        tables.add(entry)
      }
    }
  }
  return Array.from(tables).sort()
}
