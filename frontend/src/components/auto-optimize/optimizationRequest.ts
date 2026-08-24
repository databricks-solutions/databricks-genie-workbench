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

// Derive the create target (catalog.schema) from approved proposals. Approved
// proposals for a space share a schema in the common case; take the first that
// carries a three-part `proposed_object`. Returns null when none do (first-run,
// or proposals without a proposed object) — the panel then stays in suggest-only.
export function deriveMvTarget(
  proposals: MvProposal[],
): { catalog: string; schema: string } | null {
  for (const proposal of proposals) {
    const parts = (proposal.proposed_object ?? "").split(".")
    if (parts.length === 3 && parts[0] && parts[1]) {
      return { catalog: parts[0], schema: parts[1] }
    }
  }
  return null
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
