import type { GSOTriggerRequest } from "@/types"

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
}): GSOTriggerRequest {
  return {
    space_id: args.spaceId,
    apply_mode: args.applyMode,
    levers: Array.from(args.selectedLevers)
      .filter((id) => id >= 1 && id <= 6)
      .sort((a, b) => a - b),
    llm_model: args.selectedModel,
    target_accuracy: args.targetAccuracy,
    max_attempts: args.maxAttempts,
    workload_warehouse_ids: args.workloadWarehouseIds ?? [],
  }
}
