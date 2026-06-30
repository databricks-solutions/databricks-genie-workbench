import { useState } from "react"
import { AlertTriangle, Rocket, Sparkles } from "lucide-react"
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { triggerAutoOptimize } from "@/lib/api"
import { PermissionAlert } from "@/components/auto-optimize/PermissionAlert"
import { ModelPicker } from "@/components/ModelPicker"
import {
  buildOptimizationTriggerRequest,
  parseMaxAttempts,
  parseTargetAccuracy,
} from "@/components/auto-optimize/optimizationRequest"
import type { GSOPermissionCheck } from "@/types"

interface OptimizationConfigProps {
  spaceId: string
  onStarted: (runId: string) => void
  onTriggerStart?: () => void
  onTriggerError?: (message: string) => void
  hasActiveRun: boolean
  permissions: GSOPermissionCheck | null
  permsLoading: boolean
  healthIssues?: string[]
  onRefreshPermissions?: () => void
}

// Levers 1–6 scope ONLY the surgical attempts (2..N). Lever 0 (the broad
// "coverage" pass) is attempt 1 — automatic and never user-selectable — so it
// is intentionally absent here and never added to the `levers` payload.
const LEVERS = [
  { id: 1, name: "Tables & Columns", description: "Update table descriptions, column descriptions, and synonyms" },
  { id: 2, name: "Metric Views", description: "Update metric view column descriptions" },
  { id: 3, name: "SQL Queries & Functions", description: "Add and update example SQLs, and remove underperforming TVFs" },
  { id: 4, name: "Join Specifications", description: "Add, update, or remove join relationships" },
  { id: 5, name: "Text Instructions", description: "Rewrite global routing instructions" },
  { id: 6, name: "SQL Expressions", description: "Add reusable SQL expressions (measures, filters, dimensions)" },
]

// Job defaults (databricks.yml): target_accuracy 0.90, max_attempts 3.
const DEFAULT_TARGET_PERCENT = "90"
const DEFAULT_MAX_ATTEMPTS = "3"

export function OptimizationConfig({ spaceId, onStarted, onTriggerStart, onTriggerError, hasActiveRun, permissions, permsLoading, healthIssues, onRefreshPermissions }: OptimizationConfigProps) {
  const [selectedLevers, setSelectedLevers] = useState<Set<number>>(new Set(LEVERS.map((l) => l.id)))
  const [applyMode] = useState<"genie_config" | "both">("genie_config")
  const [selectedModel, setSelectedModel] = useState<string | null>(null)
  const [targetPercent, setTargetPercent] = useState(DEFAULT_TARGET_PERCENT)
  const [maxAttemptsInput, setMaxAttemptsInput] = useState(DEFAULT_MAX_ATTEMPTS)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const hasHealthIssues = (healthIssues?.length ?? 0) > 0
  const targetAccuracy = parseTargetAccuracy(targetPercent)
  const maxAttempts = parseMaxAttempts(maxAttemptsInput)
  const knobsValid = targetAccuracy !== null && maxAttempts !== null
  const canStart = permissions?.can_start === true && !hasHealthIssues

  function toggleLever(id: number) {
    setSelectedLevers((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  async function handleStart() {
    if (targetAccuracy === null || maxAttempts === null) {
      setError("Enter a target accuracy between 1–100% and a max attempts of 1 or more.")
      return
    }
    setLoading(true)
    setError(null)
    onTriggerStart?.()
    try {
      const result = await triggerAutoOptimize(
        buildOptimizationTriggerRequest({
          spaceId,
          applyMode,
          selectedLevers,
          selectedModel,
          targetAccuracy,
          maxAttempts,
        }),
      )
      onStarted(result.runId)
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Failed to start optimization"
      setError(msg)
      onTriggerError?.(msg)
    } finally {
      setLoading(false)
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Optimization Configuration</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap items-start gap-6">
          {/* Apply Mode */}
          <div className="space-y-2">
            <p className="text-xs font-medium text-muted">Apply mode</p>
            <span className="inline-block px-3 py-1.5 rounded-lg border border-default text-sm font-medium bg-accent text-white">
              Config Only
            </span>
            <p className="text-xs text-muted max-w-xs">
              Changes will be applied only to the selected Genie Space configuration. Underlying Unity Catalog tables, columns, and descriptions will not be modified.
            </p>
          </div>

          <ModelPicker
            value={selectedModel}
            onChange={setSelectedModel}
            disabled={loading || hasActiveRun}
            className="w-full max-w-xs"
            helper="We recommend Claude Opus, Claude Sonnet, or compatible GPT models for the most reliable optimization results."
          />
        </div>

        {/* What will be optimized */}
        <div className="space-y-2">
          <p className="text-xs font-medium text-muted">What will be optimized</p>

          {/* Coverage — automatic attempt 1, not a toggle */}
          <div className="flex items-start gap-2 rounded-lg border border-default bg-accent/5 px-3 py-2.5">
            <Sparkles className="mt-0.5 h-4 w-4 shrink-0 text-accent" />
            <div>
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-sm font-medium text-primary">Coverage pass</span>
                <span className="rounded-full bg-accent/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-accent">
                  Automatic · attempt 1
                </span>
              </div>
              <p className="text-xs text-muted">
                A broad first pass always runs automatically — measured against the frozen baseline and rolled back if it doesn&apos;t help. It never counts as a surgical attempt.
              </p>
            </div>
          </div>

          {/* Surgical lever selection — scopes attempts 2+ */}
          <div className="space-y-2 pt-1">
            <p className="text-xs font-medium text-muted">Surgical optimization scope (attempts 2+)</p>
            <p className="text-xs text-muted">
              Select which levers the surgical hill-climb may use after the automatic coverage pass.
            </p>
            {LEVERS.map((lever) => (
              <label key={lever.id} className="flex items-start gap-2 cursor-pointer">
                <Checkbox
                  checked={selectedLevers.has(lever.id)}
                  onCheckedChange={() => toggleLever(lever.id)}
                  className="mt-0.5"
                />
                <div>
                  <span className="text-sm font-medium text-primary">{lever.name}</span>
                  <p className="text-xs text-muted">{lever.description}</p>
                </div>
              </label>
            ))}
          </div>
        </div>

        {/* Stopping criteria — target accuracy + max surgical attempts */}
        <div className="space-y-3 rounded-lg border border-default px-4 py-3">
          <div>
            <p className="text-xs font-medium text-muted">Stopping criteria</p>
            <p className="text-xs text-muted">
              The run stops at whichever comes first — reaching the target accuracy or exhausting the surgical attempts.
            </p>
          </div>
          <div className="flex flex-wrap items-start gap-6">
            <div className="space-y-1.5">
              <label htmlFor="gso-target-accuracy" className="block text-xs font-medium text-muted">
                Target accuracy
              </label>
              <div className="relative w-28">
                <input
                  id="gso-target-accuracy"
                  type="number"
                  min={1}
                  max={100}
                  step={1}
                  value={targetPercent}
                  onChange={(e) => setTargetPercent(e.target.value)}
                  disabled={loading || hasActiveRun}
                  className="h-9 w-full rounded-lg border border-default bg-surface px-3 pr-7 text-sm text-primary shadow-sm focus:outline-none focus:ring-2 focus:ring-accent/20 focus:border-accent disabled:cursor-not-allowed disabled:opacity-60"
                />
                <span className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-sm text-muted">%</span>
              </div>
              <p className="text-xs text-muted max-w-xs">Stop early once a candidate reaches this accuracy.</p>
            </div>
            <div className="space-y-1.5">
              <label htmlFor="gso-max-attempts" className="block text-xs font-medium text-muted">
                Max surgical attempts
              </label>
              <input
                id="gso-max-attempts"
                type="number"
                min={1}
                max={10}
                step={1}
                value={maxAttemptsInput}
                onChange={(e) => setMaxAttemptsInput(e.target.value)}
                disabled={loading || hasActiveRun}
                className="h-9 w-20 rounded-lg border border-default bg-surface px-3 text-sm text-primary shadow-sm focus:outline-none focus:ring-2 focus:ring-accent/20 focus:border-accent disabled:cursor-not-allowed disabled:opacity-60"
              />
              <p className="text-xs text-muted max-w-xs">
                Caps the surgical hill-climb (attempts 2+). The automatic coverage pass is free and never uses a slot.
              </p>
            </div>
          </div>
          {!knobsValid && (
            <p className="text-xs text-danger">
              Enter a target accuracy between 1–100% and a max attempts of 1 or more.
            </p>
          )}
        </div>

        {/* Health Issues */}
        {hasHealthIssues && (
          <div className="rounded-lg bg-danger/10 border border-danger/20 px-4 py-3 space-y-1">
            <div className="flex items-center gap-2 text-sm font-medium text-danger">
              <AlertTriangle className="w-4 h-4" />
              Configuration issues detected
            </div>
            {healthIssues!.map((issue, i) => (
              <p key={i} className="text-xs text-danger/80 ml-6">{issue}</p>
            ))}
          </div>
        )}

        {/* Permission Alert */}
        {permissions && (
          <PermissionAlert permissions={permissions} loading={permsLoading} onRefresh={onRefreshPermissions} />
        )}

        {/* Error */}
        {error && (
          <div className="rounded-lg bg-danger/10 border border-danger/20 px-4 py-3 text-sm text-danger">
            {error}
          </div>
        )}

        {/* Start Button */}
        <button
          onClick={handleStart}
          disabled={loading || hasActiveRun || selectedLevers.size === 0 || !canStart || !knobsValid}
          className="flex items-center gap-2 px-5 py-2.5 rounded-lg bg-accent text-white font-semibold hover:bg-accent/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          title={!canStart ? "Required permissions are missing" : !knobsValid ? "Enter valid stopping criteria" : undefined}
        >
          <Rocket className="w-4 h-4" />
          {loading ? "Starting..." : "Start Optimization"}
        </button>
      </CardContent>
    </Card>
  )
}
