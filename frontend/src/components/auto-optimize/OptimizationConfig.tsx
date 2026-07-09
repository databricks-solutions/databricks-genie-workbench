import { useState } from "react"
import type { LucideIcon } from "lucide-react"
import { AlertTriangle, ListChecks, Rocket, Target } from "lucide-react"
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

// Levers 1–6 scope the bounded native patch/eval attempts. There is no
// user-selectable lever 0 in the 4-task runner.
const LEVERS = [
  { id: 1, name: "Tables & Columns", description: "Update table descriptions, column descriptions, and synonyms" },
  { id: 2, name: "Metric Views", description: "Update metric view column descriptions" },
  { id: 3, name: "Table-Valued Functions", description: "Tune TVF parameter handling and remove underperforming TVFs" },
  { id: 4, name: "Join Specifications", description: "Add, update, or remove join relationships" },
  { id: 5, name: "Instructions & Examples", description: "Add example SQLs and update routing guidance" },
  { id: 6, name: "SQL Expressions", description: "Add reusable SQL expressions (measures, filters, dimensions)" },
]

// Job defaults (databricks.yml): target_accuracy 0.90, max_attempts 3.
const DEFAULT_TARGET_PERCENT = "90"
const DEFAULT_MAX_ATTEMPTS = "3"

// Shared pillar header — icon + label, matching ModelPicker's own label row so
// all three columns read as equal choices (previously only Model had an icon).
function PillarHeader({ icon: Icon, children }: { icon: LucideIcon; children: React.ReactNode }) {
  return (
    <div className="flex items-center gap-1.5 text-xs font-medium text-muted">
      <Icon className="h-3.5 w-3.5 text-accent" />
      {children}
    </div>
  )
}

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
      setError("Enter a target accuracy between 80–100% and a max attempts of 1 or more.")
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
      <CardContent className="space-y-6">
        {/* Three equal pillars — scope · model · stopping criteria — separated
            by hairline dividers. Column gap comes from symmetric per-column
            padding (not grid `gap`) so content always clears the divider on
            both sides; the first column only needs right-inset since its left
            edge aligns with the card padding. */}
        <div className="grid grid-cols-1 gap-y-8 lg:grid-cols-3 lg:divide-x lg:divide-default">
          {/* Pillar 1 — Optimization scope (lever checkboxes) */}
          <div className="space-y-2 lg:pr-8">
            <PillarHeader icon={ListChecks}>Optimization scope</PillarHeader>
            <p className="text-xs text-muted">
              Select which levers the optimizer may use when proposing targeted patch sets.
            </p>
            <div className="space-y-1.5 pt-1">
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

          {/* Pillar 2 — Model (ModelPicker renders its own matching Bot + label) */}
          <div className="lg:px-8">
            <ModelPicker
              value={selectedModel}
              onChange={setSelectedModel}
              disabled={loading || hasActiveRun}
              className="w-full"
              helper="We recommend Claude Opus, Claude Sonnet, or compatible GPT models for the most reliable optimization results."
            />
          </div>

          {/* Pillar 3 — Stopping criteria. pl only: last column's right edge
              aligns with the card padding, so no right-inset needed. */}
          <div className="space-y-3 lg:pl-8">
            <div className="space-y-2">
              <PillarHeader icon={Target}>Stopping criteria</PillarHeader>
              <p className="text-xs text-muted">
                The run stops at whichever comes first: reaching the target accuracy, exhausting the patch attempts, or finding no safe new hypothesis.
              </p>
            </div>
            <div className="space-y-3">
              <div className="space-y-1.5">
                <label htmlFor="gso-target-accuracy" className="block text-xs font-medium text-muted">
                  Target accuracy
                </label>
                <div className="relative w-28">
                  <input
                    id="gso-target-accuracy"
                    type="number"
                    min={80}
                    max={100}
                    step={1}
                    value={targetPercent}
                    onChange={(e) => setTargetPercent(e.target.value)}
                    disabled={loading || hasActiveRun}
                    className="h-9 w-full rounded-lg border border-default bg-surface px-3 pr-7 text-sm text-primary shadow-sm focus:outline-none focus:ring-2 focus:ring-accent/20 focus:border-accent disabled:cursor-not-allowed disabled:opacity-60"
                  />
                  <span className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-sm text-muted">%</span>
                </div>
                <p className="text-xs text-muted">Stop early once a candidate reaches this accuracy.</p>
              </div>
              <div className="space-y-1.5">
                <label htmlFor="gso-max-attempts" className="block text-xs font-medium text-muted">
                  Max patch attempts
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
                <p className="text-xs text-muted">
                  Caps the bounded patch/eval loop inside the Optimize task.
                </p>
              </div>
            </div>
            {!knobsValid && (
              <p className="text-xs text-danger">
                Enter a target accuracy between 80–100% and a max attempts of 1 or more.
              </p>
            )}
          </div>
        </div>

        {/* Alerts + launch — a full-width footer separated by a hairline so the
            CTA reads as the form's conclusion, not an orphan under one column. */}
        <div className="space-y-4 border-t border-default pt-4">
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

          {/* Start button — right-aligned commit action */}
          <div className="flex justify-end">
            <button
              onClick={handleStart}
              disabled={loading || hasActiveRun || selectedLevers.size === 0 || !canStart || !knobsValid}
              className="flex items-center gap-2 px-5 py-2.5 rounded-lg bg-accent text-white font-semibold hover:bg-accent/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              title={!canStart ? "Required permissions are missing" : !knobsValid ? "Enter valid stopping criteria" : undefined}
            >
              <Rocket className="w-4 h-4" />
              {loading ? "Starting..." : "Start Optimization"}
            </button>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
