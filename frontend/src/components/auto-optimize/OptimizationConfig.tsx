import { useEffect, useMemo, useState } from "react"
import type { LucideIcon } from "lucide-react"
import { AlertTriangle, Database, ListChecks, Rocket, Settings2, Target } from "lucide-react"
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { fetchSpaceMvProposals, probeMvEntitlement, triggerAutoOptimize } from "@/lib/api"
import { PermissionAlert } from "@/components/auto-optimize/PermissionAlert"
import { MvSuggestSection } from "@/components/auto-optimize/MvSuggestSection"
import { ModelPicker } from "@/components/ModelPicker"
import {
  buildOptimizationTriggerRequest,
  collectMvSourceTables,
  deriveMvTarget,
  parseMaxAttempts,
  parseTargetAccuracy,
} from "@/components/auto-optimize/optimizationRequest"
import type { GSOPermissionCheck, MvProbeResult, MvProposal } from "@/types"

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
const MAX_WORKLOAD_WAREHOUSES = 20

// Shared section header for the two configuration columns and their subsections.
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
  const [workloadWarehouseIds, setWorkloadWarehouseIds] = useState<Set<string>>(new Set())
  const [allowBenchmarkRepair, setAllowBenchmarkRepair] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Metric view advisor state (Prompt 11, MV-D1/D23). All local, no store — the
  // section fetches its own space-scoped proposals and OBO probe lazily when the
  // toggle first expands. Empty approved set ⇒ first-run; a non-empty set ⇒
  // re-run, where the probe gates "Create and attach".
  const [mvEnabled, setMvEnabled] = useState(false)
  const [mvProposals, setMvProposals] = useState<MvProposal[]>([])
  const [mvProposalsLoaded, setMvProposalsLoaded] = useState(false)
  const [mvProposalsLoading, setMvProposalsLoading] = useState(false)
  const [mvSelectedIds, setMvSelectedIds] = useState<Set<string>>(new Set())
  const [mvMode, setMvMode] = useState<"suggest_only" | "create_and_attach">("suggest_only")
  const [mvProbe, setMvProbe] = useState<MvProbeResult | null>(null)
  const [mvProbeLoading, setMvProbeLoading] = useState(false)
  const [mvProbeError, setMvProbeError] = useState<string | null>(null)

  const hasHealthIssues = (healthIssues?.length ?? 0) > 0
  const targetAccuracy = parseTargetAccuracy(targetPercent)
  const maxAttempts = parseMaxAttempts(maxAttemptsInput)
  const knobsValid = targetAccuracy !== null && maxAttempts !== null
  const canStart = permissions?.can_start === true && !hasHealthIssues

  const mvTarget = useMemo(() => deriveMvTarget(mvProposals), [mvProposals])
  const mvGranted = mvProbe?.verdict === "SUFFICIENT"

  // Load the space's approved-for-rerun proposals the first time the section
  // expands (MV-D23 — space-scoped, never keyed on a prior run).
  useEffect(() => {
    if (!mvEnabled || mvProposalsLoaded || mvProposalsLoading) return
    let cancelled = false
    setMvProposalsLoading(true)
    fetchSpaceMvProposals(spaceId, true)
      .then((res) => {
        if (cancelled) return
        setMvProposals(res.proposals)
        setMvSelectedIds(new Set(res.proposals.map((p) => p.suggestion_id)))
      })
      .catch(() => {
        if (!cancelled) setMvProposals([])
      })
      .finally(() => {
        if (cancelled) return
        setMvProposalsLoading(false)
        setMvProposalsLoaded(true)
      })
    return () => {
      cancelled = true
    }
  }, [mvEnabled, mvProposalsLoaded, mvProposalsLoading, spaceId])

  // Probe entitlement once approved proposals with a target are known (re-run).
  // Fires once per target; a failure records an error rather than re-looping.
  useEffect(() => {
    if (!mvEnabled || !mvProposalsLoaded || !mvTarget) return
    if (mvProbe || mvProbeLoading || mvProbeError) return
    let cancelled = false
    setMvProbeLoading(true)
    probeMvEntitlement({
      catalog: mvTarget.catalog,
      schema: mvTarget.schema,
      space_id: spaceId,
      source_tables: collectMvSourceTables(mvProposals),
    })
      .then((res) => {
        if (!cancelled) setMvProbe(res)
      })
      .catch((e) => {
        if (!cancelled) setMvProbeError(e instanceof Error ? e.message : "Entitlement probe failed.")
      })
      .finally(() => {
        if (!cancelled) setMvProbeLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [mvEnabled, mvProposalsLoaded, mvTarget, mvProbe, mvProbeLoading, mvProbeError, mvProposals, spaceId])

  function toggleLever(id: number) {
    setSelectedLevers((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function toggleMvProposal(suggestionId: string) {
    setMvSelectedIds((prev) => {
      const next = new Set(prev)
      if (next.has(suggestionId)) next.delete(suggestionId)
      else next.add(suggestionId)
      return next
    })
  }

  function handleCopyGrant() {
    const sql = mvProbe?.remediation_sql
    if (sql) void navigator.clipboard?.writeText(sql).catch(() => {})
  }

  async function handleStart() {
    if (targetAccuracy === null || maxAttempts === null) {
      setError("Enter a target accuracy between 80–100% and a max attempts of 1 or more.")
      return
    }
    setLoading(true)
    setError(null)
    onTriggerStart?.()
    // create_and_attach only survives when the probe still says SUFFICIENT;
    // anything else sends suggest_only with no consent (downgrade-never-upgrade).
    const effectiveMvMode =
      mvEnabled && mvMode === "create_and_attach" && mvGranted ? "create_and_attach" : "suggest_only"
    try {
      const result = await triggerAutoOptimize(
        buildOptimizationTriggerRequest({
          spaceId,
          applyMode,
          selectedLevers,
          selectedModel,
          targetAccuracy,
          maxAttempts,
          workloadWarehouseIds: Array.from(workloadWarehouseIds).sort(),
          benchmarkPolicy: allowBenchmarkRepair ? "repair_allowed" : "review_only",
          mv: mvEnabled
            ? {
                enabled: true,
                mode: effectiveMvMode,
                minConfidence: null,
                approvedSuggestionIds: Array.from(mvSelectedIds).sort(),
                consent:
                  effectiveMvMode === "create_and_attach" && mvProbe
                    ? {
                        granted_by: mvProbe.checked_as,
                        granted_at: mvProbe.checked_at,
                        probe_id: mvProbe.probe_id,
                      }
                    : null,
                materialize: false,
              }
            : undefined,
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
        <div className="grid grid-cols-1 gap-y-8 lg:grid-cols-2 lg:divide-x lg:divide-default">
          <div className="space-y-4 lg:pr-8">
            <div className="space-y-2">
              <PillarHeader icon={ListChecks}>Optimization Scope</PillarHeader>
              <p className="text-xs text-muted">
                Select which changes the optimizer may make during this run.
              </p>
            </div>
            <div className="space-y-1.5">
              {LEVERS.map((lever) => (
                <label key={lever.id} className="flex cursor-pointer items-start gap-2">
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

            <div className="border-t border-default pt-4">
              <label className="flex cursor-pointer items-start gap-2">
                <Checkbox
                  checked={allowBenchmarkRepair}
                  onCheckedChange={(checked) => setAllowBenchmarkRepair(checked === true)}
                  disabled={loading || hasActiveRun}
                  className="mt-0.5"
                />
                <span>
                  <span className="block text-sm font-medium text-primary">
                    Allow GSO to repair and add benchmarks
                  </span>
                  <span className="mt-0.5 block text-xs text-muted">
                    Off by default. GSO reviews existing benchmarks without changing them and optimizes only against the valid subset. If fewer than the required minimum remain, optimization is skipped.
                  </span>
                  {allowBenchmarkRepair && (
                    <span className="mt-2 flex items-start gap-1.5 text-xs text-amber-600 dark:text-amber-400">
                      <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                      This run may add benchmarks or update benchmark questions and SQL in the live Genie Agent. Existing benchmarks are never deleted.
                    </span>
                  )}
                </span>
              </label>
            </div>
          </div>

          <div className="space-y-6 lg:pl-8">
            <div className="space-y-2">
              <PillarHeader icon={Settings2}>Optimization Config</PillarHeader>
              <p className="text-xs text-muted">
                Choose the model and the limits that determine when optimization stops.
              </p>
            </div>

            <ModelPicker
              value={selectedModel}
              onChange={setSelectedModel}
              disabled={loading || hasActiveRun}
              label="Model selection"
              className="w-full"
              helper="We recommend Claude Opus, Claude Sonnet, or compatible GPT models for the most reliable optimization results."
            />

            <div className="space-y-3 border-t border-default pt-5">
              <div className="space-y-2">
                <PillarHeader icon={Target}>Stopping criteria</PillarHeader>
                <p className="text-xs text-muted">
                  The run stops at whichever comes first: reaching the target accuracy, exhausting the patch attempts, or finding no safe new hypothesis.
                </p>
              </div>
              <div className="grid gap-4 sm:grid-cols-2">
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
        </div>

        {permissions && (
          <div className="space-y-3 rounded-lg border border-default bg-surface-subtle px-4 py-3">
            <div className="flex items-center gap-2 text-sm font-medium text-primary">
              <Database className="h-4 w-4 text-accent" />
              Query usage signal
            </div>
            {permissions.query_usage_signal?.system_table_available ? (
              <p className="text-xs text-muted">System query history available. GSO will use aggregated human-query behavior for column ranking.</p>
            ) : permissions.query_usage_signal && permissions.query_usage_signal.warehouses.length > 0 ? (
              <>
                <p className="text-xs text-muted">
                  {permissions.query_usage_signal.status === "partially_available"
                    ? `Partially available. CAN VIEW is still needed for: ${permissions.query_usage_signal.inaccessible_warehouses.join(", ")}.`
                    : permissions.query_usage_signal.warehouse_api_available
                      ? "Warehouse query history available."
                      : "Warehouse query history unavailable until CAN VIEW is granted."}
                  {" "}Optionally select representative workload warehouses; missing access never blocks optimization.
                </p>
                <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                  {permissions.query_usage_signal.warehouses.map((warehouse) => (
                    <label key={warehouse.warehouse_id} className="flex items-center gap-2 text-xs text-primary">
                      <Checkbox
                        checked={workloadWarehouseIds.has(warehouse.warehouse_id)}
                        onCheckedChange={() => setWorkloadWarehouseIds((previous) => {
                          const next = new Set(previous)
                          if (next.has(warehouse.warehouse_id)) next.delete(warehouse.warehouse_id)
                          else if (next.size < MAX_WORKLOAD_WAREHOUSES) next.add(warehouse.warehouse_id)
                          return next
                        })}
                        disabled={loading || hasActiveRun}
                      />
                      <span className="truncate" title={warehouse.name}>
                        {warehouse.name}{warehouse.accessible ? "" : " (CAN VIEW needed)"}
                      </span>
                    </label>
                  ))}
                </div>
                <p className="text-xs text-muted">
                  The GSO service principal needs CAN VIEW on each selected warehouse: <span className="font-mono">{permissions.sp_application_id || permissions.sp_display_name}</span>.
                </p>
                {permissions.query_usage_signal.warehouses.some(
                  (warehouse) => workloadWarehouseIds.has(warehouse.warehouse_id) && !warehouse.accessible,
                ) && (
                  <textarea
                    readOnly
                    aria-label="Warehouse query history permission instructions"
                    value={permissions.query_usage_signal.warehouses
                      .filter((warehouse) => workloadWarehouseIds.has(warehouse.warehouse_id) && !warehouse.accessible)
                      .map((warehouse) => `Grant CAN VIEW on SQL warehouse "${warehouse.name}" to service principal "${permissions.sp_application_id || permissions.sp_display_name}".`)
                      .join("\n")}
                    className="min-h-20 w-full resize-y rounded-md border border-default bg-surface p-2 font-mono text-xs text-primary"
                  />
                )}
              </>
            ) : (
              <p className="text-xs text-muted">
                Query usage is unavailable. Optimization will still run; GSO skips query-history-based ranking and relies on configuration, benchmark, metadata, and local profiling evidence.
              </p>
            )}
            {!permissions.query_usage_signal?.system_table_available && permissions.query_usage_signal?.system_grant_sql && (
              <details className="text-xs text-muted">
                <summary className="cursor-pointer font-medium text-primary">System query history grants</summary>
                <textarea
                  readOnly
                  value={permissions.query_usage_signal.system_grant_sql}
                  aria-label="System query history grants"
                  className="mt-2 min-h-24 w-full resize-y rounded-md border border-default bg-surface p-2 font-mono text-xs text-primary"
                />
              </details>
            )}
          </div>
        )}

        <MvSuggestSection
          enabled={mvEnabled}
          onToggle={setMvEnabled}
          disabled={loading || hasActiveRun}
          proposalsLoading={mvProposalsLoading}
          proposals={mvProposals}
          selectedProposalIds={mvSelectedIds}
          onToggleProposal={toggleMvProposal}
          mode={mvMode}
          onModeChange={setMvMode}
          target={mvTarget}
          probe={mvProbe}
          probeLoading={mvProbeLoading}
          probeError={mvProbeError}
          onCopyGrant={handleCopyGrant}
        />

        {/* Alerts + launch — a full-width footer separated by a hairline so the
            CTA reads as the form's conclusion. */}
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
