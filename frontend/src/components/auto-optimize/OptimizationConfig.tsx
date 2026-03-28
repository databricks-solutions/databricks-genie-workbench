import { useState } from "react"
import { AlertTriangle, Rocket } from "lucide-react"
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { triggerAutoOptimize } from "@/lib/api"
import { PermissionAlert } from "@/components/auto-optimize/PermissionAlert"
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

const LEVERS = [
  { id: 1, name: "Tables & Columns", description: "Update table descriptions, column descriptions, and synonyms" },
  { id: 2, name: "Metric Views", description: "Update metric view column descriptions" },
  { id: 3, name: "Table-Valued Functions", description: "Remove underperforming TVFs" },
  { id: 4, name: "Join Specifications", description: "Add, update, or remove join relationships" },
  { id: 5, name: "Genie Space Instructions", description: "Rewrite global routing instructions" },
  { id: 6, name: "SQL Expressions", description: "Add reusable SQL expressions (measures, filters, dimensions)" },
]

export function OptimizationConfig({ spaceId, onStarted, onTriggerStart, onTriggerError, hasActiveRun, permissions, permsLoading, healthIssues, onRefreshPermissions }: OptimizationConfigProps) {
  const [selectedLevers, setSelectedLevers] = useState<Set<number>>(new Set(LEVERS.map((l) => l.id)))
  const [applyMode] = useState<"genie_config" | "both">("genie_config")
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const hasHealthIssues = (healthIssues?.length ?? 0) > 0
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
    setLoading(true)
    setError(null)
    onTriggerStart?.()
    try {
      const result = await triggerAutoOptimize({
        space_id: spaceId,
        apply_mode: applyMode,
        levers: Array.from(selectedLevers).sort(),
      })
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
            <div className="inline-flex rounded-lg border border-default p-1">
              <button
                className="px-3 py-1.5 rounded-md text-sm font-medium bg-accent text-white"
              >
                Config Only
              </button>
              <div className="relative">
                <button
                  disabled
                  className="px-3 py-1.5 rounded-md text-sm font-medium text-muted cursor-not-allowed opacity-50"
                >
                  Config + UC Write Backs
                </button>
                <span className="absolute -top-2 -right-2 rounded-full bg-amber-100 text-amber-700 text-[10px] font-medium px-1.5 py-0.5 border border-amber-200">
                  Coming soon
                </span>
              </div>
            </div>
            <p className="text-xs text-muted max-w-xs">
              Changes will be applied only to the selected Genie Space configuration. Underlying Unity Catalog tables, columns, and descriptions will not be modified.
            </p>
          </div>

          {/* Lever Selection */}
          <div className="space-y-2">
            <p className="text-xs font-medium text-muted">What will be optimized</p>
            <div className="space-y-2">
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
          disabled={loading || hasActiveRun || selectedLevers.size === 0 || !canStart}
          className="flex items-center gap-2 px-5 py-2.5 rounded-lg bg-accent text-white font-semibold hover:bg-accent/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          title={!canStart ? "Required permissions are missing" : undefined}
        >
          <Rocket className="w-4 h-4" />
          {loading ? "Starting..." : "Start Optimization"}
        </button>
      </CardContent>
    </Card>
  )
}
