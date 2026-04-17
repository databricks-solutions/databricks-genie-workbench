import { useState } from "react"
import { AlertTriangle, ChevronDown, ChevronRight, Plus, Rocket, Trash2, Upload } from "lucide-react"
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
  { id: 3, name: "SQL Queries & Functions", description: "Add and update example SQLs, and remove underperforming TVFs" },
  { id: 4, name: "Join Specifications", description: "Add, update, or remove join relationships" },
  { id: 5, name: "Text Instructions", description: "Rewrite global routing instructions" },
  { id: 6, name: "SQL Expressions", description: "Add reusable SQL expressions (measures, filters, dimensions)" },
]

export function OptimizationConfig({ spaceId, onStarted, onTriggerStart, onTriggerError, hasActiveRun, permissions, permsLoading, healthIssues, onRefreshPermissions }: OptimizationConfigProps) {
  const [selectedLevers, setSelectedLevers] = useState<Set<number>>(new Set(LEVERS.map((l) => l.id)))
  const [applyMode] = useState<"genie_config" | "both">("genie_config")
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Deployment config
  const [deployExpanded, setDeployExpanded] = useState(false)
  const [deployTarget, setDeployTarget] = useState("")
  const [deploySpaceId, setDeploySpaceId] = useState("")
  const [catalogMappings, setCatalogMappings] = useState<{ source: string; target: string }[]>([])

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
      const catalogMap = catalogMappings.reduce<Record<string, string>>((acc, m) => {
        if (m.source.trim() && m.target.trim()) acc[m.source.trim()] = m.target.trim()
        return acc
      }, {})

      const result = await triggerAutoOptimize({
        space_id: spaceId,
        apply_mode: applyMode,
        levers: Array.from(selectedLevers).sort(),
        deploy_target: deployTarget.trim() || undefined,
        deploy_space_id: deploySpaceId.trim() || undefined,
        catalog_map: Object.keys(catalogMap).length > 0 ? catalogMap : undefined,
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
            <span className="inline-block px-3 py-1.5 rounded-lg border border-default text-sm font-medium bg-accent text-white">
              Config Only
            </span>
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

        {/* Deployment (collapsible) */}
        <div className="border border-default rounded-lg">
          <button
            type="button"
            onClick={() => setDeployExpanded(!deployExpanded)}
            className="w-full flex items-center gap-2 px-4 py-3 text-sm font-medium text-secondary hover:text-primary transition-colors"
          >
            {deployExpanded ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
            <Upload className="w-4 h-4" />
            Cross-Workspace Deployment
            {deployTarget && <span className="ml-2 text-xs text-accent font-normal">Configured</span>}
          </button>
          {deployExpanded && (
            <div className="px-4 pb-4 space-y-3 border-t border-default pt-3">
              <p className="text-xs text-muted">
                After optimization, deploy the optimized config to a target workspace. Leave blank to skip deployment.
              </p>

              <div className="space-y-1">
                <label className="text-xs font-medium text-muted">Target workspace URL</label>
                <input
                  type="url"
                  value={deployTarget}
                  onChange={(e) => setDeployTarget(e.target.value)}
                  placeholder="https://my-prod-workspace.cloud.databricks.com"
                  className="w-full px-3 py-1.5 text-sm border border-default rounded-lg bg-elevated text-primary placeholder:text-muted focus:outline-none focus:ring-1 focus:ring-accent"
                />
              </div>

              <div className="space-y-1">
                <label className="text-xs font-medium text-muted">Target space ID <span className="text-muted font-normal">(optional — creates new if blank)</span></label>
                <input
                  type="text"
                  value={deploySpaceId}
                  onChange={(e) => setDeploySpaceId(e.target.value)}
                  placeholder="01f1347d7f1516ceaea7e5853166498f"
                  className="w-full px-3 py-1.5 text-sm border border-default rounded-lg bg-elevated text-primary placeholder:text-muted focus:outline-none focus:ring-1 focus:ring-accent"
                />
              </div>

              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <label className="text-xs font-medium text-muted">Catalog mapping <span className="text-muted font-normal">(source → target)</span></label>
                  <button
                    type="button"
                    onClick={() => setCatalogMappings([...catalogMappings, { source: "", target: "" }])}
                    className="flex items-center gap-1 text-xs text-accent hover:text-accent/80 transition-colors"
                  >
                    <Plus className="w-3 h-3" /> Add mapping
                  </button>
                </div>
                {catalogMappings.map((m, i) => (
                  <div key={i} className="flex items-center gap-2">
                    <input
                      type="text"
                      value={m.source}
                      onChange={(e) => {
                        const next = [...catalogMappings]
                        next[i] = { ...next[i], source: e.target.value }
                        setCatalogMappings(next)
                      }}
                      placeholder="dev_catalog"
                      className="flex-1 px-3 py-1.5 text-sm border border-default rounded-lg bg-elevated text-primary placeholder:text-muted focus:outline-none focus:ring-1 focus:ring-accent"
                    />
                    <span className="text-xs text-muted">→</span>
                    <input
                      type="text"
                      value={m.target}
                      onChange={(e) => {
                        const next = [...catalogMappings]
                        next[i] = { ...next[i], target: e.target.value }
                        setCatalogMappings(next)
                      }}
                      placeholder="prod_catalog"
                      className="flex-1 px-3 py-1.5 text-sm border border-default rounded-lg bg-elevated text-primary placeholder:text-muted focus:outline-none focus:ring-1 focus:ring-accent"
                    />
                    <button
                      type="button"
                      onClick={() => setCatalogMappings(catalogMappings.filter((_, j) => j !== i))}
                      className="text-muted hover:text-danger transition-colors"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  </div>
                ))}
                {catalogMappings.length === 0 && (
                  <p className="text-xs text-muted italic">No catalog mappings — table references will be used as-is</p>
                )}
              </div>
            </div>
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
