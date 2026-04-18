/**
 * DeployTab — Cross-workspace deployment for Genie Spaces.
 * Deploys the current space config to a target workspace with optional catalog remapping.
 * Settings are remembered via localStorage.
 */
import { useState, useEffect } from "react"
import { CheckCircle2, ExternalLink, Plus, Rocket, Trash2, Upload } from "lucide-react"
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card"
import { deploySpace } from "@/lib/api"

const DEPLOY_STORAGE_KEY = "genie-workbench:deploy-config"

interface DeployConfig {
  targetUrl: string
  spaceId: string
  catalogMappings: { source: string; target: string }[]
}

function loadDeployConfig(): DeployConfig {
  try {
    const raw = localStorage.getItem(DEPLOY_STORAGE_KEY)
    if (raw) return JSON.parse(raw)
  } catch {}
  return { targetUrl: "", spaceId: "", catalogMappings: [] }
}

function saveDeployConfig(config: DeployConfig) {
  localStorage.setItem(DEPLOY_STORAGE_KEY, JSON.stringify(config))
}

interface DeployTabProps {
  spaceId: string
}

export function DeployTab({ spaceId }: DeployTabProps) {
  const [targetUrl, setTargetUrl] = useState("")
  const [targetSpaceId, setTargetSpaceId] = useState("")
  const [catalogMappings, setCatalogMappings] = useState<{ source: string; target: string }[]>([])

  const [deploying, setDeploying] = useState(false)
  const [deployError, setDeployError] = useState<string | null>(null)
  const [deploySuccess, setDeploySuccess] = useState<{ targetUrl: string; targetSpaceId: string; spaceUrl?: string } | null>(null)

  // Load config from localStorage on mount
  useEffect(() => {
    const saved = loadDeployConfig()
    setTargetUrl(saved.targetUrl)
    setTargetSpaceId(saved.spaceId)
    setCatalogMappings(saved.catalogMappings)
  }, [])

  async function handleDeploy() {
    if (!targetUrl.trim()) return
    setDeploying(true)
    setDeployError(null)
    setDeploySuccess(null)

    const catalogMap = catalogMappings.reduce<Record<string, string>>((acc, m) => {
      if (m.source.trim() && m.target.trim()) acc[m.source.trim()] = m.target.trim()
      return acc
    }, {})

    try {
      const result = await deploySpace(spaceId, {
        target_workspace_url: targetUrl.trim(),
        target_space_id: targetSpaceId.trim() || undefined,
        catalog_map: Object.keys(catalogMap).length > 0 ? catalogMap : undefined,
      })
      saveDeployConfig({ targetUrl: targetUrl.trim(), spaceId: targetSpaceId.trim(), catalogMappings })
      setDeploySuccess({ targetUrl: result.targetUrl, targetSpaceId: result.targetSpaceId, spaceUrl: result.spaceUrl })
    } catch (e) {
      const msg = e instanceof Error ? e.message : typeof e === "object" ? JSON.stringify(e) : String(e)
      setDeployError(msg || "Deployment failed")
    } finally {
      setDeploying(false)
    }
  }

  return (
    <div className="space-y-6">
      {/* Deployment Config */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Upload className="w-5 h-5" />
            Deploy to Workspace
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-xs text-muted">
            Deploy this Genie Space's current config to a target workspace. Catalog references are remapped automatically. Settings are remembered for next time.
          </p>

          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-1">
              <label className="text-xs font-medium text-muted">Target workspace URL</label>
              <input
                type="url"
                value={targetUrl}
                onChange={(e) => setTargetUrl(e.target.value)}
                placeholder="https://my-prod-workspace.cloud.databricks.com"
                className="w-full px-3 py-1.5 text-sm border border-default rounded-lg bg-elevated text-primary placeholder:text-muted focus:outline-none focus:ring-1 focus:ring-accent"
              />
            </div>
            <div className="space-y-1">
              <label className="text-xs font-medium text-muted">
                Target space ID <span className="font-normal">(optional — updates existing space)</span>
              </label>
              <input
                type="text"
                value={targetSpaceId}
                onChange={(e) => setTargetSpaceId(e.target.value)}
                placeholder="01f1347d7f1516ceaea7e5853166498f"
                className="w-full px-3 py-1.5 text-sm border border-default rounded-lg bg-elevated text-primary placeholder:text-muted focus:outline-none focus:ring-1 focus:ring-accent"
              />
            </div>
          </div>

          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <label className="text-xs font-medium text-muted">
                Catalog mapping <span className="font-normal">(source → target)</span>
              </label>
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
        </CardContent>
      </Card>

      {/* Error */}
      {deployError && (
        <div className="rounded-lg bg-danger/10 border border-danger/20 px-4 py-3 text-sm text-danger">
          {deployError}
        </div>
      )}

      {/* Success */}
      {deploySuccess && (
        <div className="flex items-start gap-3 rounded-lg border border-emerald-500/30 bg-emerald-500/5 px-4 py-3">
          <CheckCircle2 className="mt-0.5 h-5 w-5 text-emerald-600 shrink-0" />
          <div className="flex-1">
            <h3 className="text-sm font-semibold text-emerald-900 dark:text-emerald-300">
              Deployed successfully
            </h3>
            <p className="mt-0.5 text-xs text-muted">
              Space config deployed to {deploySuccess.targetUrl} (space: {deploySuccess.targetSpaceId})
            </p>
          </div>
          <a
            href={deploySuccess.spaceUrl || deploySuccess.targetUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="shrink-0 inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium rounded-md border border-emerald-500/30 text-emerald-700 dark:text-emerald-300 hover:bg-emerald-500/10 transition-colors"
          >
            {deploySuccess.spaceUrl ? "Open Genie Space" : "Open Workspace"}
            <ExternalLink className="h-3.5 w-3.5" />
          </a>
        </div>
      )}

      {/* Deploy button */}
      <button
        onClick={handleDeploy}
        disabled={deploying || !targetUrl.trim()}
        className="flex items-center gap-2 px-5 py-2.5 rounded-lg bg-accent text-white font-semibold hover:bg-accent/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
      >
        <Rocket className="w-4 h-4" />
        {deploying ? "Deploying..." : "Deploy to Target Workspace"}
      </button>
    </div>
  )
}
