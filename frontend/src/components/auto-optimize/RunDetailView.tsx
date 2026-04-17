import { useEffect, useState } from "react"
import { ArrowLeft, ChevronDown, ChevronRight, Cog, UserCheck, ExternalLink, Plus, Rocket, Trash2, Upload, CheckCircle2, Clock, AlertTriangle } from "lucide-react"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { ScoreSummary } from "@/components/auto-optimize/ScoreSummary"
import { QuestionList } from "@/components/auto-optimize/QuestionList"
import { QuestionDetail } from "@/components/auto-optimize/QuestionDetail"
import { PipelineDetailsModal } from "@/components/auto-optimize/PipelineDetailsModal"
import {
  getAutoOptimizeRun,
  getAutoOptimizeQuestionResults,
  deployOptimizationRun,
} from "@/lib/api"
import type { GSOPipelineRun, GSOQuestionDetail } from "@/types"

interface RunDetailViewProps {
  runId: string
  onBack: () => void
}

type EvalTab = "baseline" | "final"

const DEPLOY_STORAGE_KEY = "genie-workbench:deploy-config"
const TERMINAL_STATUSES = new Set(["CONVERGED", "STALLED", "MAX_ITERATIONS", "APPLIED"])

function loadDeployConfig(): { targetUrl: string; spaceId: string; catalogMappings: { source: string; target: string }[] } {
  try {
    const raw = localStorage.getItem(DEPLOY_STORAGE_KEY)
    if (raw) return JSON.parse(raw)
  } catch {}
  return { targetUrl: "", spaceId: "", catalogMappings: [] }
}

function saveDeployConfig(config: { targetUrl: string; spaceId: string; catalogMappings: { source: string; target: string }[] }) {
  localStorage.setItem(DEPLOY_STORAGE_KEY, JSON.stringify(config))
}

const STATUS_VARIANT: Record<string, "default" | "success" | "warning" | "danger" | "info" | "secondary"> = {
  CONVERGED: "success",
  APPLIED: "success",
  STALLED: "warning",
  MAX_ITERATIONS: "warning",
  FAILED: "danger",
  CANCELLED: "secondary",
  DISCARDED: "secondary",
  IN_PROGRESS: "info",
  RUNNING: "info",
  QUEUED: "secondary",
}

export function RunDetailView({ runId, onBack }: RunDetailViewProps) {
  const [run, setRun] = useState<GSOPipelineRun | null>(null)
  const [activeTab, setActiveTab] = useState<EvalTab>("final")
  const [baselineQuestions, setBaselineQuestions] = useState<GSOQuestionDetail[]>([])
  const [finalQuestions, setFinalQuestions] = useState<GSOQuestionDetail[]>([])
  const [selectedQuestionId, setSelectedQuestionId] = useState<string | null>(null)
  const [showPipeline, setShowPipeline] = useState(false)

  // Deploy dialog state
  const [deployOpen, setDeployOpen] = useState(false)
  const [deployTargetUrl, setDeployTargetUrl] = useState("")
  const [deploySpaceId, setDeploySpaceId] = useState("")
  const [deployCatalogMappings, setDeployCatalogMappings] = useState<{ source: string; target: string }[]>([])
  const [deploying, setDeploying] = useState(false)
  const [deployError, setDeployError] = useState<string | null>(null)
  const [deploySuccess, setDeploySuccess] = useState(false)

  useEffect(() => {
    getAutoOptimizeRun(runId)
      .then((r) => {
        setRun(r)
        const fetches: Promise<void>[] = []
        if (r.baselineIteration != null) {
          fetches.push(
            getAutoOptimizeQuestionResults(runId, r.baselineIteration)
              .then(setBaselineQuestions)
              .catch(() => {})
          )
        }
        if (r.bestIteration != null) {
          fetches.push(
            getAutoOptimizeQuestionResults(runId, r.bestIteration)
              .then(setFinalQuestions)
              .catch(() => {})
          )
        }
        return Promise.all(fetches)
      })
      .catch(() => {})
  }, [runId])

  useEffect(() => {
    setSelectedQuestionId(null)
  }, [activeTab])

  const questions = activeTab === "baseline" ? baselineQuestions : finalQuestions
  const selectedQuestion = questions.find((q) => q.question_id === selectedQuestionId) ?? null

  const nonExcluded = questions.filter((q) => q.passed !== null)
  const passingCount = nonExcluded.filter((q) => q.passed === true).length
  const totalCount = nonExcluded.length

  const baselineNonExcluded = baselineQuestions.filter((q) => q.passed !== null)
  const baselinePassing = baselineNonExcluded.filter((q) => q.passed === true).length
  const finalNonExcluded = finalQuestions.filter((q) => q.passed !== null)
  const finalPassing = finalNonExcluded.filter((q) => q.passed === true).length

  if (!run) {
    return <div className="py-8 text-center text-muted text-sm">Loading run details...</div>
  }

  const baselineAccuracy = baselineNonExcluded.length > 0
    ? Math.round((baselinePassing / baselineNonExcluded.length) * 100)
    : null
  const finalAccuracy = finalNonExcluded.length > 0
    ? Math.round((finalPassing / finalNonExcluded.length) * 100)
    : null

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <button
            onClick={onBack}
            className="p-2 rounded-lg border border-default hover:bg-elevated text-muted hover:text-primary transition-colors"
          >
            <ArrowLeft className="w-4 h-4" />
          </button>
          <div>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted">
                {run.startedAt ? new Date(run.startedAt).toLocaleDateString(undefined, {
                  month: "short", day: "numeric", year: "numeric",
                }) : ""}
              </span>
              <Badge variant={STATUS_VARIANT[run.status] ?? "secondary"}>
                {run.status}
              </Badge>
            </div>
            {totalCount > 0 && (
              <p className="text-lg font-semibold text-primary mt-1">
                {((passingCount / totalCount) * 100).toFixed(0)}% accurate ({passingCount}/{totalCount})
              </p>
            )}
          </div>
        </div>

        <button
          onClick={() => setShowPipeline(true)}
          className="p-2 rounded-lg border border-default hover:bg-elevated text-muted hover:text-primary transition-colors"
          title="Pipeline Details"
        >
          <Cog className="w-4 h-4" />
        </button>
      </div>

      {/* Score summary cards */}
      <ScoreSummary baselineScore={run.baselineScore} optimizedScore={run.optimizedScore} />

      {/* Human Review Banner */}
      {run.labelingSessionUrl && (
        <div className="flex items-start gap-3 rounded-lg border border-amber-500/30 bg-amber-500/5 px-4 py-3">
          <UserCheck className="mt-0.5 h-5 w-5 text-amber-600 shrink-0" />
          <div className="flex-1">
            <h3 className="text-sm font-semibold text-amber-900 dark:text-amber-300">
              Questions flagged for human review
            </h3>
            <p className="mt-0.5 text-xs text-amber-700 dark:text-amber-400">
              The optimizer identified questions that could not be resolved automatically and flagged them for your review.
            </p>
          </div>
          <a
            href={run.labelingSessionUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="shrink-0 inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium rounded-md border border-amber-500/30 text-amber-700 dark:text-amber-300 hover:bg-amber-500/10 transition-colors"
          >
            Open Review Session
            <ExternalLink className="h-3.5 w-3.5" />
          </a>
        </div>
      )}

      {/* Deployment Status Banner (when already deployed) */}
      {run.deployTarget && run.deploymentStatus && (
        <div className={`flex items-start gap-3 rounded-lg border px-4 py-3 ${
          run.deploymentStatus === "DEPLOYED"
            ? "border-emerald-500/30 bg-emerald-500/5"
            : run.deploymentStatus === "FAILED"
              ? "border-red-500/30 bg-red-500/5"
              : "border-blue-500/30 bg-blue-500/5"
        }`}>
          {run.deploymentStatus === "DEPLOYED" ? (
            <CheckCircle2 className="mt-0.5 h-5 w-5 text-emerald-600 shrink-0" />
          ) : run.deploymentStatus === "FAILED" ? (
            <AlertTriangle className="mt-0.5 h-5 w-5 text-red-600 shrink-0" />
          ) : (
            <Clock className="mt-0.5 h-5 w-5 text-blue-600 shrink-0" />
          )}
          <div className="flex-1">
            <h3 className={`text-sm font-semibold ${
              run.deploymentStatus === "DEPLOYED" ? "text-emerald-900 dark:text-emerald-300"
                : run.deploymentStatus === "FAILED" ? "text-red-900 dark:text-red-300"
                : "text-blue-900 dark:text-blue-300"
            }`}>
              {run.deploymentStatus === "DEPLOYED" ? "Deployed to target workspace"
                : run.deploymentStatus === "PENDING_APPROVAL" ? "Deployment awaiting approval"
                : run.deploymentStatus === "FAILED" ? "Deployment failed"
                : `Deployment: ${run.deploymentStatus}`}
            </h3>
            <p className="mt-0.5 text-xs text-muted">Target: {run.deployTarget}</p>
          </div>
          {run.deploymentStatus === "DEPLOYED" && (
            <a href={run.deployTarget} target="_blank" rel="noopener noreferrer"
              className="shrink-0 inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium rounded-md border border-emerald-500/30 text-emerald-700 dark:text-emerald-300 hover:bg-emerald-500/10 transition-colors">
              Open Workspace <ExternalLink className="h-3.5 w-3.5" />
            </a>
          )}
        </div>
      )}

      {/* Deploy to Workspace — on-demand for completed runs */}
      {TERMINAL_STATUSES.has(run.status) && !run.deploymentStatus && (
        <div className="border border-default rounded-lg">
          <button type="button"
            onClick={() => {
              if (!deployOpen) {
                const saved = loadDeployConfig()
                setDeployTargetUrl(saved.targetUrl)
                setDeploySpaceId(saved.spaceId)
                setDeployCatalogMappings(saved.catalogMappings)
              }
              setDeployOpen(!deployOpen)
            }}
            className="w-full flex items-center gap-2 px-4 py-3 text-sm font-medium text-secondary hover:text-primary transition-colors"
          >
            {deployOpen ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
            <Upload className="w-4 h-4" />
            Deploy to Workspace
            {deploySuccess && <span className="ml-2 text-xs text-emerald-600 font-normal">Triggered</span>}
          </button>
          {deployOpen && (
            <div className="px-4 pb-4 space-y-3 border-t border-default pt-3">
              <p className="text-xs text-muted">
                Deploy this optimized config to another workspace. Previous settings are remembered.
              </p>
              <div className="space-y-1">
                <label className="text-xs font-medium text-muted">Target workspace URL</label>
                <input type="url" value={deployTargetUrl}
                  onChange={(e) => setDeployTargetUrl(e.target.value)}
                  placeholder="https://my-prod-workspace.cloud.databricks.com"
                  className="w-full px-3 py-1.5 text-sm border border-default rounded-lg bg-elevated text-primary placeholder:text-muted focus:outline-none focus:ring-1 focus:ring-accent" />
              </div>
              <div className="space-y-1">
                <label className="text-xs font-medium text-muted">Target space ID <span className="font-normal">(optional)</span></label>
                <input type="text" value={deploySpaceId}
                  onChange={(e) => setDeploySpaceId(e.target.value)}
                  placeholder="01f1347d7f1516ceaea7e5853166498f"
                  className="w-full px-3 py-1.5 text-sm border border-default rounded-lg bg-elevated text-primary placeholder:text-muted focus:outline-none focus:ring-1 focus:ring-accent" />
              </div>
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <label className="text-xs font-medium text-muted">Catalog mapping <span className="font-normal">(source → target)</span></label>
                  <button type="button"
                    onClick={() => setDeployCatalogMappings([...deployCatalogMappings, { source: "", target: "" }])}
                    className="flex items-center gap-1 text-xs text-accent hover:text-accent/80 transition-colors">
                    <Plus className="w-3 h-3" /> Add mapping
                  </button>
                </div>
                {deployCatalogMappings.map((m, i) => (
                  <div key={i} className="flex items-center gap-2">
                    <input type="text" value={m.source}
                      onChange={(e) => { const next = [...deployCatalogMappings]; next[i] = { ...next[i], source: e.target.value }; setDeployCatalogMappings(next) }}
                      placeholder="dev_catalog"
                      className="flex-1 px-3 py-1.5 text-sm border border-default rounded-lg bg-elevated text-primary placeholder:text-muted focus:outline-none focus:ring-1 focus:ring-accent" />
                    <span className="text-xs text-muted">→</span>
                    <input type="text" value={m.target}
                      onChange={(e) => { const next = [...deployCatalogMappings]; next[i] = { ...next[i], target: e.target.value }; setDeployCatalogMappings(next) }}
                      placeholder="prod_catalog"
                      className="flex-1 px-3 py-1.5 text-sm border border-default rounded-lg bg-elevated text-primary placeholder:text-muted focus:outline-none focus:ring-1 focus:ring-accent" />
                    <button type="button" onClick={() => setDeployCatalogMappings(deployCatalogMappings.filter((_, j) => j !== i))}
                      className="text-muted hover:text-danger transition-colors">
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  </div>
                ))}
                {deployCatalogMappings.length === 0 && (
                  <p className="text-xs text-muted italic">No catalog mappings — table references will be used as-is</p>
                )}
              </div>
              {deployError && (
                <div className="rounded-lg bg-danger/10 border border-danger/20 px-3 py-2 text-xs text-danger">{deployError}</div>
              )}
              {deploySuccess && (
                <div className="rounded-lg bg-emerald-500/10 border border-emerald-500/20 px-3 py-2 text-xs text-emerald-700 dark:text-emerald-300">
                  Deployment job triggered successfully.
                </div>
              )}
              <button
                onClick={async () => {
                  if (!deployTargetUrl.trim()) { setDeployError("Target workspace URL is required"); return }
                  setDeploying(true); setDeployError(null); setDeploySuccess(false)
                  const catalogMap = deployCatalogMappings.reduce<Record<string, string>>((acc, m) => {
                    if (m.source.trim() && m.target.trim()) acc[m.source.trim()] = m.target.trim()
                    return acc
                  }, {})
                  try {
                    await deployOptimizationRun(runId, {
                      target_workspace_url: deployTargetUrl.trim(),
                      target_space_id: deploySpaceId.trim() || undefined,
                      catalog_map: Object.keys(catalogMap).length > 0 ? catalogMap : undefined,
                    })
                    saveDeployConfig({ targetUrl: deployTargetUrl.trim(), spaceId: deploySpaceId.trim(), catalogMappings: deployCatalogMappings })
                    setDeploySuccess(true)
                  } catch (e) {
                    setDeployError(e instanceof Error ? e.message : "Deployment failed")
                  } finally {
                    setDeploying(false)
                  }
                }}
                disabled={deploying || !deployTargetUrl.trim()}
                className="flex items-center gap-2 px-4 py-2 rounded-lg bg-accent text-white font-medium text-sm hover:bg-accent/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                <Rocket className="w-4 h-4" />
                {deploying ? "Deploying..." : "Deploy"}
              </button>
            </div>
          )}
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1 border-b border-default">
        <button
          onClick={() => setActiveTab("baseline")}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
            activeTab === "baseline"
              ? "border-accent text-accent"
              : "border-transparent text-muted hover:text-primary"
          }`}
        >
          Baseline Evaluation
          {baselineAccuracy != null && (
            <span className="ml-2 text-xs opacity-75">({baselineAccuracy}%)</span>
          )}
        </button>
        <button
          onClick={() => setActiveTab("final")}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
            activeTab === "final"
              ? "border-accent text-accent"
              : "border-transparent text-muted hover:text-primary"
          }`}
        >
          Final Evaluation
          {finalAccuracy != null && (
            <span className="ml-2 text-xs opacity-75">({finalAccuracy}%)</span>
          )}
        </button>
      </div>

      {/* Two-column layout */}
      <div className="grid grid-cols-3 gap-4 min-h-[400px]">
        {/* Left sidebar: Question list */}
        <Card className="col-span-1">
          <CardContent className="p-4 h-full">
            {questions.length === 0 ? (
              <div className="flex items-center justify-center h-full text-muted text-sm">
                No evaluation results available
              </div>
            ) : (
              <QuestionList
                questions={questions}
                selectedId={selectedQuestionId}
                onSelect={setSelectedQuestionId}
              />
            )}
          </CardContent>
        </Card>

        {/* Right: Question detail */}
        <Card className="col-span-2">
          <CardContent className="p-6">
            <QuestionDetail question={selectedQuestion} />
          </CardContent>
        </Card>
      </div>

      {/* Pipeline Details Modal */}
      <PipelineDetailsModal runId={runId} isOpen={showPipeline} onClose={() => setShowPipeline(false)} />
    </div>
  )
}
