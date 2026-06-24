/**
 * SpaceDetail - 3-tab detail view for a Genie Space.
 * Tabs: Score (default) | Optimize | History
 */
import { useState, useEffect, useRef } from "react"
import { ArrowLeft, Star, BarChart2, Clock, ExternalLink, Rocket, Play, ChevronDown, ChevronRight, Settings, RefreshCw, Package } from "lucide-react"
import { scanSpace, toggleStar, getSpaceHistory, getSpaceDetail, getActiveRunForSpace, exportSpaceBundle } from "@/lib/api"
import { MATURITY_COLORS, getOptimizationLabel } from "@/lib/utils"
import type { ScanResult, ScoreHistoryPoint, OptimizationEvent } from "@/types"
import { IQScoreTab } from "./IQScoreTab"
import { HistoryTab } from "./HistoryTab"
import { useAnalysis } from "@/hooks/useAnalysis"
import { SpaceOverview } from "@/components/SpaceOverview"
import { AutoOptimizeTab } from "@/components/auto-optimize/AutoOptimizeTab"
import { Input } from "@/components/ui/input"
import {
  AlertDialog,
  AlertDialogContent,
  AlertDialogHeader,
  AlertDialogFooter,
  AlertDialogTitle,
  AlertDialogDescription,
  AlertDialogAction,
  AlertDialogCancel,
} from "@/components/ui/alert-dialog"

type Tab = "score" | "optimize" | "history"
const VALID_TABS: readonly string[] = ["score", "optimize", "history"]

interface SpaceDetailProps {
  spaceId: string
  displayName: string
  spaceUrl?: string
  initialTab?: string
  autoScan?: boolean
  onBack: () => void
}

export function SpaceDetail({ spaceId, displayName, spaceUrl, initialTab, autoScan, onBack }: SpaceDetailProps) {
  const [activeTab, setActiveTab] = useState<Tab>(initialTab && VALID_TABS.includes(initialTab) ? initialTab as Tab : "score")
  const [scanResult, setScanResult] = useState<ScanResult | null>(null)
  const [isStarred, setIsStarred] = useState(false)
  const [isScanning, setIsScanning] = useState(false)
  const [history, setHistory] = useState<ScoreHistoryPoint[]>([])
  const [optimizationEvents, setOptimizationEvents] = useState<OptimizationEvent[]>([])
  const [hasActiveOptRun, setHasActiveOptRun] = useState(false)
  const [isLoadingScan, setIsLoadingScan] = useState(true)
  const [isLoadingHistory, setIsLoadingHistory] = useState(false)

  const [configExpanded, setConfigExpanded] = useState(false)

  const [isExporting, setIsExporting] = useState(false)
  const [exportMsg, setExportMsg] = useState<string | null>(null)
  // Export-as-bundle dialog: collects the optional prod-target details that get
  // baked into the bundle's databricks.yml. Leave them blank for a dev-only bundle.
  const [exportDialogOpen, setExportDialogOpen] = useState(false)
  const [prodHost, setProdHost] = useState("")
  const [prodCatalog, setProdCatalog] = useState("")
  const [prodSchema, setProdSchema] = useState("")
  const [prodWarehouseId, setProdWarehouseId] = useState("")

  const { state, actions } = useAnalysis()

  // Guard against getSpaceDetail overwriting a fresh scan result
  const freshScanDoneRef = useRef(false)

  // Load space data + persisted score on mount
  useEffect(() => {
    freshScanDoneRef.current = false
    setIsLoadingScan(true)
    if (spaceId) {
      actions.handleFetchSpace(spaceId)
      // Load latest persisted scan result (skip if a fresh scan already completed)
      getSpaceDetail(spaceId)
        .then((detail) => {
          setIsStarred(detail.is_starred)
          if (detail.scan_result && !freshScanDoneRef.current) {
            setScanResult({
              space_id: spaceId,
              score: detail.scan_result.score,
              total: detail.scan_result.total ?? 12,
              maturity: detail.scan_result.maturity,
              optimization_accuracy: detail.scan_result.optimization_accuracy ?? null,
              checks: detail.scan_result.checks ?? [],
              findings: detail.scan_result.findings ?? [],
              next_steps: detail.scan_result.next_steps ?? [],
              warnings: detail.scan_result.warnings ?? [],
              warning_next_steps: detail.scan_result.warning_next_steps ?? [],
              scanned_at: detail.scan_result.scanned_at ?? "",
            })
          }
        })
        .catch((e) => console.error("Failed to load space detail:", e))
        .finally(() => setIsLoadingScan(false))
    }
  }, [spaceId])

  useEffect(() => {
    getActiveRunForSpace(spaceId)
      .then((res) => setHasActiveOptRun(res.hasActiveRun))
      .catch(() => {})
  }, [spaceId])

  const handleScan = async () => {
    setIsScanning(true)
    try {
      const result = await scanSpace(spaceId)
      freshScanDoneRef.current = true
      setScanResult(result)
    } catch (e) {
      console.error("Scan failed:", e)
    } finally {
      setIsScanning(false)
    }
  }

  const handleRescanFromOptimize = () => {
    setActiveTab("score")
    handleScan()
  }

  const handleToggleStar = async () => {
    const newStarred = !isStarred
    setIsStarred(newStarred)
    try {
      await toggleStar(spaceId, newStarred)
    } catch {
      setIsStarred(!newStarred)
    }
  }

  // How many of the four prod fields are filled — used to enforce all-or-nothing.
  const prodFilledCount = [prodHost, prodCatalog, prodSchema, prodWarehouseId].filter(
    (v) => v.trim()
  ).length

  const handleExportBundle = async () => {
    // Prod target is all-or-nothing: either supply all four or none (dev-only).
    if (prodFilledCount > 0 && prodFilledCount < 4) {
      setExportMsg("Fill in all four prod fields, or leave them all blank for a dev-only bundle.")
      return
    }
    setIsExporting(true)
    setExportMsg(null)
    try {
      const hasProd = prodFilledCount === 4
      const { tables, multiPrefix } = await exportSpaceBundle(
        spaceId,
        hasProd
          ? {
              prodHost: prodHost.trim(),
              prodCatalog: prodCatalog.trim(),
              prodSchema: prodSchema.trim(),
              prodWarehouseId: prodWarehouseId.trim(),
            }
          : undefined
      )
      const target = hasProd ? "dev + prod targets" : "dev target only"
      setExportMsg(
        multiPrefix
          ? `Exported ${tables} tables (${target}). Note: multiple catalog.schema prefixes detected — only the dominant one was parameterized.`
          : `Exported ${tables} tables (${target}).`
      )
      setExportDialogOpen(false)
    } catch (e) {
      console.error("Export failed:", e)
      setExportMsg(e instanceof Error ? `Export failed: ${e.message}` : "Export failed")
    } finally {
      setIsExporting(false)
      setTimeout(() => setExportMsg(null), 8000)
    }
  }

  // Auto-scan on mount when requested (e.g., returning from create/update flows)
  useEffect(() => {
    if (autoScan && !isScanning) {
      handleScan()
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (activeTab === "history") {
      setIsLoadingHistory(true)
      getSpaceHistory(spaceId)
        .then(({ scans, optimization_events }) => {
          setHistory(scans)
          setOptimizationEvents(optimization_events)
        })
        .catch(console.error)
        .finally(() => setIsLoadingHistory(false))
    }
  }, [activeTab, spaceId])

  const tabs: { id: Tab; label: string; icon: React.ReactNode }[] = [
    { id: "score", label: "Score", icon: <BarChart2 className="w-4 h-4" /> },
    { id: "optimize", label: "Optimize", icon: <Rocket className="w-4 h-4" /> },
    { id: "history", label: "History", icon: <Clock className="w-4 h-4" /> },
  ]

  // Determine contextual action(s) based on scan results
  const hasRemediationItems = scanResult && scanResult.maturity !== "Trusted" && (
    scanResult.findings.length > 0 || (scanResult.warnings ?? []).length > 0
  )
  const maturity = scanResult?.maturity
  let actionProps: { onAction?: () => void; actionLabel?: string; actionIcon?: React.ReactNode; actionDescription?: React.ReactNode } = {}
  if (maturity === "Ready to Optimize") {
    // No failing config checks left — show optimization CTA.
    actionProps = {
      onAction: () => setActiveTab("optimize"),
      actionLabel: "Run Optimization",
      actionIcon: <Rocket className="w-4 h-4" />,
      actionDescription: (
        <>
          This space passed the configuration checks. Auto-Optimize will benchmark real
          questions, tune the selected levers, and apply only changes that improve the
          measured result.
        </>
      ),
    }
  } else if (hasRemediationItems) {
    actionProps = {
      onAction: () => setActiveTab("optimize"),
      actionLabel: "Open Optimize",
      actionIcon: <Rocket className="w-4 h-4" />,
      actionDescription: (
        <>
          Auto-Optimize is the recommended path for improving spaces that fail IQ checks.
          It runs benchmarks and applies validated configuration changes. Some issues, such
          as missing data sources, permissions, or bulk Unity Catalog metadata gaps, may
          still require manual setup.
        </>
      ),
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start gap-4">
        <button
          onClick={onBack}
          className="mt-1 p-2 rounded-lg border border-default hover:bg-surface-secondary text-muted hover:text-secondary transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
        </button>
        <div className="flex-1">
          <div className="flex items-center gap-3">
            <h2 className="text-2xl font-display font-bold text-primary">{displayName}</h2>
            <button onClick={handleToggleStar}>
              <Star className={`w-5 h-5 ${isStarred ? "fill-amber-400 text-amber-400" : "text-muted hover:text-amber-400"} transition-colors`} />
            </button>
            <button
              onClick={() => { setExportMsg(null); setExportDialogOpen(true) }}
              title="Export this space as a parameterized Databricks Asset Bundle (.zip)"
              className="ml-auto inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-default text-sm font-medium text-secondary hover:bg-surface-secondary hover:text-accent transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Package className="w-4 h-4" />
              Export as bundle
            </button>
          </div>
          {exportMsg && (
            <div className="mt-2 text-xs px-3 py-1.5 rounded-lg bg-surface-secondary text-secondary border border-default">
              {exportMsg}
            </div>
          )}

          <AlertDialog open={exportDialogOpen} onOpenChange={setExportDialogOpen}>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Export as Databricks Asset Bundle</AlertDialogTitle>
                <AlertDialogDescription>
                  Downloads a <code>.zip</code> bundle with every table reference
                  parameterized as <code>${'{'}var.catalog{'}'}.${'{'}var.schema{'}'}</code>. The
                  <strong> dev</strong> target points back at this workspace automatically.
                  To make the bundle deployable to another workspace, fill in the
                  <strong> prod</strong> target below — all four fields, or leave them all
                  blank for a dev-only bundle.
                </AlertDialogDescription>
              </AlertDialogHeader>

              <div className="mt-4 space-y-3 text-left">
                <div>
                  <label className="text-sm font-medium text-secondary">Prod workspace host</label>
                  <Input
                    value={prodHost}
                    onChange={(e) => setProdHost(e.target.value)}
                    placeholder="https://your-prod-workspace.cloud.databricks.com"
                  />
                </div>
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="text-sm font-medium text-secondary">Prod catalog</label>
                    <Input
                      value={prodCatalog}
                      onChange={(e) => setProdCatalog(e.target.value)}
                      placeholder="prod_catalog"
                    />
                  </div>
                  <div>
                    <label className="text-sm font-medium text-secondary">Prod schema</label>
                    <Input
                      value={prodSchema}
                      onChange={(e) => setProdSchema(e.target.value)}
                      placeholder="prod_schema"
                    />
                  </div>
                </div>
                <div>
                  <label className="text-sm font-medium text-secondary">Prod SQL warehouse ID</label>
                  <Input
                    value={prodWarehouseId}
                    onChange={(e) => setProdWarehouseId(e.target.value)}
                    placeholder="0123456789abcdef"
                  />
                </div>
                <p className="text-xs text-muted">
                  Genie Workbench runs in one workspace, so it can&apos;t deploy across a
                  boundary itself. It generates the bundle; you deploy it to the prod
                  workspace with <code>databricks bundle deploy -t prod</code> using your own
                  prod credentials. The included README has the exact steps.
                </p>
              </div>

              <AlertDialogFooter>
                <AlertDialogCancel onClick={() => setExportDialogOpen(false)} disabled={isExporting}>
                  Cancel
                </AlertDialogCancel>
                <AlertDialogAction
                  onClick={handleExportBundle}
                  disabled={isExporting}
                  className="disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {isExporting
                    ? "Exporting…"
                    : prodFilledCount === 4
                    ? "Export dev + prod bundle"
                    : "Export dev-only bundle"}
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
          <div className="flex items-center gap-3 mt-2">
            {scanResult ? (
              <>
                <span className={`text-xs font-medium px-2 py-0.5 rounded-full border ${MATURITY_COLORS[scanResult.maturity]?.badge ?? "bg-surface-secondary text-muted border-default"}`}>
                  {scanResult.maturity}
                </span>
                <span className="text-muted text-sm">
                  {scanResult.score}/{scanResult.total} checks · {getOptimizationLabel(scanResult.optimization_accuracy)}
                </span>
              </>
            ) : (
              <span className="text-muted text-sm">Not scanned yet</span>
            )}
          </div>
          {spaceUrl ? (
            <a
              href={spaceUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-muted mt-1 font-mono hover:text-accent transition-colors inline-flex items-center gap-1"
            >
              {spaceId}
              <ExternalLink className="w-3 h-3 flex-shrink-0" />
            </a>
          ) : (
            <p className="text-xs text-muted mt-1 font-mono">{spaceId}</p>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-default">
        {tabs.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              activeTab === tab.id
                ? "border-accent text-accent"
                : "border-transparent text-muted hover:text-secondary"
            }`}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div>
        {activeTab === "score" && (
          <>
            {hasActiveOptRun && (
              <div className="flex items-center justify-between rounded-lg border border-blue-500/30 bg-blue-500/5 px-4 py-3 mb-4">
                <div>
                  <h3 className="text-sm font-semibold text-primary">Optimization in progress</h3>
                  <p className="text-xs text-muted mt-0.5">An optimization run is currently running for this space.</p>
                </div>
                <button
                  onClick={() => setActiveTab("optimize")}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors shrink-0"
                >
                  <Play className="w-3.5 h-3.5" />
                  View Run
                </button>
              </div>
            )}
            <IQScoreTab
              scanResult={scanResult}
              isLoading={isLoadingScan}
              onScan={handleScan}
              isScanning={isScanning}
              spaceId={spaceId}
              {...actionProps}
              onNavigateToOptimize={() => setActiveTab("optimize")}
            />

            {/* Collapsible space configuration */}
            <div className="mt-6 bg-surface border border-default rounded-xl">
              <div className="flex items-center justify-between px-5 py-3">
                <button
                  onClick={() => setConfigExpanded(!configExpanded)}
                  className="flex items-center gap-2 text-left"
                >
                  {configExpanded
                    ? <ChevronDown className="w-4 h-4 text-muted" />
                    : <ChevronRight className="w-4 h-4 text-muted" />
                  }
                  <Settings className="w-4 h-4 text-muted" />
                  <span className="text-sm font-semibold text-secondary uppercase tracking-wide">
                    Space Configuration
                  </span>
                </button>
                <button
                  onClick={() => actions.handleFetchSpace(spaceId)}
                  disabled={state.isLoading}
                  className="flex items-center gap-1 text-xs text-muted hover:text-accent transition-colors disabled:opacity-50"
                  title="Reload space configuration"
                >
                  <RefreshCw className={`w-3 h-3 ${state.isLoading ? "animate-spin" : ""}`} />
                  Reload
                </button>
              </div>
              {configExpanded && (
                <div className="border-t border-default">
                  <SpaceOverview spaceData={state.spaceData} isLoading={state.isLoading} />
                </div>
              )}
            </div>
          </>
        )}

        {activeTab === "optimize" && (
          <AutoOptimizeTab spaceId={spaceId} onRescan={handleRescanFromOptimize} />
        )}

        {activeTab === "history" && (
          <HistoryTab history={history} optimizationEvents={optimizationEvents} isLoading={isLoadingHistory} />
        )}
      </div>

    </div>
  )
}
