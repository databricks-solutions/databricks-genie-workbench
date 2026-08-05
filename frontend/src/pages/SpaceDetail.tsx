/**
 * SpaceDetail - 3-tab detail view for a Genie Agent.
 * Tabs: Score (default) | Optimize | History
 */
import { useState, useEffect, useRef } from "react"
import { ArrowLeft, Star, BarChart2, Clock, ExternalLink, Rocket, Play, ChevronDown, ChevronRight, Settings, RefreshCw } from "lucide-react"
import { scanSpace, toggleStar, getSpaceHistory, getSpaceDetail, getActiveRunForSpace } from "@/lib/api"
import { MATURITY_COLORS, getOptimizationLabel } from "@/lib/utils"
import type { ScanResult, ScoreHistoryPoint, OptimizationEvent } from "@/types"
import { IQScoreTab } from "./IQScoreTab"
import { HistoryTab } from "./HistoryTab"
import { useAnalysis } from "@/hooks/useAnalysis"
import { SpaceOverview } from "@/components/SpaceOverview"
import { AutoOptimizeTab } from "@/components/auto-optimize/AutoOptimizeTab"
import type { SpaceTab } from "@/lib/navigation"

interface SpaceDetailProps {
  spaceId: string
  displayName: string
  spaceUrl?: string
  activeTab: SpaceTab
  runId?: string
  autoScan?: boolean
  onBack: () => void
  onNavigate: (tab: SpaceTab, runId?: string) => void
}

export function SpaceDetail({ spaceId, displayName, spaceUrl, activeTab, runId, autoScan, onBack, onNavigate }: SpaceDetailProps) {
  const [scanResult, setScanResult] = useState<ScanResult | null>(null)
  const [isStarred, setIsStarred] = useState(false)
  const [isScanning, setIsScanning] = useState(false)
  const [history, setHistory] = useState<ScoreHistoryPoint[]>([])
  const [optimizationEvents, setOptimizationEvents] = useState<OptimizationEvent[]>([])
  const [activeOptRunId, setActiveOptRunId] = useState<string | null>(null)
  const [isLoadingScan, setIsLoadingScan] = useState(true)
  const [isLoadingHistory, setIsLoadingHistory] = useState(false)

  const [configExpanded, setConfigExpanded] = useState(false)

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
      .then((res) => setActiveOptRunId(res.hasActiveRun ? res.activeRunId : null))
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
    onNavigate("score")
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

  const tabs: { id: SpaceTab; label: string; icon: React.ReactNode }[] = [
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
      onAction: () => onNavigate("optimize"),
      actionLabel: "Run Optimization",
      actionIcon: <Rocket className="w-4 h-4" />,
      actionDescription: (
        <>
          This agent passed the configuration checks. Auto-Optimize will benchmark real
          questions, tune the selected levers, and apply only changes that improve the
          measured result.
        </>
      ),
    }
  } else if (hasRemediationItems) {
    actionProps = {
      onAction: () => onNavigate("optimize"),
      actionLabel: "Open Optimize",
      actionIcon: <Rocket className="w-4 h-4" />,
      actionDescription: (
        <>
          Auto-Optimize is the recommended path for improving agents that fail IQ checks.
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
          </div>
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
            onClick={() => onNavigate(tab.id)}
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
            {activeOptRunId && (
              <div className="flex items-center justify-between rounded-lg border border-blue-500/30 bg-blue-500/5 px-4 py-3 mb-4">
                <div>
                  <h3 className="text-sm font-semibold text-primary">Optimization in progress</h3>
                  <p className="text-xs text-muted mt-0.5">An optimization run is currently running for this agent.</p>
                </div>
                <button
                  onClick={() => onNavigate("optimize", activeOptRunId)}
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
              onNavigateToOptimize={() => onNavigate("optimize")}
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
                    Agent Configuration
                  </span>
                </button>
                <button
                  onClick={() => actions.handleFetchSpace(spaceId)}
                  disabled={state.isLoading}
                  className="flex items-center gap-1 text-xs text-muted hover:text-accent transition-colors disabled:opacity-50"
                  title="Reload agent configuration"
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
          <AutoOptimizeTab
            key={runId ?? "configure"}
            spaceId={spaceId}
            requestedRunId={runId}
            onRunChange={(nextRunId) => onNavigate("optimize", nextRunId)}
            onRescan={handleRescanFromOptimize}
          />
        )}

        {activeTab === "history" && (
          <HistoryTab history={history} optimizationEvents={optimizationEvents} isLoading={isLoadingHistory} />
        )}
      </div>

    </div>
  )
}
