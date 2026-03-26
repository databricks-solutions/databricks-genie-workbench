/**
 * SpaceDetail - Unified 4-tab detail view for a Genie Space.
 * Tabs: Overview | Score | Optimize | History
 */
import { useState, useEffect, useRef } from "react"
import { ArrowLeft, Star, Eye, BarChart2, Clock, ExternalLink, Rocket, Play } from "lucide-react"
import { scanSpace, toggleStar, getSpaceHistory, getSpaceDetail, getActiveRunForSpace } from "@/lib/api"
import { MATURITY_COLORS, getOptimizationLabel } from "@/lib/utils"
import type { ScanResult, ScoreHistoryPoint, OptimizationEvent } from "@/types"
import { IQScoreTab } from "./IQScoreTab"
import { HistoryTab } from "./HistoryTab"
import { useAnalysis } from "@/hooks/useAnalysis"
import { SpaceOverview } from "@/components/SpaceOverview"
import { AutoOptimizeTab } from "@/components/auto-optimize/AutoOptimizeTab"

type Tab = "overview" | "score" | "optimize" | "history"
const VALID_TABS: readonly string[] = ["overview", "score", "optimize", "history"]

interface SpaceDetailProps {
  spaceId: string
  displayName: string
  spaceUrl?: string
  initialTab?: string
  autoScan?: boolean
  onBack: () => void
  onFixWithAgent?: (spaceId: string, displayName: string, spaceUrl: string | undefined, scanResult: ScanResult) => void
}

export function SpaceDetail({ spaceId, displayName, spaceUrl, initialTab, autoScan, onBack, onFixWithAgent }: SpaceDetailProps) {
  const [activeTab, setActiveTab] = useState<Tab>(initialTab && VALID_TABS.includes(initialTab) ? initialTab as Tab : "overview")
  const [scanResult, setScanResult] = useState<ScanResult | null>(null)
  const [isStarred, setIsStarred] = useState(false)
  const [isScanning, setIsScanning] = useState(false)
  const [history, setHistory] = useState<ScoreHistoryPoint[]>([])
  const [optimizationEvents, setOptimizationEvents] = useState<OptimizationEvent[]>([])
  const [hasActiveOptRun, setHasActiveOptRun] = useState(false)

  const { state, actions } = useAnalysis()

  // Guard against getSpaceDetail overwriting a fresh scan result
  const freshScanDoneRef = useRef(false)

  // Load space data + persisted score on mount
  useEffect(() => {
    freshScanDoneRef.current = false
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
              scanned_at: detail.scan_result.scanned_at ?? "",
            })
          }
        })
        .catch((e) => console.error("Failed to load space detail:", e))
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

  // Auto-scan on mount when requested (e.g., returning from fix flow)
  useEffect(() => {
    if (autoScan && !isScanning) {
      handleScan()
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (activeTab === "history") {
      getSpaceHistory(spaceId)
        .then(({ scans, optimization_events }) => {
          setHistory(scans)
          setOptimizationEvents(optimization_events)
        })
        .catch(console.error)
    }
  }, [activeTab, spaceId])

  const tabs: { id: Tab; label: string; icon: React.ReactNode }[] = [
    { id: "overview", label: "Overview", icon: <Eye className="w-4 h-4" /> },
    { id: "score", label: "Score", icon: <BarChart2 className="w-4 h-4" /> },
    { id: "optimize", label: "Optimize", icon: <Rocket className="w-4 h-4" /> },
    { id: "history", label: "History", icon: <Clock className="w-4 h-4" /> },
  ]

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
        {activeTab === "overview" && (
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
            <SpaceOverview spaceData={state.spaceData} isLoading={state.isLoading} />
          </>
        )}

        {activeTab === "score" && (
          <IQScoreTab
            scanResult={scanResult}
            onScan={handleScan}
            isScanning={isScanning}
            spaceId={spaceId}
            spaceConfig={state.spaceData ?? undefined}
            onFixWithAgent={onFixWithAgent && scanResult ? () => onFixWithAgent(spaceId, displayName, spaceUrl, scanResult) : undefined}
            onNavigateToOptimize={() => setActiveTab("optimize")}
          />
        )}

        {activeTab === "optimize" && (
          <AutoOptimizeTab spaceId={spaceId} onRescan={handleRescanFromOptimize} />
        )}

        {activeTab === "history" && (
          <HistoryTab history={history} optimizationEvents={optimizationEvents} />
        )}
      </div>
    </div>
  )
}
