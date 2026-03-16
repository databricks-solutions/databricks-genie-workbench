/**
 * IQScoreTab - Maturity S-curve + side-by-side check columns + recommendations with inline fix agent.
 */
import { useState, useRef, useEffect } from "react"
import { Zap, RefreshCw, TrendingUp, CheckCircle, AlertCircle, Code2, ChevronDown, ChevronRight, Square } from "lucide-react"
import { streamFixAgent } from "@/lib/api"
import { getScoreHex, MATURITY_COLORS } from "@/lib/utils"
import { MaturityCurve } from "@/components/MaturityCurve"
import type { ScanResult, CheckDetail, FixAgentEvent, FixPatch } from "@/types"

interface IQScoreTabProps {
  scanResult: ScanResult | null
  onScan: () => void
  isScanning: boolean
  spaceId: string
  spaceConfig?: Record<string, unknown>
}

/** Ordered tier keys for flattening checks (Connected first → Optimized last). */
const CHECK_TIER_ORDER = ["connected", "configured", "calibrated", "trusted", "optimized"] as const

/** Map tier key → display label for the color dot. */
const TIER_LABELS: Record<string, string> = {
  connected: "Connected",
  configured: "Configured",
  calibrated: "Calibrated",
  trusted: "Trusted",
  optimized: "Trusted",
}

export function IQScoreTab({ scanResult, onScan, isScanning, spaceId, spaceConfig }: IQScoreTabProps) {
  const [checksExpanded, setChecksExpanded] = useState(false)

  // Inline fix agent state
  const [fixEvents, setFixEvents] = useState<FixAgentEvent[]>([])
  const [fixRunning, setFixRunning] = useState(false)
  const [fixCompleted, setFixCompleted] = useState(false)
  const [fixPatches, setFixPatches] = useState<FixPatch[]>([])
  const [fixError, setFixError] = useState<string | null>(null)
  const [expandedPatch, setExpandedPatch] = useState<number | null>(null)
  const stopRef = useRef<(() => void) | null>(null)
  const logRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight
  }, [fixEvents])

  const handleRunFix = () => {
    if (!scanResult) return
    stopRef.current?.()
    setFixEvents([])
    setFixPatches([])
    setFixCompleted(false)
    setFixError(null)
    setFixRunning(true)

    stopRef.current = streamFixAgent(
      spaceId,
      scanResult.findings,
      spaceConfig ?? {},
      (event) => {
        setFixEvents(prev => [...prev, event])
        if (event.status === "complete") {
          setFixCompleted(true)
          setFixRunning(false)
          if (event.diff?.patches) {
            setFixPatches(event.diff.patches)
            if (event.diff.patches.length > 0) {
              onScan()
            }
          }
        } else if (event.status === "error") {
          setFixError(event.message || "Fix agent failed")
          setFixRunning(false)
        }
      },
      (err) => { setFixError(err.message); setFixRunning(false) },
    )
  }

  const handleStopFix = () => { stopRef.current?.(); setFixRunning(false) }

  if (!scanResult) {
    return (
      <div className="text-center py-16">
        <div className="w-20 h-20 rounded-full border-2 border-default flex items-center justify-center mx-auto mb-6">
          <span className="text-2xl text-muted font-bold">?</span>
        </div>
        <h3 className="text-lg font-semibold text-primary mb-2">Not yet scanned</h3>
        <p className="text-muted mb-6">Run an IQ scan to assess this Genie Space's maturity</p>
        <button
          onClick={onScan}
          disabled={isScanning}
          className="flex items-center gap-2 mx-auto px-4 py-2 rounded-lg bg-accent text-white hover:bg-accent/90 disabled:opacity-50 transition-colors"
        >
          {isScanning ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Zap className="w-4 h-4" />}
          {isScanning ? "Scanning..." : "Run IQ Scan"}
        </button>
      </div>
    )
  }

  // Flatten all checks, split into passed/failed
  const passedChecks: { check: CheckDetail; tierKey: string }[] = []
  const failedChecks: { check: CheckDetail; tierKey: string }[] = []
  for (const key of CHECK_TIER_ORDER) {
    for (const c of (scanResult.checks?.[key] ?? [])) {
      ;(c.passed ? passedChecks : failedChecks).push({ check: c, tierKey: key })
    }
  }
  const totalChecks = passedChecks.length + failedChecks.length

  const maturityColors = MATURITY_COLORS[scanResult.maturity]
  const fixAgentActive = fixRunning || fixCompleted || fixError

  return (
    <div className="space-y-6">
      {/* Maturity Curve */}
      <div className="bg-surface border border-default rounded-xl p-5">
        {/* Score header */}
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide">Maturity Curve</h3>
          <div className="flex items-center gap-3">
            {maturityColors && (
              <span className={`text-xs font-medium px-2 py-0.5 rounded-full border ${maturityColors.badge}`}>
                {scanResult.maturity}
              </span>
            )}
            <span className="text-2xl font-bold" style={{ color: getScoreHex(scanResult.score) }}>
              {scanResult.score}
            </span>
          </div>
        </div>

        {/* S-curve visualization */}
        <MaturityCurve score={scanResult.score} maturity={scanResult.maturity} optimizationPoints={scanResult.breakdown.optimized} />

        {/* Expandable check list — two columns: passed / not passed */}
        {totalChecks > 0 && (
          <div className="mt-3 pt-3 border-t border-default">
            <button
              onClick={() => setChecksExpanded(!checksExpanded)}
              className="flex items-center gap-2 w-full text-left group"
            >
              {checksExpanded
                ? <ChevronDown className="w-4 h-4 text-muted" />
                : <ChevronRight className="w-4 h-4 text-muted" />
              }
              <span className="text-sm font-medium text-secondary group-hover:text-primary transition-colors">
                {passedChecks.length}/{totalChecks} checks passed — {scanResult.score}/100 points
              </span>
            </button>

            {checksExpanded && (
              <div className="grid grid-cols-2 gap-6 mt-3">
                {/* Passed column */}
                <div>
                  <div className="flex items-center gap-1.5 text-xs font-semibold text-emerald-500 mb-2">
                    <CheckCircle className="w-3.5 h-3.5" />
                    Passed ({passedChecks.length})
                  </div>
                  <div className="space-y-0.5">
                    {passedChecks.map(({ check, tierKey }, i) => {
                      const color = MATURITY_COLORS[TIER_LABELS[tierKey] ?? "Connected"]?.hex ?? "#6b7280"
                      return (
                        <div key={i} className="flex items-center gap-2 py-1.5">
                          <span className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
                          <span className="flex-1 text-sm text-secondary truncate">{check.label}</span>
                          <span className="text-xs font-mono text-emerald-500">{check.points}/{check.max_points}</span>
                        </div>
                      )
                    })}
                    {passedChecks.length === 0 && (
                      <p className="text-xs text-muted py-2">No checks passed yet</p>
                    )}
                  </div>
                </div>

                {/* Not passed column */}
                <div>
                  <div className="flex items-center gap-1.5 text-xs font-semibold text-red-400 mb-2">
                    <AlertCircle className="w-3.5 h-3.5" />
                    Not Passed ({failedChecks.length})
                  </div>
                  <div className="space-y-0.5">
                    {failedChecks.map(({ check, tierKey }, i) => {
                      const color = MATURITY_COLORS[TIER_LABELS[tierKey] ?? "Connected"]?.hex ?? "#6b7280"
                      return (
                        <div key={i} className="flex items-center gap-2 py-1.5">
                          <span className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
                          <span className="flex-1 text-sm text-muted truncate">{check.label}</span>
                          <span className="text-xs font-mono text-muted">{check.points}/{check.max_points}</span>
                        </div>
                      )
                    })}
                    {failedChecks.length === 0 && (
                      <p className="text-xs text-emerald-500 py-2">All checks passed!</p>
                    )}
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Hint when no check data (old scan) */}
        {totalChecks === 0 && (
          <p className="mt-4 text-sm text-muted text-center">
            Check details not available for this scan. Run a new IQ Scan to see individual checks.
          </p>
        )}
      </div>

      {/* Recommendations + inline fix agent */}
      {scanResult.next_steps.length > 0 && (
        <div className="bg-surface border border-default rounded-xl p-5">
          <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide mb-4 flex items-center gap-2">
            <TrendingUp className="w-4 h-4 text-amber-400" />
            Recommendations
          </h3>
          <div className="space-y-3">
            {scanResult.next_steps.map((step, i) => (
              <div key={i} className="flex items-start gap-3">
                <span className="w-5 h-5 rounded-full bg-amber-500/15 border border-amber-500/30 flex items-center justify-center text-xs font-medium text-amber-400 flex-shrink-0 mt-0.5">
                  {i + 1}
                </span>
                <div>
                  {scanResult.findings[i] && (
                    <p className="text-sm font-medium text-primary">{scanResult.findings[i]}</p>
                  )}
                  <p className="text-sm text-muted">{step}</p>
                </div>
              </div>
            ))}
          </div>

          {/* Fix agent controls */}
          {scanResult.findings.length > 0 && (
            <div className="mt-4 pt-4 border-t border-default">
              {!fixAgentActive && (
                <button
                  onClick={handleRunFix}
                  className="flex items-center gap-2 text-sm px-3 py-2 rounded-lg border border-accent/40 text-accent hover:bg-accent/10 transition-colors"
                >
                  <Zap className="w-4 h-4" />
                  Fix with AI Agent
                </button>
              )}

              {fixRunning && (
                <div className="flex items-center gap-3 mb-3">
                  <span className="flex items-center gap-1.5 text-sm text-accent">
                    <span className="w-1.5 h-1.5 rounded-full bg-accent animate-pulse" />
                    AI Fix Agent running...
                  </span>
                  <button
                    onClick={handleStopFix}
                    className="flex items-center gap-1 text-xs px-2 py-1 rounded border border-red-500/30 text-red-400 hover:bg-red-500/10 transition-colors"
                  >
                    <Square className="w-3 h-3" />
                    Stop
                  </button>
                </div>
              )}

              {/* Event log */}
              {fixEvents.length > 0 && (
                <div
                  ref={logRef}
                  className="bg-surface-secondary rounded-lg p-3 space-y-1.5 max-h-48 overflow-y-auto text-xs font-mono mb-3"
                >
                  {fixEvents.map((event, i) => {
                    if (event.status === "thinking") return <div key={i} className="text-muted">&rsaquo; {event.message}</div>
                    if (event.status === "patch") return <div key={i} className="text-blue-400">&rdsh; {event.field_path}</div>
                    if (event.status === "applying") return <div key={i} className="text-amber-400">&#9889; {event.message}</div>
                    if (event.status === "complete") return <div key={i} className="text-emerald-400">&#10003; {event.summary}</div>
                    if (event.status === "error") return <div key={i} className="text-red-400">&#10007; {event.message}</div>
                    return null
                  })}
                </div>
              )}

              {/* Error */}
              {fixError && (
                <div className="flex items-center gap-2 text-sm text-red-400 p-3 bg-red-500/10 rounded-lg mb-3">
                  <AlertCircle className="w-4 h-4 flex-shrink-0" />
                  {fixError}
                </div>
              )}

              {/* Patches applied */}
              {fixCompleted && fixPatches.length > 0 && (
                <div>
                  <div className="flex items-center gap-2 text-sm text-emerald-400 mb-3">
                    <CheckCircle className="w-4 h-4" />
                    Applied {fixPatches.length} patch{fixPatches.length !== 1 ? "es" : ""}
                  </div>
                  <div className="space-y-2">
                    {fixPatches.map((patch, i) => (
                      <div key={i} className="border border-default rounded-lg overflow-hidden">
                        <button
                          onClick={() => setExpandedPatch(expandedPatch === i ? null : i)}
                          className="w-full flex items-center gap-2 px-3 py-2 text-left hover:bg-surface-secondary transition-colors"
                        >
                          {expandedPatch === i ? <ChevronDown className="w-3.5 h-3.5 text-muted" /> : <ChevronRight className="w-3.5 h-3.5 text-muted" />}
                          <Code2 className="w-3.5 h-3.5 text-blue-400" />
                          <span className="text-xs font-mono text-secondary flex-1 truncate">{patch.field_path}</span>
                        </button>
                        {expandedPatch === i && (
                          <div className="px-3 pb-3 space-y-2 bg-surface-secondary/50 text-xs">
                            <div>
                              <span className="text-red-400">- </span>
                              <span className="text-muted font-mono">{JSON.stringify(patch.old_value)}</span>
                            </div>
                            <div>
                              <span className="text-emerald-400">+ </span>
                              <span className="text-secondary font-mono">{JSON.stringify(patch.new_value)}</span>
                            </div>
                            <p className="text-muted italic">{patch.rationale}</p>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {fixCompleted && fixPatches.length === 0 && !fixError && (
                <div className="flex items-center gap-2 text-sm text-muted">
                  <CheckCircle className="w-4 h-4 text-emerald-400" />
                  No patches needed — space configuration looks good!
                </div>
              )}

              {/* Run again after completion */}
              {(fixCompleted || fixError) && (
                <button
                  onClick={handleRunFix}
                  className="mt-3 flex items-center gap-2 text-xs px-3 py-1.5 rounded-lg border border-default text-muted hover:bg-surface-secondary transition-colors"
                >
                  <RefreshCw className="w-3 h-3" />
                  Run Again
                </button>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
