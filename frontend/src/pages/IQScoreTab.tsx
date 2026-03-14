/**
 * IQScoreTab - 5 donut rings + expandable check details + recommendations with inline fix agent.
 */
import { useState, useRef, useEffect } from "react"
import { Zap, RefreshCw, TrendingUp, Check, X, CheckCircle, AlertCircle, Code2, ChevronDown, ChevronRight, Square } from "lucide-react"
import { streamFixAgent } from "@/lib/api"
import { MATURITY_COLORS } from "@/lib/utils"
import type { ScanResult, ScoreBreakdown, CheckDetail, FixAgentEvent, FixPatch } from "@/types"

interface IQScoreTabProps {
  scanResult: ScanResult | null
  onScan: () => void
  isScanning: boolean
  spaceId: string
  spaceConfig?: Record<string, unknown>
}

const TIERS: { key: keyof ScoreBreakdown; label: string }[] = [
  { key: "connected",  label: "Connected" },
  { key: "configured", label: "Configured" },
  { key: "calibrated", label: "Calibrated" },
  { key: "trusted",    label: "Trusted" },
  { key: "optimized",  label: "Optimized" },
]

function DonutRing({ score, max, color, label, selected, onClick }: {
  score: number; max: number; color: string; label: string; selected: boolean; onClick: () => void
}) {
  const radius = 36
  const stroke = 6
  const circumference = 2 * Math.PI * radius
  const pct = max > 0 ? score / max : 0
  const dashOffset = circumference * (1 - pct)
  const size = (radius + stroke) * 2

  return (
    <button
      onClick={onClick}
      className={`flex flex-col items-center gap-2 p-3 rounded-xl transition-all cursor-pointer ${
        selected ? "bg-surface-secondary ring-2 ring-accent/40 scale-105" : "hover:bg-surface-secondary/50"
      }`}
    >
      <div className="relative" style={{ width: size, height: size }}>
        <svg className="w-full h-full -rotate-90" viewBox={`0 0 ${size} ${size}`}>
          <circle cx={size/2} cy={size/2} r={radius} fill="none" stroke="currentColor" strokeWidth={stroke} className="text-surface-secondary" />
          <circle cx={size/2} cy={size/2} r={radius} fill="none" stroke={color} strokeWidth={stroke}
            strokeDasharray={circumference} strokeDashoffset={dashOffset} strokeLinecap="round"
            style={{ transition: "stroke-dashoffset 0.7s ease" }} />
        </svg>
        <span className="absolute inset-0 flex items-center justify-center text-lg font-bold text-primary">{score}</span>
      </div>
      <span className="text-xs font-medium text-secondary">{label}</span>
    </button>
  )
}

function CheckRow({ check }: { check: CheckDetail }) {
  return (
    <div className="flex items-center gap-3 py-2">
      {check.passed ? (
        <div className="w-5 h-5 rounded-full bg-emerald-500/20 flex items-center justify-center flex-shrink-0">
          <Check className="w-3 h-3 text-emerald-400" />
        </div>
      ) : (
        <div className="w-5 h-5 rounded-full bg-red-500/20 flex items-center justify-center flex-shrink-0">
          <X className="w-3 h-3 text-red-400" />
        </div>
      )}
      <span className="flex-1 text-sm text-secondary">{check.label}</span>
      <span className={`text-sm font-mono font-medium ${check.passed ? "text-emerald-400" : "text-muted"}`}>
        {check.points}/{check.max_points}
      </span>
    </div>
  )
}

export function IQScoreTab({ scanResult, onScan, isScanning, spaceId, spaceConfig }: IQScoreTabProps) {
  const [selectedTier, setSelectedTier] = useState<string | null>(null)

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
            // Auto-rescan to show improved score
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

  const { breakdown, checks } = scanResult
  const activeTier = TIERS.find(t => t.key === selectedTier)
  const activeTierColors = activeTier ? MATURITY_COLORS[activeTier.label] : null
  const activeChecks = selectedTier && checks?.[selectedTier]?.length ? checks[selectedTier] : []
  const missingChecks = selectedTier && (!checks || !checks[selectedTier]?.length)
  const fixAgentActive = fixRunning || fixCompleted || fixError

  return (
    <div className="space-y-6">
      {/* Donut rings */}
      <div className="bg-surface border border-default rounded-xl p-5">
        <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide mb-4">Score Breakdown</h3>
        <div className="flex items-start justify-around">
          {TIERS.map(tier => {
            const colors = MATURITY_COLORS[tier.label]
            return (
              <DonutRing
                key={tier.key}
                score={breakdown[tier.key]}
                max={20}
                color={colors.hex}
                label={tier.label}
                selected={selectedTier === tier.key}
                onClick={() => setSelectedTier(selectedTier === tier.key ? null : tier.key)}
              />
            )
          })}
        </div>
      </div>

      {/* Expanded check details */}
      {activeTier && activeTierColors && activeChecks.length > 0 && (
        <div className={`border rounded-xl p-5 ${activeTierColors.bg} ${activeTierColors.border}`}>
          <h3 className="text-sm font-semibold text-primary mb-3 flex items-center gap-2">
            <span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: activeTierColors.hex }} />
            {activeTier.label} — {breakdown[activeTier.key]}/20
          </h3>
          <div className="divide-y divide-default/50">
            {activeChecks.map((check, i) => <CheckRow key={i} check={check} />)}
          </div>
        </div>
      )}

      {/* Hint when checks data is missing (old scan) */}
      {activeTier && activeTierColors && missingChecks && (
        <div className={`border rounded-xl p-5 ${activeTierColors.bg} ${activeTierColors.border}`}>
          <p className="text-sm text-muted text-center">
            Check details not available for this scan. Run a new IQ Scan to see individual checks.
          </p>
        </div>
      )}

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
