/**
 * HistoryTab - Unified timeline: IQ score trend, optimization accuracy, and optimization run markers.
 *
 * Left Y-axis:  IQ Score (0–12 checklist)
 * Right Y-axis: Optimization Accuracy (0–100%)
 * X-axis:       Time-based
 * Markers:      Diamond icons for optimization runs (color-coded by status)
 */
import { Loader2 } from "lucide-react"
import { getScoreHex } from "@/lib/utils"
import type { ScoreHistoryPoint, OptimizationEvent } from "@/types"

interface HistoryTabProps {
  history: ScoreHistoryPoint[]
  optimizationEvents: OptimizationEvent[]
  isLoading?: boolean
}

const OPT_STATUS_COLORS: Record<string, { fill: string; stroke: string; label: string }> = {
  CONVERGED: { fill: "#22c55e", stroke: "#16a34a", label: "Converged" },
  APPLIED: { fill: "#22c55e", stroke: "#16a34a", label: "Applied" },
  FAILED: { fill: "#ef4444", stroke: "#dc2626", label: "Failed" },
  CANCELLED: { fill: "#ef4444", stroke: "#dc2626", label: "Cancelled" },
}
const OPT_DEFAULT_COLOR = { fill: "#f59e0b", stroke: "#d97706", label: "Running" }

function getOptColor(status: string) {
  return OPT_STATUS_COLORS[status] ?? OPT_DEFAULT_COLOR
}

const ACCURACY_COLOR = "#8b5cf6" // purple for accuracy line

export function HistoryTab({ history, optimizationEvents, isLoading }: HistoryTabProps) {
  if (isLoading) {
    return (
      <div className="text-center py-16">
        <Loader2 className="w-8 h-8 text-accent animate-spin mx-auto mb-4" />
        <p className="text-sm text-muted">Loading history...</p>
      </div>
    )
  }

  const hasScans = history.length > 0
  const hasOptEvents = optimizationEvents.length > 0
  const hasAccuracy = history.some((h) => h.optimization_accuracy != null)

  if (!hasScans && !hasOptEvents) {
    return (
      <div className="text-center py-16 text-muted">
        <p>No scan history yet. Run IQ scans over time to see trends.</p>
      </div>
    )
  }

  if (hasScans && history.length === 1 && !hasOptEvents) {
    return (
      <div className="bg-surface border border-default rounded-xl p-5">
        <p className="text-muted text-sm">Only 1 scan recorded. Run more scans to see trends.</p>
        <div className="mt-4 flex items-center gap-3">
          <div className="text-4xl font-bold text-primary">{history[0].score}</div>
          <div>
            <div className="text-muted text-sm">{history[0].maturity}</div>
            <div className="text-xs text-muted">{new Date(history[0].scanned_at).toLocaleDateString()}</div>
          </div>
        </div>
      </div>
    )
  }

  // Gather all timestamps for the time-based X axis
  const allTimestamps: number[] = []
  for (const h of history) allTimestamps.push(new Date(h.scanned_at).getTime())
  for (const e of optimizationEvents) {
    if (e.started_at) allTimestamps.push(new Date(e.started_at).getTime())
  }
  const tMin = Math.min(...allTimestamps)
  const tMax = Math.max(...allTimestamps)
  const tRange = tMax - tMin || 1

  // Chart dimensions — extra right padding for accuracy axis
  const width = 620
  const height = 220
  const padding = { top: 30, right: hasAccuracy ? 50 : 20, bottom: 40, left: 40 }
  const chartWidth = width - padding.left - padding.right
  const chartHeight = height - padding.top - padding.bottom

  function timeToX(ts: number) {
    return padding.left + ((ts - tMin) / tRange) * chartWidth
  }

  // Left Y-axis: IQ Score 0–12
  const maxScore = 12
  function scoreToY(score: number) {
    return padding.top + chartHeight - (score / maxScore) * chartHeight
  }

  // Right Y-axis: Accuracy 0–100%
  function accuracyToY(acc: number) {
    return padding.top + chartHeight - (acc * chartHeight)
  }

  // Scan data points (score line)
  const scorePoints = history.map((h) => {
    const ts = new Date(h.scanned_at).getTime()
    return {
      x: timeToX(ts),
      y: scoreToY(h.score),
      score: h.score,
      date: new Date(h.scanned_at).toLocaleDateString(),
      maturity: h.maturity,
    }
  })

  // Accuracy data points (only where accuracy exists)
  const accuracyPoints = history
    .filter((h) => h.optimization_accuracy != null)
    .map((h) => {
      const ts = new Date(h.scanned_at).getTime()
      const acc = h.optimization_accuracy!
      return {
        x: timeToX(ts),
        y: accuracyToY(acc),
        accuracy: acc,
        date: new Date(h.scanned_at).toLocaleDateString(),
      }
    })

  const scorePathD = scorePoints.length >= 2
    ? scorePoints.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x} ${p.y}`).join(" ")
    : null

  const accPathD = accuracyPoints.length >= 2
    ? accuracyPoints.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x} ${p.y}`).join(" ")
    : null

  const lineColor = hasScans ? getScoreHex(history[history.length - 1].maturity) : "#6b7280"

  // Optimization event markers
  const optMarkers = optimizationEvents
    .filter((e) => e.started_at)
    .map((e) => {
      const ts = new Date(e.started_at!).getTime()
      const color = getOptColor(e.status)
      return { ...e, x: timeToX(ts), color }
    })

  // X-axis date labels
  const labelCount = Math.min(5, allTimestamps.length)
  const xLabels: { x: number; label: string }[] = []
  for (let i = 0; i < labelCount; i++) {
    const frac = labelCount === 1 ? 0 : i / (labelCount - 1)
    const ts = tMin + frac * tRange
    xLabels.push({ x: timeToX(ts), label: new Date(ts).toLocaleDateString() })
  }

  return (
    <div className="bg-surface border border-default rounded-xl p-5">
      <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide mb-4">Score History</h3>
      <div className="overflow-x-auto">
        <svg viewBox={`0 0 ${width} ${height}`} className="w-full" style={{ minWidth: 300 }}>
          {/* Left Y-axis grid lines (IQ Score 0–12) */}
          {[0, 3, 6, 9, 12].map((score) => {
            const y = scoreToY(score)
            return (
              <g key={score}>
                <line x1={padding.left} y1={y} x2={padding.left + chartWidth} y2={y} stroke="currentColor" strokeOpacity="0.1" strokeDasharray="4 4" />
                <text x={padding.left - 6} y={y} textAnchor="end" dominantBaseline="middle" className="fill-current text-muted" fontSize="10">{score}</text>
              </g>
            )
          })}

          {/* Right Y-axis labels (Accuracy %) */}
          {hasAccuracy && [0, 25, 50, 75, 100].map((pct) => {
            const y = accuracyToY(pct / 100)
            return (
              <text key={pct} x={padding.left + chartWidth + 6} y={y} textAnchor="start" dominantBaseline="middle" fill={ACCURACY_COLOR} fontSize="10" fillOpacity="0.7">{pct}%</text>
            )
          })}

          {/* Optimization markers — vertical dashed lines + diamonds */}
          {optMarkers.map((m) => (
            <g key={m.run_id}>
              <line
                x1={m.x} y1={padding.top} x2={m.x} y2={padding.top + chartHeight}
                stroke={m.color.stroke} strokeWidth="1" strokeDasharray="4 3" strokeOpacity="0.6"
              />
              <polygon
                points={`${m.x},${padding.top - 2} ${m.x + 6},${padding.top + 6} ${m.x},${padding.top + 14} ${m.x - 6},${padding.top + 6}`}
                fill={m.color.fill} stroke={m.color.stroke} strokeWidth="1"
              />
              <title>{`${m.color.label}: ${m.status}${m.best_accuracy != null ? ` · Accuracy: ${(m.best_accuracy * 100).toFixed(0)}%` : ""}${m.convergence_reason ? ` · ${m.convergence_reason}` : ""}`}</title>
            </g>
          ))}

          {/* Score trend line + area */}
          {scorePathD && (
            <>
              <path d={scorePathD} fill="none" stroke={lineColor} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
              <path
                d={`${scorePathD} L ${scorePoints[scorePoints.length - 1].x} ${padding.top + chartHeight} L ${scorePoints[0].x} ${padding.top + chartHeight} Z`}
                fill={lineColor}
                fillOpacity="0.08"
              />
            </>
          )}

          {/* Score data points */}
          {scorePoints.map((p, i) => (
            <g key={i}>
              <circle cx={p.x} cy={p.y} r="4" fill={lineColor} />
              <title>{`${p.date}: ${p.score}/12 (${p.maturity})`}</title>
            </g>
          ))}

          {/* Accuracy trend line */}
          {accPathD && (
            <path d={accPathD} fill="none" stroke={ACCURACY_COLOR} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" strokeDasharray="6 3" />
          )}

          {/* Accuracy data points */}
          {accuracyPoints.map((p, i) => (
            <g key={i}>
              <circle cx={p.x} cy={p.y} r="3.5" fill={ACCURACY_COLOR} fillOpacity="0.8" />
              <title>{`${p.date}: Accuracy ${(p.accuracy * 100).toFixed(0)}%`}</title>
            </g>
          ))}

          {/* X-axis date labels */}
          {xLabels.map((l, i) => (
            <text key={i} x={l.x} y={height - 10} textAnchor="middle" className="fill-current text-muted" fontSize="10">{l.label}</text>
          ))}
        </svg>
      </div>

      {/* Legend */}
      <div className="mt-3 flex flex-wrap items-center gap-4 text-xs text-muted">
        {hasScans && (
          <span className="flex items-center gap-1.5">
            <span className="inline-block w-4 h-0.5 rounded" style={{ backgroundColor: lineColor }} />
            IQ Score (/12)
          </span>
        )}
        {hasAccuracy && (
          <span className="flex items-center gap-1.5">
            <span className="inline-block w-4 h-0.5 rounded" style={{ backgroundColor: ACCURACY_COLOR, borderTop: "1px dashed" }} />
            Optimization Accuracy (%)
          </span>
        )}
        {hasOptEvents && (
          <>
            <span className="flex items-center gap-1.5">
              <svg width="10" height="10" viewBox="0 0 10 10"><polygon points="5,0 10,5 5,10 0,5" fill="#22c55e" /></svg>
              Optimization (success)
            </span>
            <span className="flex items-center gap-1.5">
              <svg width="10" height="10" viewBox="0 0 10 10"><polygon points="5,0 10,5 5,10 0,5" fill="#ef4444" /></svg>
              Optimization (failed)
            </span>
            <span className="flex items-center gap-1.5">
              <svg width="10" height="10" viewBox="0 0 10 10"><polygon points="5,0 10,5 5,10 0,5" fill="#f59e0b" /></svg>
              Optimization (in progress)
            </span>
          </>
        )}
      </div>

      {/* First vs latest summary */}
      {history.length >= 2 && (
        <div className="mt-4 flex items-center gap-4 text-sm">
          <span className="text-muted">First scan: <strong className="text-primary">{history[0].score}/12</strong></span>
          <span className="text-muted">&rarr;</span>
          <span className="text-muted">Latest: <strong className="text-primary">{history[history.length - 1].score}/12</strong></span>
          {(() => {
            const delta = history[history.length - 1].score - history[0].score
            if (delta === 0) return null
            const color = delta > 0 ? "text-emerald-400" : "text-red-400"
            return <span className={`font-semibold ${color}`}>{delta > 0 ? "+" : ""}{delta}</span>
          })()}
        </div>
      )}
    </div>
  )
}
