/**
 * HistoryTab - Score trend over time.
 */
import { getScoreHex } from "@/lib/utils"
import type { ScoreHistoryPoint } from "@/types"

interface HistoryTabProps {
  history: ScoreHistoryPoint[]
}

export function HistoryTab({ history }: HistoryTabProps) {
  if (history.length === 0) {
    return (
      <div className="text-center py-16 text-muted">
        <p>No scan history yet. Run IQ scans over time to see trends.</p>
      </div>
    )
  }

  if (history.length === 1) {
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

  // Simple SVG line chart
  const width = 600
  const height = 200
  const padding = { top: 20, right: 20, bottom: 40, left: 40 }
  const chartWidth = width - padding.left - padding.right
  const chartHeight = height - padding.top - padding.bottom

  const minScore = 0
  const maxScore = 100

  const points = history.map((h, i) => ({
    x: padding.left + (i / (history.length - 1)) * chartWidth,
    y: padding.top + chartHeight - ((h.score - minScore) / (maxScore - minScore)) * chartHeight,
    score: h.score,
    date: new Date(h.scanned_at).toLocaleDateString(),
    maturity: h.maturity,
  }))

  const pathD = points.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x} ${p.y}`).join(" ")

  // Score color based on latest
  const lastScore = history[history.length - 1].score
  const lineColor = getScoreHex(lastScore)

  return (
    <div className="bg-surface border border-default rounded-xl p-5">
      <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide mb-4">Score History</h3>
      <div className="overflow-x-auto">
        <svg viewBox={`0 0 ${width} ${height}`} className="w-full" style={{ minWidth: 300 }}>
          {/* Grid lines */}
          {[0, 25, 50, 75, 100].map(score => {
            const y = padding.top + chartHeight - (score / 100) * chartHeight
            return (
              <g key={score}>
                <line x1={padding.left} y1={y} x2={padding.left + chartWidth} y2={y} stroke="currentColor" strokeOpacity="0.1" strokeDasharray="4 4" />
                <text x={padding.left - 6} y={y} textAnchor="end" dominantBaseline="middle" className="fill-current text-muted" fontSize="10">{score}</text>
              </g>
            )
          })}

          {/* Line */}
          <path d={pathD} fill="none" stroke={lineColor} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />

          {/* Area fill */}
          <path
            d={`${pathD} L ${points[points.length - 1].x} ${padding.top + chartHeight} L ${points[0].x} ${padding.top + chartHeight} Z`}
            fill={lineColor}
            fillOpacity="0.1"
          />

          {/* Data points */}
          {points.map((p, i) => (
            <g key={i}>
              <circle cx={p.x} cy={p.y} r="4" fill={lineColor} />
              <title>{`${p.date}: ${p.score} (${p.maturity})`}</title>
            </g>
          ))}

          {/* X-axis labels */}
          {points.filter((_, i) => i === 0 || i === points.length - 1 || (i % Math.max(1, Math.floor(points.length / 4)) === 0)).map((p, i) => (
            <text key={i} x={p.x} y={height - 10} textAnchor="middle" className="fill-current text-muted" fontSize="10">{p.date}</text>
          ))}
        </svg>
      </div>

      {/* Latest vs first */}
      {history.length >= 2 && (
        <div className="mt-4 flex items-center gap-4 text-sm">
          <span className="text-muted">First scan: <strong className="text-primary">{history[0].score}</strong></span>
          <span className="text-muted">→</span>
          <span className="text-muted">Latest: <strong className="text-primary">{history[history.length - 1].score}</strong></span>
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
