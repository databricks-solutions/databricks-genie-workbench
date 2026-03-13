/**
 * IQScoreTab - Score breakdown + maturity level + next steps.
 */
import { useState } from "react"
import { Zap, RefreshCw, TrendingUp, AlertCircle } from "lucide-react"
import type { ScanResult } from "@/types"
import { FixAgentPanel } from "@/components/FixAgentPanel"

interface IQScoreTabProps {
  scanResult: ScanResult | null
  onScan: () => void
  isScanning: boolean
  spaceId: string
  spaceConfig?: Record<string, unknown>
}

function DimensionBar({ label, score, max, color }: { label: string; score: number; max: number; color: string }) {
  const pct = (score / max) * 100
  return (
    <div>
      <div className="flex justify-between text-sm mb-1">
        <span className="text-secondary">{label}</span>
        <span className="text-primary font-medium">{score}/{max}</span>
      </div>
      <div className="h-2 bg-surface-secondary rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all duration-700 ${color}`}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  )
}

export function IQScoreTab({ scanResult, onScan, isScanning, spaceId, spaceConfig }: IQScoreTabProps) {
  const [showFixAgent, setShowFixAgent] = useState(false)

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

  const { breakdown } = scanResult

  return (
    <div className="space-y-6">
      {/* Score breakdown */}
      <div className="bg-surface border border-default rounded-xl p-5">
        <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide mb-4">Score Breakdown</h3>
        <div className="space-y-4">
          <DimensionBar label="Nascent" score={breakdown.nascent} max={25} color="bg-red-500" />
          <DimensionBar label="Basic" score={breakdown.basic} max={15} color="bg-orange-500" />
          <DimensionBar label="Developing" score={breakdown.developing} max={20} color="bg-yellow-500" />
          <DimensionBar label="Proficient" score={breakdown.proficient} max={22} color="bg-blue-500" />
          <DimensionBar label="Optimized" score={breakdown.optimized} max={18} color="bg-emerald-500" />
        </div>
      </div>

      {/* Findings */}
      {scanResult.findings.length > 0 && (
        <div className="bg-surface border border-default rounded-xl p-5">
          <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide mb-4 flex items-center gap-2">
            <AlertCircle className="w-4 h-4 text-amber-400" />
            Findings
          </h3>
          <ul className="space-y-2">
            {scanResult.findings.map((f, i) => (
              <li key={i} className="flex items-start gap-2 text-sm text-secondary">
                <span className="w-1.5 h-1.5 rounded-full bg-amber-400 mt-2 flex-shrink-0" />
                {f}
              </li>
            ))}
          </ul>
          {/* Fix Agent CTA */}
          <button
            onClick={() => setShowFixAgent(true)}
            className="mt-4 flex items-center gap-2 text-sm px-3 py-2 rounded-lg border border-accent/40 text-accent hover:bg-accent/10 transition-colors"
          >
            <Zap className="w-4 h-4" />
            Fix with AI Agent
          </button>
        </div>
      )}

      {/* Next steps */}
      {scanResult.next_steps.length > 0 && (
        <div className="bg-surface border border-default rounded-xl p-5">
          <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide mb-4 flex items-center gap-2">
            <TrendingUp className="w-4 h-4 text-emerald-400" />
            Next Steps
          </h3>
          <ol className="space-y-2">
            {scanResult.next_steps.map((s, i) => (
              <li key={i} className="flex items-start gap-3 text-sm text-secondary">
                <span className="w-5 h-5 rounded-full bg-surface-secondary border border-default flex items-center justify-center text-xs font-medium text-muted flex-shrink-0 mt-0.5">
                  {i + 1}
                </span>
                {s}
              </li>
            ))}
          </ol>
        </div>
      )}

      {/* Fix Agent Panel (modal-ish) */}
      {showFixAgent && (
        <FixAgentPanel
          spaceId={spaceId}
          findings={scanResult.findings}
          spaceConfig={spaceConfig ?? {}}
          onClose={() => setShowFixAgent(false)}
        />
      )}
    </div>
  )
}
