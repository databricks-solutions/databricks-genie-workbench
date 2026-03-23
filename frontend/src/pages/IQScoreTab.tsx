/**
 * IQScoreTab - Maturity S-curve + side-by-side check columns + recommendations.
 */
import { useState } from "react"
import { Zap, RefreshCw, TrendingUp, CheckCircle, AlertCircle, ChevronDown, ChevronRight, Check, X, Settings2 } from "lucide-react"
import { MATURITY_COLORS, getOptimizationLabel } from "@/lib/utils"
import { MaturityCurve } from "@/components/MaturityCurve"
import type { ScanResult, CheckDetail } from "@/types"

interface IQScoreTabProps {
  scanResult: ScanResult | null
  onScan: () => void
  isScanning: boolean
  spaceId: string
  spaceConfig?: Record<string, unknown>
  onFixWithAgent?: () => void
  onRunOptimization?: () => void
}

export function IQScoreTab({ scanResult, onScan, isScanning, onFixWithAgent, onRunOptimization }: IQScoreTabProps) {
  const [checksExpanded, setChecksExpanded] = useState(false)

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

  // Flat checks list — handle both new (array) and old (dict) formats
  const allChecks: CheckDetail[] = Array.isArray(scanResult.checks)
    ? scanResult.checks
    : Object.values(scanResult.checks as unknown as Record<string, CheckDetail[]>).flat()
  const passedChecks = allChecks.filter(c => c.passed)
  const failedChecks = allChecks.filter(c => !c.passed)
  const totalChecks = allChecks.length
  const total = scanResult.total ?? 12

  const maturityColors = MATURITY_COLORS[scanResult.maturity]

  return (
    <div className="space-y-6">
      {/* Maturity Curve */}
      <div className="bg-surface border border-default rounded-xl p-5">
        {/* Score header */}
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide">Maturity Curve</h3>
          <div className="flex items-center gap-2">
            {maturityColors && (
              <span className={`text-xs font-medium px-2 py-0.5 rounded-full border ${maturityColors.badge}`}>
                {scanResult.maturity}
              </span>
            )}
            <button
              onClick={onScan}
              disabled={isScanning}
              className="flex items-center gap-1 text-xs text-muted hover:text-accent transition-colors disabled:opacity-50"
              title="Re-run IQ Scan"
            >
              <RefreshCw className={`w-3 h-3 ${isScanning ? "animate-spin" : ""}`} />
              {isScanning ? "Scanning..." : "Re-scan"}
            </button>
          </div>
        </div>

        {/* S-curve visualization */}
        <MaturityCurve score={scanResult.score} total={total} maturity={scanResult.maturity} />

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
                {passedChecks.length}/{totalChecks} checks passed
                {" · "}
                {getOptimizationLabel(scanResult.optimization_accuracy)}
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
                    {passedChecks.map((check, i) => (
                      <div key={i} className="flex items-center gap-2 py-1.5">
                        <Check className="w-3.5 h-3.5 text-emerald-500 flex-shrink-0" />
                        <span className="flex-1 text-sm text-secondary truncate">{check.label}</span>
                      </div>
                    ))}
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
                    {failedChecks.map((check, i) => (
                      <div key={i} className="flex items-center gap-2 py-1.5">
                        <X className="w-3.5 h-3.5 text-red-400 flex-shrink-0" />
                        <span className="flex-1 text-sm text-muted truncate">{check.label}</span>
                      </div>
                    ))}
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

      {/* Recommendations */}
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

          {/* Action buttons */}
          {scanResult.findings.length > 0 && (onFixWithAgent || onRunOptimization) && (
            <div className="mt-4 pt-4 border-t border-default flex items-center gap-2">
              {onFixWithAgent && (
                <button
                  onClick={onFixWithAgent}
                  className="flex items-center gap-2 text-sm px-3 py-2 rounded-lg border border-accent/40 text-accent hover:bg-accent/10 transition-colors"
                >
                  <Zap className="w-4 h-4" />
                  Fix with AI Agent
                </button>
              )}
              {onRunOptimization && (
                <button
                  onClick={onRunOptimization}
                  className="flex items-center gap-2 text-sm px-3 py-2 rounded-lg border border-default text-secondary hover:bg-surface-secondary transition-colors"
                >
                  <Settings2 className="w-4 h-4" />
                  Run Optimization
                </button>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
