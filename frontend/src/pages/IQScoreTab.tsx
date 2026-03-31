/**
 * IQScoreTab - Maturity S-curve + three-column check grid + split recommendations.
 */
import { useState } from "react"
import { Zap, RefreshCw, TrendingUp, CheckCircle, AlertCircle, AlertTriangle, ChevronDown, ChevronRight, Check, X, Rocket, Loader2 } from "lucide-react"
import { MATURITY_COLORS, getOptimizationLabel } from "@/lib/utils"
import { MaturityCurve } from "@/components/MaturityCurve"
import type { ScanResult, CheckDetail } from "@/types"

interface IQScoreTabProps {
  scanResult: ScanResult | null
  isLoading?: boolean
  onScan: () => void
  isScanning: boolean
  spaceId: string
  /** Single contextual action — label/icon/callback determined by maturity tier */
  onAction?: () => void
  actionLabel?: string
  actionIcon?: React.ReactNode
  onNavigateToOptimize?: () => void
}

export function IQScoreTab({ scanResult, isLoading, onScan, isScanning, onAction, actionLabel, actionIcon, onNavigateToOptimize }: IQScoreTabProps) {
  const [checksExpanded, setChecksExpanded] = useState(false)

  if (isLoading) {
    return (
      <div className="text-center py-16">
        <Loader2 className="w-8 h-8 text-accent animate-spin mx-auto mb-4" />
        <p className="text-sm text-muted">Loading score...</p>
      </div>
    )
  }

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

  // Three-way split: passed (clean), warning (passed but suboptimal), failed
  const passedChecks = allChecks.filter(c => c.passed && c.severity !== "warning")
  const warningChecks = allChecks.filter(c => c.severity === "warning")
  const failedChecks = allChecks.filter(c => !c.passed)
  const totalChecks = allChecks.length
  const total = scanResult.total ?? 12

  const maturityColors = MATURITY_COLORS[scanResult.maturity]

  // Safely access warnings arrays (may be absent on old scan results)
  const warnMessages = scanResult.warnings ?? []
  const warnNextSteps = scanResult.warning_next_steps ?? []
  const hasIssues = scanResult.next_steps.length > 0
  const hasOpportunities = warnMessages.length > 0

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

        {/* Expandable check list — three columns: passed / warnings / failed */}
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
                {passedChecks.length + warningChecks.length}/{totalChecks} checks passed
                {warningChecks.length > 0 && ` · ${warningChecks.length} warning${warningChecks.length !== 1 ? "s" : ""}`}
                {" · "}
                {getOptimizationLabel(scanResult.optimization_accuracy)}
              </span>
            </button>

            {checksExpanded && (
              <div className="grid grid-cols-3 gap-4 mt-3">
                {/* Passed column */}
                <div>
                  <div className="flex items-center gap-1.5 text-xs font-semibold text-emerald-500 mb-2">
                    <CheckCircle className="w-3.5 h-3.5" />
                    Passed ({passedChecks.length})
                  </div>
                  <div className="space-y-0.5">
                    {passedChecks.map((check, i) => (
                      <div key={i} className="py-1.5">
                        <div className="flex items-center gap-2">
                          <Check className="w-3.5 h-3.5 text-emerald-500 flex-shrink-0" />
                          <span className="flex-1 text-sm text-secondary truncate">{check.label}</span>
                        </div>
                        {check.detail && (
                          <p className="text-xs text-muted ml-5.5 mt-0.5 pl-[22px]">{check.detail}</p>
                        )}
                      </div>
                    ))}
                    {passedChecks.length === 0 && (
                      <p className="text-xs text-muted py-2">No clean passes</p>
                    )}
                  </div>
                </div>

                {/* Warnings column */}
                <div>
                  <div className="flex items-center gap-1.5 text-xs font-semibold text-amber-400 mb-2">
                    <AlertTriangle className="w-3.5 h-3.5" />
                    Warnings ({warningChecks.length})
                  </div>
                  <div className="space-y-0.5">
                    {warningChecks.map((check, i) => (
                      <div key={i} className="py-1.5">
                        <div className="flex items-center gap-2">
                          <AlertTriangle className="w-3.5 h-3.5 text-amber-400 flex-shrink-0" />
                          <span className="flex-1 text-sm text-secondary truncate">{check.label}</span>
                        </div>
                        {check.detail && (
                          <p className="text-xs text-amber-400/70 pl-[22px] mt-0.5">{check.detail}</p>
                        )}
                      </div>
                    ))}
                    {warningChecks.length === 0 && (
                      <p className="text-xs text-muted py-2">No warnings</p>
                    )}
                  </div>
                </div>

                {/* Failed column */}
                <div>
                  <div className="flex items-center gap-1.5 text-xs font-semibold text-red-400 mb-2">
                    <AlertCircle className="w-3.5 h-3.5" />
                    Not Passed ({failedChecks.length})
                  </div>
                  <div className="space-y-0.5">
                    {failedChecks.map((check, i) => {
                      const isOptCheck = onNavigateToOptimize && (
                        check.label === "Optimization workflow completed" ||
                        check.label === "Optimization accuracy ≥ 85%"
                      )
                      return isOptCheck ? (
                        <button
                          key={i}
                          onClick={onNavigateToOptimize}
                          className="py-1.5 w-full text-left group"
                        >
                          <div className="flex items-center gap-2">
                            <X className="w-3.5 h-3.5 text-red-400 flex-shrink-0" />
                            <span className="flex-1 text-sm text-muted truncate group-hover:text-accent group-hover:underline transition-colors">
                              {check.label}
                            </span>
                            <Rocket className="w-3 h-3 text-muted opacity-0 group-hover:opacity-100 transition-opacity" />
                          </div>
                          {check.detail && (
                            <p className="text-xs text-muted pl-[22px] mt-0.5">{check.detail}</p>
                          )}
                        </button>
                      ) : (
                        <div key={i} className="py-1.5">
                          <div className="flex items-center gap-2">
                            <X className="w-3.5 h-3.5 text-red-400 flex-shrink-0" />
                            <span className="flex-1 text-sm text-muted truncate">{check.label}</span>
                          </div>
                          {check.detail && (
                            <p className="text-xs text-muted pl-[22px] mt-0.5">{check.detail}</p>
                          )}
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

      {/* Recommendations — split into Issues + Opportunities */}
      {(hasIssues || hasOpportunities) && (
        <div className="bg-surface border border-default rounded-xl p-5">
          <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide mb-4 flex items-center gap-2">
            <TrendingUp className="w-4 h-4 text-amber-400" />
            Recommendations
          </h3>

          {/* Issues (from failed checks) */}
          {hasIssues && (
            <div className={hasOpportunities ? "mb-5" : ""}>
              <div className="flex items-center gap-1.5 text-xs font-semibold text-red-400 mb-2">
                <AlertCircle className="w-3.5 h-3.5" />
                Issues ({scanResult.next_steps.length})
              </div>
              <div className="space-y-3">
                {scanResult.next_steps.map((step, i) => (
                  <div key={i} className="flex items-start gap-3">
                    <span className="w-5 h-5 rounded-full bg-red-500/15 border border-red-500/30 flex items-center justify-center text-xs font-medium text-red-400 flex-shrink-0 mt-0.5">
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
            </div>
          )}

          {/* Opportunities (from warning checks) */}
          {hasOpportunities && (
            <div>
              {hasIssues && <div className="border-t border-default mb-4" />}
              <div className="flex items-center gap-1.5 text-xs font-semibold text-amber-400 mb-2">
                <AlertTriangle className="w-3.5 h-3.5" />
                Opportunities ({warnMessages.length})
              </div>
              <div className="space-y-3">
                {warnNextSteps.map((step, i) => (
                  <div key={i} className="flex items-start gap-3">
                    <span className="w-5 h-5 rounded-full bg-amber-500/15 border border-amber-500/30 flex items-center justify-center text-xs font-medium text-amber-400 flex-shrink-0 mt-0.5">
                      {i + 1}
                    </span>
                    <div>
                      {warnMessages[i] && (
                        <p className="text-sm font-medium text-primary">{warnMessages[i]}</p>
                      )}
                      <p className="text-sm text-muted">{step}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Single contextual action */}
          {onAction && actionLabel && (
            <div className="mt-4 pt-4 border-t border-default">
              <button
                onClick={onAction}
                className="flex items-center gap-2 text-sm font-medium px-4 py-2.5 rounded-lg bg-accent text-white hover:bg-accent/90 transition-colors"
              >
                {actionIcon}
                {actionLabel}
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
