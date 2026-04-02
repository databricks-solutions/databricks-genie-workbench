/**
 * FixAgentPanel — Task-list UI for fixing a Genie Space.
 * Uses the dedicated fix agent endpoint (POST /api/spaces/{id}/fix) which
 * generates JSON patches and applies them directly — no conversational chat.
 */
import { useState, useRef, useEffect } from "react"
import {
  X,
  Loader2,
  Check,
  AlertCircle,
  Wrench,
  RefreshCw,
  ChevronDown,
  ChevronRight,
  Info,
} from "lucide-react"
import { streamFixAgent } from "@/lib/api"
import type { FixPatch } from "@/types"

type Phase = "running" | "applying" | "complete" | "error"
interface Issue {
  text: string
  status: "pending" | "fixing" | "fixed" | "error"
  patch?: FixPatch
}

interface FixAgentPanelProps {
  spaceId: string
  displayName: string
  findings: string[]
  spaceConfig: Record<string, unknown>
  onClose: () => void
  onComplete: () => void
}

export function FixAgentPanel({ spaceId, displayName, findings, spaceConfig, onClose, onComplete }: FixAgentPanelProps) {
  const [issues, setIssues] = useState<Issue[]>(() =>
    findings.map(text => ({ text, status: "pending" as const }))
  )
  const [phase, setPhase] = useState<Phase>("running")
  const [statusMessage, setStatusMessage] = useState("Analyzing issues...")
  const [summary, setSummary] = useState("")
  const [errorMessage, setErrorMessage] = useState("")
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null)

  const abortRef = useRef<(() => void) | null>(null)
  const patchIndexRef = useRef(0)
  const phaseRef = useRef<Phase>("running")

  // Start fix agent on mount
  useEffect(() => {
    const abort = streamFixAgent(
      spaceId,
      findings,
      spaceConfig,
      (event) => {
        switch (event.status) {
          case "thinking":
            setStatusMessage(event.message || "Analyzing...")
            // Mark first pending issue as "fixing" to show activity
            setIssues(prev => {
              const idx = prev.findIndex(i => i.status === "pending")
              if (idx === -1) return prev
              return prev.map((item, i) => i === idx ? { ...item, status: "fixing" } : item)
            })
            break

          case "patch":
            // Advance the next pending/fixing issue to "fixed"
            setIssues(prev => {
              const idx = patchIndexRef.current
              patchIndexRef.current++
              return prev.map((item, i) => {
                if (i === idx) {
                  return {
                    ...item,
                    status: "fixed" as const,
                    patch: event.field_path ? {
                      field_path: event.field_path,
                      old_value: event.old_value,
                      new_value: event.new_value,
                      rationale: event.rationale || "",
                    } : undefined,
                  }
                }
                // Mark the next one as "fixing"
                if (i === idx + 1 && item.status === "pending") {
                  return { ...item, status: "fixing" as const }
                }
                return item
              })
            })
            setStatusMessage(`Applied patch: ${event.field_path || "config update"}`)
            break

          case "applying":
            setPhase("applying")
            phaseRef.current = "applying"
            setStatusMessage(event.message || "Applying changes to Databricks...")
            break

          case "complete": {
            const applied = event.patches_applied ?? 0
            if (applied > 0) {
              // Only mark remaining as fixed if patches were actually applied
              setIssues(prev => prev.map(item =>
                item.status === "pending" || item.status === "fixing"
                  ? { ...item, status: "fixed" as const }
                  : item
              ))
            } else {
              // No patches applied — reset any "fixing" back to pending
              setIssues(prev => prev.map(item =>
                item.status === "fixing" ? { ...item, status: "pending" as const } : item
              ))
            }
            const newPhase = applied > 0 ? "complete" : "error"
            setPhase(newPhase)
            phaseRef.current = newPhase
            setSummary(event.summary || (applied > 0
              ? `Applied ${applied} fix(es)`
              : "Could not generate fixes — try re-scanning first"))
            if (applied === 0) setErrorMessage(event.summary || "No patches could be generated")
            break
          }

          case "error":
            setPhase("error")
            phaseRef.current = "error"
            setErrorMessage(event.message || "Fix agent encountered an error")
            // Mark any in-progress issue as error
            setIssues(prev => prev.map(item =>
              item.status === "fixing" ? { ...item, status: "error" as const } : item
            ))
            break
        }
      },
      (error) => {
        // If the stream drops during the "applying" phase, the backend's
        // executor thread will complete the PATCH call independently.
        // Treat as success — the user can re-scan to verify.
        if (phaseRef.current === "applying") {
          setIssues(prev => prev.map(item =>
            item.status === "pending" || item.status === "fixing"
              ? { ...item, status: "fixed" as const }
              : item
          ))
          setPhase("complete")
          phaseRef.current = "complete"
          setSummary("Changes applied — re-scan to verify")
        } else {
          setPhase("error")
          phaseRef.current = "error"
          setErrorMessage(error.message || "Connection failed")
        }
      },
    )
    abortRef.current = abort

    return () => { abort() }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const fixedCount = issues.filter(i => i.status === "fixed").length
  const totalCount = issues.length
  // Detect if any finding mentions 50+ columns needing descriptions
  const hasBulkColumns = findings.some(f => {
    const m = f.match(/(\d+)\s+columns?\s+have\s+descriptions/i) || f.match(/(\d+)\/(\d+)\s+columns/i)
    if (!m) return false
    const total = parseInt(m[2] ?? m[1], 10)
    return total >= 50
  })

  return (
    <div className="border border-default rounded-xl bg-surface flex flex-col h-[70vh] max-h-[700px] shadow-2xl">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-default">
        <div className="flex items-center gap-2 min-w-0">
          <div className="w-6 h-6 rounded-lg bg-accent/10 flex items-center justify-center flex-shrink-0">
            <Wrench className="w-3.5 h-3.5 text-accent" />
          </div>
          <h3 className="text-sm font-semibold text-primary truncate">Fixing: {displayName}</h3>
        </div>
        <button
          onClick={onClose}
          className="p-1 rounded-md text-muted hover:text-secondary hover:bg-surface-secondary transition-colors"
        >
          <X className="w-4 h-4" />
        </button>
      </div>

      {/* Status bar */}
      <div className="px-4 py-2.5 border-b border-default bg-surface-secondary/50">
        {phase === "complete" ? (
          <div className="flex items-center gap-2">
            <Check className="w-4 h-4 text-emerald-500" />
            <span className="text-xs font-medium text-emerald-500">{summary}</span>
          </div>
        ) : phase === "error" ? (
          <div className="flex items-center gap-2">
            <AlertCircle className="w-4 h-4 text-red-400" />
            <span className="text-xs font-medium text-red-400">Fix failed</span>
          </div>
        ) : (
          <div className="flex items-center gap-2">
            <Loader2 className="w-4 h-4 text-accent animate-spin" />
            <span className="text-xs font-medium text-muted">
              {phase === "applying"
                ? "Applying changes to Databricks..."
                : fixedCount > 0
                  ? `Fixed ${fixedCount} of ${totalCount}...`
                  : statusMessage
              }
            </span>
          </div>
        )}
        {/* Progress bar */}
        {totalCount > 0 && (
          <div className="mt-2 h-1 bg-elevated rounded-full overflow-hidden">
            <div
              className={`h-full rounded-full transition-all duration-500 ${
                phase === "complete" ? "bg-emerald-500" : phase === "error" ? "bg-red-400" : "bg-accent"
              }`}
              style={{ width: `${phase === "complete" ? 100 : (fixedCount / totalCount) * 100}%` }}
            />
          </div>
        )}
      </div>

      {/* Issue list */}
      <div className="flex-1 overflow-y-auto px-4 py-3 space-y-1">
        {issues.map((issue, idx) => (
          <div key={idx}>
            <button
              onClick={() => issue.patch && setExpandedIdx(expandedIdx === idx ? null : idx)}
              className={`flex items-start gap-3 w-full text-left py-2 px-2 rounded-lg transition-colors ${
                issue.patch ? "hover:bg-surface-secondary cursor-pointer" : "cursor-default"
              }`}
            >
              {/* Status icon */}
              <div className="mt-0.5 flex-shrink-0">
                {issue.status === "fixed" ? (
                  <div className="w-5 h-5 rounded-full bg-emerald-500/20 flex items-center justify-center">
                    <Check className="w-3 h-3 text-emerald-500" />
                  </div>
                ) : issue.status === "fixing" ? (
                  <div className="w-5 h-5 rounded-full bg-accent/15 flex items-center justify-center">
                    <Loader2 className="w-3 h-3 text-accent animate-spin" />
                  </div>
                ) : issue.status === "error" ? (
                  <div className="w-5 h-5 rounded-full bg-red-500/20 flex items-center justify-center">
                    <AlertCircle className="w-3 h-3 text-red-400" />
                  </div>
                ) : (
                  <div className="w-5 h-5 rounded-full bg-elevated flex items-center justify-center">
                    <div className="w-2 h-2 rounded-full bg-[var(--border-color)]" />
                  </div>
                )}
              </div>

              {/* Issue text */}
              <div className="flex-1 min-w-0">
                <p className={`text-sm ${
                  issue.status === "fixed" ? "text-secondary" : issue.status === "error" ? "text-red-400" : "text-primary"
                }`}>
                  {issue.text}
                </p>
                {issue.patch && (
                  <div className="flex items-center gap-1 mt-0.5">
                    {expandedIdx === idx
                      ? <ChevronDown className="w-3 h-3 text-muted" />
                      : <ChevronRight className="w-3 h-3 text-muted" />
                    }
                    <span className="text-xs text-muted font-mono">{issue.patch.field_path}</span>
                  </div>
                )}
              </div>
            </button>

            {/* Expanded patch detail */}
            {expandedIdx === idx && issue.patch && (
              <div className="ml-10 mb-2 px-3 py-2 bg-surface-secondary rounded-lg text-xs space-y-1.5">
                {issue.patch.rationale && (
                  <p className="text-secondary">{issue.patch.rationale}</p>
                )}
                <div className="font-mono text-muted">
                  <span className="text-red-400 line-through">{formatValue(issue.patch.old_value)}</span>
                  {" → "}
                  <span className="text-emerald-400">{formatValue(issue.patch.new_value)}</span>
                </div>
              </div>
            )}
          </div>
        ))}

        {/* Error detail */}
        {phase === "error" && errorMessage && (
          <div className="mt-2 px-3 py-2 bg-red-500/10 border border-red-500/25 rounded-lg">
            <p className="text-xs text-red-400">{errorMessage}</p>
          </div>
        )}

        {/* UC AI Generate tip for bulk column descriptions */}
        {phase === "complete" && hasBulkColumns && (
          <div className="mt-3 mx-1 px-3 py-2.5 bg-blue-500/5 border border-blue-500/20 rounded-lg">
            <div className="flex items-start gap-2">
              <Info className="w-3.5 h-3.5 text-blue-400 flex-shrink-0 mt-0.5" />
              <div className="text-xs text-blue-300/90 leading-relaxed">
                <p className="font-medium text-blue-400 mb-1">Many columns still need descriptions</p>
                <p>
                  For bulk column descriptions, use <strong>AI Generate</strong> in Unity Catalog
                  (Table &rarr; columns tab &rarr; "AI Generate"). It uses actual data samples to produce
                  more accurate descriptions than config-level fixes.
                </p>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Footer */}
      <div className="border-t border-default px-4 py-3">
        {phase === "complete" ? (
          <button
            onClick={onComplete}
            className="w-full flex items-center justify-center gap-2 px-4 py-2.5 text-sm font-medium bg-accent text-white rounded-lg hover:bg-accent/90 transition-colors"
          >
            <RefreshCw className="w-4 h-4" />
            Re-scan Score
          </button>
        ) : phase === "error" ? (
          <button
            onClick={onClose}
            className="w-full flex items-center justify-center gap-2 px-4 py-2.5 text-sm font-medium border border-default text-secondary rounded-lg hover:bg-elevated transition-colors"
          >
            Close
          </button>
        ) : (
          <button
            onClick={() => abortRef.current?.()}
            className="w-full flex items-center justify-center gap-2 px-4 py-2.5 text-sm font-medium border border-red-500/30 text-red-400 rounded-lg hover:bg-red-500/10 transition-colors"
          >
            Cancel
          </button>
        )}
      </div>
    </div>
  )
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return "null"
  if (typeof value === "string") return value.length > 80 ? value.slice(0, 80) + "..." : value
  return JSON.stringify(value).slice(0, 80)
}
