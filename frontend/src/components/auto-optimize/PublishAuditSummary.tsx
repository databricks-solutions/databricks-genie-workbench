import { useState, type ReactNode } from "react"
import { Sparkles, AlertTriangle, ChevronDown, ChevronRight, FileText } from "lucide-react"
import type { GSOPublishRecord } from "@/types"

interface PublishAuditSummaryProps {
  /** The publish_and_audit record (arch §7.3). Null on legacy runs / pre-publish. */
  publishRecord: GSOPublishRecord | null | undefined
  /**
   * The legacy per-iteration narrative, demoted to a collapsed expandable
   * detail beneath the LLM headline. Optional — the cockpit doesn't pass one
   * (the Attempt Ladder/Ledger is its trajectory).
   */
  children?: ReactNode
}

/**
 * GSO v2 Phase 13 (arch §7.3) — the publish/audit summary as the headline of
 * the terminal / history view: the LLM-generated 1–2 paragraph human-readable
 * summary of all changes + a concerns callout (the surface where concerns are
 * raised — there is no separate escalation branch).
 *
 * The existing ``OptimizationNarrative`` (passed as ``children``) is demoted to
 * a collapsed expandable detail beneath this headline. For legacy runs that
 * have no publish record, there is no headline to demote into — the narrative
 * stays visible (not collapsed) so the page is never left empty.
 */
export function PublishAuditSummary({ publishRecord, children }: PublishAuditSummaryProps) {
  const [detailOpen, setDetailOpen] = useState(false)

  const summary = publishRecord?.auditSummary?.trim() || null
  const concerns = publishRecord?.concerns ?? []
  const hasHeadline = Boolean(summary) || concerns.length > 0

  // Legacy / no publish record: keep the narrative visible (no headline to
  // demote it under). Render nothing if there's also no narrative.
  if (!hasHeadline) {
    return children ? <>{children}</> : null
  }

  return (
    <div className="space-y-3">
      <div className="rounded-xl border border-indigo-200 bg-gradient-to-br from-indigo-50 to-surface p-5 dark:border-indigo-900/40 dark:from-indigo-950/20 dark:to-surface">
        <div className="flex items-start gap-3">
          <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-indigo-500/10">
            <Sparkles className="h-4 w-4 text-indigo-500" />
          </span>
          <div className="min-w-0 flex-1">
            <h3 className="text-sm font-semibold text-primary">Optimization summary</h3>
            {summary ? (
              <p className="mt-1.5 whitespace-pre-line text-sm leading-relaxed text-primary">
                {summary}
              </p>
            ) : (
              <p className="mt-1.5 text-sm italic text-muted">
                No audit summary was generated for this run.
              </p>
            )}
          </div>
        </div>

        {concerns.length > 0 && (
          <div className="mt-4 rounded-lg border border-amber-300 bg-amber-50 px-3 py-2.5 dark:border-amber-800 dark:bg-amber-950/20">
            <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-amber-700 dark:text-amber-400">
              <AlertTriangle className="h-3.5 w-3.5" />
              Concerns ({concerns.length})
            </p>
            <ul className="mt-1.5 space-y-1">
              {concerns.map((c, i) => (
                <li
                  key={i}
                  className="flex items-start gap-1.5 text-xs text-amber-700 dark:text-amber-300"
                >
                  <span className="mt-1 h-1 w-1 shrink-0 rounded-full bg-amber-500" />
                  <span>{c}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>

      {/* Demoted legacy narrative — collapsed by default. */}
      {children && (
        <div className="rounded-xl border border-default">
          <button
            onClick={() => setDetailOpen((o) => !o)}
            className="flex w-full items-center gap-1.5 px-4 py-2.5 text-xs font-medium text-muted transition-colors hover:text-primary"
          >
            {detailOpen ? (
              <ChevronDown className="h-3.5 w-3.5" />
            ) : (
              <ChevronRight className="h-3.5 w-3.5" />
            )}
            <FileText className="h-3.5 w-3.5" />
            {detailOpen ? "Hide" : "Show"} per-iteration detail
          </button>
          {detailOpen && <div className="border-t border-default px-4 py-3">{children}</div>}
        </div>
      )}
    </div>
  )
}
