/**
 * Phase 5 (17i) — the prop-driven diff panel for the consented governed-tag apply.
 * Split out of ApplyPreview so the adds / moves / blocked-copy-ready rows can be
 * rendered and asserted directly (no fetch/effects), and so the zero-burden contract
 * (MV-D23) has a dedicated home: describe the human EFFECT of each change, never the
 * SQL that realizes it (the server owns `ApplyItem.statement`; it is never rendered).
 */
import { ArrowRight, Lock, Plus } from "lucide-react"
import { describeChange } from "@/ontology/applyDescribe"
import type { ApplyItem } from "@/ontology/types"

export function ApplyDiff({
  executable,
  blocked,
  grants,
}: {
  executable: ApplyItem[]
  blocked: ApplyItem[]
  grants: string[]
}) {
  return (
    <>
      {executable.length > 0 && (
        <ul className="space-y-1.5">
          {executable.map((item, idx) => (
            <li key={`${item.proposal_id}-${idx}`} className="flex items-start gap-2 text-sm text-primary">
              {item.shape === "create_tag" ? (
                <Plus className="mt-0.5 h-3.5 w-3.5 shrink-0 text-accent" />
              ) : (
                <ArrowRight className="mt-0.5 h-3.5 w-3.5 shrink-0 text-accent" />
              )}
              <span>{describeChange(item)}</span>
            </li>
          ))}
        </ul>
      )}

      {blocked.length > 0 && (
        <div className="mt-3 rounded-lg border border-warning/30 bg-warning/5 px-3 py-2.5">
          <p className="flex items-center gap-1.5 text-xs font-semibold text-warning-foreground">
            <Lock className="h-3.5 w-3.5" />
            {blocked.length} change{blocked.length === 1 ? "" : "s"} need a permission first
          </p>
          {grants.length > 0 && (
            <ul className="mt-1.5 list-disc space-y-0.5 pl-5 text-xs text-secondary">
              {grants.map((g) => (
                <li key={g}>{g}</li>
              ))}
            </ul>
          )}
          <p className="mt-1.5 text-xs text-muted">
            Ask an account admin to grant these, then preview again.
          </p>
        </div>
      )}
    </>
  )
}
