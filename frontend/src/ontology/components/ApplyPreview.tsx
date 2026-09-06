/**
 * 17.0d — "Apply for me → Preview changes" (Phase 5 / 17i). The single surface for
 * the subsystem's ONLY governed-tag write. It dry-runs the apply (POST /apply/preview),
 * shows the pending changes in PLAIN language grouped as adds / moves / new groupings
 * (+ any that need a permission first, with the exact copy-ready grant), then — behind
 * an explicit "I've reviewed these" confirm — executes them (POST /apply/execute) under
 * the curator's own identity and reports what landed.
 *
 * Zero-burden (MV-D23): the server owns the SQL; this component NEVER renders the raw
 * statement, plan fingerprint, table names, or any tag/DDL jargon — only the human
 * effect of each change. The two-step consent (approve the card → confirm here) is the
 * gate; a stale plan is rejected server-side and surfaced as a refresh prompt.
 */
import { useEffect, useState } from "react"
import { AlertTriangle, ArrowRight, CheckCircle2, Loader2, Lock, Plus, X } from "lucide-react"
import { Button } from "@/components/ui/button"
import { applyExecute, applyPreview } from "@/ontology/api"
import { describeChange } from "@/ontology/applyDescribe"
import type { ApplyPlan, ApplyResult } from "@/ontology/types"

type Phase = "loading" | "preview" | "executing" | "done" | "error"

function resultCount(r: ApplyResult): number {
  return r.applied.length + r.failed.length + r.blocked.length
}

export function ApplyPreview({ onClose }: { onClose: () => void }) {
  const [phase, setPhase] = useState<Phase>("loading")
  const [plan, setPlan] = useState<ApplyPlan | null>(null)
  const [result, setResult] = useState<ApplyResult | null>(null)
  const [confirmed, setConfirmed] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let live = true
    ;(async () => {
      try {
        const p = await applyPreview()
        if (!live) return
        setPlan(p)
        setPhase("preview")
      } catch (e) {
        if (!live) return
        setError(e instanceof Error ? e.message : "Couldn't preview the changes.")
        setPhase("error")
      }
    })()
    return () => {
      live = false
    }
  }, [])

  const execute = async () => {
    if (!plan || !confirmed) return
    setPhase("executing")
    setError(null)
    try {
      const r = await applyExecute({ plan_hash: plan.plan_hash, confirm: true })
      setResult(r)
      setPhase("done")
    } catch (e) {
      // A 409 means the underlying suggestions changed since this preview.
      const stale = e && typeof e === "object" && "status" in e && (e as { status: number }).status === 409
      setError(
        stale
          ? "These suggestions changed since you opened this. Close and preview again to see the latest."
          : e instanceof Error
            ? e.message
            : "Couldn't apply the changes — please try again.",
      )
      setPhase("error")
    }
  }

  const executable = plan?.items.filter((i) => i.executable) ?? []
  const blocked = plan?.items.filter((i) => !i.executable) ?? []
  // Distinct copy-ready grant lines across all blocked items (the admin runs these once).
  const grants = Array.from(new Set(blocked.flatMap((i) => i.required_grants)))

  return (
    <section className="rounded-xl border border-accent/30 bg-accent/5 p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold text-primary">Apply approved changes</h3>
          <p className="mt-0.5 text-xs text-secondary">
            Organizes the assets you approved. Nothing is changed until you confirm below.
          </p>
        </div>
        <Button variant="ghost" size="sm" onClick={onClose} aria-label="Close">
          <X className="h-4 w-4" />
        </Button>
      </div>

      <div className="mt-3">
        {phase === "loading" && (
          <p className="flex items-center gap-2 text-sm text-secondary">
            <Loader2 className="h-4 w-4 animate-spin" /> Checking what's ready to apply…
          </p>
        )}

        {phase === "error" && (
          <div className="flex items-start gap-2 rounded-lg border border-danger/30 bg-danger/5 px-3 py-2.5 text-sm text-danger-foreground">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
            <span>{error}</span>
          </div>
        )}

        {phase === "preview" && plan && executable.length === 0 && blocked.length === 0 && (
          <p className="text-sm text-secondary">
            Nothing to apply yet. Approve a domain or sub-domain suggestion first, then come back.
          </p>
        )}

        {(phase === "preview" || phase === "executing") && plan && (executable.length > 0 || blocked.length > 0) && (
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

            {executable.length > 0 && (
              <div className="mt-4 border-t border-default pt-3">
                <label className="flex items-start gap-2 text-sm text-primary">
                  <input
                    type="checkbox"
                    className="mt-0.5 h-4 w-4"
                    checked={confirmed}
                    disabled={phase === "executing"}
                    onChange={(e) => setConfirmed(e.target.checked)}
                  />
                  <span>
                    I've reviewed these {executable.length} change{executable.length === 1 ? "" : "s"} and want to
                    apply them under my account.
                  </span>
                </label>
                <div className="mt-3 flex items-center gap-2">
                  <Button size="sm" onClick={execute} disabled={!confirmed || phase === "executing"}>
                    {phase === "executing" ? (
                      <>
                        <Loader2 className="h-4 w-4 animate-spin" /> Applying…
                      </>
                    ) : (
                      "Apply these changes"
                    )}
                  </Button>
                  <Button variant="ghost" size="sm" onClick={onClose} disabled={phase === "executing"}>
                    Cancel
                  </Button>
                </div>
              </div>
            )}
          </>
        )}

        {phase === "done" && result && (
          <div>
            <p className="flex items-center gap-2 text-sm font-semibold text-primary">
              <CheckCircle2 className="h-4 w-4 text-info-foreground" />
              Applied {result.applied.length} of {resultCount(result)} change
              {resultCount(result) === 1 ? "" : "s"}.
            </p>
            {(result.failed.length > 0 || result.blocked.length > 0) && (
              <p className="mt-1 text-xs text-secondary">
                {result.blocked.length > 0 && `${result.blocked.length} still need a permission. `}
                {result.failed.length > 0 && `${result.failed.length} couldn't be applied. `}
                The rest are done and won't show up again.
              </p>
            )}
            <div className="mt-3">
              <Button size="sm" variant="secondary" onClick={onClose}>
                Done
              </Button>
            </div>
          </div>
        )}
      </div>
    </section>
  )
}
