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
import { useEffect, useRef, useState } from "react"
import { AlertTriangle, CheckCircle2, Loader2, Undo2, X } from "lucide-react"
import { Button } from "@/components/ui/button"
import { applyExecute, applyPreview, applyUndo, applyUndoPreview } from "@/ontology/api"
import { ApplyDiff } from "@/ontology/components/ApplyDiff"
import { OntologyErrorBoundary } from "@/ontology/components/OntologyErrorBoundary"
import type { ApplyPlan, ApplyResult } from "@/ontology/types"

type Phase = "loading" | "preview" | "executing" | "done" | "error"
// Undo is a self-contained sub-flow that only opens FROM a persisted applied result
// (terminal-state rule): undo-preview → inverse diff (same ApplyDiff) → confirm → undo.
type UndoPhase = "idle" | "loading" | "preview" | "executing" | "done" | "error"

function resultCount(r: ApplyResult): number {
  return r.applied.length + r.failed.length + r.blocked.length
}

// A 409 from execute/undo means the underlying suggestions changed since this preview.
function is409(e: unknown): boolean {
  return !!e && typeof e === "object" && "status" in e && (e as { status: number }).status === 409
}

// Shared error-message builder for the apply AND undo execute paths — removes the
// duplicated 409 detection + copy that previously drifted between the two flows.
function executeErrorMessage(e: unknown, staleMsg: string, fallback: string): string {
  if (is409(e)) return staleMsg
  return e instanceof Error ? e.message : fallback
}

function ApplyPreviewInner({ onClose }: { onClose: () => void }) {
  const [phase, setPhase] = useState<Phase>("loading")
  const [plan, setPlan] = useState<ApplyPlan | null>(null)
  const [result, setResult] = useState<ApplyResult | null>(null)
  const [confirmed, setConfirmed] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Undo sub-flow (opens only from a persisted applied result — terminal-state rule).
  const [undoPhase, setUndoPhase] = useState<UndoPhase>("idle")
  const [undoPlan, setUndoPlan] = useState<ApplyPlan | null>(null)
  const [undoResult, setUndoResult] = useState<ApplyResult | null>(null)
  const [undoConfirmed, setUndoConfirmed] = useState(false)
  const [undoError, setUndoError] = useState<string | null>(null)
  // When the inverse diff appears, move focus to its heading so a keyboard/AT user
  // isn't dropped to <body> when the "Undo these changes" trigger unmounts.
  const undoHeadingRef = useRef<HTMLParagraphElement | null>(null)

  useEffect(() => {
    if (undoPhase === "preview") undoHeadingRef.current?.focus()
  }, [undoPhase])

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
      setError(
        executeErrorMessage(
          e,
          "These suggestions changed since you opened this. Close and preview again to see the latest.",
          "Couldn't apply the changes — please try again.",
        ),
      )
      setPhase("error")
    }
  }

  const executable = plan?.items.filter((i) => i.executable) ?? []
  const blocked = plan?.items.filter((i) => !i.executable) ?? []
  // Distinct copy-ready grant lines across all blocked items (the admin runs these once).
  const grants = Array.from(new Set(blocked.flatMap((i) => i.required_grants)))

  // Undo initializes ONLY from persisted applied rows (terminal-state rule): the
  // proposals to reverse are exactly those that landed (result.applied).
  const undoProposalIds = Array.from(new Set((result?.applied ?? []).map((o) => o.proposal_id)))

  const openUndo = async () => {
    setUndoPhase("loading")
    setUndoError(null)
    setUndoConfirmed(false)
    try {
      const p = await applyUndoPreview(undoProposalIds)
      setUndoPlan(p)
      setUndoPhase("preview")
    } catch (e) {
      setUndoError(e instanceof Error ? e.message : "Couldn't preview the undo.")
      setUndoPhase("error")
    }
  }

  const runUndo = async () => {
    if (!undoPlan || !undoConfirmed) return
    setUndoPhase("executing")
    setUndoError(null)
    try {
      const r = await applyUndo({
        plan_hash: undoPlan.plan_hash,
        confirm: true,
        proposal_ids: undoProposalIds,
      })
      setUndoResult(r)
      setUndoPhase("done")
    } catch (e) {
      setUndoError(
        executeErrorMessage(
          e,
          "These changes moved on since you opened this. Close and preview again to see the latest.",
          "Couldn't undo the changes — please try again.",
        ),
      )
      setUndoPhase("error")
    }
  }

  const undoExecutable = undoPlan?.items.filter((i) => i.executable) ?? []
  const undoBlocked = undoPlan?.items.filter((i) => !i.executable) ?? []
  const undoGrants = Array.from(new Set(undoBlocked.flatMap((i) => i.required_grants)))
  const undoNotes = undoPlan?.notes ?? []

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
          <X aria-hidden className="h-4 w-4" />
        </Button>
      </div>

      <div className="mt-3">
        {phase === "loading" && (
          <p role="status" className="flex items-center gap-2 text-sm text-secondary">
            <Loader2 aria-hidden className="h-4 w-4 animate-spin" /> Checking what's ready to apply…
          </p>
        )}

        {phase === "error" && (
          <div role="alert" className="flex items-start gap-2 rounded-lg border border-danger/30 bg-danger/5 px-3 py-2.5 text-sm text-danger-foreground">
            <AlertTriangle aria-hidden className="mt-0.5 h-4 w-4 shrink-0" />
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
            <ApplyDiff executable={executable} blocked={blocked} grants={grants} />

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
                        <Loader2 aria-hidden className="h-4 w-4 animate-spin" /> Applying…
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
            <p role="status" className="flex items-center gap-2 text-sm font-semibold text-primary">
              <CheckCircle2 aria-hidden className="h-4 w-4 text-info-foreground" />
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

            {/* Undo: opens only from a persisted applied result (terminal-state rule). */}
            {undoProposalIds.length > 0 && undoPhase !== "done" && (
              <div className="mt-4 border-t border-default pt-3">
                {undoPhase === "idle" && (
                  <Button size="sm" variant="ghost" onClick={openUndo}>
                    <Undo2 aria-hidden className="h-4 w-4" /> Undo these changes
                  </Button>
                )}

                {undoPhase === "loading" && (
                  <p role="status" className="flex items-center gap-2 text-sm text-secondary">
                    <Loader2 aria-hidden className="h-4 w-4 animate-spin" /> Checking what can be undone…
                  </p>
                )}

                {undoPhase === "error" && (
                  <div>
                    <div role="alert" className="flex items-start gap-2 rounded-lg border border-danger/30 bg-danger/5 px-3 py-2.5 text-sm text-danger-foreground">
                      <AlertTriangle aria-hidden className="mt-0.5 h-4 w-4 shrink-0" />
                      <span>{undoError}</span>
                    </div>
                    <Button size="sm" variant="ghost" className="mt-2" onClick={openUndo}>
                      Try again
                    </Button>
                  </div>
                )}

                {(undoPhase === "preview" || undoPhase === "executing") && undoPlan && (
                  <>
                    <p
                      ref={undoHeadingRef}
                      tabIndex={-1}
                      className="mb-2 rounded text-xs font-semibold text-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/40"
                    >
                      Undo these changes
                    </p>
                    {undoExecutable.length === 0 && undoBlocked.length === 0 && undoNotes.length === 0 ? (
                      <p className="text-sm text-secondary">There's nothing to undo here.</p>
                    ) : (
                      <ApplyDiff
                        executable={undoExecutable}
                        blocked={undoBlocked}
                        grants={undoGrants}
                        notes={undoNotes}
                      />
                    )}
                    {undoExecutable.length > 0 && (
                      <div className="mt-3 border-t border-default pt-3">
                        <label className="flex items-start gap-2 text-sm text-primary">
                          <input
                            type="checkbox"
                            className="mt-0.5 h-4 w-4"
                            checked={undoConfirmed}
                            disabled={undoPhase === "executing"}
                            onChange={(e) => setUndoConfirmed(e.target.checked)}
                          />
                          <span>
                            I've reviewed these {undoExecutable.length} change
                            {undoExecutable.length === 1 ? "" : "s"} and want to undo them under my account.
                          </span>
                        </label>
                        <div className="mt-3 flex items-center gap-2">
                          <Button size="sm" onClick={runUndo} disabled={!undoConfirmed || undoPhase === "executing"}>
                            {undoPhase === "executing" ? (
                              <>
                                <Loader2 aria-hidden className="h-4 w-4 animate-spin" /> Undoing…
                              </>
                            ) : (
                              "Undo these changes"
                            )}
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => setUndoPhase("idle")}
                            disabled={undoPhase === "executing"}
                          >
                            Cancel
                          </Button>
                        </div>
                      </div>
                    )}
                  </>
                )}
              </div>
            )}

            {undoPhase === "done" && undoResult && (
              <div className="mt-4 border-t border-default pt-3">
                <p role="status" className="flex items-center gap-2 text-sm font-semibold text-primary">
                  <CheckCircle2 aria-hidden className="h-4 w-4 text-info-foreground" />
                  Undid {undoResult.applied.length} of {resultCount(undoResult)} change
                  {resultCount(undoResult) === 1 ? "" : "s"}.
                </p>
                {(undoResult.failed.length > 0 || undoResult.blocked.length > 0) && (
                  <p className="mt-1 text-xs text-secondary">
                    {undoResult.blocked.length > 0 && `${undoResult.blocked.length} still need a permission. `}
                    {undoResult.failed.length > 0 && `${undoResult.failed.length} couldn't be undone. `}
                  </p>
                )}
              </div>
            )}

            <div className="mt-3">
              <Button
                size="sm"
                variant="secondary"
                onClick={onClose}
                disabled={undoPhase === "executing"}
              >
                Done
              </Button>
            </div>
          </div>
        )}
      </div>
    </section>
  )
}

// Wrap the write surface in a render-error boundary (playbook: mandatory on these
// surfaces). The inner component owns async failures via phase state; the boundary
// catches a render-time throw so it degrades to a recoverable card, not a blank page.
export function ApplyPreview(props: { onClose: () => void }) {
  return (
    <OntologyErrorBoundary>
      <ApplyPreviewInner {...props} />
    </OntologyErrorBoundary>
  )
}
