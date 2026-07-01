import { useState } from "react"
import { Check, RotateCcw, Loader2, AlertCircle, ShieldCheck } from "lucide-react"
import { performResolution } from "@/components/auto-optimize/resolution"

interface ResolutionActionsProps {
  runId: string
  /** Current run status string (APPLIED / DISCARDED reflect a prior resolution). */
  status: string
  /**
   * Whether the run auto-published a champion to the live Genie Space. The
   * Keep/Discard affordance only makes sense once something was published —
   * Discard rolls that back via the space_snapshot re-PATCH (arch §7.3 / D3).
   */
  published?: boolean | null
  /** Fired after a successful Keep (APPLIED) or Discard (DISCARDED). */
  onResolved?: (status: "APPLIED" | "DISCARDED") => void
}

type Pending = "apply" | "discard" | null

/**
 * GSO v2 Phase 13 (item 2) — wires the previously-unused ``applyAutoOptimize`` /
 * ``discardAutoOptimize`` client calls into a Keep / Discard-rollback
 * affordance on the post-run resolution surface.
 *
 * Auto-publish model: the champion config is already live, so **Keep** confirms
 * and marks the run APPLIED, while **Discard** re-PATCHes the original
 * ``space_snapshot`` to roll the live space back (the backend ``discard``
 * endpoint). Discard is guarded behind an inline confirm because it mutates the
 * live production space.
 */
export function ResolutionActions({ runId, status, published, onResolved }: ResolutionActionsProps) {
  const [pending, setPending] = useState<Pending>(null)
  const [error, setError] = useState<string | null>(null)
  const [confirmingDiscard, setConfirmingDiscard] = useState(false)
  // Local resolution overrides the incoming status the moment an action lands,
  // so the surface flips to the resolved state without waiting for a refetch.
  const [resolved, setResolved] = useState<"APPLIED" | "DISCARDED" | null>(null)

  const effectiveStatus = resolved ?? status

  if (effectiveStatus === "APPLIED") {
    return (
      <ResolvedBanner
        tone="success"
        icon={<ShieldCheck className="h-4 w-4" />}
        title="Changes kept"
        detail="The champion configuration is live in this Genie Space."
      />
    )
  }
  if (effectiveStatus === "DISCARDED") {
    return (
      <ResolvedBanner
        tone="secondary"
        icon={<RotateCcw className="h-4 w-4" />}
        title="Changes discarded"
        detail="The Genie Space was rolled back to its pre-optimization snapshot."
      />
    )
  }

  // Nothing was published to the live space — there is nothing to keep or roll
  // back from this surface.
  if (published === false) return null

  async function run(action: "apply" | "discard") {
    setPending(action)
    setError(null)
    const result = await performResolution(action, runId)
    if (result.kind === "resolved") {
      setResolved(result.status)
      onResolved?.(result.status)
    } else {
      setError(result.message)
    }
    setPending(null)
    setConfirmingDiscard(false)
  }

  const busy = pending != null

  return (
    <div className="rounded-xl border border-default bg-surface p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="min-w-0">
          <h3 className="text-sm font-semibold text-primary">Keep or roll back</h3>
          <p className="mt-0.5 text-xs text-muted">
            The optimized configuration is live in this Genie Space. Keep it, or discard the run to
            roll the space back to its pre-optimization snapshot.
          </p>
        </div>
        {!confirmingDiscard ? (
          <div className="flex shrink-0 items-center gap-2">
            <button
              onClick={() => run("apply")}
              disabled={busy}
              className="flex items-center gap-1.5 rounded-md bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white transition-colors hover:bg-emerald-700 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {pending === "apply" ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Check className="h-3.5 w-3.5" />
              )}
              Keep changes
            </button>
            <button
              onClick={() => {
                setError(null)
                setConfirmingDiscard(true)
              }}
              disabled={busy}
              className="flex items-center gap-1.5 rounded-md border border-default px-3 py-1.5 text-sm font-medium text-muted transition-colors hover:text-primary disabled:cursor-not-allowed disabled:opacity-50"
            >
              <RotateCcw className="h-3.5 w-3.5" />
              Discard &amp; roll back
            </button>
          </div>
        ) : (
          <div className="flex shrink-0 flex-col items-end gap-1.5">
            <p className="text-xs font-medium text-amber-600 dark:text-amber-400">
              Roll the live space back to its pre-optimization snapshot?
            </p>
            <div className="flex items-center gap-2">
              <button
                onClick={() => run("discard")}
                disabled={busy}
                className="flex items-center gap-1.5 rounded-md bg-red-600 px-3 py-1.5 text-sm font-medium text-white transition-colors hover:bg-red-700 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {pending === "discard" ? (
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                ) : (
                  <RotateCcw className="h-3.5 w-3.5" />
                )}
                Yes, roll back
              </button>
              <button
                onClick={() => setConfirmingDiscard(false)}
                disabled={busy}
                className="rounded-md border border-default px-3 py-1.5 text-sm font-medium text-muted transition-colors hover:text-primary disabled:cursor-not-allowed disabled:opacity-50"
              >
                Cancel
              </button>
            </div>
          </div>
        )}
      </div>

      {error && (
        <p className="mt-3 flex items-center gap-1.5 text-xs text-red-500">
          <AlertCircle className="h-3.5 w-3.5 shrink-0" />
          {error}
        </p>
      )}
    </div>
  )
}

function ResolvedBanner({
  tone,
  icon,
  title,
  detail,
}: {
  tone: "success" | "secondary"
  icon: React.ReactNode
  title: string
  detail: string
}) {
  const wrap =
    tone === "success"
      ? "border-emerald-300 bg-emerald-50 dark:border-emerald-800 dark:bg-emerald-950/20"
      : "border-default bg-surface"
  const iconColor = tone === "success" ? "text-emerald-600 dark:text-emerald-400" : "text-muted"
  return (
    <div className={`flex items-start gap-3 rounded-xl border px-4 py-3 ${wrap}`}>
      <span className={`mt-0.5 shrink-0 ${iconColor}`}>{icon}</span>
      <div className="min-w-0">
        <h3 className="text-sm font-semibold text-primary">{title}</h3>
        <p className="mt-0.5 text-xs text-muted">{detail}</p>
      </div>
    </div>
  )
}
