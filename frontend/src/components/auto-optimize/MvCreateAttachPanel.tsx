/**
 * Create-and-attach output panel (Prompt 13, mockup frame 5).
 *
 * One metric view the backend created under OBO for this run, wired to
 * GET /runs/{run_id}/mv-created. Shows baseline vs post-attach accuracy from the
 * isolated lift report (needs-review counted separately, never folded in), the
 * DETACHED regression state with an OBO-only [Drop view] flow, the join strategy,
 * and the copy-ready GRANT. MV-D23: run_id is presentational; this component keys
 * nothing on it — it takes the object as a prop.
 *
 * `tables_freed` is intentionally omitted: it has no producer anywhere (the
 * gap-report row stays DOES-NOT-EXIST-YET), so per the mv_materialize precedent
 * it is not shipped rather than fabricated.
 */
import { useState } from "react"
import { AlertTriangle, ArrowUpRight, GitBranch, Trash2 } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { SqlCodeBlock } from "@/components/SqlCodeBlock"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { dropMvCreated } from "@/lib/api"
import { joinStrategyLabel } from "@/components/auto-optimize/mvFormat"
import type { MvCreatedObject, MvDdlArtifact } from "@/types"

interface MvCreateAttachPanelProps {
  obj: MvCreatedObject
  ddl?: MvDdlArtifact | null
  /** Catalog Explorer deep link, or null when no workspace origin is known. */
  catalogUrl: string | null
  /** Called after a successful drop so the container can refresh the ledger. */
  onDropped?: (suggestionId: string) => void
}

function pct(value: number | null | undefined): string {
  if (value == null) return "—"
  return `${Math.round(value * 100)}%`
}

function EvalRunId({ id }: { id: string | null | undefined }) {
  if (!id) return null
  return <p className="mt-0.5 font-mono text-xs text-muted">{id}</p>
}

export function MvCreateAttachPanel({ obj, ddl, catalogUrl, onDropped }: MvCreateAttachPanelProps) {
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [dropping, setDropping] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const detached = obj.status === "DETACHED"
  const lift = obj.lift_report
  const joinLabel = joinStrategyLabel(ddl?.join_strategy)
  const baselineEvalId = obj.baseline_eval_run_id ?? lift?.pre_eval_run_id ?? null
  const postEvalId = obj.post_attach_eval_run_id ?? lift?.post_eval_run_id ?? null

  async function handleDrop() {
    setDropping(true)
    setError(null)
    try {
      await dropMvCreated(obj.suggestion_id, { run_id: obj.run_id, confirm: true })
      setConfirmOpen(false)
      onDropped?.(obj.suggestion_id)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to drop the metric view.")
    } finally {
      setDropping(false)
    }
  }

  return (
    <div className="space-y-4 rounded-xl border border-default bg-surface p-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          {catalogUrl ? (
            <a
              className="inline-flex items-center gap-1 font-mono text-sm font-medium text-accent hover:underline"
              href={catalogUrl}
              target="_blank"
              rel="noopener noreferrer"
            >
              {obj.full_name}
              <ArrowUpRight className="h-3.5 w-3.5" />
            </a>
          ) : (
            <p className="font-mono text-sm font-medium text-primary">{obj.full_name}</p>
          )}
          <p className="mt-0.5 text-xs text-muted">
            Created under OBO{obj.created_by ? ` by ${obj.created_by}` : ""}
            {catalogUrl ? " · opens in Catalog Explorer" : ""}
          </p>
          <p className="mt-0.5 text-xs text-muted">
            Provenance <span className="font-mono">OBO_CREATED</span> — the app created this view, so
            the app can drop it. Views you register yourself are{" "}
            <span className="font-mono">USER_CREATED</span> and the app never drops them.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          <Badge variant={detached ? "danger" : "secondary"}>{obj.status}</Badge>
          <Badge variant="secondary">OBO_CREATED</Badge>
          {joinLabel && (
            <Badge variant="secondary">
              <GitBranch className="mr-1 h-3 w-3" />
              {joinLabel}
            </Badge>
          )}
        </div>
      </div>

      {detached && (
        <div className="flex items-start gap-1.5 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-700 dark:text-amber-300">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <span>
            Attaching this metric view lowered benchmark accuracy, so the run reverted to the
            pre-attach snapshot (whole-snapshot revert). The view still exists in Unity Catalog and
            can be dropped below.
          </span>
        </div>
      )}

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
        <div className="rounded-lg border border-default bg-elevated px-3 py-2">
          <p className="text-xs text-muted">Baseline accuracy</p>
          <p className="text-lg font-semibold text-primary">{pct(lift?.pre_accuracy_affected)}</p>
          <EvalRunId id={baselineEvalId} />
        </div>
        <div className="rounded-lg border border-default bg-elevated px-3 py-2">
          <p className="text-xs text-muted">Post-attach accuracy</p>
          <p
            className={`text-lg font-semibold ${detached ? "text-danger" : "text-primary"}`}
          >
            {pct(lift?.post_accuracy_affected)}
          </p>
          <EvalRunId id={postEvalId} />
        </div>
        <div className="rounded-lg border border-default bg-elevated px-3 py-2">
          <p className="text-xs text-muted">Needs review</p>
          <p className="text-lg font-semibold text-primary">{lift?.needs_review_count ?? "—"}</p>
          <p className="text-xs text-muted">counted separately</p>
        </div>
      </div>

      {ddl?.grant_sql && (
        <div className="space-y-1.5">
          <SqlCodeBlock code={ddl.grant_sql} />
          <p className="text-xs text-muted">
            Without this grant, other users&rsquo; Genie answers silently degrade — they cannot read
            the metric view.
          </p>
        </div>
      )}

      {detached && (
        <div className="flex flex-wrap items-center gap-2 pt-1">
          <Button size="sm" variant="danger" onClick={() => setConfirmOpen(true)}>
            <Trash2 className="mr-1.5 h-3.5 w-3.5" />
            Drop view
          </Button>
          <span className="text-xs text-muted">Drop is allowed only while DETACHED.</span>
        </div>
      )}

      {error && <p className="text-xs text-danger">{error}</p>}

      <AlertDialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Drop {obj.full_name}?</AlertDialogTitle>
            <AlertDialogDescription>
              Other consumers may depend on it. This permanently drops the metric view from Unity
              Catalog. This cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => setConfirmOpen(false)} disabled={dropping}>
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              className="bg-danger hover:bg-danger/90"
              onClick={handleDrop}
              disabled={dropping}
            >
              {dropping ? "Dropping…" : "Drop view"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
