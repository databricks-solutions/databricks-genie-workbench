/**
 * MV-advisor mockups — REVIEW SCAFFOLD, NOT PRODUCTION CODE (see mvMockData.ts).
 *
 * Frames 4–5: the run output / results screen panels, composed into
 * RunDetailView.tsx for real by Prompt 13 (which deletes this file).
 *  - Frame 4 (suggest-only, POV §7.5): the shared MvProposalCard with the DDL +
 *    GRANT panels, the VERBATIM "Lift not measured…" label passed via the
 *    liftLabel slot, and [Approve for re-run] + [Re-run with this metric view]
 *    passed via the actions slot.
 *  - Frame 5 (create_and_attach): the created-object result with baseline vs
 *    post-attach accuracy (both eval_run_ids linked), tables_freed, the DETACHED
 *    regression badge + [Drop view], a downgrade banner, and the GRANT panel.
 * MV-D23: run_id is presentational here; nothing keys state/fetch on it.
 */
import { AlertTriangle, ArrowUpRight, Copy, ShieldAlert, Trash2 } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { MvProposalCard } from "./MvProposalCard"
import { createdDetached, ddlRevenue, proposalMargin, proposalRevenue } from "./mvMockData"

// POV §7.5 — verbatim, mandatory on every run-1 output. Prompt 13 reuses this string.
const LIFT_NOT_MEASURED = "Lift not measured — this metric view was not created or attached during this run."

export function LiftNotMeasuredLabel() {
  return (
    <div className="flex items-start gap-1.5 rounded-lg border border-default bg-elevated px-3 py-2 text-xs text-secondary">
      <ShieldAlert className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted" />
      <span>{LIFT_NOT_MEASURED}</span>
    </div>
  )
}

// ── Frame 4 — suggest-only output (POV §7.5) ────────────────────────────────
export function SuggestOnlyOutputFrame() {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold uppercase tracking-wide text-secondary">
          Metric views proposed
        </h3>
        <span className="text-xs text-muted">2 proposed · none created</span>
      </div>
      <MvProposalCard
        proposal={proposalRevenue}
        ddl={ddlRevenue}
        liftLabel={<LiftNotMeasuredLabel />}
        actions={
          <>
            <Button size="sm" variant="secondary">Approve for re-run</Button>
            <Button size="sm">Re-run with this metric view</Button>
          </>
        }
      />
      <MvProposalCard
        proposal={proposalMargin}
        liftLabel={<LiftNotMeasuredLabel />}
        actions={
          <>
            <Button size="sm" variant="secondary">Approve for re-run</Button>
            <Button size="sm">Re-run with this metric view</Button>
          </>
        }
      />
    </div>
  )
}

// The surrounding metric card already says which run this is (baseline vs
// post-attach), so the link carries only the eval_run_id — prepending a "eval"
// label duplicated the id's own prefix ("eval eval_a1").
function EvalLink({ id }: { id: string }) {
  return (
    <a className="inline-flex items-center gap-0.5 text-accent hover:underline" href="#">
      <span className="font-mono text-xs">{id}</span>
      <ArrowUpRight className="h-3 w-3" />
    </a>
  )
}

// ── Frame 5 — create_and_attach output, regression/DETACHED variant ─────────
export function CreateAndAttachOutputFrame() {
  const obj = createdDetached
  return (
    <div className="space-y-4 rounded-xl border border-default bg-surface p-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <a className="inline-flex items-center gap-1 font-mono text-sm font-medium text-accent hover:underline" href="#">
            {obj.full_name}
            <ArrowUpRight className="h-3.5 w-3.5" />
          </a>
          <p className="mt-0.5 text-xs text-muted">Created under OBO by {obj.created_by} · opens in Catalog Explorer</p>
          {/* Provenance line (MV-D24): this frame is an app-created view. Frame 8
              introduces USER_CREATED views the app must NOT drop, so name the
              discriminator here rather than implying every created view is the
              app's to drop. */}
          <p className="mt-0.5 text-xs text-muted">
            Provenance <span className="font-mono">OBO_CREATED</span> — the app created this view, so the app can drop it.
            Views you register yourself are <span className="font-mono">USER_CREATED</span> and the app never drops them.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          <Badge variant="danger">DETACHED</Badge>
          {/* No join-strategy badge: order_revenue is a single direct join, and
              the "nested" rung is unreachable today (MV-D14/D15). */}
          <Badge variant="secondary">OBO_CREATED</Badge>
        </div>
      </div>

      {/* Downgrade banner — shown when the run degraded and reverted. */}
      <div className="flex items-start gap-1.5 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-700 dark:text-amber-300">
        <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
        <span>
          Attaching this metric view lowered benchmark accuracy, so the run reverted to the pre-attach snapshot
          (whole-snapshot revert). The view still exists in Unity Catalog and can be dropped below.
        </span>
      </div>

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <div className="rounded-lg border border-default bg-elevated px-3 py-2">
          <p className="text-xs text-muted">Baseline accuracy</p>
          <p className="text-lg font-semibold text-primary">78%</p>
          <EvalLink id={obj.baseline_eval_run_id} />
        </div>
        <div className="rounded-lg border border-default bg-elevated px-3 py-2">
          <p className="text-xs text-muted">Post-attach accuracy</p>
          <p className="text-lg font-semibold text-danger">71%</p>
          <EvalLink id={obj.post_attach_eval_run_id} />
        </div>
        <div className="rounded-lg border border-default bg-elevated px-3 py-2">
          <p className="text-xs text-muted">Needs review</p>
          <p className="text-lg font-semibold text-primary">3</p>
          <p className="text-xs text-muted">counted separately</p>
        </div>
        <div className="rounded-lg border border-default bg-elevated px-3 py-2">
          <p className="text-xs text-muted">Tables freed</p>
          <p className="text-lg font-semibold text-primary">2</p>
          <p className="text-xs text-muted">raw tables the MV covers</p>
        </div>
      </div>

      {/* GRANT access panel — copy-ready, never auto-applied. */}
      <div className="rounded-lg border border-default bg-sunken px-3 py-2.5">
        <div className="flex items-center justify-between">
          <span className="text-xs font-medium text-secondary">Grant access</span>
          <span className="inline-flex items-center gap-1 text-xs text-muted"><Copy className="h-3.5 w-3.5" /> Copy</span>
        </div>
        <pre className="mt-2 overflow-x-auto font-mono text-xs text-primary">{ddlRevenue.grant_sql}</pre>
        <p className="mt-1.5 text-xs text-muted">
          Without this grant, other users&rsquo; Genie answers silently degrade — they cannot read the metric view.
        </p>
      </div>

      <div className="flex flex-wrap items-center gap-2 pt-1">
        <Button size="sm" variant="danger">
          <Trash2 className="mr-1.5 h-3.5 w-3.5" />
          Drop view
        </Button>
        <span className="text-xs text-muted">
          Drop asks for confirmation: &ldquo;Other consumers may depend on it.&rdquo; Drop is allowed only while DETACHED.
        </span>
      </div>
    </div>
  )
}
