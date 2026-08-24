/**
 * Suggest-only output panel (Prompt 13, mockup frame 4, POV §7.5).
 *
 * Renders the proposals the advisor recorded for a run that did NOT create or
 * attach anything: each as a proposal card with the verbatim "lift not measured"
 * label, the space-config diff (proposed side synthesized client-side), and the
 * two actions — [Approve for re-run] (records the decision) and [Re-run with this
 * metric view] (opens the run config pre-filled in create_and_attach mode).
 *
 * MV-D23: every card/diff takes its data as props and keys nothing on run_id, so
 * Prompt 13.5 can feed the same components from a space-scoped source unchanged.
 */
import { useState } from "react"
import { CheckCircle2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { MvProposalCard } from "@/components/auto-optimize/MvProposalCard"
import { MvSpaceConfigDiff } from "@/components/auto-optimize/MvSpaceConfigDiff"
import { LIFT_NOT_MEASURED } from "@/components/auto-optimize/mvFormat"
import { decideMvProposal } from "@/lib/api"
import type { MvDdlArtifact, MvProposal } from "@/types"

interface MvSuggestOnlyPanelProps {
  runId: string
  proposals: MvProposal[]
  /** The single rendered DDL artifact for this run, matched by suggestion_id. */
  ddl: MvDdlArtifact | null
  /** Current data_sources.metric_views[] identifiers, for the config diff. */
  currentIdentifiers: string[]
  /** Opens the run config pre-filled in create_and_attach mode (MV-D1 flow). */
  onRerun: (proposal: MvProposal) => void
}

function LiftNotMeasuredLabel() {
  return (
    <p className="rounded-lg border border-default bg-elevated px-3 py-2 text-xs text-muted">
      {LIFT_NOT_MEASURED}
    </p>
  )
}

export function MvSuggestOnlyPanel({
  runId,
  proposals,
  ddl,
  currentIdentifiers,
  onRerun,
}: MvSuggestOnlyPanelProps) {
  const [approved, setApproved] = useState<Record<string, boolean>>({})
  const [busy, setBusy] = useState<Record<string, boolean>>({})
  const [error, setError] = useState<string | null>(null)

  async function handleApprove(proposal: MvProposal) {
    setBusy((b) => ({ ...b, [proposal.suggestion_id]: true }))
    setError(null)
    try {
      const res = await decideMvProposal(proposal.suggestion_id, {
        space_id: proposal.target_space_id,
        run_id: runId,
        decision: "approved",
      })
      setApproved((a) => ({ ...a, [proposal.suggestion_id]: res.approved_for_rerun }))
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not record the decision.")
    } finally {
      setBusy((b) => ({ ...b, [proposal.suggestion_id]: false }))
    }
  }

  const createdCount = 0
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold uppercase tracking-wide text-secondary">
          Metric views proposed
        </h3>
        <span className="text-xs text-muted">
          {proposals.length} proposed · {createdCount === 0 ? "none created" : `${createdCount} created`}
        </span>
      </div>

      {error && <p className="text-xs text-danger">{error}</p>}

      {proposals.map((proposal) => {
        const isApproved =
          approved[proposal.suggestion_id] ?? proposal.approved_for_rerun
        const proposalDdl = ddl && ddl.suggestion_id === proposal.suggestion_id ? ddl : null
        return (
          <div key={proposal.suggestion_id} className="space-y-2">
            <MvProposalCard
              proposal={proposal}
              ddl={proposalDdl}
              liftLabel={<LiftNotMeasuredLabel />}
              actions={
                <>
                  {isApproved ? (
                    <span className="inline-flex items-center gap-1 text-xs font-medium text-success">
                      <CheckCircle2 className="h-3.5 w-3.5" />
                      Approved for re-run
                    </span>
                  ) : (
                    <Button
                      size="sm"
                      variant="secondary"
                      disabled={busy[proposal.suggestion_id]}
                      onClick={() => handleApprove(proposal)}
                    >
                      {busy[proposal.suggestion_id] ? "Approving…" : "Approve for re-run"}
                    </Button>
                  )}
                  <Button size="sm" onClick={() => onRerun(proposal)}>
                    Re-run with this metric view
                  </Button>
                </>
              }
            />
            {proposal.proposed_object && (
              <MvSpaceConfigDiff
                currentIdentifiers={currentIdentifiers}
                proposedObject={proposal.proposed_object}
              />
            )}
          </div>
        )
      })}
    </div>
  )
}
