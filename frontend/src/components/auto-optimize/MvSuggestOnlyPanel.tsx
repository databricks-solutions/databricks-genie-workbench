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
import { MvProposalCard } from "@/components/auto-optimize/MvProposalCard"
import { MvProposalsSummary } from "@/components/auto-optimize/MvProposalsSummary"
import { MvAcceptFlow } from "@/components/auto-optimize/MvAcceptFlow"
import { MvSpaceConfigDiff } from "@/components/auto-optimize/MvSpaceConfigDiff"
import {
  LIFT_NOT_MEASURED,
  orthogonalityCallout,
  rankProposals,
  recommendedReason,
} from "@/components/auto-optimize/mvFormat"
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
  // Fix #1 (count truth): the header counts what actually happened. `created`
  // tracks suggestion ids the shared accept flow created inline this session, on
  // top of any already-created ledger provenance the row carries; `proposed`
  // is the rendered count, one-to-one with the cards below. No hardcoded zero.
  const [created, setCreated] = useState<Set<string>>(new Set())
  const createdCount = created.size
  const proposedCount = proposals.length
  // MV-D35: the shared display module ranks and picks ONE Recommended on BOTH
  // surfaces (the divergence this prompt ends), UNLESS every proposal governs a
  // disjoint measure set — then the orthogonality callout replaces the forced
  // ranking. The first card opens (fix #2); the rest collapse.
  const ranked = rankProposals(proposals)
  const callout = orthogonalityCallout(ranked)

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold uppercase tracking-wide text-secondary">
          Metric views proposed
        </h3>
        <span className="text-xs text-muted">
          {proposedCount} proposed · {createdCount === 0 ? "none created" : `${createdCount} created`}
        </span>
      </div>

      {/* Deployed review #5: lead with what the run is proposing — count, names,
          and what they govern — so the customer can decide before the detail. */}
      <MvProposalsSummary proposals={ranked} />

      {callout && <p className="text-xs text-secondary">{callout}</p>}

      {ranked.map((proposal, i) => {
        const proposalDdl = ddl && ddl.suggestion_id === proposal.suggestion_id ? ddl : null
        return (
          <div key={proposal.suggestion_id} className="space-y-2">
            <MvProposalCard
              proposal={proposal}
              ddl={proposalDdl}
              recommended={!callout && i === 0}
              recommendedReason={!callout && i === 0 ? recommendedReason(proposal) : undefined}
              defaultExpanded={i === 0}
              liftLabel={<LiftNotMeasuredLabel />}
              actions={
                <MvAcceptFlow
                  proposal={proposal}
                  runId={runId}
                  onStartRun={onRerun}
                  grantSql={proposalDdl?.grant_sql}
                  onCreated={(p) =>
                    setCreated((c) => new Set(c).add(p.suggestion_id))
                  }
                />
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
