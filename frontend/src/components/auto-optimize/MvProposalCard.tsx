/**
 * Production proposal card for the run output/results screen (Prompt 13).
 *
 * Graduated from the review mockup (mockups/MvProposalCard.tsx) onto the real
 * MvProposal / MvDdlArtifact API shapes. The honesty label and footer actions
 * remain SLOTS (liftLabel / actions) so the same card renders the suggest-only
 * output today and, unchanged, a space-scoped source at Prompt 13.5 (MV-D23).
 */
import { FlaskConical, GitBranch, Layers } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { SqlCodeBlock } from "@/components/SqlCodeBlock"
import {
  joinStrategyLabel,
  proposalGainSentence,
  tierVariant,
} from "@/components/auto-optimize/mvFormat"
import type { MvDdlArtifact, MvProposal } from "@/types"

export interface MvProposalCardProps {
  proposal: MvProposal
  /** When present, renders the DDL + GRANT panels (POV §7.5). */
  ddl?: MvDdlArtifact | null
  /** SLOT — the honesty label (suggest-only passes the verbatim §7.5 string). */
  liftLabel?: React.ReactNode
  /** SLOT — footer actions (Approve for re-run / Re-run with this metric view). */
  actions?: React.ReactNode
}

// Evidence arrives as a decoded JSON object; read the known keys defensively so
// a shape drift degrades to fewer rows rather than a crash.
function EvidenceBlock({ evidence }: { evidence: Record<string, unknown> }) {
  const recurrence = evidence.recurrence_count
  const questionIds = Array.isArray(evidence.benchmark_question_ids)
    ? (evidence.benchmark_question_ids as unknown[]).map(String)
    : []
  const sourceTables = Array.isArray(evidence.source_tables)
    ? (evidence.source_tables as unknown[]).map(String)
    : []
  if (recurrence == null && !questionIds.length && !sourceTables.length) return null

  return (
    <div className="rounded-lg border border-default bg-elevated px-3 py-2.5">
      <div className="flex items-center gap-1.5 text-xs font-semibold text-secondary">
        <FlaskConical className="h-3.5 w-3.5 text-accent" />
        Evidence
      </div>
      <dl className="mt-1.5 space-y-1 text-xs text-muted">
        {recurrence != null && (
          <div className="flex gap-2">
            <dt className="text-secondary">Recurrence</dt>
            <dd>{String(recurrence)} occurrences in generated SQL</dd>
          </div>
        )}
        {questionIds.length > 0 && (
          <div className="flex gap-2">
            <dt className="text-secondary">From questions</dt>
            <dd className="font-mono">{questionIds.join(", ")}</dd>
          </div>
        )}
        {sourceTables.length > 0 && (
          <div className="flex gap-2">
            <dt className="text-secondary">Source tables</dt>
            <dd className="font-mono">{sourceTables.join(", ")}</dd>
          </div>
        )}
      </dl>
    </div>
  )
}

// MV-D30 justification: the measures this one view would govern, plus the gain
// line — assembled from the carried bundle members (no LLM). A legacy
// single-measure row reads back as a one-element list, so this renders
// uniformly; a row that somehow carries no members degrades to no block rather
// than an empty shell.
function GovernedMeasures({ proposal }: { proposal: MvProposal }) {
  const measures = (proposal.measures ?? []).filter(
    (m) => (m.display_name && m.display_name.trim()) || (m.expr && m.expr.trim()),
  )
  if (measures.length === 0) return null
  const count = measures.length
  return (
    <div className="rounded-lg border border-default bg-elevated px-3 py-2.5">
      <div className="flex items-center gap-1.5 text-xs font-semibold text-secondary">
        <Layers className="h-3.5 w-3.5 text-accent" />
        Governs {count} {count === 1 ? "measure" : "measures"}
      </div>
      <ul className="mt-1.5 space-y-1 text-xs text-muted">
        {measures.map((m, i) => (
          <li key={m.dedup_fingerprint ?? i} className="flex flex-col">
            {m.display_name && <span className="text-secondary">{m.display_name}</span>}
            {m.expr && <span className="font-mono text-[11px] text-muted">{m.expr}</span>}
          </li>
        ))}
      </ul>
      <p className="mt-2 text-xs text-muted">{proposalGainSentence(proposal)}</p>
    </div>
  )
}

export function MvProposalCard({ proposal, ddl, liftLabel, actions }: MvProposalCardProps) {
  const joinLabel = joinStrategyLabel(ddl?.join_strategy)
  return (
    <div className="space-y-4 rounded-xl border border-default bg-surface p-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <p className="font-mono text-sm font-medium text-primary">{proposal.proposed_object}</p>
          <p className="mt-0.5 text-xs text-muted">Proposed metric view</p>
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          {proposal.confidence_score !== null && (
            <Badge variant={tierVariant(proposal.tier)}>
              {Math.round(proposal.confidence_score)}% confidence
            </Badge>
          )}
          {proposal.tier && <Badge variant="secondary">{proposal.tier}</Badge>}
          {joinLabel && (
            <Badge variant="secondary">
              <GitBranch className="mr-1 h-3 w-3" />
              {joinLabel}
            </Badge>
          )}
        </div>
      </div>

      <GovernedMeasures proposal={proposal} />

      {proposal.evidence && <EvidenceBlock evidence={proposal.evidence} />}

      {ddl?.ddl && (
        <div className="space-y-2">
          {/* Copy must yield an executable statement (POV §7.5): the CREATE VIEW
              wrapper, not the bare YAML body. */}
          <SqlCodeBlock code={ddl.ddl} />
          {ddl.grant_sql && <SqlCodeBlock code={ddl.grant_sql} />}
        </div>
      )}

      {liftLabel}

      {actions && <div className="flex flex-wrap items-center gap-2 pt-1">{actions}</div>}
    </div>
  )
}
