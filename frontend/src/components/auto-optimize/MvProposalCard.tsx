/**
 * Production proposal card for the run output/results screen (Prompt 13).
 *
 * Graduated from the review mockup (mockups/MvProposalCard.tsx) onto the real
 * MvProposal / MvDdlArtifact API shapes. The honesty label and footer actions
 * remain SLOTS (liftLabel / actions) so the same card renders the suggest-only
 * output today and, unchanged, a space-scoped source at Prompt 13.5 (MV-D23).
 *
 * Prompt 15.6 (second smoke run): every card renders the SAME skeleton —
 * identifier, tier, governs-N-measures, gain line — always visible, with the
 * detail (measures, evidence, DDL, GRANT) behind an explicit expand/collapse
 * chevron so density is the user's choice (finding 2/1). Evidence is shown as
 * human counts+labels, with the raw provenance ids behind a "details"
 * disclosure (finding 3). An optional Recommended badge marks the ranked pick.
 */
import { useState } from "react"
import { ChevronDown, ChevronRight, FlaskConical, GitBranch, Layers, Star } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { SqlCodeBlock } from "@/components/SqlCodeBlock"
import {
  evidenceSummary,
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
  /** Finding 4 — marks the deterministically-ranked top pick. */
  recommended?: boolean
  /** One-line justification shown under the Recommended badge. */
  recommendedReason?: string
  /**
   * Detail (measures/evidence/DDL) initial state. Defaults to OPEN so the
   * run-output and space-scoped surfaces keep showing the DDL they always did;
   * the IQ-scan list collapses secondary cards by passing `false`. Either way
   * the chevron gives the user explicit control (Prompt 15.6 finding 2).
   */
  defaultExpanded?: boolean
}

// Evidence for humans (finding 3): counts and labels for the default view, with
// the raw provenance ids tucked behind a "details" disclosure for the debugging
// user. Renders nothing when there is no categorizable evidence.
function EvidenceBlock({ proposal }: { proposal: MvProposal }) {
  const [showRaw, setShowRaw] = useState(false)
  const { chips, rawIds } = evidenceSummary(proposal)
  const sourceTables = Array.isArray(proposal.evidence?.source_tables)
    ? (proposal.evidence!.source_tables as unknown[]).map(String)
    : []
  if (chips.length === 0 && sourceTables.length === 0) return null

  return (
    <div className="rounded-lg border border-default bg-elevated px-3 py-2.5">
      <div className="flex items-center gap-1.5 text-xs font-semibold text-secondary">
        <FlaskConical className="h-3.5 w-3.5 text-accent" />
        Evidence
      </div>
      {chips.length > 0 && (
        <p className="mt-1.5 text-xs text-muted">
          {chips.map((c) => `${c.count} ${c.label}`).join(" \u00b7 ")}
        </p>
      )}
      {sourceTables.length > 0 && (
        <p className="mt-1 text-xs text-muted">
          <span className="text-secondary">Source tables:</span>{" "}
          <span className="font-mono">{sourceTables.join(", ")}</span>
        </p>
      )}
      {rawIds.length > 0 && (
        <div className="mt-1.5">
          <button
            onClick={() => setShowRaw((v) => !v)}
            className="flex items-center gap-1 text-[11px] text-muted hover:text-accent transition-colors"
          >
            {showRaw ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
            details
          </button>
          {showRaw && (
            <ul className="mt-1 space-y-0.5 font-mono text-[11px] text-muted">
              {rawIds.map((id) => (
                <li key={id}>{id}</li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  )
}

// MV-D30 justification: the measures this one view would govern. A legacy
// single-measure row reads back as a one-element list, so this renders
// uniformly; a row that carries no members degrades to no block.
function GovernedMeasures({ proposal }: { proposal: MvProposal }) {
  const measures = (proposal.measures ?? []).filter(
    (m) => (m.display_name && m.display_name.trim()) || (m.expr && m.expr.trim()),
  )
  if (measures.length === 0) return null
  return (
    <div className="rounded-lg border border-default bg-elevated px-3 py-2.5">
      <ul className="space-y-1 text-xs text-muted">
        {measures.map((m, i) => (
          <li key={m.dedup_fingerprint ?? i} className="flex flex-col">
            {m.display_name && <span className="text-secondary">{m.display_name}</span>}
            {m.expr && <span className="font-mono text-[11px] text-muted">{m.expr}</span>}
          </li>
        ))}
      </ul>
    </div>
  )
}

export function MvProposalCard({
  proposal,
  ddl,
  liftLabel,
  actions,
  recommended,
  recommendedReason,
  defaultExpanded = true,
}: MvProposalCardProps) {
  const [expanded, setExpanded] = useState(defaultExpanded)
  const joinLabel = joinStrategyLabel(ddl?.join_strategy)
  // The always-visible skeleton line: how many measures this view governs.
  const measureCount = (proposal.measures ?? []).filter(
    (m) => (m.display_name && m.display_name.trim()) || (m.expr && m.expr.trim()),
  ).length

  return (
    <div
      className={`space-y-3 rounded-xl border bg-surface p-4 ${
        recommended ? "border-accent/50 ring-1 ring-accent/20" : "border-default"
      }`}
    >
      {recommended && (
        <div className="flex items-start gap-1.5 text-xs">
          <Badge variant="high">
            <Star className="mr-1 h-3 w-3" />
            Recommended
          </Badge>
          {recommendedReason && <span className="text-muted">{recommendedReason}</span>}
        </div>
      )}

      {/* Uniform skeleton (finding 2): identifier, tier/confidence, governs-N. */}
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="truncate font-mono text-sm font-medium text-primary">
            {proposal.proposed_object}
          </p>
          <p className="mt-0.5 text-xs text-muted">
            {measureCount > 0
              ? `Governs ${measureCount} ${measureCount === 1 ? "measure" : "measures"}`
              : "Proposed metric view"}
          </p>
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

      {/* Gain line — part of the always-visible skeleton (MV-D30 justification). */}
      <p className="text-xs text-muted">{proposalGainSentence(proposal)}</p>

      {/* Explicit expand/collapse of the detail — no implicit inconsistency. */}
      <button
        onClick={() => setExpanded((v) => !v)}
        className="flex items-center gap-1 text-xs text-muted hover:text-accent transition-colors"
        aria-expanded={expanded}
      >
        {expanded ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
        <Layers className="h-3.5 w-3.5" />
        {expanded ? "Hide detail" : "Show detail"}
      </button>

      {expanded && (
        <div className="space-y-3">
          <GovernedMeasures proposal={proposal} />
          {proposal.evidence && <EvidenceBlock proposal={proposal} />}
          {ddl?.ddl && (
            <div className="space-y-2">
              {/* Copy must yield an executable statement (POV §7.5): the CREATE
                  VIEW wrapper, not the bare YAML body. */}
              <SqlCodeBlock code={ddl.ddl} />
              {ddl.grant_sql && <SqlCodeBlock code={ddl.grant_sql} />}
            </div>
          )}
        </div>
      )}

      {liftLabel}

      {actions && <div className="flex flex-wrap items-center gap-2 pt-1">{actions}</div>}
    </div>
  )
}
