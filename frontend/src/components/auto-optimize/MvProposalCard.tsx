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
import { Check, ChevronDown, ChevronRight, ChevronUp, FlaskConical, GitBranch, Link2, Star, TrendingUp } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { SqlCodeBlock } from "@/components/SqlCodeBlock"
import {
  confidenceDisplay,
  evidenceGrowth,
  evidenceSummary,
  factsChecks,
  joinStrategyLabel,
  proposalGainSentence,
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

// Evidence for humans (finding 3 + Prompt 15.9 item d): counts for the summary
// line, then the resolved provenance labels — the example-question text, snippet
// name, or generated-SQL match a raw id stood for — so the card never shows a
// bare `sql_snippet:measures:01f13…` string. The raw ids stay reachable behind a
// "show raw ids" DEBUG affordance (the only place a raw id may render). Labels
// are resolved serve-time by the backend (`proposal.provenance_labels`); absent
// them the block still shows counts. Renders nothing with no categorizable
// evidence.
// The counts line already conveys the totals; the labels are the "say something
// specific" layer, so a handful is enough — cap the wall the deployed review flagged.
const EVIDENCE_LABEL_CAP = 6

function EvidenceBlock({ proposal }: { proposal: MvProposal }) {
  const [showRaw, setShowRaw] = useState(false)
  const { chips, rawIds } = evidenceSummary(proposal)
  const labels = proposal.provenance_labels ?? []
  // Source tables moved to the Source/Joins/Measures attributes block (deployed
  // review #3); the evidence block stays about WHY the proposal exists.
  if (chips.length === 0 && labels.length === 0) return null

  return (
    <div className="rounded-lg border border-default bg-elevated px-3 py-2.5">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5">
        <span className="flex items-center gap-1.5 text-xs font-semibold text-secondary">
          <FlaskConical className="h-3.5 w-3.5 text-accent" />
          Evidence
        </span>
        {/* Counts as quiet pills so the summary reads at a glance instead of a
            run-on sentence (deployed review #1). */}
        {chips.map((c) => (
          <span
            key={c.label}
            className="rounded-full border border-default bg-surface px-2 py-0.5 text-[11px] text-muted"
          >
            {c.count} {c.label}
          </span>
        ))}
      </div>
      {labels.length > 0 && (
        <ul className="mt-2.5 space-y-2 text-xs">
          {labels.slice(0, EVIDENCE_LABEL_CAP).map((l) => (
            <li key={l.id} className="flex gap-2" title={l.detail || l.label}>
              <span className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-accent/60" aria-hidden />
              <div className="min-w-0 flex-1 space-y-1">
                <p className="text-secondary">{l.label}</p>
                {/* The SQL an id stood for reads as CODE, set off from the human
                    label — a distinct mono chip instead of a run-together line
                    (deployed review: "make it clear and easy to read"). */}
                {l.detail && (
                  <code className="block truncate rounded border border-default bg-surface px-1.5 py-0.5 font-mono text-[11px] text-muted">
                    {l.detail}
                  </code>
                )}
              </div>
            </li>
          ))}
          {labels.length > EVIDENCE_LABEL_CAP && (
            <li className="pl-3.5 text-[11px] text-muted">
              +{labels.length - EVIDENCE_LABEL_CAP} more
            </li>
          )}
        </ul>
      )}
      {rawIds.length > 0 && (
        <div className="mt-1.5">
          <button
            onClick={() => setShowRaw((v) => !v)}
            className="flex items-center gap-1 text-[11px] text-muted hover:text-accent transition-colors"
          >
            {showRaw ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
            show raw ids
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

function AttrColumn({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <p className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-muted">{title}</p>
      {children}
    </div>
  )
}

// MV-D30 justification, laid out as the metric view's attributes (deployed
// review #3): instead of the governed measures spanning the full width, the card
// reviews Source / Joins / Measures side by side. Sections with nothing to show
// are omitted, so a single-source view with no joins degrades cleanly; a row that
// carries no attributes at all renders no block.
function MetricViewAttributes({
  proposal,
  ddl,
}: {
  proposal: MvProposal
  ddl?: MvDdlArtifact | null
}) {
  const measures = (proposal.measures ?? []).filter(
    (m) => (m.display_name && m.display_name.trim()) || (m.expr && m.expr.trim()),
  )
  // Source tables from the rendered DDL first (the authority — parsed from
  // `source:` of the base + each join), falling back to the candidate's evidence.
  // The DDL path is what makes the Source column populate for existing rows the
  // advisor never stamped (deployed review #2/#3).
  const sourceTables = (
    Array.isArray(ddl?.source_tables) && ddl.source_tables.length > 0
      ? ddl.source_tables
      : Array.isArray(proposal.evidence?.source_tables)
        ? (proposal.evidence!.source_tables as unknown[]).map(String)
        : []
  ) as string[]
  // Only assert joins when the DDL is loaded (join_strategy known). On the
  // run-output surface only the selected proposal carries its ddl, so a
  // "Single-source view" fallback would falsely label multi-table views. When
  // the DDL IS loaded and names one source, say so plainly rather than omitting.
  const joinLabel =
    joinStrategyLabel(ddl?.join_strategy) ??
    (ddl && sourceTables.length === 1 ? "Single-source view" : null)
  const hasLeft = sourceTables.length > 0 || !!joinLabel
  if (measures.length === 0 && !hasLeft) return null
  return (
    <div
      className={`grid gap-x-5 gap-y-3 rounded-lg border border-default bg-elevated px-3 py-2.5 ${
        hasLeft && measures.length > 0 ? "sm:grid-cols-2" : "grid-cols-1"
      }`}
    >
      {hasLeft && (
        <div className="space-y-3">
          {sourceTables.length > 0 && (
            <AttrColumn title="Source">
              <ul className="space-y-1">
                {sourceTables.map((t) => (
                  <li key={t}>
                    <code
                      className="block truncate rounded border border-default bg-surface px-1.5 py-0.5 font-mono text-[11px] text-secondary"
                      title={t}
                    >
                      {t}
                    </code>
                  </li>
                ))}
              </ul>
            </AttrColumn>
          )}
          {joinLabel && (
            <AttrColumn title="Joins">
              <span className="inline-block rounded-full border border-default bg-surface px-2 py-0.5 text-[11px] text-secondary">
                {joinLabel}
              </span>
            </AttrColumn>
          )}
        </div>
      )}
      {measures.length > 0 && (
        <AttrColumn title={`Measures (${measures.length})`}>
          {/* Each measure: the human name on its own line, the SQL as a distinct
              mono chip below (deployed review — clearer than name+expr run together). */}
          <ul className="space-y-2">
            {measures.map((m, i) => (
              <li key={m.dedup_fingerprint ?? i} className="space-y-1">
                {m.display_name && (
                  <span className="block text-xs text-secondary">{m.display_name}</span>
                )}
                {m.expr && (
                  <code className="block break-all rounded border border-default bg-surface px-1.5 py-0.5 font-mono text-[11px] text-muted">
                    {m.expr}
                  </code>
                )}
              </li>
            ))}
          </ul>
        </AttrColumn>
      )}
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
  defaultExpanded,
}: MvProposalCardProps) {
  // Fix #2 — uniform collapse across BOTH surfaces: the detail is collapsed by
  // default beyond the first Recommended card. An explicit defaultExpanded still
  // wins; absent it, only the recommended card opens.
  const [expanded, setExpanded] = useState(defaultExpanded ?? Boolean(recommended))
  const joinLabel = joinStrategyLabel(ddl?.join_strategy)
  // The always-visible skeleton line: how many measures this view governs.
  const measureCount = (proposal.measures ?? []).filter(
    (m) => (m.display_name && m.display_name.trim()) || (m.expr && m.expr.trim()),
  ).length
  // MV-D35 (Prompt 15.8) — the card LEADS with the proven quality gates, not a
  // percent. `confidence` now carries only the evidence-basis caption (its
  // percent is unused here — the score orders the list and picks Recommended,
  // it is never rendered as a number or the word "confidence").
  const facts = factsChecks(proposal)
  const confidence = confidenceDisplay(proposal)
  const growth = evidenceGrowth(proposal)

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

      {/* Uniform skeleton (finding 2): identifier, governs-N, join strategy. The
          score is gone from the card face (MV-D35) — no percent, no "confidence"
          badge, no bare tier; the facts row below carries what is proven. */}
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          {/* "Proposed" prefix (deployed review): a not-yet-attached proposal is a
              view that DOES NOT EXIST yet — tag it so the 3-part name is never read
              as an existing object. Attached views drop this tag (they're real). */}
          <div className="flex min-w-0 items-center gap-2">
            {!proposal.attached && (
              <span className="shrink-0 rounded-md bg-amber-500/10 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-amber-600 ring-1 ring-inset ring-amber-500/20 dark:text-amber-400">
                Proposed
              </span>
            )}
            <p
              className="truncate font-mono text-sm font-medium text-primary"
              title={proposal.proposed_object ?? undefined}
            >
              {proposal.proposed_object}
            </p>
          </div>
          <p className="mt-0.5 text-xs text-muted">
            {measureCount > 0
              ? `Governs ${measureCount} ${measureCount === 1 ? "measure" : "measures"}`
              : "Proposed metric view"}
          </p>
        </div>
        {/* Always-visible expand/collapse for the whole suggestion. The old
            control was a faint text link buried between the evidence line and
            the Create button — users reported they "don't see the option to
            expand or collapse the suggestion" (deployed review). A labeled,
            bordered chevron in the header reads as the accordion control it is. */}
        <div className="flex shrink-0 flex-wrap items-center gap-1.5">
          {/* MV-D34: a proposal already shelved on the Agent config is badged
              "Attached" so the list stops reading as "still N to create" once the
              user has created-and-attached it. The footer flow renders its own
              attached terminal; this is the scannable header signal. */}
          {proposal.attached && (
            <Badge variant="high">
              <Link2 className="mr-1 h-3 w-3" />
              Attached
            </Badge>
          )}
          {joinLabel && (
            <Badge variant="secondary">
              <GitBranch className="mr-1 h-3 w-3" />
              {joinLabel}
            </Badge>
          )}
          <button
            type="button"
            onClick={() => setExpanded((v) => !v)}
            aria-expanded={expanded}
            aria-label={expanded ? "Collapse suggestion detail" : "Expand suggestion detail"}
            className="inline-flex items-center gap-1 rounded-md border border-default px-2 py-1 text-xs font-medium text-secondary transition-colors hover:border-accent hover:text-accent"
          >
            {expanded ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
            {expanded ? "Hide detail" : "Show detail"}
          </button>
        </div>
      </div>

      {/* MV-D35 — FACTS LEAD. The proven, gated checks, each rendered ONLY when
          its gate ran for this row (proposal.checks). Quality is binary and
          already gated; this states it plainly where the percent used to sow
          doubt. Renders nothing when no check is proven (never decorative). */}
      {facts.length > 0 && (
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
          {facts.map((f) => (
            <span key={f.key} className="inline-flex items-center gap-1 text-xs font-medium text-success">
              <Check className="h-3.5 w-3.5 shrink-0" />
              {f.label}
            </span>
          ))}
        </div>
      )}

      {/* Gain line — part of the always-visible skeleton (MV-D30 justification). */}
      <p className="text-xs text-muted">{proposalGainSentence(proposal)}</p>

      {/* MV-D35 fix #4: the evidence BASIS as a human sentence — reflects signal
          CONTRIBUTION (value above a floor), not mere execution. No percent, no
          "confidence": evidence-poor is presented as evidence-poor. */}
      {confidence.caption && (
        <p className="text-xs text-muted">
          <span className="text-secondary">Evidence:</span> {confidence.caption}
        </p>
      )}

      {/* Prompt 15.7 / MV-D32(3): cross-surface enrichment made visible. These
          signals cannot come from a cold scan, so their presence is proof the
          proposal grew beyond the initial scan. */}
      {growth.length > 0 && (
        <p className="flex items-center gap-1 text-xs text-accent">
          <TrendingUp className="h-3.5 w-3.5 shrink-0" />
          Evidence grew beyond the initial scan: +{growth.join(", +")}
        </p>
      )}

      {expanded && (
        <div className="space-y-3">
          <MetricViewAttributes proposal={proposal} ddl={ddl} />
          {proposal.evidence && <EvidenceBlock proposal={proposal} />}
          {ddl?.ddl && (
            <div className="space-y-2">
              {/* Copy must yield an executable statement (POV §7.5): the CREATE
                  VIEW wrapper, not the bare YAML body. */}
              <SqlCodeBlock code={ddl.ddl} />
              {ddl.grant_sql && <SqlCodeBlock code={ddl.grant_sql} />}
            </div>
          )}
          {/* Deployed review #4b: the top toggle scrolls out of reach once the
              detail (measures + evidence + two SQL blocks) is open, so collapse
              was effectively unreachable. Repeat it at the foot of the detail. */}
          <button
            onClick={() => setExpanded(false)}
            className="flex items-center gap-1 text-xs text-muted hover:text-accent transition-colors"
            aria-expanded={expanded}
          >
            <ChevronUp className="h-3.5 w-3.5" />
            Hide detail
          </button>
        </div>
      )}

      {liftLabel}

      {actions && <div className="flex flex-wrap items-center gap-2 pt-1">{actions}</div>}
    </div>
  )
}
