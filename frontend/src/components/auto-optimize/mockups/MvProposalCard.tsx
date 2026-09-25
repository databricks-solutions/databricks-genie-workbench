/**
 * MV-advisor mockups — REVIEW SCAFFOLD, NOT PRODUCTION CODE (see mvMockData.ts).
 *
 * Shared proposal card reused by frame 4 (suggest-only output) and frame 7
 * (IQ-Scan advisory). Per the Prompt 10 correction, the "Lift not measured…"
 * label and the [Re-run with this metric view] action are SLOTS (liftLabel /
 * actions), never baked in: frame 7 has no run to have-not-measured and no run
 * to re-run, so it passes neither. Prompt 13 fills both slots.
 */
import { Highlight } from "prism-react-renderer"
import { Copy, FlaskConical, GitBranch } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import type { MvDdlFixture, MvProposalFixture } from "./mvMockData"

const TIER_VARIANT = { HIGH: "high", MEDIUM: "medium", LOW: "low" } as const

const JOIN_STRATEGY_LABEL: Record<NonNullable<MvProposalFixture["join_strategy"]>, string> = {
  denormalized: "Denormalized",
  "subquery-source": "Subquery source",
}

function CodePanel({ label, code, language }: { label: string; code: string; language: string }) {
  return (
    <div className="rounded-lg border border-default bg-sunken">
      <div className="flex items-center justify-between border-b border-default px-3 py-2">
        <span className="text-xs font-medium text-secondary">{label}</span>
        <span className="inline-flex items-center gap-1 text-xs text-muted">
          <Copy className="h-3.5 w-3.5" /> Copy
        </span>
      </div>
      <Highlight code={code.trimEnd()} language={language} theme={{ plain: {}, styles: [] }}>
        {({ className, style, tokens, getLineProps, getTokenProps }) => (
          <pre className={`${className} overflow-x-auto p-3 font-mono text-xs leading-relaxed text-primary`} style={style}>
            {tokens.map((line, i) => (
              <div key={i} {...getLineProps({ line })}>
                {line.map((token, key) => (
                  <span key={key} {...getTokenProps({ token })} />
                ))}
              </div>
            ))}
          </pre>
        )}
      </Highlight>
    </div>
  )
}

export interface MvProposalCardProps {
  proposal: MvProposalFixture
  /** When present, renders the DDL + GRANT panels (POV §7.5). */
  ddl?: MvDdlFixture
  /**
   * SLOT — the honesty label. Frame 4 passes the verbatim POV §7.5 string;
   * frame 7 passes nothing (nothing was run to measure).
   */
  liftLabel?: React.ReactNode
  /**
   * SLOT — footer actions. Frame 4 passes [Approve for re-run] +
   * [Re-run with this metric view]; frame 7 passes its own consent-opening CTA.
   */
  actions?: React.ReactNode
}

export function MvProposalCard({ proposal, ddl, liftLabel, actions }: MvProposalCardProps) {
  const tierVariant = proposal.tier ? TIER_VARIANT[proposal.tier] : "secondary"
  return (
    <div className="space-y-4 rounded-xl border border-default bg-surface p-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <p className="font-mono text-sm font-medium text-primary">{proposal.proposed_object}</p>
          <p className="mt-0.5 text-xs text-muted">Proposed metric view</p>
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          {proposal.confidence_score !== null && (
            <Badge variant={tierVariant}>{Math.round(proposal.confidence_score)}% confidence</Badge>
          )}
          {proposal.tier && <Badge variant="secondary">{proposal.tier}</Badge>}
          {proposal.join_strategy && (
            <Badge variant="secondary">
              <GitBranch className="mr-1 h-3 w-3" />
              {JOIN_STRATEGY_LABEL[proposal.join_strategy]}
            </Badge>
          )}
        </div>
      </div>

      {proposal.evidence && (
        <div className="rounded-lg border border-default bg-elevated px-3 py-2.5">
          <div className="flex items-center gap-1.5 text-xs font-semibold text-secondary">
            <FlaskConical className="h-3.5 w-3.5 text-accent" />
            Evidence
          </div>
          <dl className="mt-1.5 space-y-1 text-xs text-muted">
            <div className="flex gap-2">
              <dt className="text-secondary">Recurrence</dt>
              <dd>{proposal.evidence.recurrence_count} occurrences in generated SQL</dd>
            </div>
            <div className="flex gap-2">
              <dt className="text-secondary">From questions</dt>
              <dd className="font-mono">{proposal.evidence.benchmark_question_ids.join(", ")}</dd>
            </div>
            <div className="flex gap-2">
              <dt className="text-secondary">Source tables</dt>
              <dd className="font-mono">{proposal.evidence.source_tables.join(", ")}</dd>
            </div>
          </dl>
        </div>
      )}

      {ddl && (
        <div className="space-y-2">
          {/* The full CREATE VIEW … WITH METRICS LANGUAGE YAML AS $$ … $$ — what
              a user copies must be executable (POV §7.5), so show ddl.ddl, not
              the bare YAML body. */}
          <CodePanel label="CREATE VIEW … WITH METRICS" code={ddl.ddl} language="sql" />
          <CodePanel label="GRANT (run before others query this Agent)" code={ddl.grant_sql} language="sql" />
        </div>
      )}

      {liftLabel}

      {actions && <div className="flex flex-wrap items-center gap-2 pt-1">{actions}</div>}
    </div>
  )
}
