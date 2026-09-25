/**
 * At-a-glance summary of a proposal set (deployed review #5).
 *
 * Before the per-card detail, the customer wants the decision framed: how many
 * metric views are proposed, what they are named, and what they would govern.
 * Pure, deterministic assembly from the same MvProposal[] the cards render — no
 * LLM, no new fetch. Renders nothing when no proposal carries a name (a set the
 * cards themselves would drop), so it never asserts a summary that isn't backed
 * by a card below it.
 */
import { Sparkles } from "lucide-react"
import type { MvProposal } from "@/types"

function shortName(obj: string | null | undefined): string | null {
  if (!obj) return null
  const parts = obj.split(".")
  return parts[parts.length - 1] || obj
}

function measureCountOf(p: MvProposal): number {
  return (p.measures ?? []).filter(
    (m) => (m.display_name && m.display_name.trim()) || (m.expr && m.expr.trim()),
  ).length
}

export function MvProposalsSummary({ proposals }: { proposals: MvProposal[] }) {
  const named = proposals
    .map((p) => ({ name: shortName(p.proposed_object), measures: measureCountOf(p) }))
    .filter((x): x is { name: string; measures: number } => Boolean(x.name))
  if (named.length === 0) return null

  const viewCount = named.length
  const measureTotal = named.reduce((sum, x) => sum + x.measures, 0)

  return (
    <div className="rounded-xl border border-accent/30 bg-accent/5 px-4 py-3">
      <p className="flex items-center gap-1.5 text-sm font-medium text-primary">
        <Sparkles className="h-4 w-4 shrink-0 text-accent" />
        Suggesting {viewCount} metric {viewCount === 1 ? "view" : "views"}
        {measureTotal > 0 &&
          ` to govern ${measureTotal} recurring ${measureTotal === 1 ? "measure" : "measures"}`}
      </p>
      <ul className="mt-2 space-y-1">
        {named.map((x) => (
          <li key={x.name} className="flex items-baseline justify-between gap-3 text-xs">
            <span className="truncate font-mono text-secondary" title={x.name}>
              {x.name}
            </span>
            {x.measures > 0 && (
              <span className="shrink-0 text-muted">
                {x.measures} {x.measures === 1 ? "measure" : "measures"}
              </span>
            )}
          </li>
        ))}
      </ul>
      <p className="mt-2 text-xs text-muted">
        These measures recur across this Agent&rsquo;s curated queries and aren&rsquo;t governed yet
        &mdash; creating these views makes the definitions consistent and reusable.
      </p>
    </div>
  )
}
