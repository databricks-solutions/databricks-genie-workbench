/**
 * Shared formatting helpers for the metric view output panels (Prompt 13).
 *
 * Kept in one place so the suggest-only card and the create-and-attach panel
 * agree on the join-strategy vocabulary and the verbatim honesty copy.
 */

// POV §7.5, verbatim. The suggest-only panel states plainly that nothing was
// created or attached this run, so no lift could be measured.
export const LIFT_NOT_MEASURED =
  "Lift not measured — this metric view was not created or attached during this run."

// The reachable ladder rungs only. `nested` is unreachable today (MV-D14/D15),
// so it is deliberately absent — an unexpected value renders no badge rather
// than inventing a label. Accepts either the backend's `subquery_source` or the
// hyphenated display spelling.
const JOIN_STRATEGY_LABEL: Record<string, string> = {
  denormalized: "Denormalized",
  subquery_source: "Subquery source",
  "subquery-source": "Subquery source",
}

export function joinStrategyLabel(raw: string | null | undefined): string | null {
  if (!raw) return null
  return JOIN_STRATEGY_LABEL[raw] ?? null
}

// Tier drives the confidence badge color; unknown tiers fall back to neutral.
const TIER_VARIANT: Record<string, "high" | "medium" | "low"> = {
  HIGH: "high",
  MEDIUM: "medium",
  LOW: "low",
}

export function tierVariant(
  tier: string | null | undefined,
): "high" | "medium" | "low" | "secondary" {
  if (!tier) return "secondary"
  return TIER_VARIANT[tier.toUpperCase()] ?? "secondary"
}

// Prompt 15.3 / MV-D30 surfacing rules, in one place so the panel and its test
// agree. A proposal with no `proposed_object` is a VALIDATION FAILURE (the
// backend drops it before persistence; this is defense-in-depth for legacy rows
// and belt-and-braces so a blank card can never render — the smoke-run defect).
// Of the renderable proposals, MEDIUM+ surface by default and LOW hides behind
// an explicit "show N low-confidence" disclosure so a wall of weak cards never
// greets the user. An unknown/absent tier is treated as MEDIUM+ (surfaced): the
// floor suppresses only what is explicitly LOW, never what is merely unlabeled.
import type { MvProposal } from "@/types"

function hasProposedObject(p: MvProposal): boolean {
  return typeof p.proposed_object === "string" && p.proposed_object.trim().length > 0
}

export function isLowConfidence(tier: string | null | undefined): boolean {
  return (tier ?? "").toUpperCase() === "LOW"
}

export interface SplitProposals {
  /** MEDIUM+ (and unlabeled) renderable proposals — surfaced by default. */
  primary: MvProposal[]
  /** LOW-confidence renderable proposals — behind the disclosure. */
  low: MvProposal[]
}

export function splitProposalsByConfidence(proposals: MvProposal[]): SplitProposals {
  const renderable = proposals.filter(hasProposedObject)
  return {
    primary: renderable.filter((p) => !isLowConfidence(p.tier)),
    low: renderable.filter((p) => isLowConfidence(p.tier)),
  }
}

// Prompt 15.3 / MV-D30: every surfaced card states WHY it is proposed — the
// measures it would govern and the gain, assembled purely from the carried
// evidence (no LLM). The bundle's members ride in `proposal.measures`; the
// distinct contributing benchmark questions come from the members' question
// ids, falling back to the bundle-level evidence for a legacy one-element row.
function distinctQuestionCount(proposal: MvProposal): number {
  const ids = new Set<string>()
  for (const m of proposal.measures ?? []) {
    for (const q of m.benchmark_question_ids ?? []) if (q) ids.add(String(q))
  }
  if (ids.size === 0 && proposal.evidence) {
    const ev = proposal.evidence.benchmark_question_ids
    if (Array.isArray(ev)) for (const q of ev) if (q != null) ids.add(String(q))
  }
  return ids.size
}

export function proposalGainSentence(proposal: MvProposal): string {
  const n = (proposal.measures ?? []).length || 1
  const measureWord = n === 1 ? "measure" : "measures"
  const q = distinctQuestionCount(proposal)
  if (q > 0) {
    const queryWord = q === 1 ? "curated query" : "curated queries"
    return `These ${n} ${measureWord} recur across ${q} ${queryWord} and are ungoverned today.`
  }
  return `These ${n} ${measureWord} recur in this Agent\u2019s generated SQL and are ungoverned today.`
}

// A metric_views[] entry identifies its UC object by `identifier`.
export function metricViewIdentifiers(spaceData: unknown): string[] {
  if (!spaceData || typeof spaceData !== "object") return []
  const ds = (spaceData as Record<string, unknown>).data_sources
  if (!ds || typeof ds !== "object") return []
  const views = (ds as Record<string, unknown>).metric_views
  if (!Array.isArray(views)) return []
  return views
    .map((m) =>
      m && typeof m === "object"
        ? String((m as Record<string, unknown>).identifier ?? "")
        : "",
    )
    .filter(Boolean)
}

// Derive a workspace origin from the run's resource links, which point into the
// workspace. Used to build a Catalog Explorer deep link for a created view.
export function workspaceOriginFromLinks(
  links: { url: string }[] | undefined,
): string | null {
  for (const link of links ?? []) {
    try {
      return new URL(link.url).origin
    } catch {
      // Non-absolute URL; keep looking.
    }
  }
  return null
}

// Catalog Explorer deep link for a `catalog.schema.view` full name, or null when
// the origin or name is unavailable (the panel then shows the name without a link).
export function catalogExplorerUrl(
  origin: string | null,
  fullName: string | null | undefined,
): string | null {
  if (!origin || !fullName) return null
  const parts = fullName.split(".")
  if (parts.length !== 3) return null
  const [catalog, schema, view] = parts
  return `${origin}/explore/data/${catalog}/${schema}/${view}`
}
