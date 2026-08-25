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

// ── MV-D32 as-implemented (Prompt 15.7b) — coverage-capped-strong surfacing ──
//
// MV-D15 caps the SERVED tier by evidence coverage: on a fresh table lineage (L)
// and usage/demand (D) are structurally absent, so even a strongly-recurring
// measure is held at LOW no matter how high the score-only tier was. Under the
// 15.7 tier-only split that proposal sat behind the disclosure — the exact
// "strong candidate buried" defect the cold-start caption only half-closed. Now
// that `uncapped_tier` and `tier_capped_by_coverage` are persisted (they were
// always computed in mv_scoring), the panel can tell "strong but
// evidence-limited" apart from "genuinely weak": a proposal whose UNCAPPED tier
// is MEDIUM+ AND which coverage capped joins the default list under a distinct
// "Strong (evidence-limited)" badge; a plain LOW (uncapped LOW, or not capped)
// stays behind the disclosure. Legacy rows carry NULL for both fields and fall
// back to the tier-only split unchanged.
const TIER_RANK: Record<string, number> = { HIGH: 3, MEDIUM: 2, LOW: 1 }

function tierRank(tier: string | null | undefined): number {
  return TIER_RANK[(tier ?? "").toUpperCase()] ?? 0
}

/** The badge a coverage-capped-strong proposal wears in the default list. */
export const MV_CAPPED_STRONG_LABEL = "Strong (evidence-limited)"

// A proposal whose score earned MEDIUM+ but which MV-D15 coverage held below
// that. `tier_capped_by_coverage === true` is the engine's own flag; the
// uncapped-tier MEDIUM+ guard makes this the "strong, just evidence-limited"
// case rather than any capping (a HIGH capped to MEDIUM is still primary anyway,
// but it too is honestly evidence-limited, so it earns the badge). Legacy rows
// (flag NULL) are never capped-strong.
export function isCappedStrong(proposal: MvProposal): boolean {
  return proposal.tier_capped_by_coverage === true && tierRank(proposal.uncapped_tier) >= TIER_RANK.MEDIUM
}

// The tier used for ORDERING and for the recommended-reason phrasing: a
// capped-strong proposal ranks by the strength its evidence earned (its uncapped
// tier), with the caption carrying the honesty that coverage limited it. Every
// other proposal ranks by its served tier.
function effectiveTier(proposal: MvProposal): string | null {
  return isCappedStrong(proposal) ? proposal.uncapped_tier : proposal.tier
}

export interface SplitProposals {
  /** MEDIUM+ (and unlabeled) + coverage-capped-strong renderable proposals — surfaced by default. */
  primary: MvProposal[]
  /** Plain LOW-confidence renderable proposals — behind the disclosure. */
  low: MvProposal[]
}

export function splitProposalsByConfidence(proposals: MvProposal[]): SplitProposals {
  const renderable = proposals.filter(hasProposedObject)
  // A capped-strong proposal joins the default list even when its SERVED tier is
  // LOW; a plain LOW (not capped-strong) stays behind the disclosure.
  const inPrimary = (p: MvProposal) => !isLowConfidence(p.tier) || isCappedStrong(p)
  return {
    primary: renderable.filter(inPrimary),
    low: renderable.filter((p) => !inPrimary(p)),
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

// Prompt 15.6 finding 1 — evidence for humans, never raw ids. The advisor's
// provenance ids carry a source-type prefix (mv_advisor._curated_provenance:
// `sql_snippet:` / `trusted_asset:` / `gso_patch:`) or, for a benchmark answer,
// no prefix at all. Rendering those raw strings (`sql_snippet:measures:01f13…`)
// to the user violated the justification clause of the Suggest-Surface Contract.
// Categorize by prefix into human counts+labels; keep the raw ids for a
// debugging "details" disclosure. Deterministic assembly, no LLM.
export interface EvidenceSummary {
  chips: { label: string; count: number }[]
  rawIds: string[]
}

const EVIDENCE_CATEGORIES: { prefix: string; singular: string; plural: string }[] = [
  { prefix: "sql_snippet:", singular: "curated snippet", plural: "curated snippets" },
  { prefix: "trusted_asset:", singular: "trusted asset", plural: "trusted assets" },
  { prefix: "gso_patch:", singular: "generated-SQL match", plural: "generated-SQL matches" },
]

export function evidenceSummary(proposal: MvProposal): EvidenceSummary {
  const rawIds = new Set<string>()
  for (const m of proposal.measures ?? []) {
    for (const q of m.benchmark_question_ids ?? []) if (q) rawIds.add(String(q))
  }
  if (rawIds.size === 0 && proposal.evidence) {
    const ev = proposal.evidence.benchmark_question_ids
    if (Array.isArray(ev)) for (const q of ev) if (q != null) rawIds.add(String(q))
  }
  const ids = [...rawIds]
  const counts = new Map<string, number>()
  let curatedQueries = 0
  for (const id of ids) {
    const cat = EVIDENCE_CATEGORIES.find((c) => id.startsWith(c.prefix))
    if (cat) counts.set(cat.prefix, (counts.get(cat.prefix) ?? 0) + 1)
    else curatedQueries += 1 // a bare id is a benchmark/curated-query answer
  }
  const chips: { label: string; count: number }[] = []
  for (const cat of EVIDENCE_CATEGORIES) {
    const n = counts.get(cat.prefix) ?? 0
    if (n > 0) chips.push({ label: n === 1 ? cat.singular : cat.plural, count: n })
  }
  if (curatedQueries > 0) {
    chips.push({
      label: curatedQueries === 1 ? "curated query" : "curated queries",
      count: curatedQueries,
    })
  }
  return { chips, rawIds: ids.sort() }
}

// Prompt 15.6 finding 4 — deterministic ranking so one proposal can be
// "Recommended" and the default list shows only the strongest few. Order:
// tier (HIGH > MEDIUM > LOW/other), then measures governed (coverage), then
// distinct curated-query count, then suggestion_id for a stable tiebreak. Pure
// assembly — never an LLM call. (MV-D32/15.7b) a capped-strong proposal is
// ranked by its UNCAPPED tier via `effectiveTier`, so a cold-start proposal that
// coverage held at LOW still orders among the strong candidates it belongs with.
export function rankProposals(proposals: MvProposal[]): MvProposal[] {
  return [...proposals].sort((a, b) => {
    const tier = tierRank(effectiveTier(b)) - tierRank(effectiveTier(a))
    if (tier !== 0) return tier
    const cover = (b.measures?.length ?? 0) - (a.measures?.length ?? 0)
    if (cover !== 0) return cover
    const q = distinctQuestionCount(b) - distinctQuestionCount(a)
    if (q !== 0) return q
    return a.suggestion_id.localeCompare(b.suggestion_id)
  })
}

// The one-line "why this is the pick" for the Recommended badge, assembled from
// the same facts the ranking used — so the badge explains itself rather than
// asserting authority.
export function recommendedReason(proposal: MvProposal): string {
  const n = proposal.measures?.length ?? 0
  const q = distinctQuestionCount(proposal)
  // (MV-D32/15.7b) phrase the confidence by the EFFECTIVE tier, so a
  // capped-strong pick reads "medium confidence (evidence-limited)" rather than
  // the bare "low" its served tier would give — the honesty rides the parens,
  // and the §2 caption on the card carries the full evidence basis.
  const tier = (effectiveTier(proposal) ?? "").toUpperCase()
  const capped = isCappedStrong(proposal)
  const parts: string[] = []
  if (tier === "HIGH" || tier === "MEDIUM") {
    parts.push(`${tier.toLowerCase()} confidence${capped ? " (evidence-limited)" : ""}`)
  }
  if (n > 0) parts.push(`governs ${n} ${n === 1 ? "measure" : "measures"}`)
  if (q > 0) parts.push(`recurs across ${q} ${q === 1 ? "curated query" : "curated queries"}`)
  return parts.length > 0
    ? `Strongest candidate — ${parts.join(", ")}.`
    : "Strongest candidate for this Agent."
}

// The default list shows the top N; the rest hide behind "show all". Kept as a
// constant so the card list and its test agree.
export const MV_DEFAULT_VISIBLE = 5

// Prompt 15.6 finding 8 — a weighted progress fraction for the scan bar. The
// four stages advance a bar whose segment widths come from the LAST scan's
// per-stage durations when available (measured client-side, since the sub-stages
// are transient and unpersisted), and equal weights otherwise. Completed stages
// count fully; the active one counts half (it is in flight, not done), so the
// bar always moves on entry without ever claiming completion early.
export function stageProgressFraction(
  totalStages: number,
  currentIdx: number,
  weights?: number[],
): number {
  if (totalStages <= 0) return 0
  const w =
    weights && weights.length === totalStages && weights.every((x) => x > 0)
      ? weights
      : new Array(totalStages).fill(1)
  const total = w.reduce((s, x) => s + x, 0)
  const idx = Math.max(0, Math.min(currentIdx, totalStages - 1))
  const done = w.slice(0, idx).reduce((s, x) => s + x, 0)
  const active = w[idx] ?? 0
  return Math.min(1, (done + active / 2) / total)
}

// ── Prompt 15.7 / MV-D32(1) — coverage-aware confidence display ───────────
//
// The surfaced confidence is the LYDS blend renormalized over the signals that
// were actually measured (mv_scoring.blended_score divides by evidence_coverage
// — MV-D15). On a fresh table lineage (L) and usage/demand (D) are STRUCTURALLY
// absent, so the number reflects the strength of curated-SQL recurrence (Y) and
// the semantic match (S) alone. Shown bare, "34%" reads as "this candidate is
// doubtful", which is false — it is a statement about how much evidence exists,
// not how strong it is. The fix is DISPLAY-only: the number is unchanged (the
// blend arithmetic is byte-untouched), but a caption states the evidence basis
// so evidence-poor is presented as evidence-poor. The raw blend, weights and
// coverage stay in `score_components` for the debugging user.
//
// Signal → producer, from mv_scoring: L = lineage overlap, Y = curated/generated
// SQL recurrence, S = semantic match, D = usage/demand (query history). A signal
// is "available" when its status is COMPUTED or EMPTY (a measured zero counts);
// UNAVAILABLE means no producer ran (nobody looked).
const MV_SIGNAL_UNAVAILABLE = "UNAVAILABLE"

function signalStatuses(proposal: MvProposal): Record<string, string> {
  const sc = proposal.score_components
  const statuses = sc && typeof sc === "object" ? (sc as Record<string, unknown>).statuses : null
  if (statuses && typeof statuses === "object") {
    return statuses as Record<string, string>
  }
  return {}
}

// A signal counts as available unless it is explicitly UNAVAILABLE. An absent
// status is treated as available (COMPUTED) — the same default the engine uses,
// so a payload that names only the absent signals reads the same on both sides.
function signalAvailable(statuses: Record<string, string>, key: string): boolean {
  return (statuses[key] ?? "COMPUTED") !== MV_SIGNAL_UNAVAILABLE
}

export interface ConfidenceDisplay {
  /** The rounded blend percent (unchanged — display framing only). */
  percent: number | null
  /** Human caption naming the evidence basis, or null when unknowable. */
  caption: string | null
  /** True when usage history (D) and lineage (L) are both absent — cold start. */
  evidencePoor: boolean
}

export function confidenceDisplay(proposal: MvProposal): ConfidenceDisplay {
  const percent =
    proposal.confidence_score === null ? null : Math.round(proposal.confidence_score)
  const statuses = signalStatuses(proposal)
  const hasUsage = signalAvailable(statuses, "D")
  const hasLineage = signalAvailable(statuses, "L")
  const evidencePoor = !hasUsage && !hasLineage

  // No score_components → we cannot honestly caption the basis; say nothing
  // rather than assert an evidence profile we did not read.
  const caption =
    Object.keys(statuses).length === 0
      ? null
      : evidencePoor
        ? "Based on curated SQL only — no usage history yet."
        : hasUsage && hasLineage
          ? "Backed by usage history and lineage."
          : hasUsage
            ? "Backed by curated SQL and usage history."
            : "Backed by curated SQL and lineage."

  return { percent, caption, evidencePoor }
}

// ── Prompt 15.7 / MV-D32(3) — cross-surface enrichment made visible ───────
//
// The advisor upserts the SAME candidate by fingerprint across surfaces: an IQ
// scan (advice run) seeds it from curated SQL (Y) and the semantic match (S),
// and a later GSO run adds generated-SQL recurrence, lineage (L) and usage/
// demand (D) — signals a COLD scan structurally cannot produce. So a COMPUTED
// D or L, or a non-empty query-history statement set, is proof the proposal was
// enriched beyond the initial scan. Surfacing it is assembly from what already
// rides on the row — no new machinery, and no fabricated "run N" delta: the
// claim is falsifiable ("these signals cannot come from a scan"), not a stored
// snapshot comparison. Returns [] for a scan-only proposal, so the line only
// appears when there is genuine cross-surface growth.
export function evidenceGrowth(proposal: MvProposal): string[] {
  const statuses = signalStatuses(proposal)
  if (Object.keys(statuses).length === 0) return []
  const grew: string[] = []
  const ev = proposal.evidence
  const qh = ev && typeof ev === "object" ? (ev as Record<string, unknown>).query_history_statement_ids : null
  if (Array.isArray(qh) && qh.length > 0) grew.push("generated-SQL recurrence")
  if (signalAvailable(statuses, "D")) grew.push("usage signals")
  if (signalAvailable(statuses, "L")) grew.push("lineage")
  return grew
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
