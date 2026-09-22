/**
 * Estate view — pure, side-effect-free logic (MV-D107 Phase 2). The navigable/provenant
 * Estate reads the already-loaded taxonomy + drafts; nothing here fetches or mutates. Kept
 * out of the component so the reconcile/filter/sort join is unit-testable and TaxonomyView
 * stays a component-only module (react-refresh).
 */
import type { DomainDraft, DomainNode, GovernedTag, OntologyTaxonomy } from "@/ontology/types"

// ── Provenance reconcile (taxonomy row × drafts) ──────────────────────────────
// One badge per declared domain, joining what EXISTS (governed-tag taxonomy) against what
// the engine PROPOSES (drafts). A live reassign/conflict outranks a plain reuse; an empty
// declared tag with no proposal reads as "unpopulated".
export type EstateProvenance = "confirmed" | "better" | "unpopulated" | "none"

export interface DomainReconcile {
  provenance: EstateProvenance
  /** Set only for "better": the draft to deep-link to in Review. */
  proposalId?: string
}

function norm(s: string): string {
  return s.trim().toLowerCase()
}

/**
 * Map each taxonomy domain (keyed by tag_key) to its provenance badge. A draft is "related"
 * to a domain when it reuses/conflicts with that tag key, or (fallback) shares its name.
 * Priority: Better proposal (reassign / conflict_tag) → Confirmed (reuse) → unpopulated
 * (0 members) → none.
 */
export function reconcile(
  taxonomy: OntologyTaxonomy | null,
  draftDomains: DomainDraft[],
): Map<string, DomainReconcile> {
  const result = new Map<string, DomainReconcile>()
  for (const d of taxonomy?.domains ?? []) {
    const related = draftDomains.filter(
      (dr) =>
        (dr.conflict_tag != null && dr.conflict_tag === d.tag_key) || norm(dr.name) === norm(d.name),
    )
    const better = related.find(
      (dr) => dr.kind === "reassign" || dr.tag_decision === "reassign" || dr.conflict_tag != null,
    )
    if (better) {
      result.set(d.tag_key, { provenance: "better", proposalId: better.proposal_id })
      continue
    }
    if (related.some((dr) => dr.tag_decision === "reuse")) {
      result.set(d.tag_key, { provenance: "confirmed" })
      continue
    }
    result.set(d.tag_key, { provenance: d.member_count === 0 ? "unpopulated" : "none" })
  }
  return result
}

/**
 * Lead-in for a scoped-but-untagged estate: no domain has any members yet, but drafts are
 * ready. The Estate view leads with "nothing tagged yet — N proposals ready" → Review.
 */
export function estateNeedsReview(
  taxonomy: OntologyTaxonomy | null,
  draftDomains: DomainDraft[],
): { show: boolean; count: number } {
  const domains = taxonomy?.domains ?? []
  const anyMembers = domains.some((d) => d.member_count > 0)
  return { show: !anyMembers && draftDomains.length > 0, count: draftDomains.length }
}

// ── Search / hide-empty / sort ────────────────────────────────────────────────
export type EstateSort = "members" | "name"

export interface EstateControls {
  query: string
  hideEmpty: boolean
  sortBy: EstateSort
}

/**
 * Smart default for the "hide 0-member" toggle. On a freshly-scanned estate NOTHING is tagged
 * yet, so every domain is 0-member; defaulting hide-empty ON there would blank the whole tree
 * (and make search/sort look dead). So: default ON only when at least one domain is populated —
 * otherwise OFF, so the declared-but-unpopulated taxonomy is visible on load.
 */
export function defaultHideEmpty(domains: DomainNode[]): boolean {
  return domains.some((d) => d.member_count > 0)
}

/**
 * How many governed tags are NOT part of the Domain → Sub-Domain tree (neither a domain nor a
 * sub-domain). These never appear in Taxonomy — only in the Tags lens — which is why the domain
 * list is shorter than the full governed-tag list. Powers the "where are the rest?" explainer.
 */
export function countNonDomainTags(tags: GovernedTag[] | null | undefined): number {
  return (tags ?? []).filter((t) => !t.acts_as_domain && !t.acts_as_subdomain).length
}

/** Filter the governed-tag lens by tag key or any allowed value (case-insensitive). */
export function filterTags(tags: GovernedTag[], query: string): GovernedTag[] {
  const needle = query.trim().toLowerCase()
  if (!needle) return tags
  return tags.filter(
    (t) =>
      t.tag_key.toLowerCase().includes(needle) ||
      t.allowed_values.some((v) => v.toLowerCase().includes(needle)),
  )
}

/** Whether a domain (its name, tag key, sub-domains, or member FQNs) matches a search needle. */
export function domainMatches(domain: DomainNode, query: string): boolean {
  const needle = query.trim().toLowerCase()
  if (!needle) return true
  if (domain.name.toLowerCase().includes(needle)) return true
  if (domain.tag_key.toLowerCase().includes(needle)) return true
  for (const sub of domain.subdomains) {
    if (sub.name.toLowerCase().includes(needle)) return true
    if (sub.members.some((m) => m.fqn.toLowerCase().includes(needle))) return true
  }
  return domain.members.some((m) => m.fqn.toLowerCase().includes(needle))
}

/**
 * Apply hide-empty + search, then sort. "name" is alpha; "members" is member_count desc with a
 * SUB-DOMAIN-COUNT tiebreak before name — so on an all-0-member estate (where a plain member sort
 * collapses to name order and looks inert) the richer domains still float to the top.
 */
export function filterAndSortDomains(domains: DomainNode[], controls: EstateControls): DomainNode[] {
  let out = domains
  if (controls.hideEmpty) out = out.filter((d) => d.member_count > 0)
  if (controls.query.trim()) out = out.filter((d) => domainMatches(d, controls.query))
  const sorted = [...out]
  if (controls.sortBy === "name") {
    sorted.sort((a, b) => a.name.localeCompare(b.name))
  } else {
    sorted.sort(
      (a, b) =>
        b.member_count - a.member_count ||
        b.subdomains.length - a.subdomains.length ||
        a.name.localeCompare(b.name),
    )
  }
  return sorted
}
