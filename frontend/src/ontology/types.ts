// Ontology types — 1:1 mirror of backend/ontology/models.py (§4/§5).
// Keep field-for-field in sync with the Pydantic contracts.

export type TierStatus = "ok" | "degraded" | "blocked" | "not_exercised"
export type TierId =
  | "inventory"
  | "signals"
  | "tag_graph"
  | "membership_write"
  | "external_enrichment"

export interface PermissionTier {
  id: TierId
  label: string
  identity: "obo" | "sp" | "batch"
  status: TierStatus
  grants: string[]
  reason?: string | null
}

export interface OntologyPreflight {
  tiers: PermissionTier[]
  can_render_taxonomy: boolean
  company_name?: string | null
  catalog_allowlist: string[]
  as_of: string
}

export interface OntologyInventory {
  catalogs_scanned: string[]
  metric_view_count: number
  genie_agent_count: number
  governed_tag_count: number
  as_of: string
}

export type AssetType = "table" | "metric_view" | "dashboard" | "genie_agent"

export interface MemberAsset {
  fqn: string
  asset_type: AssetType
}

export interface SubDomainNode {
  tag_value: string
  name: string
  member_count: number
  members: MemberAsset[]
}

export interface DomainNode {
  tag_key: string
  name: string
  member_count: number
  subdomains: SubDomainNode[]
  members: MemberAsset[]
}

export interface UngroupedBucket {
  metric_views: MemberAsset[]
  genie_agents: MemberAsset[]
}

export interface OntologyTaxonomy {
  domains: DomainNode[]
  ungrouped: UngroupedBucket
  as_of: string
}

export interface GovernedTag {
  tag_key: string
  allowed_values: string[]
  assignment_count: number
  acts_as_domain: boolean
  acts_as_subdomain: boolean
}

export type CollisionKind = "exact" | "fuzzy_case" | "fuzzy_plural" | "fuzzy_token"
export type CleanupFlag = "orphan" | "near_empty" | "deprecated_but_assigned"

export interface TagCollision {
  kind: CollisionKind
  members: string[]
  suggestion: string
}

export interface TagCleanup {
  tag_key: string
  flag: CleanupFlag
  detail: string
}

export interface TagLens {
  tags: GovernedTag[]
  collisions: TagCollision[]
  cleanup: TagCleanup[]
  as_of: string
}

// Identity the two foundation reads run under (MV-D50). Optional + defaulted
// server-side to "obo" (the viewing admin); "sp" = the app service principal
// (opt-in), "auto" = SP when its probe succeeds, else OBO.
export type ReadIdentity = "obo" | "sp" | "auto"

export interface OntologySettings {
  company_name?: string | null
  catalog_allowlist: string[]
  read_identity?: ReadIdentity
}

// ── Phase 2: refresh / freshness surface (the one new model) ───────────────
export type RefreshState = "cold" | "queued" | "running" | "fresh" | "stale" | "failed" | "skipped"

export interface OntologyRefreshStatus {
  state: RefreshState
  source: "mirror" | "live"
  mirror_as_of?: string | null
  last_run_id?: string | null
  last_run_state: "succeeded" | "failed" | "running" | "none" | "skipped"
  freshness_window_hours: number
  message?: string | null
}

// ── Phase 3d: ranked drafts + decisions (§4) ───────────────────────────────
// 1:1 mirror of the append-only backend models. Sub-threshold is never served.
export type DraftTier = "high" | "medium" | "low"
export type DecisionKind = "domain" | "subdomain" | "page" | "reassign"
export type DecisionAction = "approve" | "dismiss" | "reassign_accept" | "reassign_reject"

export type EvidenceChipKind =
  | "usage"
  | "centrality"
  | "governance"
  | "corroboration"
  | "conflict"

export interface EvidenceChip {
  label: string
  kind: EvidenceChipKind
}

export interface DomainDraft {
  proposal_id: string
  kind: "domain" | "subdomain" | "reassign"
  name: string
  description: string
  tag_decision: "create" | "reuse" | "reassign"
  conflict_tag?: string | null
  subdomains: string[]
  members: MemberAsset[]
  why: string
  evidence: EvidenceChip[]
  tier: DraftTier
}

export interface PageDraft {
  proposal_id: string
  archetype: "Routing" | "Disambiguation" | "Guardrail" | "Taxonomy"
  title: string
  reason: string
  body: string
  synonyms: string[]
  related_fqns: string[]
  source_fqns: string[]
  certify: boolean
  evidence: EvidenceChip[]
  tier: DraftTier
}

export interface OntologyDrafts {
  domains: DomainDraft[]
  pages: PageDraft[]
  source: "mirror" | "live" | "cold"
  as_of: string
}

export interface DecisionRequest {
  kind: DecisionKind
  proposal_id: string
  action: DecisionAction
}

export interface DecisionResponse {
  ok: boolean
  recorded: "consent" | "suppression"
  as_of: string
}
