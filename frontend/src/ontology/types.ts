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

export interface OntologySettings {
  company_name?: string | null
  catalog_allowlist: string[]
}

// ── Phase 2: refresh / freshness surface (the one new model) ───────────────
export type RefreshState = "cold" | "queued" | "running" | "fresh" | "stale" | "failed"

export interface OntologyRefreshStatus {
  state: RefreshState
  source: "mirror" | "live"
  mirror_as_of?: string | null
  last_run_id?: string | null
  last_run_state: "succeeded" | "failed" | "running" | "none"
  freshness_window_hours: number
  message?: string | null
}
