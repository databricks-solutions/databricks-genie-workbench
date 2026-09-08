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

// Industry-reference alignment (MV-D58) — STORED + DORMANT; §9 is Phase 4.
export interface IndustryAlignment {
  enabled: boolean
  reference_model?: string | null
}

export interface OntologySettings {
  company_name?: string | null
  catalog_allowlist: string[]
  read_identity?: ReadIdentity
  // Stage 3 (MV-D57): per-enterprise curation policy — additive + defaulted.
  domain_facet_denylist?: string[]
  domain_min_tables?: number
  domain_min_schemas?: number
  domain_require_connection?: boolean
  // Stage 3.2 (MV-D61/62): edge-hygiene + diffuseness net — additive + defaulted.
  domain_schema_denylist?: string[]
  domain_join_col_suffixes?: string[]
  domain_join_col_max_schemas?: number
  domain_join_col_denylist?: string[]
  domain_max_diffuse_schemas?: number
  domain_min_home_concentration?: number
  industry_alignment?: IndustryAlignment
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

// The honest confidence (MV-D56): a readable band + the signals present + the one
// useful gap — NEVER a percent (MV-D35). Rendered in place of the bare tier.
export interface ConfidenceBand {
  band: "High" | "Medium" | "Low" | null
  signals_present: string[]
  gap: string
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
  confidence?: ConfidenceBand | null
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
  // Stage 4 (MV-D55): one-line "why this asset" keyed by Source/Related FQN. Additive.
  asset_why: Record<string, string>
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

// ── Phase 3e (17h): the Estate Graph / "Ontology Map" (append-only, MV-D48) ──
// 1:1 with backend/ontology/models.py OntologyGraph*.
export type GraphState = "fresh" | "stale" | "cold"

// Provenance of a rollup node (MV-D74, Map v2 §3). `applied` = backed by a governed-tag
// assignment (authoritative current state); `proposed` = a pure engine cluster (rendered
// dashed + "Suggested"). Optional + defaulted server-side so older snapshots still parse.
export type GraphOrigin = "applied" | "proposed"

export interface OntologyGraphNode {
  id: string
  label: string
  kind: string
  domain_id?: string | null
  parent_id?: string | null
  parent_name?: string | null
  x: number
  y: number
  size: number
  cost?: number | null
  member_count?: number | null
  // Map v2 §2.1/§2.4 (MV-D74): provenance marker threaded onto every rollup node.
  origin?: string | null
  // Northstar Data Lane (MV-D82): containment attach point — "asset" | "subdomain" | "domain".
  attach_level?: string | null
  // Northstar gap-closure (MV-D86, Lane D2): plain-language description + a compact meta
  // bag (rows/format/freshness, measure count, expression, …) for the hover-snippet +
  // inspector (MV-D85, Lane P). Pre-seed carve: contract only; filled wheel-side by Lane D2.
  // Optional so a pre-MV-D86 blob still parses and Lane P degrades to generic copy.
  description?: string | null
  meta?: Record<string, string> | null
}

export interface OntologyGraphEdge {
  src: string
  dst: string
  kind: string
  weight?: number | null
  // Northstar Data Lane (MV-D82): plain-language verb + within/cross class ("shared" | "xdom").
  verb?: string | null
  rel_class?: string | null
}

export interface OntologyGraphLevel {
  nodes: OntologyGraphNode[]
  edges: OntologyGraphEdge[]
  truncated: boolean
}

export interface OntologyGraph {
  domains: OntologyGraphLevel
  assets: OntologyGraphLevel
  layout: string
  node_count: number
  edge_count: number
  state: GraphState
  as_of?: string | null
  // Northstar Data Lane (MV-D82): the single `org` estate root above the Domains.
  // Optional so a pre-MV-D82 / cold graph (no root) still parses.
  root?: OntologyGraphNode | null
}

// Map v2 §2.3/§2.4 (MV-D73): expand-on-demand "business snippet" layer. Returns the
// children of ONE node, hydrated on click (Bloom `addAndUpdateElementsInGraph` pattern):
// a metric_view's measures (`kind="measure"`, edge `mv_measure`) and a node's attached
// Pages (`kind="page"`, edge `page_source`). 1:1 with backend OntologyGraphExpand.
export interface OntologyGraphExpand {
  nodes: OntologyGraphNode[]
  edges: OntologyGraphEdge[]
  parent_id: string
  as_of: string | null
}

// ── Stage 4.1d: Draft endpoints (MV-D66) ───────────────────────────────────
export interface DraftBodyResponse {
  ok: boolean
  page_id: string
  body: string
  body_source: string
  as_of: string
  // Backend attaches a plain reason only when ok=false (backward-compatible; the
  // frozen contract omitted it). Surfaced on the card so a failure explains itself.
  reason?: string | null
}

export interface BulkDraftStart {
  task_id: string
  total: number
}

export interface BulkDraftResult {
  page_id: string
  ok: boolean
  reason?: string | null
}

export interface BulkDraftStatus {
  done: number
  total: number
  running: boolean
  results: BulkDraftResult[]
}

// ── Phase 5 (17i): consented governed-tag apply (MV-D37/D49/D26/D50) ────────
// Mirrors backend/ontology/models.py 1:1. `statement` is server-owned SQL — it is
// NEVER rendered verbatim to a curator (MV-D23 zero-burden); the UI describes each
// item in plain language.
export type ApplyShape = "create_tag" | "set_tag" | "unset_tag"

export interface ApplyItem {
  proposal_id: string
  proposal_kind: "domain" | "subdomain" | "reassign"
  shape: ApplyShape
  target_fqn: string
  tag_key: string
  tag_value?: string | null
  current_value?: string | null
  statement: string
  executable: boolean
  blocked_reason?: string | null
  required_grants: string[]
}

export interface ApplyPlan {
  items: ApplyItem[]
  executable_count: number
  blocked_count: number
  plan_hash: string
  source: "mirror" | "live" | "cold"
  as_of: string
}

export interface ApplyExecuteRequest {
  plan_hash: string
  confirm: boolean
  proposal_ids?: string[] | null
}

export interface ApplyOutcome {
  proposal_id: string
  shape: ApplyShape
  target_fqn: string
  ok: boolean
  state: "applied" | "failed" | "blocked"
  error?: string | null
}

export interface ApplyResult {
  applied: ApplyOutcome[]
  failed: ApplyOutcome[]
  blocked: ApplyOutcome[]
  as_of: string
}
