"""Pydantic v2 contracts for the Ontology surface (Phase 1).

Keep these 1:1 with ``frontend/src/ontology/types.ts`` (the repo's Pydantic ↔ TS
mirror rule). Every payload carries an ``as_of`` ISO-8601 stamp — the
``Provenanced<T>`` discipline (architecture doc §6) reduced to a timestamp for
Phase 1.

Source of truth: ``docs/design/ontology-phase1-build.md`` §4.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

TierStatus = Literal["ok", "degraded", "blocked", "not_exercised"]
TierId = Literal[
    "inventory",
    "signals",
    "tag_graph",
    "membership_write",
    "external_enrichment",
]


class PermissionTier(BaseModel):
    id: TierId
    label: str
    identity: Literal["obo", "sp", "batch"]
    status: TierStatus
    grants: list[str] = Field(default_factory=list)  # copy-ready grant/entitlement lines
    reason: str | None = None  # why blocked/degraded, plain language


class OntologyPreflight(BaseModel):
    tiers: list[PermissionTier]  # 5 rows; Phase 1 exercises 1–3
    can_render_taxonomy: bool  # tier "tag_graph" == ok
    company_name: str | None = None
    catalog_allowlist: list[str] = Field(default_factory=list)
    as_of: str


class OntologyInventory(BaseModel):
    catalogs_scanned: list[str]
    metric_view_count: int
    genie_agent_count: int
    governed_tag_count: int
    as_of: str


class MemberAsset(BaseModel):
    fqn: str
    asset_type: Literal["table", "metric_view", "dashboard", "genie_agent"]


class SubDomainNode(BaseModel):
    tag_value: str  # e.g. "Tax" in Finance/Tax
    name: str
    member_count: int
    members: list[MemberAsset] = Field(default_factory=list)


class DomainNode(BaseModel):
    tag_key: str  # the governed tag acting as domain
    name: str
    member_count: int
    subdomains: list[SubDomainNode] = Field(default_factory=list)
    members: list[MemberAsset] = Field(default_factory=list)  # direct, un-sub-domained


class UngroupedBucket(BaseModel):
    metric_views: list[MemberAsset] = Field(default_factory=list)
    genie_agents: list[MemberAsset] = Field(default_factory=list)


class OntologyTaxonomy(BaseModel):
    domains: list[DomainNode]
    ungrouped: UngroupedBucket
    as_of: str


# ── Phase 3e (17h): the Estate Graph / "Ontology Map" (append-only, MV-D48) ──
# Served read-only from the pre-laid-out genie_ont_graph_snapshot; 1:1 with the
# layout.build_graph_snapshot blob. Keep in sync with frontend/src/ontology/types.ts.
GraphState = Literal["fresh", "stale", "cold"]


class OntologyGraphNode(BaseModel):
    id: str
    label: str
    kind: str  # tag | measure | metric_view | agent | table | domain | subdomain | ungrouped …
    domain_id: str | None = None
    parent_id: str | None = None  # domain rollup nodes: Sub-Domain → Domain link (MV-D71)
    parent_name: str | None = None  # resolved human name of parent_id (MV-D71)
    x: float = 0.0
    y: float = 0.0
    size: float = 1.0
    cost: float | None = None
    member_count: int | None = None  # domain-level rollup nodes only


class OntologyGraphEdge(BaseModel):
    src: str
    dst: str
    kind: str
    weight: float | None = None


class OntologyGraphLevel(BaseModel):
    nodes: list[OntologyGraphNode] = Field(default_factory=list)
    edges: list[OntologyGraphEdge] = Field(default_factory=list)
    truncated: bool = False


class OntologyGraph(BaseModel):
    domains: OntologyGraphLevel = Field(default_factory=OntologyGraphLevel)
    assets: OntologyGraphLevel = Field(default_factory=OntologyGraphLevel)
    layout: str = "none"
    node_count: int = 0
    edge_count: int = 0
    state: GraphState = "cold"
    as_of: str | None = None


class GovernedTag(BaseModel):
    tag_key: str
    allowed_values: list[str] = Field(default_factory=list)
    assignment_count: int
    acts_as_domain: bool
    acts_as_subdomain: bool


CollisionKind = Literal["exact", "fuzzy_case", "fuzzy_plural", "fuzzy_token"]
CleanupFlag = Literal["orphan", "near_empty", "deprecated_but_assigned"]


class TagCollision(BaseModel):
    kind: CollisionKind
    members: list[str]  # tag keys that collide
    suggestion: str  # "reuse `finance` instead of creating `Finance`"


class TagCleanup(BaseModel):
    tag_key: str
    flag: CleanupFlag
    detail: str


class TagLens(BaseModel):
    tags: list[GovernedTag]
    collisions: list[TagCollision]
    cleanup: list[TagCleanup]
    as_of: str


# Curation-policy defaults (MV-D57), shipped moderate + inspectable. The facet
# denylist mirrors the wheel's in-code Stage-1 constants (transforms._FACET_*), lifted
# here so an enterprise can inspect + extend it; the wheel treats config entries as
# ADDITIVE on top of the shipped patterns, never removing one.
DEFAULT_DOMAIN_FACET_DENYLIST: list[str] = [
    "contains_synthetic", "data_tier", "certification", "certified",
    "controlled_placeholder", "governance", "open_reference", "reference",
    "demo", "demos", "demo_domain", "techsummit", "domain",
    "sensitivity", "pii", "quality", "status", "lifecycle",
]

# Stage 3.2 edge-hygiene + diffuseness-net defaults (MV-D61/62), shipped conservative
# and inspectable. Only ``information_schema`` is denylisted by default (always non-
# business, estate-neutral); the ``_code`` suffix is intentionally absent (a reference/
# enum, not a join key); the generic-name seed and the diffuseness thresholds mirror the
# wheel's in-code constants (schema_signals / transforms), lifted here so an enterprise
# can inspect + extend them per its estate (spec §3).
DEFAULT_DOMAIN_SCHEMA_DENYLIST: list[str] = ["information_schema"]
DEFAULT_DOMAIN_JOIN_COL_SUFFIXES: list[str] = ["_id", "_key"]
DEFAULT_DOMAIN_JOIN_COL_DENYLIST: list[str] = [
    "id", "user_id", "workspace_id", "category_id", "tenant_id", "account_id",
]


class IndustryAlignment(BaseModel):
    """Industry-reference alignment toggle (MV-D58) — STORED + DORMANT here; §9 is
    Phase 4. Persisted so the surface exists, but nothing reads it in Stage 3."""
    enabled: bool = False
    reference_model: str | None = None


class OntologySettings(BaseModel):
    company_name: str | None = None
    catalog_allowlist: list[str] = Field(default_factory=list)
    # Identity the two foundation reads (tag graph + signals) run under (MV-D50).
    # Additive + defaulted: old clients and stored rows without the field read as
    # "obo" (the viewing admin). "sp" = the app service principal (opt-in, requires
    # the banner's grants); "auto" = SP when its probe succeeds, else OBO.
    read_identity: Literal["obo", "sp", "auto"] = "obo"
    # ── Stage 3 (MV-D57): per-enterprise curation policy — additive + defaulted, so an
    # old stored row (missing these keys) reads the shipped moderate defaults. Persisted
    # via the MV-D50 ADD COLUMN IF NOT EXISTS pattern; threaded to the job as params. ──
    domain_facet_denylist: list[str] = Field(default_factory=lambda: list(DEFAULT_DOMAIN_FACET_DENYLIST))
    domain_min_tables: int = 3
    domain_min_schemas: int = 2
    domain_require_connection: bool = True
    # ── Stage 3.2 (MV-D61/62): edge-hygiene + diffuseness net — additive + defaulted,
    # threaded to the job as params; a param-less run applies the shipped defaults. ──
    domain_schema_denylist: list[str] = Field(default_factory=lambda: list(DEFAULT_DOMAIN_SCHEMA_DENYLIST))
    domain_join_col_suffixes: list[str] = Field(default_factory=lambda: list(DEFAULT_DOMAIN_JOIN_COL_SUFFIXES))
    domain_join_col_max_schemas: int = 2
    domain_join_col_denylist: list[str] = Field(default_factory=lambda: list(DEFAULT_DOMAIN_JOIN_COL_DENYLIST))
    domain_max_diffuse_schemas: int = 6
    domain_min_home_concentration: float = 0.5
    # ── Stage 4.1d (MV-D66): bounded batch auto-drafting — additive + defaulted, threaded
    # to the job as params; a param-less run applies the shipped defaults. The batch
    # LLM-drafts only the "super sure" set (certify + corroboration ≥ min, top-N by score);
    # everyone else carries the deterministic stub. max_pages=0 ⇒ a pure-stub batch. ──
    page_autodraft_min_corroboration: int = 3
    page_autodraft_max_pages: int = 50
    industry_alignment: IndustryAlignment = Field(default_factory=IndustryAlignment)


# ── Phase 2: refresh / freshness surface (the ONLY new model) ──────────────
# The Phase-1 models above are FROZEN (byte-identical to Phase 1). `as_of` on the
# existing payloads now reports the mirror materialization time when served from
# the mirror, and the live read time on the fallback — the field type is unchanged.

RefreshState = Literal["cold", "queued", "running", "fresh", "stale", "failed", "skipped"]


class OntologyRefreshStatus(BaseModel):
    state: RefreshState  # cold=never run; fresh=within window; stale=beyond window
    source: Literal["mirror", "live"]  # what the read routes are currently serving
    mirror_as_of: str | None = None  # materialization time of the current mirror (ISO-8601)
    last_run_id: str | None = None
    last_run_state: Literal["succeeded", "failed", "running", "none", "skipped"] = "none"
    freshness_window_hours: int = 24  # how old the mirror may be before "stale"
    message: str | None = None  # plain-language, zero-burden (e.g. "Updated 3 hours ago")


# ── Phase 3d: serve the ranked drafts + record decisions (§4) ──────────────
# APPEND-ONLY. The Phase-1/2/3a-c models above are FROZEN (byte-identical). These
# new models mirror 1:1 into frontend/src/ontology/types.ts. The card is prop-driven
# (MV-D23): the backend assembles the zero-burden ``why``/``reason`` + evidence chips;
# the component renders them and assembles nothing.

DraftTier = Literal["high", "medium", "low"]  # sub-threshold is never served
DecisionKind = Literal["domain", "subdomain", "page", "reassign"]
DecisionAction = Literal["approve", "dismiss", "reassign_accept", "reassign_reject"]


class EvidenceChip(BaseModel):
    label: str  # plain-language, e.g. "41% of Genie questions" — never a rendered "NN% confidence"
    kind: Literal["usage", "centrality", "governance", "corroboration", "conflict"]


class ConfidenceBand(BaseModel):
    """The honest confidence (MV-D56): a readable band + the signals present + the one
    useful gap — NEVER a percent (MV-D35). Replaces the opaque bare tier on the card.
    ``band`` is None only when sub-threshold (never served)."""
    band: Literal["High", "Medium", "Low"] | None = None
    signals_present: list[str] = Field(default_factory=list)
    gap: str = ""


class DomainDraft(BaseModel):
    proposal_id: str  # = domain_id (member-fingerprint-derived, metastore-stable)
    kind: Literal["domain", "subdomain", "reassign"]
    name: str
    description: str
    tag_decision: Literal["create", "reuse", "reassign"]
    conflict_tag: str | None = None  # for reassign: the governed tag it conflicts with
    subdomains: list[str] = Field(default_factory=list)
    members: list[MemberAsset] = Field(default_factory=list)
    why: str  # "Why we're suggesting this" — zero-burden, assembled server-side
    evidence: list[EvidenceChip] = Field(default_factory=list)
    tier: DraftTier
    # Stage 3 (MV-D56): the honest confidence band, additive. The card renders this in
    # place of the bare tier; ``tier`` stays for ordering + back-compat.
    confidence: ConfidenceBand | None = None


class PageDraft(BaseModel):
    proposal_id: str  # = page_id (canonical-concept-derived, 17f)
    archetype: Literal["Routing", "Disambiguation", "Guardrail", "Taxonomy"]
    title: str
    reason: str  # leads the card
    body: str
    synonyms: list[str] = Field(default_factory=list)
    related_fqns: list[str] = Field(default_factory=list)
    source_fqns: list[str] = Field(default_factory=list)
    # Stage 4 (MV-D55): a one-line "why this asset" per Source/Related FQN, assembled
    # server-side from the wheel's ``evidence.asset_why``. Additive; the card renders it
    # under Sources/Related. Absent → an empty map (older rows, or a degraded run).
    asset_why: dict[str, str] = Field(default_factory=dict)
    certify: bool
    evidence: list[EvidenceChip] = Field(default_factory=list)
    tier: DraftTier


class OntologyDrafts(BaseModel):
    domains: list[DomainDraft] = Field(default_factory=list)
    pages: list[PageDraft] = Field(default_factory=list)
    source: Literal["mirror", "live", "cold"]  # cold = empty/degraded (MV-D43)
    as_of: str


class DecisionRequest(BaseModel):
    kind: DecisionKind
    proposal_id: str
    action: DecisionAction


class DecisionResponse(BaseModel):
    ok: bool
    recorded: Literal["consent", "suppression"]
    as_of: str


# ── Phase 5 (17i): apply models (APPEND-ONLY) ──────────────────────────────────────
# The governed-tag membership apply (the ONLY write path). The Phase-1/2/3a-c models
# above are FROZEN (byte-identical). These new models mirror 1:1 into types.ts.

ApplyShape = Literal["create_tag", "set_tag", "unset_tag"]


class ApplyItem(BaseModel):
    proposal_id: str                 # the consented proposal this realizes (domain_id/reassign id)
    proposal_kind: Literal["domain", "subdomain", "reassign"]  # never "page"
    shape: ApplyShape
    target_fqn: str                  # the asset the membership lands on (or the tag, for create_tag)
    tag_key: str
    tag_value: str | None = None     # None for a bare create_tag
    current_value: str | None = None # for the diff ("moves" vs "adds"); None if unknown/absent
    statement: str                   # the exact SQL (server-owned; NOT rendered verbatim to curators)
    executable: bool                 # False → blocked → copy-ready
    blocked_reason: str | None = None
    required_grants: list[str] = Field(default_factory=list)  # copy-ready grant lines when blocked


class ApplyPlan(BaseModel):
    items: list[ApplyItem] = Field(default_factory=list)
    executable_count: int
    blocked_count: int
    plan_hash: str                   # fingerprint of the ordered statements — execute must echo it
    source: Literal["mirror", "live", "cold"]
    as_of: str


class ApplyExecuteRequest(BaseModel):
    plan_hash: str                   # must equal the preview's — else 409 (stale plan)
    confirm: bool                    # must be True — the explicit second consent
    proposal_ids: list[str] | None = None  # optional filter (default: the whole plan)


class ApplyOutcome(BaseModel):
    proposal_id: str
    shape: ApplyShape
    target_fqn: str
    ok: bool
    state: Literal["applied", "failed", "blocked"]
    error: str | None = None


class ApplyResult(BaseModel):
    applied: list[ApplyOutcome] = Field(default_factory=list)
    failed: list[ApplyOutcome] = Field(default_factory=list)
    blocked: list[ApplyOutcome] = Field(default_factory=list)   # missing grant → copy-ready
    as_of: str


# ── Stage 4.1d (Steps 3–4): on-demand + bulk body drafting (APPEND-ONLY) ──────
# Human-initiated curator "Draft with AI" for a single Page or a whole sub-domain.
# Uses the wheel-native LLM client (MV-D65), runs the SAME gates as the batch
# drafter, and UPDATEs genie_ont_pages via the SQL warehouse (MV-D49: body_source/
# facts_hash/body_stale ride the evidence JSON). Returns typed responses.


class DraftBodyResponse(BaseModel):
    """Response from a single Page draft."""
    ok: bool
    page_id: str
    body: str
    body_source: Literal["llm_ondemand", "llm_bulk", "unknown"]
    as_of: str
    reason: str | None = None  # error reason only when ok=false


class BulkDraftStart(BaseModel):
    """Response from starting a bulk (sub-domain) draft task."""
    task_id: str
    total: int


class BulkDraftResult(BaseModel):
    """Per-page result in a bulk draft status."""
    page_id: str
    ok: bool
    reason: str | None = None


class BulkDraftStatus(BaseModel):
    """Status of a bulk draft task (polled via task_id)."""
    done: int
    total: int
    running: bool
    results: list[BulkDraftResult] = Field(default_factory=list)
