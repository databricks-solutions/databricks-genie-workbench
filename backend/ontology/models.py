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


class OntologySettings(BaseModel):
    company_name: str | None = None
    catalog_allowlist: list[str] = Field(default_factory=list)
    # Identity the two foundation reads (tag graph + signals) run under (MV-D50).
    # Additive + defaulted: old clients and stored rows without the field read as
    # "obo" (the viewing admin). "sp" = the app service principal (opt-in, requires
    # the banner's grants); "auto" = SP when its probe succeeds, else OBO.
    read_identity: Literal["obo", "sp", "auto"] = "obo"


# ── Phase 2: refresh / freshness surface (the ONLY new model) ──────────────
# The Phase-1 models above are FROZEN (byte-identical to Phase 1). `as_of` on the
# existing payloads now reports the mirror materialization time when served from
# the mirror, and the live read time on the fallback — the field type is unchanged.

RefreshState = Literal["cold", "queued", "running", "fresh", "stale", "failed"]


class OntologyRefreshStatus(BaseModel):
    state: RefreshState  # cold=never run; fresh=within window; stale=beyond window
    source: Literal["mirror", "live"]  # what the read routes are currently serving
    mirror_as_of: str | None = None  # materialization time of the current mirror (ISO-8601)
    last_run_id: str | None = None
    last_run_state: Literal["succeeded", "failed", "running", "none"] = "none"
    freshness_window_hours: int = 24  # how old the mirror may be before "stale"
    message: str | None = None  # plain-language, zero-burden (e.g. "Updated 3 hours ago")
