"""Ontology contracts — every model round-trips model_dump(mode='json') and
enums reject unknown values (spec §11)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from backend.ontology.models import (
    DecisionRequest,
    DecisionResponse,
    DomainDraft,
    DomainNode,
    EvidenceChip,
    GovernedTag,
    MemberAsset,
    OntologyDrafts,
    OntologyInventory,
    OntologyPreflight,
    OntologySettings,
    OntologyTaxonomy,
    PageDraft,
    PermissionTier,
    SubDomainNode,
    TagCleanup,
    TagCollision,
    TagLens,
    UngroupedBucket,
)


def test_all_models_round_trip_json():
    models = [
        PermissionTier(id="inventory", label="Inventory", identity="obo", status="ok"),
        OntologyPreflight(tiers=[], can_render_taxonomy=False, as_of="2026-08-29T00:00:00Z"),
        OntologyInventory(
            catalogs_scanned=["finance"],
            metric_view_count=3,
            genie_agent_count=2,
            governed_tag_count=5,
            as_of="2026-08-29T00:00:00Z",
        ),
        MemberAsset(fqn="c.s.t", asset_type="table"),
        SubDomainNode(tag_value="Tax", name="Tax", member_count=1),
        DomainNode(tag_key="Finance", name="Finance", member_count=2),
        UngroupedBucket(),
        OntologyTaxonomy(domains=[], ungrouped=UngroupedBucket(), as_of="2026-08-29T00:00:00Z"),
        GovernedTag(tag_key="Finance", assignment_count=2, acts_as_domain=True, acts_as_subdomain=False),
        TagCollision(kind="fuzzy_case", members=["a", "A"], suggestion="reuse `a`"),
        TagCleanup(tag_key="x", flag="orphan", detail="no assignments"),
        TagLens(tags=[], collisions=[], cleanup=[], as_of="2026-08-29T00:00:00Z"),
        OntologySettings(company_name="Acme", catalog_allowlist=["finance"]),
        # ── Phase 3d: the new drafts + decision models (append-only) ──────────
        EvidenceChip(label="Actively queried", kind="usage"),
        DomainDraft(
            proposal_id="sug_d", kind="reassign", name="Finance", description="d",
            tag_decision="reassign", conflict_tag="finance", subdomains=["Tax"],
            members=[MemberAsset(fqn="c.s.t", asset_type="table")],
            why="worth confirming", evidence=[EvidenceChip(label="x", kind="conflict")],
            tier="high",
        ),
        PageDraft(
            proposal_id="pg_1", archetype="Routing", title="[Routing] total_revenue",
            reason="answer from the metric view", body="Description: ...",
            synonyms=["TR"], related_fqns=[], source_fqns=["c.s.mv"], certify=True,
            evidence=[EvidenceChip(label="Backed by 2 sources", kind="corroboration")],
            tier="medium",
        ),
        OntologyDrafts(domains=[], pages=[], source="cold", as_of="2026-08-31T00:00:00Z"),
        DecisionRequest(kind="reassign", proposal_id="sug_d", action="reassign_reject"),
        DecisionResponse(ok=True, recorded="suppression", as_of="2026-08-31T00:00:00Z"),
    ]
    for m in models:
        dumped = m.model_dump(mode="json")
        # Re-validate the JSON round-trip.
        type(m).model_validate(dumped)


def test_tier_status_enum_rejects_unknown():
    with pytest.raises(ValidationError):
        PermissionTier(id="inventory", label="x", identity="obo", status="totally_unknown")


def test_tier_id_enum_rejects_unknown():
    with pytest.raises(ValidationError):
        PermissionTier(id="not_a_tier", label="x", identity="obo", status="ok")


def test_collision_kind_enum_rejects_unknown():
    with pytest.raises(ValidationError):
        TagCollision(kind="semantic_embedding", members=["a"], suggestion="x")


def test_cleanup_flag_enum_rejects_unknown():
    with pytest.raises(ValidationError):
        TagCleanup(tag_key="x", flag="totally_stale", detail="x")


def test_member_asset_type_enum_rejects_unknown():
    with pytest.raises(ValidationError):
        MemberAsset(fqn="c.s.t", asset_type="notebook")
