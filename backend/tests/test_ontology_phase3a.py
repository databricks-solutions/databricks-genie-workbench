"""Ontology Phase-3a backend tests (spec §11): the contract-frozen guard proves
Phase 3a adds NO API model, and the Tags lens surfaces the embedding-backed
dedupe verdicts through the UNCHANGED TagLens contract on the mirror path."""

from __future__ import annotations

import inspect

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from backend.ontology import models
from backend.ontology.models import OntologySettings
from backend.ontology.routers import tags as tags_router
from backend.ontology.services import ont_settings


# ── Contract-frozen: Phase 3a adds no model (byte-identical surface) ────────

# The complete API model surface after Phase 1 + Phase 2. Phase 3a adds NONE.
_EXPECTED_MODELS = {
    "PermissionTier", "OntologyPreflight", "OntologyInventory", "MemberAsset",
    "SubDomainNode", "DomainNode", "UngroupedBucket", "OntologyTaxonomy",
    "GovernedTag", "TagCollision", "TagCleanup", "TagLens", "OntologySettings",
    "OntologyRefreshStatus",
}


def test_no_new_api_model_added_in_phase3a():
    defined = {
        name for name, obj in inspect.getmembers(models, inspect.isclass)
        if issubclass(obj, BaseModel) and obj.__module__ == models.__name__
    }
    assert defined == _EXPECTED_MODELS, f"model surface changed: {defined ^ _EXPECTED_MODELS}"


def test_tag_collision_kind_vocabulary_unchanged():
    # The frozen 4-value CollisionKind — embedding merges must map into it, not extend it.
    import typing

    args = set(typing.get_args(models.CollisionKind))
    assert args == {"exact", "fuzzy_case", "fuzzy_plural", "fuzzy_token"}


# ── Enriched collisions surfaced through the frozen TagLens (mirror path) ───


def _tags_app() -> FastAPI:
    app = FastAPI()
    app.include_router(tags_router.router)
    return app


def _patch_settings(monkeypatch):
    async def _settings():
        return OntologySettings(company_name=None, catalog_allowlist=["finance"])
    monkeypatch.setattr(ont_settings, "get_settings", _settings)
    monkeypatch.setattr(ont_settings, "_workspace_id", lambda: "ws1")
    # The ontology grain is the metastore (MV-D49); mirror reads scope by it.
    monkeypatch.setattr(ont_settings, "_metastore_id", lambda: "ms1")


def test_mirror_surfaces_embedding_backed_collisions_through_frozen_shape(monkeypatch):
    _patch_settings(monkeypatch)

    async def _fresh(ws):
        return True

    # Mirror graph carrying Phase-3a per-tag dedupe_verdicts (an embedding-backed
    # collision mapped to the frozen 'fuzzy_token' kind).
    async def _graph(ws):
        return {
            "tags": [
                {
                    "tag_key": "net_revenue", "allowed_values": [], "assignment_count": 5,
                    "members": [],
                    "dedupe_verdicts": {
                        "collisions": [{
                            "kind": "fuzzy_token",
                            "members": ["net_revenue", "revenue_after_discount"],
                            "suggestion": "reuse `net_revenue` instead of creating `revenue_after_discount`",
                        }],
                        "cleanup": [],
                    },
                },
                {
                    "tag_key": "revenue_after_discount", "allowed_values": [], "assignment_count": 1,
                    "members": [],
                    "dedupe_verdicts": {
                        "collisions": [{
                            "kind": "fuzzy_token",
                            "members": ["net_revenue", "revenue_after_discount"],
                            "suggestion": "reuse `net_revenue` instead of creating `revenue_after_discount`",
                        }],
                        "cleanup": [{"tag_key": "revenue_after_discount", "flag": "near_empty", "detail": "only 1"}],
                    },
                },
            ],
            "as_of": "2026-08-30T09:00:00+00:00",
        }

    monkeypatch.setattr(tags_router.refresh, "mirror_is_fresh", _fresh)
    monkeypatch.setattr(tags_router.mirror, "read_tag_graph", _graph)
    monkeypatch.setattr(tags_router.tag_graph, "build_graph",
                        lambda a, *_a, **_k: (_ for _ in ()).throw(AssertionError("live path used")))

    data = TestClient(_tags_app()).get("/api/ontology/tags").json()

    # Embedding-backed collision surfaced (deduped to one group), frozen shape intact.
    assert len(data["collisions"]) == 1
    coll = data["collisions"][0]
    assert set(coll.keys()) == {"kind", "members", "suggestion"}
    assert coll["kind"] == "fuzzy_token"
    assert set(coll["members"]) == {"net_revenue", "revenue_after_discount"}
    # Cleanup surfaced from verdicts; TagLens shape unchanged.
    assert data["cleanup"] and set(data["cleanup"][0].keys()) == {"tag_key", "flag", "detail"}
    assert data["as_of"] == "2026-08-30T09:00:00+00:00"


def test_cold_path_still_uses_string_collisions(monkeypatch):
    _patch_settings(monkeypatch)

    async def _not_fresh(ws):
        return False

    monkeypatch.setattr(tags_router.refresh, "mirror_is_fresh", _not_fresh)
    # Live graph (no dedupe_verdicts) → string collisions path.
    monkeypatch.setattr(
        tags_router.tag_graph, "build_graph",
        lambda a, *_a, **_k: {"tags": [
            {"tag_key": "finance", "allowed_values": [], "assignment_count": 3, "members": []},
            {"tag_key": "Finance", "allowed_values": [], "assignment_count": 1, "members": []},
        ], "as_of": "2026-08-30T12:00:00+00:00"},
    )
    data = TestClient(_tags_app()).get("/api/ontology/tags").json()
    # finance ~ Finance collapses via the string (case) signal.
    assert any(set(c["members"]) == {"Finance", "finance"} for c in data["collisions"])
