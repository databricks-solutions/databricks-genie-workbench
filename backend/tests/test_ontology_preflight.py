"""Ontology preflight (spec §11): with SP grants present the signals/tag_graph
tiers are ok and can_render_taxonomy=True; with a permission error injected on the
tag read the tag_graph tier is blocked, can_render_taxonomy=False, and the page
does not raise."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.ontology.models import OntologySettings
from backend.ontology.routers import preflight as preflight_mod
from backend.ontology.routers.preflight import router as preflight_router
from backend.ontology.services import inventory, ont_settings, tag_graph
from backend.watch.services import system_tables


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(preflight_router)
    return TestClient(app)


def _patch_settings(monkeypatch, allowlist):
    async def _fake():
        return OntologySettings(company_name="Northwind", catalog_allowlist=allowlist)
    monkeypatch.setattr(ont_settings, "get_settings", _fake)


def _patch_membership(monkeypatch, sp_seen: int, obo_seen: int):
    """Stub the BROWSE differential probes so preflight never hits a warehouse."""
    monkeypatch.setattr(tag_graph, "sp_assignment_count", lambda _allow: sp_seen)
    monkeypatch.setattr(inventory, "governed_tag_count", lambda _client, _allow: obo_seen)
    # Avoid constructing a real default client in the no-auth test env.
    monkeypatch.setattr(preflight_mod, "get_workspace_client", lambda: object())


def test_preflight_all_read_tiers_ok(monkeypatch):
    _patch_settings(monkeypatch, ["finance"])
    monkeypatch.setattr(tag_graph, "probe", lambda: True)
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: True)
    _patch_membership(monkeypatch, sp_seen=7, obo_seen=7)  # SP sees members → ok

    data = _client().get("/api/ontology/preflight").json()
    assert data["can_render_taxonomy"] is True

    tiers = {t["id"]: t for t in data["tiers"]}
    assert len(tiers) == 5
    assert tiers["inventory"]["status"] == "ok"
    assert tiers["signals"]["status"] == "ok"
    assert tiers["tag_graph"]["status"] == "ok"
    assert tiers["membership_write"]["status"] == "not_exercised"
    assert tiers["external_enrichment"]["status"] == "not_exercised"


def test_preflight_tag_graph_blocked_does_not_raise(monkeypatch):
    _patch_settings(monkeypatch, ["finance"])
    monkeypatch.setattr(tag_graph, "probe", lambda: False)  # permission error → blocked
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: True)

    resp = _client().get("/api/ontology/preflight")
    assert resp.status_code == 200  # page-does-not-raise
    data = resp.json()
    assert data["can_render_taxonomy"] is False
    tiers = {t["id"]: t for t in data["tiers"]}
    assert tiers["tag_graph"]["status"] == "blocked"
    # The blocked tier surfaces the copy-ready grant.
    assert any("governed_tags" in g for g in tiers["tag_graph"]["grants"])
    assert tiers["tag_graph"]["reason"]


def test_preflight_signals_degraded_when_grants_missing(monkeypatch):
    _patch_settings(monkeypatch, ["finance"])
    monkeypatch.setattr(tag_graph, "probe", lambda: True)
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: False)
    _patch_membership(monkeypatch, sp_seen=7, obo_seen=7)

    data = _client().get("/api/ontology/preflight").json()
    tiers = {t["id"]: t for t in data["tiers"]}
    assert tiers["signals"]["status"] == "degraded"
    # signals never gates rendering.
    assert data["can_render_taxonomy"] is True


def test_preflight_tag_graph_degraded_when_browse_missing(monkeypatch):
    """Tree renders (governed_tags readable) but the SP is blind to assignments
    while the admin sees them → BROWSE-needed: degraded tier + copy-ready grant,
    and the taxonomy still renders (member counts just read 0)."""
    _patch_settings(monkeypatch, ["finance", "sales"])
    monkeypatch.setattr(tag_graph, "probe", lambda: True)
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: True)
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "sp-app-42")
    _patch_membership(monkeypatch, sp_seen=0, obo_seen=9)  # SP blind, OBO sees tags

    data = _client().get("/api/ontology/preflight").json()
    assert data["can_render_taxonomy"] is True  # tree still renders
    tag = {t["id"]: t for t in data["tiers"]}["tag_graph"]
    assert tag["status"] == "degraded"
    assert tag["reason"] and "BROWSE" in tag["reason"]
    # Copy-ready GRANT for each catalog, targeting the resolved SP.
    assert "GRANT BROWSE ON CATALOG `finance` TO `sp-app-42`" in tag["grants"]
    assert "GRANT BROWSE ON CATALOG `sales` TO `sp-app-42`" in tag["grants"]


def test_preflight_no_browse_nag_when_no_tags(monkeypatch):
    """Both SP and OBO see zero assignments → genuinely no tags, not a grant gap."""
    _patch_settings(monkeypatch, ["finance"])
    monkeypatch.setattr(tag_graph, "probe", lambda: True)
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: True)
    _patch_membership(monkeypatch, sp_seen=0, obo_seen=0)

    data = _client().get("/api/ontology/preflight").json()
    tiers = {t["id"]: t for t in data["tiers"]}
    assert tiers["tag_graph"]["status"] == "ok"
