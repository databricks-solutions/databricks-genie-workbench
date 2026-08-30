"""Ontology preflight (spec §11): with SP grants present the signals/tag_graph
tiers are ok and can_render_taxonomy=True; with a permission error injected on the
tag read the tag_graph tier is blocked, can_render_taxonomy=False, and the page
does not raise."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.ontology.models import OntologySettings
from backend.ontology.routers.preflight import router as preflight_router
from backend.ontology.services import ont_settings, tag_graph
from backend.watch.services import system_tables


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(preflight_router)
    return TestClient(app)


def _patch_settings(monkeypatch, allowlist):
    async def _fake():
        return OntologySettings(company_name="Northwind", catalog_allowlist=allowlist)
    monkeypatch.setattr(ont_settings, "get_settings", _fake)


def test_preflight_all_read_tiers_ok(monkeypatch):
    _patch_settings(monkeypatch, ["finance"])
    monkeypatch.setattr(tag_graph, "probe", lambda: True)
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: True)

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

    data = _client().get("/api/ontology/preflight").json()
    tiers = {t["id"]: t for t in data["tiers"]}
    assert tiers["signals"]["status"] == "degraded"
    # signals never gates rendering.
    assert data["can_render_taxonomy"] is True
