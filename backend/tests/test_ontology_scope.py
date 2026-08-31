"""Scope boundary (spec §11, MV-D42): an empty allowlist yields empty
inventory/taxonomy and a preflight hint to choose catalogs — never a scan of
everything."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.ontology.models import OntologySettings
from backend.ontology.routers.preflight import router as preflight_router
from backend.ontology.services import inventory, ont_settings, tag_graph, taxonomy
from backend.watch.services import system_tables


def test_empty_allowlist_inventory_counts_zero_without_querying():
    # Passing a sentinel client that would explode if used proves no query runs.
    class _Boom:
        def __getattr__(self, _):
            raise AssertionError("no query should run for an empty allowlist")

    assert inventory.metric_view_count(_Boom(), []) == 0
    assert inventory.governed_tag_count(_Boom(), []) == 0
    assert inventory.metric_view_fqns(_Boom(), []) == []


def test_empty_allowlist_tag_graph_is_empty_without_querying(monkeypatch):
    # build_graph must return early for an empty allowlist and never touch the SP.
    def _boom(*a, **k):
        raise AssertionError("no SP query should run for an empty allowlist")

    monkeypatch.setattr(tag_graph, "_run", _boom)
    graph = tag_graph.build_graph([])
    assert graph["tags"] == []
    # A taxonomy built from the empty graph is empty.
    result = taxonomy.build_taxonomy(graph, metric_views=[], genie_agents=[])
    assert result.domains == []
    assert result.ungrouped.metric_views == []


def test_preflight_hints_to_choose_catalogs_when_empty(monkeypatch):
    async def _fake():
        return OntologySettings(company_name=None, catalog_allowlist=[])
    monkeypatch.setattr(ont_settings, "get_settings", _fake)
    monkeypatch.setattr(tag_graph, "probe", lambda *a, **k: True)
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: True)

    app = FastAPI()
    app.include_router(preflight_router)
    data = TestClient(app).get("/api/ontology/preflight").json()

    assert data["catalog_allowlist"] == []
    tiers = {t["id"]: t for t in data["tiers"]}
    # The inventory tier carries the "choose catalogs" hint.
    assert tiers["inventory"]["reason"] and "catalog" in tiers["inventory"]["reason"].lower()
