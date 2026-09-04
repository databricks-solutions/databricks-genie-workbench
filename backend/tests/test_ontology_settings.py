"""Ontology settings service — normalization + round-trip through the Lakebase
accessors (in-memory), and the router wire shape. The PUT is the ONLY Phase-1
write and it targets our own config, never Unity Catalog."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.ontology.models import OntologySettings
from backend.ontology.routers.settings import router as settings_router
from backend.ontology.services import ont_settings


@pytest.fixture
def fake_store(monkeypatch):
    store: dict[str, dict] = {}

    async def _get(ws):
        return store.get(ws)

    async def _upsert(ws, company, allowlist, read_identity="obo", **kw):
        store[ws] = {
            "company_name": company,
            "catalog_allowlist": allowlist,
            "read_identity": read_identity,
            # Stage 3 curation policy (MV-D57) — additive, keyword-only.
            "domain_facet_denylist": kw.get("domain_facet_denylist"),
            "domain_min_tables": kw.get("domain_min_tables", 3),
            "domain_min_schemas": kw.get("domain_min_schemas", 2),
            "domain_require_connection": kw.get("domain_require_connection", True),
            # Stage 3.2 edge-hygiene + diffuseness net (MV-D61/62) — additive, keyword-only.
            "domain_schema_denylist": kw.get("domain_schema_denylist"),
            "domain_join_col_suffixes": kw.get("domain_join_col_suffixes"),
            "domain_join_col_max_schemas": kw.get("domain_join_col_max_schemas", 2),
            "domain_join_col_denylist": kw.get("domain_join_col_denylist"),
            "domain_max_diffuse_schemas": kw.get("domain_max_diffuse_schemas", 6),
            "domain_min_home_concentration": kw.get("domain_min_home_concentration", 0.5),
            # Stage 4.1d bounded auto-drafting (MV-D66) — additive, keyword-only.
            "page_autodraft_min_corroboration": kw.get("page_autodraft_min_corroboration", 3),
            "page_autodraft_max_pages": kw.get("page_autodraft_max_pages", 50),
            "industry_alignment": kw.get("industry_alignment"),
        }
        return store[ws]

    monkeypatch.setattr(ont_settings, "_workspace_id", lambda: "ws1")
    monkeypatch.setattr(ont_settings.lakebase, "ont_get_settings", _get)
    monkeypatch.setattr(ont_settings.lakebase, "ont_upsert_settings", _upsert)
    return store


async def test_defaults_when_unset(fake_store):
    s = await ont_settings.get_settings()
    assert s.company_name is None
    assert s.catalog_allowlist == []
    # read_identity defaults to OBO — the viewing admin (MV-D50).
    assert s.read_identity == "obo"


async def test_read_identity_round_trips_and_defaults_obo(fake_store):
    # An explicit choice round-trips through the store…
    saved = await ont_settings.save_settings(
        OntologySettings(company_name="Acme", catalog_allowlist=["finance"], read_identity="sp")
    )
    assert saved.read_identity == "sp"
    assert (await ont_settings.get_settings()).read_identity == "sp"

    # …and an old row missing the field (additive/defaulted) reads as "obo".
    fake_store["ws1"] = {"company_name": "Acme", "catalog_allowlist": ["finance"]}
    assert (await ont_settings.get_settings()).read_identity == "obo"


async def test_save_normalizes_and_round_trips(fake_store):
    saved = await ont_settings.save_settings(
        OntologySettings(company_name="  Northwind  ", catalog_allowlist=["finance", " finance ", "", "marketing"])
    )
    # Company trimmed; allowlist de-duped/blank-stripped, order preserved.
    assert saved.company_name == "Northwind"
    assert saved.catalog_allowlist == ["finance", "marketing"]

    reread = await ont_settings.get_settings()
    assert reread.company_name == "Northwind"
    assert reread.catalog_allowlist == ["finance", "marketing"]


async def test_blank_company_becomes_none(fake_store):
    saved = await ont_settings.save_settings(OntologySettings(company_name="   ", catalog_allowlist=[]))
    assert saved.company_name is None


async def test_curation_policy_round_trips_and_old_row_reads_defaults(fake_store):
    # An explicit curation policy round-trips through the store (MV-D57)…
    saved = await ont_settings.save_settings(
        OntologySettings(
            catalog_allowlist=["airline"],
            domain_facet_denylist=["widget_kind", " widget_kind ", ""],  # normalized
            domain_min_tables=4, domain_min_schemas=3, domain_require_connection=False,
        )
    )
    assert saved.domain_facet_denylist == ["widget_kind"]  # de-duped/blank-stripped
    assert saved.domain_min_tables == 4 and saved.domain_min_schemas == 3
    assert saved.domain_require_connection is False
    reread = await ont_settings.get_settings()
    assert reread.domain_min_tables == 4 and reread.domain_require_connection is False

    # …and an old row missing the Stage-3 columns reads the shipped moderate defaults.
    fake_store["ws1"] = {"company_name": "Acme", "catalog_allowlist": ["finance"]}
    d = await ont_settings.get_settings()
    assert d.domain_min_tables == 3 and d.domain_min_schemas == 2
    assert d.domain_require_connection is True
    assert d.domain_facet_denylist  # falls back to the shipped default list
    assert d.industry_alignment.enabled is False


async def test_stage32_policy_round_trips_and_old_row_reads_defaults(fake_store):
    # An explicit Stage-3.2 edge-hygiene + diffuseness policy round-trips (MV-D61/62)…
    saved = await ont_settings.save_settings(
        OntologySettings(
            catalog_allowlist=["airline"],
            domain_schema_denylist=["migration", " e2e_* ", ""],       # normalized
            domain_join_col_suffixes=["_id", "_sk"],
            domain_join_col_max_schemas=3,
            domain_join_col_denylist=["id", "tenant_id"],
            domain_max_diffuse_schemas=8,
            domain_min_home_concentration=0.4,
        )
    )
    assert saved.domain_schema_denylist == ["migration", "e2e_*"]      # de-duped/blank-stripped
    assert saved.domain_join_col_suffixes == ["_id", "_sk"]
    assert saved.domain_join_col_max_schemas == 3
    assert saved.domain_max_diffuse_schemas == 8
    assert saved.domain_min_home_concentration == 0.4
    reread = await ont_settings.get_settings()
    assert reread.domain_join_col_denylist == ["id", "tenant_id"]
    assert reread.domain_min_home_concentration == 0.4

    # …and an old row missing the Stage-3.2 columns reads the shipped defaults.
    fake_store["ws1"] = {"company_name": "Acme", "catalog_allowlist": ["finance"]}
    d = await ont_settings.get_settings()
    assert d.domain_schema_denylist == ["information_schema"]
    assert d.domain_join_col_suffixes == ["_id", "_key"]
    assert d.domain_join_col_max_schemas == 2
    assert d.domain_join_col_denylist == [
        "id", "user_id", "workspace_id", "category_id", "tenant_id", "account_id"]
    assert d.domain_max_diffuse_schemas == 6
    assert d.domain_min_home_concentration == 0.5


async def test_stage41d_autodraft_config_round_trips_and_old_row_reads_defaults(fake_store):
    # An explicit Stage-4.1d bounded-auto-draft policy round-trips (MV-D66)…
    saved = await ont_settings.save_settings(
        OntologySettings(
            catalog_allowlist=["airline"],
            page_autodraft_min_corroboration=4,
            page_autodraft_max_pages=25,
        )
    )
    assert saved.page_autodraft_min_corroboration == 4
    assert saved.page_autodraft_max_pages == 25
    reread = await ont_settings.get_settings()
    assert reread.page_autodraft_min_corroboration == 4
    assert reread.page_autodraft_max_pages == 25

    # …and an old row missing the Stage-4.1d columns reads the shipped defaults.
    fake_store["ws1"] = {"company_name": "Acme", "catalog_allowlist": ["finance"]}
    d = await ont_settings.get_settings()
    assert d.page_autodraft_min_corroboration == 3
    assert d.page_autodraft_max_pages == 50


def test_settings_router_wire_shape(monkeypatch):
    async def _get():
        return OntologySettings(company_name="Acme", catalog_allowlist=["finance"])

    async def _save(payload):
        return payload

    monkeypatch.setattr(ont_settings, "get_settings", _get)
    monkeypatch.setattr(ont_settings, "save_settings", _save)

    app = FastAPI()
    app.include_router(settings_router)
    client = TestClient(app)

    got = client.get("/api/ontology/settings").json()
    # The Phase-1 keys are unchanged; Stage 3 adds the curation-policy keys additively.
    assert got["company_name"] == "Acme"
    assert got["catalog_allowlist"] == ["finance"]
    assert got["read_identity"] == "obo"
    assert got["domain_min_tables"] == 3 and got["domain_min_schemas"] == 2
    assert got["domain_require_connection"] is True
    assert isinstance(got["domain_facet_denylist"], list) and got["domain_facet_denylist"]
    assert got["industry_alignment"] == {"enabled": False, "reference_model": None}

    # PUT without read_identity → the additive default "obo" fills in; the curation
    # policy fills its moderate defaults too.
    put = client.put(
        "/api/ontology/settings",
        json={"company_name": "Beta", "catalog_allowlist": ["ops"]},
    )
    assert put.status_code == 200
    body = put.json()
    assert body["company_name"] == "Beta" and body["catalog_allowlist"] == ["ops"]
    assert body["read_identity"] == "obo"
    assert body["domain_min_tables"] == 3 and body["domain_require_connection"] is True
