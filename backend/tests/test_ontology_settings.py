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

    async def _upsert(ws, company, allowlist):
        store[ws] = {"company_name": company, "catalog_allowlist": allowlist}
        return store[ws]

    monkeypatch.setattr(ont_settings, "_workspace_id", lambda: "ws1")
    monkeypatch.setattr(ont_settings.lakebase, "ont_get_settings", _get)
    monkeypatch.setattr(ont_settings.lakebase, "ont_upsert_settings", _upsert)
    return store


async def test_defaults_when_unset(fake_store):
    s = await ont_settings.get_settings()
    assert s.company_name is None
    assert s.catalog_allowlist == []


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
    assert got == {"company_name": "Acme", "catalog_allowlist": ["finance"]}

    put = client.put(
        "/api/ontology/settings",
        json={"company_name": "Beta", "catalog_allowlist": ["ops"]},
    )
    assert put.status_code == 200
    assert put.json() == {"company_name": "Beta", "catalog_allowlist": ["ops"]}
