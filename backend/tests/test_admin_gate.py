"""Server-side workspace-admin gate on every /api/ontology/* router (MV-D109 P1).

Before P1 the only admin check was the frontend ``isAdmin`` (PRD Appendix D #26).
These tests pin: one gate (``backend.services.admin_gate.require_admin``) at
router level on all nine ontology modules; a non-admin is refused 403 on one
route per router before any handler runs; an admin passes via the header or via
the OBO identity's groups (the Databricks Apps path, which does not forward
``X-Forwarded-Groups``); the gate fails closed and never consults the SP.
"""

from __future__ import annotations

import pathlib

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.ontology import routers as ontology_routers
from backend.services import admin_gate
from backend.tests._admin import ADMIN_HEADERS, admin_client

_ROUTERS_DIR = pathlib.Path(ontology_routers.__file__).parent

# One route per router module; the gate runs before body/path validation.
_ONE_ROUTE_PER_ROUTER = [
    ("ontology_apply_router", "post", "/api/ontology/apply/preview"),
    ("ontology_drafts_router", "get", "/api/ontology/drafts"),
    ("ontology_graph_router", "get", "/api/ontology/graph"),
    ("ontology_inventory_router", "get", "/api/ontology/inventory"),
    ("ontology_preflight_router", "get", "/api/ontology/preflight"),
    ("ontology_refresh_router", "post", "/api/ontology/refresh"),
    ("ontology_settings_router", "put", "/api/ontology/settings"),
    ("ontology_tags_router", "get", "/api/ontology/tags"),
    ("ontology_taxonomy_router", "get", "/api/ontology/taxonomy"),
]

_NON_ADMIN_HEADERS = {
    "X-Forwarded-Email": "analyst@example.com",
    "x-forwarded-access-token": "tok-analyst",
}


@pytest.fixture(autouse=True)
def _clean_gate(monkeypatch):
    monkeypatch.delenv("DEV_ADMIN", raising=False)
    monkeypatch.delenv("DEV_USER_EMAIL", raising=False)
    admin_gate._cache.clear()
    yield
    admin_gate._cache.clear()


def _app() -> FastAPI:
    app = FastAPI()
    for name in ontology_routers.__all__:
        app.include_router(getattr(ontology_routers, name))
    return app


def _groups(monkeypatch, groups):
    calls = []

    def fake():
        calls.append(1)
        if isinstance(groups, Exception):
            raise groups
        return list(groups)

    monkeypatch.setattr(admin_gate, "obo_groups", fake)
    return calls


def test_one_gate_on_all_nine_router_modules():
    modules = sorted(p.stem for p in _ROUTERS_DIR.glob("*.py") if p.stem != "__init__")
    assert modules == [
        "apply", "drafts", "graph", "inventory", "preflight",
        "refresh", "settings", "tags", "taxonomy",
    ]
    assert len(ontology_routers.__all__) == 9
    for name in ontology_routers.__all__:
        router = getattr(ontology_routers, name)
        assert [d.dependency for d in router.dependencies] == [admin_gate.require_admin], name
        assert router.routes, name
        for route in router.routes:
            assert admin_gate.require_admin in [d.dependency for d in route.dependencies], (
                name, route.path,
            )


@pytest.mark.parametrize("router_name,method,path", _ONE_ROUTE_PER_ROUTER)
def test_non_admin_is_refused_on_every_router(monkeypatch, router_name, method, path):
    _groups(monkeypatch, ["users"])
    resp = getattr(TestClient(_app()), method)(path, headers=_NON_ADMIN_HEADERS)
    assert resp.status_code == 403, (router_name, resp.text)
    assert resp.json()["detail"] == "This operation requires workspace admin access."


def test_admin_header_passes_the_gate(monkeypatch):
    calls = _groups(monkeypatch, ["users"])
    resp = admin_client(_app()).get("/api/ontology/health")
    assert resp.status_code == 200
    assert calls == []  # the header proved admin; no SDK call


def test_obo_admins_group_passes_without_the_groups_header(monkeypatch):
    """Databricks Apps forwards no X-Forwarded-Groups; the OBO identity decides."""
    _groups(monkeypatch, ["users", "admins"])
    resp = TestClient(_app()).get("/api/ontology/health", headers=_NON_ADMIN_HEADERS)
    assert resp.status_code == 200


def test_sdk_failure_fails_closed(monkeypatch):
    _groups(monkeypatch, RuntimeError("This operation requires user authorization"))
    resp = TestClient(_app()).get("/api/ontology/health", headers=_NON_ADMIN_HEADERS)
    assert resp.status_code == 403
    assert admin_gate._cache == {}  # a failure is never cached


def test_gate_never_falls_back_to_the_service_principal(monkeypatch):
    """No OBO context → require_obo_workspace_client raises → not admin."""
    monkeypatch.setattr(
        admin_gate, "require_obo_workspace_client",
        lambda: (_ for _ in ()).throw(RuntimeError("This operation requires user authorization")),
    )
    resp = TestClient(_app()).get("/api/ontology/health", headers={"X-Forwarded-Email": "a@b.c"})
    assert resp.status_code == 403


def test_resolution_is_cached_per_token(monkeypatch):
    calls = _groups(monkeypatch, ["admins"])
    client = TestClient(_app())
    for _ in range(3):
        assert client.get("/api/ontology/health", headers=_NON_ADMIN_HEADERS).status_code == 200
    assert len(calls) == 1
    other = {**_NON_ADMIN_HEADERS, "x-forwarded-access-token": "tok-other"}
    assert client.get("/api/ontology/health", headers=other).status_code == 200
    assert len(calls) == 2


def test_dev_modes_match_auth_me(monkeypatch):
    _groups(monkeypatch, RuntimeError("no OBO"))
    client = TestClient(_app())
    monkeypatch.setenv("DEV_USER_EMAIL", "dev@example.com")
    assert client.get("/api/ontology/health").status_code == 200
    # DEV_USER_EMAIL only applies with no OBO user headers.
    assert client.get("/api/ontology/health", headers={"X-Forwarded-Email": "x@y.z"}).status_code == 403
    monkeypatch.setenv("DEV_ADMIN", "true")
    assert client.get("/api/ontology/health", headers={"X-Forwarded-Email": "x@y.z"}).status_code == 200


def test_watch_reexports_the_shared_gate():
    from backend.watch import _auth
    from backend.watch.routers import admin as watch_admin

    assert _auth.require_admin is admin_gate.require_admin
    assert _auth.is_admin_request is admin_gate.is_admin_request
    route = next(r for r in watch_admin.router.routes if r.path.endswith("/refresh-rollup"))
    assert admin_gate.require_admin in [d.dependency for d in route.dependencies]


def test_auth_me_uses_the_shared_obo_resolution(monkeypatch):
    from backend.routers import auth as auth_router

    monkeypatch.setattr(auth_router, "obo_groups", lambda: ["users", "admins"])
    app = FastAPI()
    app.include_router(auth_router.router)
    me = TestClient(app).get("/api/auth/me", headers={"X-Forwarded-Email": "a@b.c"}).json()
    assert me["is_admin"] is True
    assert me["groups"] == ["users", "admins"]


def test_shared_admin_client_carries_the_real_admin_signal():
    req = type("R", (), {"headers": {k: v for k, v in ADMIN_HEADERS.items()}})()
    assert admin_gate.is_admin_request(req) is True
