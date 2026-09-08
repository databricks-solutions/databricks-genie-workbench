"""Bring-your-own metric-view registration (MV-D24): service and routes.

Tested at the seam (no Databricks). What matters:

- **Verification, not trust (invariant 2):** a non-metric-view, an invisible
  object, or YAML that fails the safety lint is refused with the reason and
  **nothing is written**.
- **Sequencing (item 3):** every verification gate passes first; the
  ``USER_CREATED`` ledger row is the LAST write, so a verified-but-unrecorded
  view can never reach the attach phase.
- **Drop refuses on provenance (invariant 1):** the app never drops a
  ``USER_CREATED`` view, even when it is DETACHED.
- **OBO (MV-D20):** registration refuses when no user token is present.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import auto_optimize
from backend.services import mv_create
from genie_space_optimizer.common import warehouse


_VALID_MV_YAML = (
    "version: '0.1'\n"
    "source: main.sales.orders\n"
    "measures:\n"
    "  - name: total_amount\n"
    "    expr: SUM(amount)\n"
)


def _obo_as(email="analyst@example.com"):
    ws = MagicMock()
    ws.current_user.me.return_value = SimpleNamespace(user_name=email)
    return ws


# ── Service: verification + sequencing ─────────────────────────────────────


@pytest.fixture
def register_env(monkeypatch):
    calls: list[str] = []
    upserts: list[dict] = []

    monkeypatch.setattr(mv_create, "require_obo_workspace_client", _obo_as)
    monkeypatch.setattr(mv_create, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(
        warehouse, "wh_ensure_optimization_tables",
        lambda *a, **k: calls.append("ensure"),
    )
    monkeypatch.setattr(
        warehouse, "wh_create_advice_run",
        lambda *a, **k: calls.append("advice_run"),
    )

    def _capture_upsert(*a, **k):
        calls.append("ledger")
        upserts.append(k)
        return k.get("full_name")

    monkeypatch.setattr(warehouse, "wh_upsert_mv_created_object", _capture_upsert)
    return calls, upserts


def test_register_writes_user_created_row_last(register_env, monkeypatch):
    calls, upserts = register_env
    monkeypatch.setattr(
        mv_create, "_recover_registered_metric_view",
        lambda *a, **k: (True, _VALID_MV_YAML, None),
    )

    result = mv_create.register_user_created_view(
        space_id="space-1",
        full_name="main.sales.revenue_metrics",
        catalog="main", schema="gso", warehouse_id="wh1",
    )

    assert result.registered is True
    assert result.provenance == "USER_CREATED"
    assert result.run_id
    # The ledger row is the LAST fallible step (item 3): bootstrap, then the
    # sentinel advice run, then the row.
    assert calls == ["ensure", "advice_run", "ledger"]
    assert upserts[0]["provenance"] == "USER_CREATED"
    assert upserts[0]["created_by"] == "analyst@example.com"


def test_register_refuses_a_non_metric_view_and_writes_nothing(register_env, monkeypatch):
    calls, _ = register_env
    monkeypatch.setattr(
        mv_create, "_recover_registered_metric_view",
        lambda *a, **k: (False, None, "main.sales.orders is not a metric view"),
    )

    result = mv_create.register_user_created_view(
        space_id="space-1", full_name="main.sales.orders",
        catalog="main", schema="gso", warehouse_id="wh1",
    )

    assert result.registered is False
    assert "not a metric view" in (result.reason or "")
    assert calls == []  # nothing written


def test_register_refuses_an_invalid_identifier(register_env):
    result = mv_create.register_user_created_view(
        space_id="space-1", full_name="not a valid; name",
        catalog="main", schema="gso", warehouse_id="wh1",
    )
    assert result.registered is False
    assert "three-part" in (result.reason or "")


def test_register_refuses_yaml_that_fails_the_safety_lint(register_env, monkeypatch):
    calls, _ = register_env
    # A metric view whose YAML has no source — the safety lint blocks it.
    monkeypatch.setattr(
        mv_create, "_recover_registered_metric_view",
        lambda *a, **k: (True, "version: '0.1'\nmeasures: []\n", None),
    )
    result = mv_create.register_user_created_view(
        space_id="space-1", full_name="main.sales.broken",
        catalog="main", schema="gso", warehouse_id="wh1",
    )
    assert result.registered is False
    assert "validation" in (result.reason or "")
    assert calls == []


def test_register_refuses_a_claim_that_does_not_match(register_env, monkeypatch):
    calls, _ = register_env
    monkeypatch.setattr(
        mv_create, "_recover_registered_metric_view",
        lambda *a, **k: (True, _VALID_MV_YAML, None),
    )
    monkeypatch.setattr(
        mv_create, "_claim_matches_view",
        lambda *a, **k: (False, "fingerprint mismatch"),
    )
    result = mv_create.register_user_created_view(
        space_id="space-1", full_name="main.sales.revenue_metrics",
        claimed_suggestion_id="sug-1",
        catalog="main", schema="gso", warehouse_id="wh1",
    )
    assert result.registered is False
    assert "mismatch" in (result.reason or "")
    assert calls == []


# ── Routes ───────────────────────────────────────────────────────────────


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client", lambda: _obo_as())
    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app)


def test_register_route_returns_verified(client, monkeypatch):
    monkeypatch.setattr(
        mv_create, "register_user_created_view",
        lambda **k: mv_create.MvRegisterResult(
            registered=True, full_name=k["full_name"],
            run_id="run-1", suggestion_id="user:abc",
        ),
    )
    resp = client.post(
        "/api/auto-optimize/spaces/space-1/mv/register",
        json={"full_name": "main.sales.revenue_metrics"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["registered"] is True
    assert body["provenance"] == "USER_CREATED"
    assert body["run_id"] == "run-1"


def test_register_route_returns_refusal_with_reason(client, monkeypatch):
    monkeypatch.setattr(
        mv_create, "register_user_created_view",
        lambda **k: mv_create.MvRegisterResult(
            registered=False, full_name=k["full_name"],
            reason="main.sales.orders is not a metric view",
        ),
    )
    resp = client.post(
        "/api/auto-optimize/spaces/space-1/mv/register",
        json={"full_name": "main.sales.orders"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["registered"] is False
    assert "not a metric view" in body["reason"]


def test_register_route_requires_obo(client, monkeypatch):
    def _no_obo():
        raise RuntimeError("This operation requires user authorization")

    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client", _no_obo)
    resp = client.post(
        "/api/auto-optimize/spaces/space-1/mv/register",
        json={"full_name": "main.sales.revenue_metrics"},
    )
    assert resp.status_code == 401


def test_drop_refuses_a_user_created_view_on_provenance_even_when_detached(client, monkeypatch):
    # Invariant 1: the app never drops a USER_CREATED view — refused on
    # provenance before the status check, so DETACHED does not let it through.
    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client",
                        lambda: _obo_as("owner@example.com"))
    monkeypatch.setattr(
        warehouse, "wh_load_mv_created_object",
        lambda *a, **k: {
            "run_id": "r1", "suggestion_id": "sug1",
            "full_name": "main.sales.revenue_metrics",
            "created_by": "owner@example.com",
            "status": "DETACHED", "provenance": "USER_CREATED",
        },
    )
    resp = client.post(
        "/api/auto-optimize/mv/created/sug1/drop",
        json={"run_id": "r1", "confirm": True},
    )
    assert resp.status_code == 409
    assert "user-created" in resp.json()["detail"].lower() or "bring-your-own" in resp.json()["detail"].lower()
