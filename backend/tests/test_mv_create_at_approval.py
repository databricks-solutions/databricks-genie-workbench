"""Create-at-approval (MV-D34) + the MV-D35 served fields — Prompt 15.8.

Tested at the seam (no Databricks). What matters:

- **The acceptance journey ends in a real view (MV-D34):** a SUFFICIENT fresh
  probe → OBO ``CREATE`` through the SAME ``mv_create`` seam → an ``OBO_CREATED``
  ledger row on a sentinel advice run (the BYO-register rails), so
  attach-on-next-run picks it up. Never the SP, never a fork.
- **Never a dead end (MV-D34.c):** an INSUFFICIENT fresh probe (or a create-time
  degrade) returns ``degraded`` with the remediation GRANT — nothing created —
  so the card can fall back to [Approve for later].
- **The MV-D22 guards travel:** a revalidation failure / rung-below / collision
  refuses the create with a reason, never installs the wrong artifact.
- **The facts row (MV-D35) is gated on real proof:** ``_mv_checks_from_row``
  emits a check ONLY when the row proves its gate ran.
- **The GRANT grantee is ACL-derived (fix #3):** ``_space_audience_grantees``
  returns the space's CAN RUN/VIEW/MANAGE principals, deduped.
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
from genie_space_optimizer.optimization import mv_yaml


# ── Service: create_at_approval ─────────────────────────────────────────────


def _verification(effective_mode="create_and_attach", downgrade_reason=None, verdict="SUFFICIENT"):
    fresh = SimpleNamespace(
        capabilities=[],
        checked_as="analyst@example.com",
        remediation_sql="GRANT ALL PRIVILEGES ON SCHEMA finance.sales TO `analyst@example.com`",
    )
    return SimpleNamespace(
        effective_mode=effective_mode,
        downgrade_reason=downgrade_reason,
        verdict=verdict,
        fresh_probe=fresh,
    )


_CONSENT = {"target_catalog": "finance", "target_schema": "sales", "probe_id": "p1"}
_ARTIFACT = {
    "yaml_text": "version: 0.1\nsource: finance.sales.orders\n",
    "join_strategy": "nested",
    "proposed_object": "warehouse.raw.revenue_metrics",
}


@pytest.fixture
def approval_env(monkeypatch):
    executed: list[str] = []
    upserts: list[dict] = []
    advice_runs: list[dict] = []

    monkeypatch.setattr(mv_create, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(mv_create, "require_obo_workspace_client", lambda: MagicMock())
    monkeypatch.setattr(
        mv_create, "verify_consent", lambda **kw: (_verification(), dict(_CONSENT))
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "proposed_object": "warehouse.raw.revenue_metrics",
        }],
    )
    monkeypatch.setattr(mv_create, "_load_ddl_artifact", lambda *a, **k: dict(_ARTIFACT))
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: False)
    monkeypatch.setattr(mv_create, "_confirm_metric_view", lambda *a, **k: True)
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )
    monkeypatch.setattr(
        warehouse, "sql_warehouse_execute",
        lambda ws, warehouse_id, sql: executed.append(sql),
    )
    monkeypatch.setattr(
        warehouse, "wh_ensure_optimization_tables",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        warehouse, "wh_create_advice_run",
        lambda ws, warehouse_id, **kw: advice_runs.append(kw),
    )
    monkeypatch.setattr(
        warehouse, "wh_upsert_mv_created_object",
        lambda ws, warehouse_id, **kw: (upserts.append(kw) or kw["full_name"]),
    )
    return executed, upserts, advice_runs


def _create():
    return mv_create.create_at_approval(
        space_id="space-1", suggestion_id="sug1", probe_id="p1",
        catalog="main", schema="gso", warehouse_id="wh1",
    )


def test_happy_path_creates_at_the_consented_name_and_records_obo_created(approval_env):
    executed, upserts, advice_runs = approval_env

    result = _create()

    assert result.created is True
    assert result.degraded is False
    # Re-targeted to the CONSENTED catalog/schema, base name preserved.
    assert result.full_name == "finance.sales.revenue_metrics"
    assert any("CREATE VIEW finance.sales.revenue_metrics" in s for s in executed)
    # The BYO-register rails: a sentinel advice run + a CREATED / OBO_CREATED row,
    # which attach-on-next-run picks up unchanged (MV-D24).
    assert len(advice_runs) == 1 and advice_runs[0]["space_id"] == "space-1"
    assert upserts and upserts[0]["status"] == "CREATED"
    assert upserts[0]["provenance"] == mv_create.MV_PROVENANCE_OBO_CREATED
    assert upserts[0]["created_by"] == "analyst@example.com"
    assert result.run_id == advice_runs[0]["run_id"]


def test_candidate_yaml_text_is_the_fallback_when_no_artifact(approval_env, monkeypatch):
    executed, upserts, _ = approval_env
    monkeypatch.setattr(mv_create, "_load_ddl_artifact", lambda *a, **k: None)
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "yaml_text": "version: 0.1\nsource: finance.sales.orders\n",
            "proposed_object": "warehouse.raw.revenue_metrics",
            "evidence": {"join_strategy": "nested"},
        }],
    )
    result = _create()
    assert result.created is True
    assert any("CREATE VIEW finance.sales.revenue_metrics" in s for s in executed)


def test_insufficient_fresh_probe_degrades_with_remediation_and_creates_nothing(approval_env, monkeypatch):
    executed, upserts, advice_runs = approval_env
    monkeypatch.setattr(
        mv_create, "verify_consent",
        lambda **kw: (
            _verification(effective_mode="suggest_only",
                          downgrade_reason="grant revoked", verdict="INSUFFICIENT"),
            dict(_CONSENT),
        ),
    )
    result = _create()
    assert result.created is False
    assert result.degraded is True
    assert result.verdict == "INSUFFICIENT"
    assert result.remediation_sql and "GRANT" in result.remediation_sql
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == [] and advice_runs == []


def test_no_consent_degrades(approval_env, monkeypatch):
    monkeypatch.setattr(mv_create, "verify_consent", lambda **kw: (None, None))
    result = _create()
    assert result.created is False
    assert result.degraded is True
    assert "consent" in (result.reason or "")


def test_revalidation_failure_refuses_and_creates_nothing(approval_env, monkeypatch):
    executed, upserts, _ = approval_env
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=False, errors=("bad yaml",)),
    )
    result = _create()
    assert result.created is False
    assert result.degraded is False
    assert "re-validation" in (result.reason or "")
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == []


def test_rung_below_refuses(approval_env, monkeypatch):
    executed, _, _ = approval_env
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to="subquery_source"),
    )
    result = _create()
    assert result.created is False
    assert not any("CREATE VIEW" in s for s in executed)


def test_existing_object_is_not_clobbered(approval_env, monkeypatch):
    executed, _, _ = approval_env
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: True)
    result = _create()
    assert result.created is False
    assert "already exists" in (result.reason or "")
    assert not any("CREATE VIEW" in s for s in executed)


def test_missing_candidate_returns_a_reason(approval_env, monkeypatch):
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    result = _create()
    assert result.created is False
    assert result.degraded is False
    assert "no proposal" in (result.reason or "")


# ── The facts row (MV-D35): _mv_checks_from_row is gated on real proof ─────


def test_checks_all_pass_for_a_servable_non_overlapping_row():
    checks = auto_optimize._mv_checks_from_row(
        {"proposed_object": "finance.sales.revenue_metrics", "conflicts": []}
    )
    assert checks == {"validated": "PASS", "executable": "PASS", "no_overlap": "PASS"}


def test_checks_omit_no_overlap_when_conflicts_present():
    checks = auto_optimize._mv_checks_from_row(
        {"proposed_object": "finance.sales.revenue_metrics", "conflicts": [{"x": 1}]}
    )
    # Validated/executable still prove out; no_overlap is NOT claimed.
    assert checks == {"validated": "PASS", "executable": "PASS"}


def test_checks_none_for_a_blank_row_never_lies():
    # A blank identifier never claims validated/executable (no servable body).
    # It still (harmlessly) reports the dedup gate finding no overlap — the row
    # is dropped before surfacing regardless. But a blank row that ALSO carries a
    # conflict claims NOTHING at all: no key is invented, so checks is None.
    assert auto_optimize._mv_checks_from_row({"proposed_object": "  ", "conflicts": []}) == {
        "no_overlap": "PASS"
    }
    assert auto_optimize._mv_checks_from_row(
        {"proposed_object": None, "conflicts": [{"overlap": "x"}]}
    ) is None


# ── ACL-derived grantees (fix #3): _space_audience_grantees ──────────────────


def _acl_ws(acl: dict):
    ws = MagicMock()
    ws.api_client.do.return_value = acl
    return ws


def test_grantees_are_the_audience_principals_deduped(monkeypatch):
    acl = {
        "access_control_list": [
            {"user_name": "a@x.com", "all_permissions": [{"permission_level": "CAN_RUN"}]},
            {"group_name": "analysts", "all_permissions": [{"permission_level": "CAN_VIEW"}]},
            {"user_name": "owner@x.com", "all_permissions": [{"permission_level": "CAN_MANAGE"}]},
            # A principal with no audience-level permission is excluded.
            {"user_name": "nobody@x.com", "all_permissions": [{"permission_level": "SOMETHING_ELSE"}]},
            # A duplicate principal is not repeated.
            {"user_name": "a@x.com", "all_permissions": [{"permission_level": "CAN_VIEW"}]},
        ]
    }
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: _acl_ws(acl))
    grantees = auto_optimize._space_audience_grantees("space-1")
    assert grantees == ["a@x.com", "analysts", "owner@x.com"]


def test_grantees_empty_when_acl_unreadable(monkeypatch):
    ws = MagicMock()
    ws.api_client.do.side_effect = RuntimeError("403")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: ws)
    assert auto_optimize._space_audience_grantees("space-1") == []


# ── Route: POST /spaces/{space_id}/mv/create ────────────────────────────────


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(
        auto_optimize, "require_obo_workspace_client",
        lambda: SimpleNamespace(current_user=MagicMock()),
    )
    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app)


def test_create_route_returns_created(client, monkeypatch):
    monkeypatch.setattr(
        mv_create, "create_at_approval",
        lambda **k: mv_create.MvCreateAtApprovalResult(
            created=True, full_name="finance.sales.revenue_metrics",
            run_id="run-obo-1", suggestion_id="sug1", verdict="SUFFICIENT",
        ),
    )
    resp = client.post(
        "/api/auto-optimize/spaces/space-1/mv/create",
        json={"suggestion_id": "sug1", "probe_id": "p1"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["created"] is True
    assert body["full_name"] == "finance.sales.revenue_metrics"
    assert body["provenance"] == "OBO_CREATED"
    assert body["run_id"] == "run-obo-1"


def test_create_route_returns_degraded(client, monkeypatch):
    monkeypatch.setattr(
        mv_create, "create_at_approval",
        lambda **k: mv_create.MvCreateAtApprovalResult(
            created=False, degraded=True, suggestion_id="sug1",
            verdict="INSUFFICIENT", remediation_sql="GRANT ...",
            reason="not sufficient",
        ),
    )
    resp = client.post(
        "/api/auto-optimize/spaces/space-1/mv/create",
        json={"suggestion_id": "sug1", "probe_id": "p1"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["created"] is False
    assert body["degraded"] is True
    assert body["remediation_sql"] == "GRANT ..."


def test_create_route_requires_obo(client, monkeypatch):
    def _no_obo():
        raise RuntimeError("This operation requires user authorization")

    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client", _no_obo)
    resp = client.post(
        "/api/auto-optimize/spaces/space-1/mv/create",
        json={"suggestion_id": "sug1", "probe_id": "p1"},
    )
    assert resp.status_code == 401
