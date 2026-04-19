"""Integration tests for the Auto-Optimize router (Bug #1 gate enforcement).

These tests lock in the three-layer gate that prevents users from starting an
optimization when MLflow Prompt Registry is unavailable:

    Layer 1 (this file):  /permissions/{space_id} -> can_start = False,
                          prompt_registry_reason_code carries the code.
    Layer 2 (this file):  POST /trigger -> 412 Precondition Failed with
                          {reason_code, prompt_registry_available: False}.
    Layer 3 (elsewhere):  preflight write-probe inside the GSO job.

The server-side /trigger gate is the critical one — UI-only checks can be
bypassed by a client that skips /permissions (Bug #1 root cause).

Style mirrors backend/tests/test_llm_utils.py: pure FastAPI TestClient + light
monkeypatching, no real Databricks connectivity required.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import auto_optimize
from backend.services.prompt_registry import ProbeResult


# ── Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture
def client(monkeypatch) -> TestClient:
    """Mount just the auto_optimize router and pin the minimum env it needs."""
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")

    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app)


@pytest.fixture
def mock_sp_ws() -> MagicMock:
    ws = MagicMock()
    ws.config.client_id = "11111111-2222-3333-4444-555555555555"
    me = MagicMock()
    me.display_name = "GSO-SP"
    me.user_name = "gso-sp"
    me.application_id = "11111111-2222-3333-4444-555555555555"
    ws.current_user.me.return_value = me
    return ws


@pytest.fixture
def mock_user_ws() -> MagicMock:
    return MagicMock()


# ── /permissions — Layer 1 (advisory UI gate) ───────────────────────────


def test_permissions_surfaces_prompt_registry_failure(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    """When the probe says unavailable, /permissions MUST return
    can_start=False, reason_code propagated, and error in errors[]."""
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)

    # Stub the subsystems /permissions reaches into so we isolate to prompt_registry.
    fake_probe = ProbeResult(
        available=False,
        reason_code="feature_not_enabled",
        actionable_by="customer",
        user_message="MLflow Prompt Registry is not enabled on this workspace.",
        raw_error="FEATURE_DISABLED",
        vendor_error_code="FEATURE_DISABLED",
    )

    with patch(
        "genie_space_optimizer.common.sp_permissions.get_sp_principal_aliases",
        return_value=["sp-alias"],
    ), patch(
        "genie_space_optimizer.common.genie_client.sp_can_manage_space",
        return_value=True,
    ), patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        return_value={},
    ), patch(
        "genie_space_optimizer.common.uc_metadata.extract_genie_space_table_refs",
        return_value=[],
    ), patch(
        "genie_space_optimizer.common.uc_metadata.get_unique_schemas",
        return_value=set(),
    ), patch(
        "backend.services.prompt_registry.check_prompt_registry",
        return_value=fake_probe,
    ):
        resp = client.get("/api/auto-optimize/permissions/space-abc")

    assert resp.status_code == 200
    data = resp.json()
    assert data["prompt_registry_available"] is False
    assert data["prompt_registry_reason_code"] == "feature_not_enabled"
    assert data["prompt_registry_error_code"] == "FEATURE_DISABLED"
    assert data["prompt_registry_actionable_by"] == "customer"
    assert data["can_start"] is False
    assert any("not enabled" in e.lower() for e in data["errors"])


def test_permissions_happy_path_allows_start(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    """With SP manage + all schemas granted + prompt registry available,
    can_start must be True and reason_code must be "ok"."""
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)

    ok_probe = ProbeResult(
        available=True,
        reason_code="ok",
        actionable_by="customer",
        user_message="",
        raw_error=None,
    )

    with patch(
        "genie_space_optimizer.common.sp_permissions.get_sp_principal_aliases",
        return_value=["sp-alias"],
    ), patch(
        "genie_space_optimizer.common.genie_client.sp_can_manage_space",
        return_value=True,
    ), patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        return_value={},
    ), patch(
        "genie_space_optimizer.common.uc_metadata.extract_genie_space_table_refs",
        return_value=[],
    ), patch(
        "genie_space_optimizer.common.uc_metadata.get_unique_schemas",
        return_value=set(),
    ), patch(
        "backend.services.prompt_registry.check_prompt_registry",
        return_value=ok_probe,
    ):
        resp = client.get("/api/auto-optimize/permissions/space-abc")

    assert resp.status_code == 200
    data = resp.json()
    assert data["prompt_registry_available"] is True
    assert data["prompt_registry_reason_code"] == "ok"
    assert data["can_start"] is True
    assert data["prompt_registry_error"] is None


def test_permissions_threads_uc_schema_and_refresh_to_probe(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    """Probe–workload parity on the API surface: /permissions must scope
    the probe to the GSO target schema, and must pass bypass_cache=True
    when the UI clicks Re-check (?refresh=true)."""
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)

    ok_probe = ProbeResult(
        available=True, reason_code="ok", actionable_by="customer"
    )

    with patch(
        "genie_space_optimizer.common.sp_permissions.get_sp_principal_aliases",
        return_value=["sp-alias"],
    ), patch(
        "genie_space_optimizer.common.genie_client.sp_can_manage_space",
        return_value=True,
    ), patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        return_value={},
    ), patch(
        "genie_space_optimizer.common.uc_metadata.extract_genie_space_table_refs",
        return_value=[],
    ), patch(
        "genie_space_optimizer.common.uc_metadata.get_unique_schemas",
        return_value=set(),
    ), patch(
        "backend.services.prompt_registry.check_prompt_registry",
        return_value=ok_probe,
    ) as probe_mock:
        # Default (no ?refresh): cache MUST be used.
        r1 = client.get("/api/auto-optimize/permissions/space-abc")
        # With ?refresh=true: cache MUST be bypassed.
        r2 = client.get("/api/auto-optimize/permissions/space-abc?refresh=true")

    assert r1.status_code == 200 and r2.status_code == 200

    # Both calls should have received uc_schema="main.gso_test" (from env fixture).
    kwargs_list = [c.kwargs for c in probe_mock.call_args_list]
    assert all(k["uc_schema"] == "main.gso_test" for k in kwargs_list), kwargs_list
    assert kwargs_list[0]["bypass_cache"] is False
    assert kwargs_list[1]["bypass_cache"] is True


def test_permissions_requires_configured_gso(client, monkeypatch) -> None:
    """Unconfigured GSO must return 503, not a crash or a false-positive can_start."""
    monkeypatch.delenv("GSO_CATALOG", raising=False)
    resp = client.get("/api/auto-optimize/permissions/space-abc")
    assert resp.status_code == 503


# ── /trigger — Layer 2 (authoritative server-side gate) ─────────────────


def test_trigger_blocked_when_prompt_registry_unavailable(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    """This is the Bug #1 regression: even if the UI allows it, the server
    must refuse to launch the job when the probe fails."""
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)

    fail_probe = ProbeResult(
        available=False,
        reason_code="missing_uc_permissions",
        actionable_by="customer",
        user_message="SP lacks USE SCHEMA on main.gso_test",
        raw_error="PERMISSION_DENIED",
        vendor_error_code="PERMISSION_DENIED",
    )

    trigger_mock = MagicMock()

    with patch(
        "backend.services.prompt_registry.check_prompt_registry",
        return_value=fail_probe,
    ), patch.object(auto_optimize, "trigger_optimization", trigger_mock):
        resp = client.post(
            "/api/auto-optimize/trigger",
            json={"space_id": "space-abc", "apply_mode": "genie_config"},
        )

    assert resp.status_code == 412, (
        f"Expected 412 Precondition Failed for customer-actionable probe failure, "
        f"got {resp.status_code}: {resp.text}"
    )
    detail = resp.json()["detail"]
    assert detail["reason_code"] == "missing_uc_permissions"
    assert detail["error_code"] == "PERMISSION_DENIED"
    assert detail["actionable_by"] == "customer"
    assert detail["prompt_registry_available"] is False
    assert "SP lacks" in detail["error"]

    # Critical invariant: the underlying job launcher must NOT have been called.
    trigger_mock.assert_not_called()


def test_trigger_returns_503_for_platform_actionable_probe_failure(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    """Root-cause follow-up: when Prompt Registry returns a vendor-side
    error (ENDPOINT_NOT_FOUND, INTERNAL_ERROR, etc.), /trigger must
    return 503 Service Unavailable rather than 412 Precondition Failed.

    - 412 tells the UI "ask the customer to fix it" (grant a privilege).
    - 503 tells the UI "this is a platform outage" and lets on-call alerts
      key off the status code without parsing the body.
    """
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)

    vendor_bug_probe = ProbeResult(
        available=False,
        reason_code="vendor_bug",
        actionable_by="platform",
        user_message="Platform error (error_code: ENDPOINT_NOT_FOUND).",
        raw_error="No API found for 'GET /mlflow/unity-catalog/prompts'",
        vendor_error_code="ENDPOINT_NOT_FOUND",
    )

    trigger_mock = MagicMock()
    with patch(
        "backend.services.prompt_registry.check_prompt_registry",
        return_value=vendor_bug_probe,
    ), patch.object(auto_optimize, "trigger_optimization", trigger_mock):
        resp = client.post(
            "/api/auto-optimize/trigger",
            json={"space_id": "space-abc", "apply_mode": "genie_config"},
        )

    assert resp.status_code == 503, (
        f"Expected 503 for platform-actionable failure, got {resp.status_code}: {resp.text}"
    )
    detail = resp.json()["detail"]
    assert detail["reason_code"] == "vendor_bug"
    assert detail["error_code"] == "ENDPOINT_NOT_FOUND"
    assert detail["actionable_by"] == "platform"
    trigger_mock.assert_not_called()


def test_trigger_proceeds_when_prompt_registry_available(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    """Happy path: probe succeeds → trigger_optimization runs and its result
    is returned. Ensures the gate doesn't over-block."""
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)

    ok_probe = ProbeResult(
        available=True, reason_code="ok", actionable_by="customer"
    )

    fake_result = MagicMock(
        run_id="run-xyz",
        job_run_id=9999,
        job_url="https://example.com/jobs/12345/runs/9999",
        status="QUEUED",
    )

    with patch(
        "backend.services.prompt_registry.check_prompt_registry",
        return_value=ok_probe,
    ), patch.object(
        auto_optimize, "trigger_optimization", return_value=fake_result
    ) as trigger_mock:
        resp = client.post(
            "/api/auto-optimize/trigger",
            json={"space_id": "space-abc", "apply_mode": "genie_config"},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {
        "runId": "run-xyz",
        "jobRunId": 9999,
        "jobUrl": "https://example.com/jobs/12345/runs/9999",
        "status": "QUEUED",
    }
    trigger_mock.assert_called_once()


def test_trigger_blocks_on_probe_error_fail_closed(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    """Fail-closed invariant: unexpected reason_codes must still block
    the job. The status code follows the probe's ``actionable_by`` axis
    so the UI can distinguish "customer-fixable" (412) from "platform
    outage" (503). Only reason_code=="ok" unlocks."""
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)

    cases = [
        ("registry_path_not_found", "customer", 412),
        ("missing_sp_scope", "customer", 412),
        ("probe_error", "platform", 503),
        ("vendor_bug", "platform", 503),
        ("unknown", "platform", 503),  # legacy — must still block, on platform axis
    ]

    for reason, actionable, expected_status in cases:
        bad_probe = ProbeResult(
            available=False,
            reason_code=reason,
            actionable_by=actionable,
            user_message=f"probe said {reason}",
        )
        trigger_mock = MagicMock()
        with patch(
            "backend.services.prompt_registry.check_prompt_registry",
            return_value=bad_probe,
        ), patch.object(auto_optimize, "trigger_optimization", trigger_mock):
            resp = client.post(
                "/api/auto-optimize/trigger",
                json={"space_id": "space-abc"},
            )
        assert resp.status_code == expected_status, (
            f"reason={reason} actionable_by={actionable} should yield "
            f"{expected_status}, got {resp.status_code}"
        )
        assert resp.json()["detail"]["reason_code"] == reason
        trigger_mock.assert_not_called()


def test_trigger_unconfigured_returns_503(client, monkeypatch) -> None:
    """Unconfigured Auto-Optimize must 503 rather than silently no-op."""
    monkeypatch.delenv("GSO_JOB_ID", raising=False)
    resp = client.post(
        "/api/auto-optimize/trigger",
        json={"space_id": "space-abc"},
    )
    assert resp.status_code == 503


def test_trigger_rejects_malformed_space_id(client) -> None:
    """Pydantic validator on TriggerRequest.space_id must reject injection
    attempts before they reach any gate logic."""
    resp = client.post(
        "/api/auto-optimize/trigger",
        json={"space_id": "'; DROP TABLE runs; --"},
    )
    assert resp.status_code == 422


# ── Bug #2 — derived accuracy ───────────────────────────────────────────


def test_derived_accuracy_prefers_correct_over_evaluated() -> None:
    """The Workbench /runs endpoints must send the UI correct/evaluated,
    not the stored overall_accuracy. Otherwise the KPI card and the
    RunDetailView tab labels can disagree (the bug the user re-filed)."""
    from backend.routers.auto_optimize import _derived_accuracy

    row = {
        "total_questions": 22,
        "correct_count": 16,
        "evaluated_count": 19,
        "excluded_count": 3,
        "overall_accuracy": 72.7,  # stale stored value (e.g. 16/22)
    }
    assert _derived_accuracy(row, run_id="r1", iteration=0) == 84.21


def test_derived_accuracy_falls_back_to_stored_for_legacy_rows() -> None:
    """Legacy rows predate evaluated_count. Honour stored overall_accuracy —
    never divide by total_questions (that's the pre-Bug-#2 regression)."""
    from backend.routers.auto_optimize import _derived_accuracy

    row = {
        "total_questions": 22,
        "correct_count": 16,
        "overall_accuracy": 84.21,
        # no evaluated_count column
    }
    assert _derived_accuracy(row, run_id="r1", iteration=0) == 84.21


def test_derived_accuracy_logs_drift(caplog) -> None:
    import logging

    from backend.routers.auto_optimize import _derived_accuracy

    row = {
        "total_questions": 22,
        "correct_count": 16,
        "evaluated_count": 19,
        "excluded_count": 3,
        "overall_accuracy": 72.7,
    }
    with caplog.at_level(logging.INFO, logger="backend.routers.auto_optimize"):
        _derived_accuracy(row, run_id="run-xyz", iteration=0)

    drift_logs = [r for r in caplog.records if "accuracy_drift" in r.getMessage()]
    assert drift_logs, "Expected gso.runs.accuracy_drift INFO log on drift"
    assert "run-xyz" in drift_logs[0].getMessage()


def test_derived_accuracy_handles_zero_evaluated() -> None:
    """evaluated_count = 0 must return None, not raise a ZeroDivisionError."""
    from backend.routers.auto_optimize import _derived_accuracy

    row = {
        "total_questions": 0,
        "correct_count": 0,
        "evaluated_count": 0,
        "excluded_count": 0,
        "overall_accuracy": 0.0,
    }
    # evaluated is 0, so we fall back to stored (0.0)
    assert _derived_accuracy(row) == 0.0


# ── Bug #2 regression — pre-migration Delta schema fallback ──────────────


class _LegacySchemaError(Exception):
    """Mimics Databricks' UNRESOLVED_COLUMN error shape."""


@pytest.fixture(autouse=True)
def _reset_schema_cache():
    """Each test starts with an unknown schema state so cached decisions
    from a prior test can't mask a broken probe."""
    from backend.routers.auto_optimize import _reset_iterations_schema_cache
    _reset_iterations_schema_cache()
    yield
    _reset_iterations_schema_cache()


def test_select_iterations_delta_falls_back_to_legacy_cols(monkeypatch) -> None:
    """The exact regression the user reported: GSO job bundle hasn't been
    redeployed, so the Delta table is missing evaluated_count /
    excluded_count / quarantined_benchmarks_json. The Workbench app must
    still render the evaluation summary by degrading to the legacy SELECT
    and relying on stored overall_accuracy via _derived_accuracy.
    """
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")

    from backend.routers import auto_optimize

    legacy_row = {
        "iteration": 0,
        "eval_scope": "full",
        "overall_accuracy": 84.21,
        "total_questions": 22,
        "correct_count": 16,
        "scores_json": "[]",
        "failures_json": "[]",
        "thresholds_met": False,
        "lever": None,
        "repeatability_pct": None,
        "reflection_json": None,
        "mlflow_run_id": None,
    }
    calls: list[str] = []

    def fake_delta_query(sql: str, *, strict: bool = False):
        calls.append(sql)
        if "evaluated_count" in sql:
            # First attempt — table missing Bug #2 columns
            if strict:
                raise _LegacySchemaError(
                    "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column with "
                    "name `evaluated_count` cannot be resolved."
                )
            return []
        # Legacy retry succeeds
        return [legacy_row]

    monkeypatch.setattr(auto_optimize, "_delta_query", fake_delta_query)

    rows = auto_optimize._select_iterations_delta("run-abc")

    assert rows == [legacy_row], "Legacy SELECT must return the stored row"
    assert any("evaluated_count" in s for s in calls), "Should try V2 first"
    assert any("evaluated_count" not in s for s in calls), "Should retry legacy"
    # Cache should now remember the table is pre-migration, so the next call
    # skips the V2 probe (prevents N+1 probing on a stable-bad deploy).
    calls.clear()
    auto_optimize._select_iterations_delta("run-abc")
    assert calls and all("evaluated_count" not in s for s in calls), (
        "After detecting legacy schema, subsequent calls must skip the V2 probe"
    )


def test_select_iterations_delta_passes_through_non_schema_errors(monkeypatch) -> None:
    """Network/warehouse errors must NOT be treated as schema drift — returning
    [] silently on a transient warehouse outage would mask a real incident."""
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")

    from backend.routers import auto_optimize

    def fake_delta_query(sql: str, *, strict: bool = False):
        if strict:
            raise RuntimeError("warehouse unreachable: connection refused")
        return []

    monkeypatch.setattr(auto_optimize, "_delta_query", fake_delta_query)

    rows = auto_optimize._select_iterations_delta("run-abc")

    assert rows == []
    # Cache must stay unknown — a transient warehouse outage shouldn't pin
    # the process to the legacy SELECT path.
    assert auto_optimize._iterations_schema_legacy is None


def test_probe_iterations_schema_detects_legacy(monkeypatch, caplog) -> None:
    """Startup probe must log gso.runs.schema_drift_startup at ERROR level
    when the Delta table lacks the Bug #2 columns — this is the signal
    oncall relies on to notice that the GSO job bundle hasn't been
    redeployed on a given workspace."""
    import logging

    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")

    from backend.routers import auto_optimize

    def fake_delta_query(sql: str, *, strict: bool = False):
        if strict:
            raise _LegacySchemaError(
                "[UNRESOLVED_COLUMN] column `evaluated_count` not found"
            )
        return []

    monkeypatch.setattr(auto_optimize, "_delta_query", fake_delta_query)

    with caplog.at_level(logging.ERROR, logger="backend.routers.auto_optimize"):
        status = auto_optimize.probe_iterations_schema()

    assert status == "legacy"
    assert auto_optimize._iterations_schema_legacy is True
    drift = [r for r in caplog.records if "schema_drift_startup" in r.getMessage()]
    assert drift, "Expected gso.runs.schema_drift_startup ERROR log"


def test_probe_iterations_schema_ok_when_migrated(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")

    from backend.routers import auto_optimize

    monkeypatch.setattr(auto_optimize, "_delta_query", lambda sql, **kw: [])

    assert auto_optimize.probe_iterations_schema() == "ok"
    assert auto_optimize._iterations_schema_legacy is False


def test_probe_iterations_schema_unconfigured(monkeypatch) -> None:
    """When the app hasn't been wired to GSO (e.g. dev laptop), the probe is
    a no-op and must never raise."""
    monkeypatch.delenv("GSO_CATALOG", raising=False)
    monkeypatch.delenv("GSO_JOB_ID", raising=False)

    from backend.routers import auto_optimize

    assert auto_optimize.probe_iterations_schema() == "unconfigured"
