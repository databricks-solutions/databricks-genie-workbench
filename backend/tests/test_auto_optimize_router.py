"""Integration tests for the Auto-Optimize router.

These tests lock in the app-layer permission/start contract for GSO v2:
the Workbench backend pre-checks Genie Space CAN_MANAGE and UC read access,
but it does not probe or gate on MLflow Prompt Registry availability.

Style mirrors backend/tests/test_llm_utils.py: pure FastAPI TestClient + light
monkeypatching, no real Databricks connectivity required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import auto_optimize


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


# ── /permissions — advisory UI gate ─────────────────────────────────────


def test_permissions_contract_omits_prompt_registry_fields(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    """Prompt Registry availability is no longer part of the app API contract."""
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)

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
    ):
        resp = client.get("/api/auto-optimize/permissions/space-abc")

    assert resp.status_code == 200
    data = resp.json()
    assert data["can_start"] is True
    assert "prompt_registry_available" not in data
    assert "prompt_registry_error" not in data
    assert "prompt_registry_reason_code" not in data
    assert "prompt_registry_error_code" not in data
    assert "prompt_registry_actionable_by" not in data


def test_permissions_happy_path_allows_start(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    """With SP manage + all schemas granted, can_start must be True."""
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)

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
    ):
        resp = client.get("/api/auto-optimize/permissions/space-abc")

    assert resp.status_code == 200
    data = resp.json()
    assert data["can_start"] is True
    assert data["errors"] == []


def test_permissions_blocks_start_when_schema_read_is_missing(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    """can_start is computed from SP manage + UC read, not Prompt Registry."""
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)

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
        return_value=["main.sales.orders"],
    ), patch(
        "genie_space_optimizer.common.uc_metadata.get_unique_schemas",
        return_value={("main", "sales")},
    ), patch(
        "genie_space_optimizer.common.sp_permissions.probe_sp_required_access",
        return_value=(set(), set()),
    ):
        resp = client.get("/api/auto-optimize/permissions/space-abc")

    assert resp.status_code == 200
    data = resp.json()
    assert data["can_start"] is False
    assert data["sp_has_manage"] is True
    assert data["schemas"] == [
        {
            "catalog": "main",
            "schema_name": "sales",
            "read_granted": False,
            "grant_sql": (
                "GRANT USE CATALOG ON CATALOG `main` TO "
                "`11111111-2222-3333-4444-555555555555`;\n"
                "GRANT USE SCHEMA ON SCHEMA `main`.`sales` TO "
                "`11111111-2222-3333-4444-555555555555`;\n"
                "GRANT SELECT ON SCHEMA `main`.`sales` TO "
                "`11111111-2222-3333-4444-555555555555`;\n"
                "GRANT EXECUTE ON SCHEMA `main`.`sales` TO "
                "`11111111-2222-3333-4444-555555555555`;"
            ),
        }
    ]


def test_permissions_requires_configured_gso(client, monkeypatch) -> None:
    """Unconfigured GSO must return 503, not a crash or a false-positive can_start."""
    monkeypatch.delenv("GSO_CATALOG", raising=False)
    resp = client.get("/api/auto-optimize/permissions/space-abc")
    assert resp.status_code == 503


# ── /trigger ────────────────────────────────────────────────────────────


def test_trigger_proceeds_without_prompt_registry_gate(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    """Prompt Registry availability no longer blocks app-layer job launch."""
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)
    monkeypatch.setenv("LLM_MODEL", "custom-trigger-model")

    fake_result = MagicMock(
        run_id="run-xyz",
        job_run_id=9999,
        job_url="https://example.com/jobs/12345/runs/9999",
        status="QUEUED",
    )

    with patch.object(
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
        # GSO v2 loop knobs default to the job defaults when the request omits
        # them, and are echoed back so the UI can confirm what the run uses.
        "targetAccuracy": 0.90,
        "maxAttempts": 3,
    }
    trigger_mock.assert_called_once()
    config = trigger_mock.call_args.kwargs["config"]
    assert config.llm_model == "custom-trigger-model"
    # Omitted knobs flow to trigger_optimization as None (it resolves defaults).
    assert trigger_mock.call_args.kwargs["target_accuracy"] is None
    assert trigger_mock.call_args.kwargs["max_attempts"] is None
    assert "deploy_target" not in trigger_mock.call_args.kwargs


def test_trigger_uses_selected_llm_model(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    """Explicit llm_model overrides the env default after metadata validation."""
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)
    monkeypatch.setenv("LLM_MODEL", "env-default-model")
    monkeypatch.setattr(
        auto_optimize,
        "validate_chat_model",
        lambda model, client=None: model,
    )

    fake_result = MagicMock(
        run_id="run-selected",
        job_run_id=123,
        job_url=None,
        status="QUEUED",
    )

    with patch.object(
        auto_optimize, "trigger_optimization", return_value=fake_result
    ) as trigger_mock:
        resp = client.post(
            "/api/auto-optimize/trigger",
            json={
                "space_id": "space-abc",
                "apply_mode": "genie_config",
                "llm_model": "selected-chat",
            },
        )

    assert resp.status_code == 200, resp.text
    config = trigger_mock.call_args.kwargs["config"]
    assert config.llm_model == "selected-chat"


def test_trigger_rejects_invalid_llm_model(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)

    def reject(model, client=None):
        raise auto_optimize.ModelValidationError("not a chat model")

    monkeypatch.setattr(auto_optimize, "validate_chat_model", reject)

    with patch.object(auto_optimize, "trigger_optimization") as trigger_mock:
        resp = client.post(
            "/api/auto-optimize/trigger",
            json={"space_id": "space-abc", "llm_model": "bad-model"},
        )

    assert resp.status_code == 400
    assert resp.json()["detail"] == "not a chat model"
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


def test_trigger_forwards_workload_warehouse_ids(
    client, mock_sp_ws, mock_user_ws, monkeypatch,
) -> None:
    monkeypatch.setattr(
        auto_optimize, "get_service_principal_client", lambda: mock_sp_ws
    )
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)
    fake_result = MagicMock(
        run_id="run-workloads", job_run_id=1, job_url=None, status="QUEUED",
    )
    with patch.object(
        auto_optimize, "trigger_optimization", return_value=fake_result,
    ) as trigger_mock:
        response = client.post(
            "/api/auto-optimize/trigger",
            json={
                "space_id": "space-abc",
                "workload_warehouse_ids": ["wh-a", "wh-b"],
            },
        )

    assert response.status_code == 200
    assert trigger_mock.call_args.kwargs["workload_warehouse_ids"] == [
        "wh-a", "wh-b",
    ]


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


def test_derived_accuracy_ignores_non_numeric_evaluated_count() -> None:
    """PR #79 review #5 — non-numeric evaluated_count must not slip past the
    guard into the derived-denominator branch."""
    from backend.routers.auto_optimize import _derived_accuracy

    row = {
        "total_questions": 22,
        "correct_count": 10,
        "excluded_count": 3,
        "evaluated_count": "unknown",
        "overall_accuracy": 55.55,
    }
    assert _derived_accuracy(row, run_id="r1", iteration=0) == 55.55


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


def test_iteration_select_includes_rolled_back_for_score_selection() -> None:
    """Run score derivation can only exclude rejected iterations when the
    lightweight iteration query carries the rollback marker through."""
    from backend.routers.auto_optimize import _ITER_COLS_V2

    assert "rolled_back" in _ITER_COLS_V2


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
    excluded_count. The Workbench app must
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
        "reflection_json": None,
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


# ── GSO v2 Phase 6 — assessment-centric API contract ─────────────────────


def test_iter_cols_v2_includes_phase6_native_eval_columns() -> None:
    """The lightweight iteration SELECT must carry the native eval-run metadata
    so /iterations can surface num_needs_review + eval_run_id/status."""
    from backend.routers.auto_optimize import _ITER_COLS_V2

    for col in ("num_needs_review", "eval_run_id", "eval_run_status"):
        assert col in _ITER_COLS_V2


def test_parse_question_rows_uses_official_assessment() -> None:
    """Question-results derives state from the API `assessment` (GOOD/BAD/
    NEEDS_REVIEW) and returns assessment + assessment_reasons, not the retired
    judge_verdicts."""
    import json

    from backend.routers.auto_optimize import _parse_question_rows

    rows = [
        {
            "question_id": "q1",
            "inputs/question": "good one",
            "assessment": "GOOD",
            "assessment_reasons": [],
            "result_correctness/value": "yes",
            "outputs/response": "SELECT 1",
        },
        {
            "question_id": "q2",
            "inputs/question": "bad one",
            "assessment": "BAD",
            "assessment_reasons": ["LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE"],
            "result_correctness/value": "no",
        },
        {
            "question_id": "q3",
            "inputs/question": "review one",
            "assessment": "NEEDS_REVIEW",
            "assessment_reasons": ["LLM_JUDGE_UNCLEAR"],
        },
    ]
    out = _parse_question_rows(json.dumps(rows))
    by_id = {r["question_id"]: r for r in out}

    assert by_id["q1"]["passed"] is True
    assert by_id["q1"]["assessment"] == "GOOD"
    assert by_id["q2"]["passed"] is False
    assert by_id["q2"]["assessment"] == "BAD"
    assert by_id["q2"]["assessment_reasons"] == ["LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE"]
    # NEEDS_REVIEW is a third state — neither pass nor fail, not excluded.
    assert by_id["q3"]["passed"] is None
    assert by_id["q3"]["assessment"] == "NEEDS_REVIEW"
    assert by_id["q3"]["excluded"] is False
    # The retired per-judge verdict map must be gone from every row.
    for r in out:
        assert "judge_verdicts" not in r


def test_parse_question_rows_legacy_fallback_without_assessment() -> None:
    """Rows predating the official runner (no `assessment`) fall back to the
    result_correctness/arbiter logic and are normalised to an assessment."""
    import json

    from backend.routers.auto_optimize import _parse_question_rows

    rows = [
        {"question_id": "lq1", "inputs/question": "q", "result_correctness/value": "yes"},
        {"question_id": "lq2", "inputs/question": "q", "result_correctness/value": "no"},
    ]
    out = {r["question_id"]: r for r in _parse_question_rows(json.dumps(rows))}
    assert out["lq1"]["passed"] is True and out["lq1"]["assessment"] == "GOOD"
    assert out["lq2"]["passed"] is False and out["lq2"]["assessment"] == "BAD"


def test_parse_official_eval_results_lightweight_shape() -> None:
    """The official eval-results endpoint emits question_id + assessment +
    assessment_reasons (the failure_type successor), no per-judge rows."""
    import json

    from backend.routers.auto_optimize import _parse_official_eval_results

    rows = [
        {"question_id": "q1", "assessment": "GOOD", "assessment_reasons": []},
        {"question_id": "q2", "assessment": "BAD", "assessment_reasons": ["LLM_JUDGE_WRONG_FILTER"]},
    ]
    out = _parse_official_eval_results(json.dumps(rows))
    assert out == [
        {"question_id": "q1", "assessment": "GOOD", "assessment_reasons": []},
        {"question_id": "q2", "assessment": "BAD", "assessment_reasons": ["LLM_JUDGE_WRONG_FILTER"]},
    ]
    # No legacy per-judge keys leak through.
    for r in out:
        assert "judge" not in r and "failure_type" not in r and "value" not in r


def test_parse_official_eval_results_empty() -> None:
    from backend.routers.auto_optimize import _parse_official_eval_results

    assert _parse_official_eval_results(None) == []
    assert _parse_official_eval_results("") == []


def test_eval_results_route_kept_asi_alias_removed(client, monkeypatch) -> None:
    """The official eval-results route remains, but the retired ASI alias is gone."""
    import json

    async def fake_rows_json(_run_id, _iteration):
        return json.dumps([
            {"question_id": "q1", "assessment": "GOOD", "assessment_reasons": []},
        ])

    monkeypatch.setattr(auto_optimize, "_load_iteration_rows_json", fake_rows_json)
    run_id = "12345678-1234-1234-1234-1234567890ab"

    eval_resp = client.get(f"/api/auto-optimize/runs/{run_id}/eval-results?iteration=0")
    assert eval_resp.status_code == 200
    assert eval_resp.json() == [
        {"question_id": "q1", "assessment": "GOOD", "assessment_reasons": []},
    ]

    asi_resp = client.get(f"/api/auto-optimize/runs/{run_id}/asi-results?iteration=0")
    assert asi_resp.status_code == 404


def test_iterations_endpoint_emits_phase6_counts_and_gate(monkeypatch) -> None:
    """/iterations adds num_done / num_correct / num_needs_review and replaces
    thresholds_met with api_accuracy_gate_met + eval_gate_status."""
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")

    from backend.routers import auto_optimize

    delta_rows = [
        {
            "iteration": 0, "eval_scope": "full", "overall_accuracy": 80.0,
            "total_questions": 10, "correct_count": 8, "evaluated_count": 10,
            "excluded_count": 0, "thresholds_met": False, "rolled_back": False,
            "scores_json": "{}", "num_needs_review": 1, "lever": None,
            "eval_run_id": "er-1", "eval_run_status": "DONE",
        },
        {
            "iteration": 2, "eval_scope": "full", "overall_accuracy": 90.0,
            "total_questions": 10, "correct_count": 9, "evaluated_count": 10,
            "excluded_count": 0, "thresholds_met": True, "rolled_back": False,
            "scores_json": "{}", "num_needs_review": 0, "lever": 1,
            "eval_run_id": "er-2", "eval_run_status": "DONE",
        },
        {
            "iteration": 3, "eval_scope": "full", "overall_accuracy": 70.0,
            "total_questions": 10, "correct_count": 7, "evaluated_count": 10,
            "excluded_count": 0, "thresholds_met": False, "rolled_back": True,
            "scores_json": "{}", "num_needs_review": 0, "lever": 5,
            "eval_run_id": "er-3", "eval_run_status": "DONE",
        },
    ]

    async def fake_lakebase(_run_id):
        return []

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_iterations", fake_lakebase)
    monkeypatch.setattr(auto_optimize, "_select_iterations_delta", lambda _rid: [dict(r) for r in delta_rows])

    app = FastAPI()
    app.include_router(auto_optimize.router)
    client = TestClient(app)

    resp = client.get("/api/auto-optimize/runs/12345678-1234-1234-1234-1234567890ab/iterations")
    assert resp.status_code == 200
    out = {r["iteration"]: r for r in resp.json()}

    # Headline accuracy + official counts.
    assert out[0]["num_correct"] == 8
    assert out[0]["num_questions"] == 10
    assert out[0]["num_done"] == 10
    assert out[0]["num_needs_review"] == 1
    # thresholds_met / scores_json are dropped; replaced by the gate fields.
    assert "thresholds_met" not in out[0]
    assert "scores_json" not in out[0]
    assert out[0]["api_accuracy_gate_met"] is False
    assert out[0]["eval_gate_status"] == "failed"
    assert out[2]["api_accuracy_gate_met"] is True
    assert out[2]["eval_gate_status"] == "passed"
    assert out[3]["eval_gate_status"] == "rolled_back"
    # Native eval-run metadata passes through.
    assert out[0]["eval_run_status"] == "DONE"


def test_benchmark_changes_endpoint_groups_by_op(monkeypatch) -> None:
    """/benchmark-changes groups the mutations ledger into added/removed/
    changed/prune_recommended with provenance + counts."""
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")

    from backend.routers import auto_optimize

    ledger = [
        {"run_id": "12345678-1234-1234-1234-1234567890ab", "question_id": "q1", "op": "added", "before": None,
         "after": '{"question": "new q", "sql": "SELECT 1"}', "reason": "preflight_push", "logged_at": "2026-06-25T00:00:00Z"},
        {"run_id": "12345678-1234-1234-1234-1234567890ab", "question_id": "q2", "op": "removed", "before": '{"question": "old"}',
         "after": None, "reason": "explain_invalid", "logged_at": "2026-06-25T00:01:00Z"},
        {"run_id": "12345678-1234-1234-1234-1234567890ab", "question_id": "q3", "op": "changed", "before": '{"sql": "a"}',
         "after": '{"sql": "b"}', "reason": "normalized", "logged_at": "2026-06-25T00:02:00Z"},
    ]

    async def fake_lakebase(_run_id):
        return []

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_benchmark_mutations", fake_lakebase)
    monkeypatch.setattr(auto_optimize, "_delta_query", lambda *a, **k: [dict(r) for r in ledger])

    app = FastAPI()
    app.include_router(auto_optimize.router)
    client = TestClient(app)

    resp = client.get("/api/auto-optimize/runs/12345678-1234-1234-1234-1234567890ab/benchmark-changes")
    assert resp.status_code == 200
    body = resp.json()
    assert body["counts"] == {"added": 1, "removed": 1, "changed": 1, "pruneRecommended": 0, "total": 3}
    assert body["added"][0]["questionId"] == "q1"
    # before/after JSON strings are parsed into objects.
    assert body["added"][0]["after"] == {"question": "new q", "sql": "SELECT 1"}
    assert body["changed"][0]["before"] == {"sql": "a"}


def test_optimize_step_summary_is_attempt_centric_no_judges() -> None:
    """The 4-task Optimize summary must not reintroduce judge-centric wording."""
    from backend.routers.auto_optimize import _build_step_summary

    defn = {"name": "Optimize"}
    matching = [{"stage": "OPTIMIZE", "detail_json": "{}"}]
    summary = _build_step_summary(
        defn,
        matching,
        [],
        {"baseline_accuracy": 80.0, "best_accuracy": 85.0},
    )

    assert summary is not None
    assert "judge" not in summary.lower()
    assert "Score improved from 80.0% to 85.0%" in summary


def test_publish_step_io_omits_retired_uc_model_fields() -> None:
    """Publish output no longer carries the retired UC model deployment fields."""
    import json

    from backend.routers.auto_optimize import _build_step_io

    _inputs, outputs = _build_step_io(
        {"name": "Publish & Audit"},
        [{
            "stage": "PUBLISH_AND_AUDIT",
            "status": "COMPLETE",
            "detail_json": json.dumps({
                "uc_model_name": "main.gso.model",
                "uc_model_version": "7",
                "uc_champion_promoted": True,
            }),
        }],
        [],
        {
            "best_iteration": 2,
            "best_accuracy": 91.0,
            "convergence_reason": "TARGET_REACHED",
        },
    )

    assert outputs is not None
    assert "ucModelName" not in outputs
    assert "ucModelVersion" not in outputs
    assert "ucChampionPromoted" not in outputs


def test_deploy_step_branches_are_removed() -> None:
    """The retired Deploy step has no special summary or deploy output payload."""
    import json

    from backend.routers.auto_optimize import _build_step_io, _build_step_summary

    matching = [{
        "stage": "DEPLOY",
        "status": "COMPLETE",
        "detail_json": json.dumps({"status": "DEPLOYED"}),
    }]

    assert _build_step_summary({"name": "Deploy"}, matching, [], {}) is None
    inputs, outputs = _build_step_io({"name": "Deploy"}, matching, [], {})

    assert inputs is None
    assert outputs is not None
    assert "deployTarget" not in outputs
    assert "deployStatus" not in outputs


def test_lever_iterations_omit_mlflow_run_id() -> None:
    """Lever iteration output no longer leaks retired MLflow run pointers."""
    from backend.routers.auto_optimize import _build_lever_iterations

    rows = _build_lever_iterations(
        lever_num=1,
        lever_stages=[{"stage": "LEVER_1_EVAL", "status": "COMPLETE", "iteration": 2}],
        iterations_rows=[{
            "iteration": 2,
            "lever": 1,
            "eval_scope": "full",
            "overall_accuracy": 90.0,
            "mlflow_run_id": "mlflow-run-1",
        }],
        patches_rows=[],
        run_status="CONVERGED",
    )

    assert rows
    assert "mlflowRunId" not in rows[0]


def test_iterations_official_accuracy_uses_num_questions_not_evaluated_count(monkeypatch) -> None:
    """On an official run with legacy evaluated_count < total_questions, the headline
    accuracy must be num_correct / num_questions — NOT correct / evaluated_count
    (which would inflate). Legacy rows keep their stored overall_accuracy."""
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")

    from backend.routers import auto_optimize

    delta_rows = [
        # Official V2: 8 correct and 10 total questions. A stale evaluated_count
        # must not make the API/UI report an 8-question assessed subset.
        # Stored overall_accuracy is the stale 100.0; the endpoint must
        # recompute 8/10 = 80.0 from the official counts.
        {
            "iteration": 0, "eval_scope": "full", "overall_accuracy": 100.0,
            "total_questions": 10, "correct_count": 8, "evaluated_count": 8,
            "excluded_count": 0, "thresholds_met": False, "rolled_back": False,
            "scores_json": "{}", "num_needs_review": 1, "lever": None,
            "eval_run_id": "er-1", "eval_run_status": "DONE",
        },
        # Legacy row (no eval-run metadata): stored overall_accuracy untouched.
        {
            "iteration": 1, "eval_scope": "full", "overall_accuracy": 88.0,
            "total_questions": 10, "correct_count": 8, "evaluated_count": 9,
            "excluded_count": 1, "thresholds_met": True, "rolled_back": False,
            "scores_json": "{}", "lever": 1,
        },
    ]

    async def fake_lakebase(_run_id):
        return []

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_iterations", fake_lakebase)
    monkeypatch.setattr(auto_optimize, "_select_iterations_delta", lambda _rid: [dict(r) for r in delta_rows])

    app = FastAPI()
    app.include_router(auto_optimize.router)
    client = TestClient(app)

    resp = client.get("/api/auto-optimize/runs/12345678-1234-1234-1234-1234567890ab/iterations")
    assert resp.status_code == 200
    out = {r["iteration"]: r for r in resp.json()}

    # Official row: recomputed to num_correct / num_questions (8/10), NOT
    # correct / evaluated_count (8/8 = 100).
    assert out[0]["overall_accuracy"] == 80.0
    assert out[0]["num_correct"] == 8
    assert out[0]["num_questions"] == 10
    assert out[0]["num_done"] == 10
    # Legacy row: stored value preserved (no official recompute).
    assert out[1]["overall_accuracy"] == 88.0


def test_iterations_official_v2_reports_full_30_question_corpus(monkeypatch) -> None:
    """Regression guard for the removed split: a 30-question official row with
    stale evaluated_count=25 must report 30 assessed questions."""
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")

    from backend.routers import auto_optimize

    delta_rows = [
        {
            "iteration": 0, "eval_scope": "full", "overall_accuracy": 96.0,
            "total_questions": 30, "correct_count": 24, "evaluated_count": 25,
            "excluded_count": 0, "thresholds_met": True, "rolled_back": False,
            "scores_json": "{}", "num_needs_review": 0, "lever": None,
            "eval_run_id": "er-30", "eval_run_status": "DONE",
        },
    ]

    async def fake_lakebase(_run_id):
        return []

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_iterations", fake_lakebase)
    monkeypatch.setattr(auto_optimize, "_select_iterations_delta", lambda _rid: [dict(r) for r in delta_rows])

    app = FastAPI()
    app.include_router(auto_optimize.router)
    client = TestClient(app)

    resp = client.get("/api/auto-optimize/runs/12345678-1234-1234-1234-1234567890ab/iterations")
    assert resp.status_code == 200
    row = resp.json()[0]

    assert row["num_questions"] == 30
    assert row["num_done"] == 30
    assert row["overall_accuracy"] == 80.0


# ── Phase 10 — app-backend loop contract ────────────────────────────────


def _gso_client(monkeypatch) -> TestClient:
    """A configured TestClient mounting just the auto_optimize router."""
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app)


_RUN = "12345678-1234-1234-1234-1234567890ab"


# Item 1 — 4-task DAG re-map.


def test_step_definitions_are_the_four_task_dag() -> None:
    names = [s["name"] for s in auto_optimize._STEP_DEFINITIONS]
    assert names == [
        "Intake & Snapshot",
        "Benchmark QC & Repair",
        "Optimize",
        "Publish & Audit",
    ]
    assert auto_optimize._TOTAL_STEPS == 4
    # The standalone Deploy step is gone (D7).
    assert "Deploy" not in names


def test_map_stages_to_steps_new_dag_stage_names() -> None:
    """The new orchestration stage names roll up into the 4 logical steps."""
    stages = [
        {"stage": "INTAKE_AND_SNAPSHOT", "status": "COMPLETE"},
        {"stage": "BENCHMARK_QC_AND_REPAIR", "status": "COMPLETE"},
        {"stage": "SPACE_QUALITY_ENRICHMENT", "status": "COMPLETE"},
        {"stage": "OPTIMIZE", "status": "COMPLETE"},
        {"stage": "PUBLISH_AND_AUDIT", "status": "STARTED"},
    ]
    steps = auto_optimize._map_stages_to_steps(stages, {"status": "IN_PROGRESS"}, [])
    by_num = {s["stepNumber"]: s for s in steps}
    assert len(steps) == 4
    assert by_num[1]["name"] == "Intake & Snapshot" and by_num[1]["status"] == "completed"
    assert by_num[2]["status"] == "completed"
    assert by_num[3]["name"] == "Optimize" and by_num[3]["status"] == "completed"
    assert by_num[4]["name"] == "Publish & Audit" and by_num[4]["status"] == "running"


def test_map_stages_to_steps_legacy_back_compat() -> None:
    """Legacy 6-notebook stage names still render against the 4-task rail."""
    stages = [
        {"stage": "PREFLIGHT_DONE", "status": "COMPLETE"},
        {"stage": "BASELINE_EVAL_DONE", "status": "COMPLETE"},
        {"stage": "LEVER_2_EVAL_DONE", "status": "COMPLETE"},
        {"stage": "FINALIZE_DONE", "status": "COMPLETE"},
    ]
    steps = auto_optimize._map_stages_to_steps(stages, {"status": "CONVERGED"}, [])
    by_num = {s["stepNumber"]: s for s in steps}
    # Legacy preflight satisfies both intake (1) and QC (2).
    assert by_num[1]["status"] == "completed"
    assert by_num[2]["status"] == "completed"
    assert by_num[3]["status"] == "completed"  # baseline/lever loop → Optimize
    assert by_num[4]["status"] == "completed"  # finalize → Publish & Audit


def test_downstream_stage_overrides_stale_started_rows() -> None:
    """An open legacy STARTED row must not pin the rail to an earlier task."""
    stages = [
        {"stage": "INTAKE_AND_SNAPSHOT", "status": "STARTED"},
        {"stage": "INTAKE_AND_SNAPSHOT", "status": "COMPLETE"},
        {"stage": "PREFLIGHT_STARTED", "status": "STARTED"},
        {"stage": "BENCHMARK_QC_AND_REPAIR", "status": "COMPLETE"},
        {"stage": "OPTIMIZE", "status": "STARTED"},
    ]
    steps = auto_optimize._map_stages_to_steps(stages, {"status": "IN_PROGRESS"}, [])
    by_num = {s["stepNumber"]: s for s in steps}
    assert by_num[1]["status"] == "completed"
    assert by_num[2]["status"] == "completed"
    assert by_num[3]["status"] == "running"


# Item 3 — typed terminal_reason.


def test_typed_terminal_reason_validates_closed_set() -> None:
    ttr = auto_optimize._typed_terminal_reason
    assert ttr({"convergence_reason": "TARGET_REACHED"}) == "TARGET_REACHED"
    assert ttr({"convergence_reason": "MAX_ATTEMPTS"}) == "MAX_ATTEMPTS"
    assert ttr({"convergence_reason": "EVAL_BUDGET_EXHAUSTED"}) == "EVAL_BUDGET_EXHAUSTED"
    # Legacy free-text / job-error reasons are NOT typed.
    assert ttr({"convergence_reason": "threshold_met"}) is None
    assert ttr({"convergence_reason": "job_submission_error: boom"}) is None
    # In-progress runs / missing reason.
    assert ttr({"convergence_reason": None}) is None
    assert ttr({}) is None
    assert ttr(None) is None


def test_status_endpoint_is_four_steps_and_typed_terminal_reason(monkeypatch) -> None:
    async def fake_run(_rid):
        return {
            "run_id": _RUN, "space_id": "space-1", "status": "CONVERGED",
            "convergence_reason": "TARGET_REACHED",
        }

    async def empty(_rid):
        return []

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_run", fake_run)
    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_stages", empty)
    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_iterations", empty)
    monkeypatch.setattr(auto_optimize, "_delta_query", lambda *a, **k: [])
    monkeypatch.setattr(auto_optimize, "_resolve_run_knobs", lambda _run: (0.9, 3))

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/status")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["totalSteps"] == 4
    assert body["terminalReason"] == "TARGET_REACHED"
    assert body["targetAccuracy"] == 0.9
    assert body["maxAttempts"] == 3
    # convergenceReason is preserved for back-compat.
    assert body["convergenceReason"] == "TARGET_REACHED"


def test_status_keeps_earliest_iteration_zero_as_baseline(monkeypatch) -> None:
    async def fake_run(_rid):
        return {
            "run_id": _RUN, "space_id": "space-1", "status": "CONVERGED",
            "convergence_reason": "TARGET_REACHED",
        }

    async def empty(_rid):
        return []

    async def duplicate_iteration_zero(_rid):
        return [
            {
                "iteration": 0, "eval_scope": "full",
                "timestamp": "2026-07-23T11:12:00+00:00",
                "overall_accuracy": 88.24, "correct_count": 15,
                "evaluated_count": 17, "rolled_back": False,
            },
            {
                "iteration": 0, "eval_scope": "full",
                "timestamp": "2026-07-23T11:22:00+00:00",
                "overall_accuracy": 94.12, "correct_count": 16,
                "evaluated_count": 17, "rolled_back": False,
            },
        ]

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_run", fake_run)
    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_stages", empty)
    monkeypatch.setattr(
        auto_optimize.gso_lakebase,
        "load_gso_iterations",
        duplicate_iteration_zero,
    )
    monkeypatch.setattr(auto_optimize, "_delta_query", lambda *a, **k: [])
    monkeypatch.setattr(auto_optimize, "_resolve_run_knobs", lambda _run: (0.9, 3))

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/status")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["baselineScore"] == 88.24
    assert body["optimizedScore"] == 94.12
    assert body["bestIteration"] == 0


def test_status_endpoint_uses_latest_downstream_task_for_current_step(monkeypatch) -> None:
    async def fake_run(_rid):
        return {"run_id": _RUN, "space_id": "space-1", "status": "IN_PROGRESS"}

    async def fake_stages(_rid):
        return [
            {"stage": "INTAKE_AND_SNAPSHOT", "status": "STARTED"},
            {"stage": "INTAKE_AND_SNAPSHOT", "status": "COMPLETE"},
            {"stage": "PREFLIGHT_STARTED", "status": "STARTED"},
            {"stage": "BENCHMARK_QC_AND_REPAIR", "status": "COMPLETE"},
            {"stage": "OPTIMIZE", "status": "STARTED"},
        ]

    async def empty(_rid):
        return []

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_run", fake_run)
    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_stages", fake_stages)
    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_iterations", empty)
    monkeypatch.setattr(auto_optimize, "_delta_query", lambda *a, **k: [])
    monkeypatch.setattr(auto_optimize, "_resolve_run_knobs", lambda _run: (0.9, 3))

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/status")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["stepsCompleted"] == 2
    assert body["currentStepName"] == "Optimize"


# ── Jobs-API-driven rail progress (fixes "stuck at 0/4 · Intake") ─────────


def _fake_run_task(task_key: str, life_cycle: str) -> MagicMock:
    """A stand-in for databricks.sdk jobs.RunTask with a RunState."""
    t = MagicMock()
    t.task_key = task_key
    t.state = MagicMock()
    # Mirror the SDK's stringification `RunLifeCycleState.RUNNING`.
    t.state.life_cycle_state = f"RunLifeCycleState.{life_cycle}"
    t.state.result_state = None
    return t


def _fake_job_run(task_states: dict[str, str]) -> MagicMock:
    job_run = MagicMock()
    job_run.tasks = [_fake_run_task(k, v) for k, v in task_states.items()]
    return job_run


def test_job_task_progress_maps_running_optimize_to_step_three(monkeypatch) -> None:
    """The core bug: intake + QC TERMINATED, optimize RUNNING → the rail must
    read 2 completed + current "Optimize", NOT 0/4 · Intake."""
    sp_ws = MagicMock()
    sp_ws.jobs.get_run.return_value = _fake_job_run({
        "intake_and_snapshot": "TERMINATED",
        "benchmark_qc_and_repair": "TERMINATED",
        "optimize": "RUNNING",
        "publish_and_audit": "PENDING",
    })
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: sp_ws)

    progress = auto_optimize._job_task_progress(
        {"run_id": _RUN, "status": "IN_PROGRESS", "job_run_id": "999"}
    )
    assert progress == (2, "Optimize")
    sp_ws.jobs.get_run.assert_called_once_with(run_id=999)


def test_job_task_progress_all_terminated_has_no_current(monkeypatch) -> None:
    sp_ws = MagicMock()
    sp_ws.jobs.get_run.return_value = _fake_job_run({
        "intake_and_snapshot": "TERMINATED",
        "benchmark_qc_and_repair": "TERMINATED",
        "optimize": "TERMINATED",
        "publish_and_audit": "TERMINATED",
    })
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: sp_ws)

    progress = auto_optimize._job_task_progress(
        {"run_id": _RUN, "status": "CONVERGED", "job_run_id": "999"}
    )
    assert progress == (4, None)


def test_job_task_progress_none_without_job_run_id(monkeypatch) -> None:
    """No job_run_id (legacy / pre-submit) → None so the caller falls back to
    the Delta-stage derivation. The SP client must never be touched."""
    sp_ws = MagicMock()
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: sp_ws)
    progress = auto_optimize._job_task_progress(
        {"run_id": _RUN, "status": "IN_PROGRESS"}
    )
    assert progress is None
    sp_ws.jobs.get_run.assert_not_called()


def test_job_task_progress_none_on_api_error(monkeypatch) -> None:
    """A Jobs API failure yields None (fall back to Delta), never an exception."""
    sp_ws = MagicMock()
    sp_ws.jobs.get_run.side_effect = RuntimeError("boom")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: sp_ws)
    progress = auto_optimize._job_task_progress(
        {"run_id": _RUN, "status": "IN_PROGRESS", "job_run_id": "999"}
    )
    assert progress is None


def test_status_endpoint_prefers_jobs_api_over_empty_stages(monkeypatch) -> None:
    """End-to-end reproduction: Lakebase disabled, the Delta stage table reads
    empty (the serverless-write → warehouse-read gap), yet the job is on task 3.
    The status endpoint must report the real progress from the Jobs API instead
    of the misleading 0/4 · Intake the empty stage list would produce."""
    async def fake_run(_rid):
        return {
            "run_id": _RUN, "space_id": "space-1", "status": "IN_PROGRESS",
            "job_run_id": "999",
        }

    async def empty(_rid):
        return []

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_run", fake_run)
    # Stages read empty on BOTH paths — this is exactly the frozen-rail case.
    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_stages", empty)
    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_iterations", empty)
    monkeypatch.setattr(auto_optimize, "_delta_query", lambda *a, **k: [])
    monkeypatch.setattr(auto_optimize, "_resolve_run_knobs", lambda _run: (0.9, 3))

    sp_ws = MagicMock()
    sp_ws.jobs.get_run.return_value = _fake_job_run({
        "intake_and_snapshot": "TERMINATED",
        "benchmark_qc_and_repair": "TERMINATED",
        "optimize": "RUNNING",
        "publish_and_audit": "PENDING",
    })
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: sp_ws)

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/status")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["stepsCompleted"] == 2
    assert body["currentStepName"] == "Optimize"
    assert body["totalSteps"] == 4


def test_status_endpoint_falls_back_to_stages_when_no_job_run_id(monkeypatch) -> None:
    """Without a job_run_id, the endpoint keeps its Delta-stage derivation."""
    async def fake_run(_rid):
        return {"run_id": _RUN, "space_id": "space-1", "status": "IN_PROGRESS"}

    async def fake_stages(_rid):
        return [
            {"stage": "INTAKE_AND_SNAPSHOT", "status": "COMPLETE"},
            {"stage": "BENCHMARK_QC_AND_REPAIR", "status": "STARTED"},
        ]

    async def empty(_rid):
        return []

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_run", fake_run)
    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_stages", fake_stages)
    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_iterations", empty)
    monkeypatch.setattr(auto_optimize, "_delta_query", lambda *a, **k: [])
    monkeypatch.setattr(auto_optimize, "_resolve_run_knobs", lambda _run: (0.9, 3))

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/status")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["stepsCompleted"] == 1
    assert body["currentStepName"] == "Benchmark QC & Repair"


def test_run_summaries_enriched_with_typed_terminal_reason() -> None:
    runs = [
        {"run_id": "r1", "convergence_reason": "MAX_ATTEMPTS"},
        {"run_id": "r2", "convergence_reason": "plateau"},  # legacy free-text
    ]
    out = auto_optimize._enrich_run_summaries(runs)
    assert out[0]["terminal_reason"] == "MAX_ATTEMPTS"
    assert out[1]["terminal_reason"] is None
    # Raw convergence_reason untouched.
    assert out[1]["convergence_reason"] == "plateau"


def test_run_summaries_enrich_has_config_snapshot_coerced() -> None:
    """``has_config_snapshot`` is coerced to a real bool so the frontend can
    hide the Revert button for runs with no captured config. The Delta/SQL-
    warehouse fallback returns columns as strings (JSON_ARRAY format), so a
    literal ``"false"`` must NOT be truthy.

    Also coerces ``best_iteration`` to int — the frontend uses ``> 0`` to
    decide whether to show the "Revert to Champion" button, and a string
    ``"2"`` would never satisfy that check."""
    runs = [
        {"run_id": "r1", "has_config_snapshot": True, "best_iteration": 2},      # Postgres path
        {"run_id": "r2", "has_config_snapshot": "true", "best_iteration": "2"},  # Delta path (strings)
        {"run_id": "r3", "has_config_snapshot": False, "best_iteration": 0},
        {"run_id": "r4", "has_config_snapshot": "false", "best_iteration": "0"}, # must stay falsy
        {"run_id": "r5"},                                                     # absent → False/None
    ]
    out = auto_optimize._enrich_run_summaries(runs)
    assert out[0]["has_config_snapshot"] is True
    assert out[1]["has_config_snapshot"] is True
    assert out[2]["has_config_snapshot"] is False
    assert out[3]["has_config_snapshot"] is False
    assert out[4]["has_config_snapshot"] is False
    # best_iteration coercion (drives the champion-button visibility).
    assert out[0]["best_iteration"] == 2
    assert out[1]["best_iteration"] == 2
    assert out[2]["best_iteration"] == 0
    assert out[3]["best_iteration"] == 0
    assert out[4]["best_iteration"] is None


# Item 4 — round-trip target_accuracy + max_attempts.


def test_trigger_round_trips_loop_knobs(client, mock_sp_ws, mock_user_ws, monkeypatch) -> None:
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: mock_sp_ws)
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)
    monkeypatch.setattr(auto_optimize, "validate_chat_model", lambda m, client=None: m)

    fake_result = MagicMock(run_id="run-1", job_run_id=1, job_url=None, status="QUEUED")
    with patch.object(auto_optimize, "trigger_optimization", return_value=fake_result) as tmock:
        resp = client.post(
            "/api/auto-optimize/trigger",
            json={"space_id": "space-abc", "target_accuracy": 0.85, "max_attempts": 5},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Echoed back exactly as requested.
    assert body["targetAccuracy"] == 0.85
    assert body["maxAttempts"] == 5
    # Threaded into trigger_optimization.
    assert tmock.call_args.kwargs["target_accuracy"] == 0.85
    assert tmock.call_args.kwargs["max_attempts"] == 5


def test_trigger_rejects_out_of_range_target_accuracy(client) -> None:
    """target_accuracy is bounded to [0, 1] by the Pydantic validator."""
    resp = client.post(
        "/api/auto-optimize/trigger",
        json={"space_id": "space-abc", "target_accuracy": 90},  # 0-100 mistake
    )
    assert resp.status_code == 422


# Item 5 — explicit is_champion / config_json on /iterations.


def test_iterations_merge_is_champion_and_config_json(monkeypatch) -> None:
    delta_rows = [
        {"iteration": 0, "eval_scope": "full", "overall_accuracy": 80.0,
         "total_questions": 10, "correct_count": 8, "thresholds_met": "false",
         "rolled_back": "false", "scores_json": "{}", "lever": None},
        {"iteration": 2, "eval_scope": "full", "overall_accuracy": 90.0,
         "total_questions": 10, "correct_count": 9, "thresholds_met": "true",
         "rolled_back": "false", "scores_json": "{}", "lever": 1},
    ]
    loop_rows = [
        {"iteration": 0, "eval_scope": "full", "is_champion": False,
         "config_json": '{"v": 0}', "attempt_no": None, "attempt_mode": None,
         "decision": None, "decision_reason": None},
        {"iteration": 2, "eval_scope": "full", "is_champion": True,
         "config_json": '{"v": 2}', "attempt_no": 2, "attempt_mode": "surgical",
         "decision": "accept", "decision_reason": "improved"},
    ]

    async def empty(_rid):
        return []

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_iterations", empty)
    monkeypatch.setattr(auto_optimize, "_select_iterations_delta", lambda _rid: [dict(r) for r in delta_rows])
    monkeypatch.setattr(auto_optimize, "_select_loop_state_delta", lambda _rid: [dict(r) for r in loop_rows])

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/iterations")
    assert resp.status_code == 200, resp.text
    out = {r["iteration"]: r for r in resp.json()}
    assert out[2]["is_champion"] is True
    assert out[2]["config_json"] == '{"v": 2}'
    assert out[2]["attempt_no"] == 2
    assert out[2]["attempt_mode"] == "surgical"
    assert out[2]["decision"] == "accept"
    assert out[2]["api_accuracy_gate_met"] is True
    assert out[2]["eval_gate_status"] == "passed"
    assert out[0]["is_champion"] is False
    assert out[0]["api_accuracy_gate_met"] is False
    assert out[0]["eval_gate_status"] == "failed"


def test_iterations_no_loop_state_columns_degrade_gracefully(monkeypatch) -> None:
    """A legacy table without loop-state columns ⇒ rows simply omit the new
    fields (the UI keeps its idxmax fallback)."""
    delta_rows = [
        {"iteration": 0, "eval_scope": "full", "overall_accuracy": 80.0,
         "total_questions": 10, "correct_count": 8, "thresholds_met": True,
         "rolled_back": False, "scores_json": "{}", "lever": None},
    ]

    async def empty(_rid):
        return []

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_iterations", empty)
    monkeypatch.setattr(auto_optimize, "_select_iterations_delta", lambda _rid: [dict(r) for r in delta_rows])
    monkeypatch.setattr(auto_optimize, "_select_loop_state_delta", lambda _rid: [])

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/iterations")
    assert resp.status_code == 200, resp.text
    assert "is_champion" not in resp.json()[0]


# Item 2 — loop-state / attempts read path.


def test_loop_state_endpoint_builds_attempts_and_aggregate(monkeypatch) -> None:
    loop_rows = [
        # Baseline iter 0 — not an attempt (no attempt_no).
        {"iteration": 0, "eval_scope": "full", "overall_accuracy": 70.0,
         "attempt_no": None, "is_champion": False},
        # Baseline written as an explicit attempt_no=0 row — still not an
        # attempt; must be excluded so the ledger/ladder don't double-render it.
        {"iteration": 0, "eval_scope": "full", "overall_accuracy": 70.0,
         "attempt_no": 0, "attempt_mode": "baseline", "best_accuracy": 70.0,
         "is_champion": False},
        # Coverage attempt 1 (rolled back).
        {"iteration": 1, "eval_scope": "enrichment", "overall_accuracy": 68.0,
         "attempt_no": 1, "attempt_mode": "coverage", "best_accuracy": 70.0,
         "decision": "reject", "decision_reason": "rolled_back (Δacc<0)",
         "rolled_back": True, "rollback_reason": "Δacc<0", "is_champion": False,
         "current_hypothesis": '{"lever": 0}', "target_accuracy": 90.0,
         "max_attempts": 3, "surgical_attempts_used": 0},
        # Surgical attempt 2 (accepted, champion, terminal).
        {"iteration": 2, "eval_scope": "full", "overall_accuracy": 92.0,
         "attempt_no": 2, "attempt_mode": "surgical", "best_accuracy": 92.0,
         "decision": "accept", "decision_reason": "improved", "rolled_back": False,
         "is_champion": True, "best_config_version_id": "cfgsha:abc",
         "terminal_reason": "TARGET_REACHED", "target_accuracy": 90.0,
         "max_attempts": 3, "surgical_attempts_used": 1,
         "do_not_repeat": '["lever5_example"]', "next_hypothesis": "null"},
    ]
    monkeypatch.setattr(auto_optimize, "_select_loop_state_delta", lambda _rid: [dict(r) for r in loop_rows])

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/loop-state")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # Only the two attempts (coverage + surgical); baseline excluded in BOTH
    # forms (attempt_no is None AND the explicit attempt_no=0 baseline row).
    assert [a["attemptNo"] for a in body["attempts"]] == [1, 2]
    coverage = body["attempts"][0]
    assert coverage["attemptMode"] == "coverage"
    assert coverage["rolledBack"] is True
    assert coverage["decisionReason"] == "rolled_back (Δacc<0)"
    assert coverage["currentHypothesis"] == {"lever": 0}

    surgical = body["attempts"][1]
    assert surgical["isChampion"] is True
    assert surgical["terminalReason"] == "TARGET_REACHED"
    # B2 — per-attempt ledger fields surfaced (not only the run-level aggregate).
    assert surgical["bestConfigVersionId"] == "cfgsha:abc"
    assert surgical["doNotRepeat"] == ["lever5_example"]
    assert surgical["nextHypothesis"] is None  # "null" JSON → None

    ls = body["loopState"]
    assert ls["bestAccuracy"] == 92.0
    assert ls["bestConfigVersionId"] == "cfgsha:abc"
    # target_accuracy normalized from the 0-100 column to the 0-1 request scale.
    assert ls["targetAccuracy"] == 0.9
    assert ls["maxAttempts"] == 3
    assert ls["surgicalAttemptsUsed"] == 1
    assert ls["terminalReason"] == "TARGET_REACHED"
    assert ls["doNotRepeat"] == ["lever5_example"]
    assert ls["attemptCount"] == 2


def test_loop_state_endpoint_does_not_treat_false_string_as_rolled_back(monkeypatch) -> None:
    loop_rows = [
        {"iteration": 1, "eval_scope": "full", "overall_accuracy": 67.0,
         "attempt_no": 1, "attempt_mode": "llm_patch", "best_accuracy": 67.0,
         "decision": "accept", "rolled_back": "false", "is_champion": "true"},
        {"iteration": 2, "eval_scope": "full", "overall_accuracy": 60.0,
         "attempt_no": 2, "attempt_mode": "llm_patch", "best_accuracy": 67.0,
         "decision": "reject", "rolled_back": "true", "is_champion": "false"},
    ]
    monkeypatch.setattr(auto_optimize, "_select_loop_state_delta", lambda _rid: [dict(r) for r in loop_rows])

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/loop-state")
    assert resp.status_code == 200, resp.text
    attempts = resp.json()["attempts"]
    assert attempts[0]["rolledBack"] is False
    assert attempts[0]["isChampion"] is True
    assert attempts[1]["rolledBack"] is True
    assert attempts[1]["isChampion"] is False


def test_loop_state_recovers_later_iteration_zero_as_enrichment_attempt(monkeypatch) -> None:
    loop_rows = [
        {"iteration": 0, "eval_scope": "full", "overall_accuracy": 88.24,
         "timestamp": "2026-07-23T11:12:00+00:00", "attempt_no": 0,
         "attempt_mode": "baseline", "best_accuracy": 88.24,
         "decision": "accept", "decision_reason": "baseline",
         "is_champion": True, "target_accuracy": 90.0, "max_attempts": 3},
        {"iteration": 0, "eval_scope": "full", "overall_accuracy": 94.12,
         "timestamp": "2026-07-23T11:22:00+00:00", "attempt_no": 0,
         "attempt_mode": "baseline", "best_accuracy": 94.12,
         "decision": "accept", "decision_reason": "baseline",
         "is_champion": True, "terminal_reason": "TARGET_REACHED",
         "target_accuracy": 90.0, "max_attempts": 3},
    ]
    monkeypatch.setattr(
        auto_optimize,
        "_select_loop_state_delta",
        lambda _rid: [dict(r) for r in loop_rows],
    )

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/loop-state")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert len(body["attempts"]) == 1
    recovered = body["attempts"][0]
    assert recovered["attemptNo"] == 1
    assert recovered["attemptMode"] == "enrichment"
    assert recovered["accuracy"] == 94.12
    assert recovered["decision"] == "accept"
    assert recovered["isChampion"] is True
    assert body["loopState"]["bestAccuracy"] == 94.12
    assert body["loopState"]["attemptCount"] == 1


def test_loop_state_endpoint_empty_for_legacy_run(monkeypatch) -> None:
    monkeypatch.setattr(auto_optimize, "_select_loop_state_delta", lambda _rid: [])
    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/loop-state")
    assert resp.status_code == 200
    body = resp.json()
    assert body["loopState"] is None
    assert body["attempts"] == []


# Item 6 — publish-record read path.


def test_publish_record_endpoint_maps_artifact(monkeypatch) -> None:
    payload = {
        "run_id": _RUN, "space_id": "space-1", "final_status": "CONVERGED",
        "terminal_reason": "TARGET_REACHED", "published": "true",
        "publish_outcome": "published", "champion_iteration": 2,
        "champion_accuracy": 92.0, "champion_config_version_id": "cfgsha:abc",
        "target_accuracy": 0.90, "max_attempts": 3,
        "audit_summary": "Improved from 70 to 92.",
        "improvement_trajectory": [
            {"iteration": 0, "attempt_no": None, "attempt_mode": "baseline",
             "eval_scope": "full", "accuracy": 70.0, "delta_vs_baseline": 0.0,
             "best_accuracy": 70.0, "decision": None, "rolled_back": "false",
             "is_champion": "false"},
            {"iteration": 2, "attempt_no": 2, "attempt_mode": "surgical",
             "eval_scope": "full", "accuracy": 92.0, "delta_vs_baseline": 22.0,
             "best_accuracy": 92.0, "decision": "accept", "rolled_back": "false",
             "is_champion": "true"},
        ],
        "concerns": ["Two questions still need review."],
    }
    monkeypatch.setattr(auto_optimize, "_load_latest_artifact",
                        lambda _rid, kind: payload if kind == "publish_record" else None)

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/publish")
    assert resp.status_code == 200, resp.text
    pr = resp.json()["publishRecord"]
    assert pr["published"] is True
    assert pr["terminalReason"] == "TARGET_REACHED"
    assert pr["championIteration"] == 2
    assert pr["championAccuracy"] == 92.0
    assert pr["championConfigVersionId"] == "cfgsha:abc"
    assert pr["auditSummary"].startswith("Improved")
    assert pr["concerns"] == ["Two questions still need review."]
    assert len(pr["improvementTrajectory"]) == 2
    assert pr["improvementTrajectory"][0]["rolledBack"] is False
    assert pr["improvementTrajectory"][0]["isChampion"] is False
    assert pr["improvementTrajectory"][1]["attemptMode"] == "surgical"
    assert pr["improvementTrajectory"][1]["deltaVsBaseline"] == 22.0
    assert pr["improvementTrajectory"][1]["rolledBack"] is False
    assert pr["improvementTrajectory"][1]["isChampion"] is True


def test_publish_record_null_when_absent(monkeypatch) -> None:
    monkeypatch.setattr(auto_optimize, "_load_latest_artifact", lambda _rid, kind: None)
    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/publish")
    assert resp.status_code == 200
    assert resp.json()["publishRecord"] is None


# Item 7 — benchmark-QC metadata alongside benchmark-changes.


def test_benchmark_changes_includes_qc(monkeypatch) -> None:
    qc_payload = {
        "run_id": _RUN, "valid_count": 32, "persisted_count": 32,
        "repair_tries_used": 1, "benchmark_repair_max_tries": 3,
        "repaired_ids": ["q5"], "repair_sweeps": 1, "final_validity": True,
        "window": {"status": "in_window", "count": 32},
        "window_target_min": 30, "window_target_max": 40,
        "gt_correction_candidates": [],
        "quality_review_version": "benchmark_quality_v1",
        "quality_review_status": "complete",
        "semantic_review_coverage": 1.0,
        "quality_counts": {
            "total": 32, "trusted": 30, "warnings": 2,
            "excluded": 1, "review_not_run": 0,
        },
        "quality_findings": [{
            "question_id": "q9", "question": "Show sales", "source": "genie_space",
            "category": "question_quality", "code": "WEAK_BUT_ANSWERABLE",
            "severity": "warning", "confidence": 0.8,
            "explanation": "Could be more specific.",
        }],
        "proposed_changes": [],
    }

    async def empty(_rid):
        return []

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_benchmark_mutations", empty)
    monkeypatch.setattr(auto_optimize, "_delta_query", lambda *a, **k: [])
    monkeypatch.setattr(auto_optimize, "_load_latest_artifact",
                        lambda _rid, kind: qc_payload if kind == "benchmark_qc" else None)

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/benchmark-changes")
    assert resp.status_code == 200, resp.text
    qc = resp.json()["qc"]
    assert qc is not None
    assert qc["validCount"] == 32
    assert qc["repairTriesUsed"] == 1
    assert qc["repairMaxTries"] == 3
    assert qc["finalValidity"] is True
    assert qc["window"] == {"status": "in_window", "count": 32}
    assert qc["windowTargetMin"] == 30
    assert qc["qualityReviewVersion"] == "benchmark_quality_v1"
    assert qc["qualityReviewStatus"] == "complete"
    assert qc["semanticReviewCoverage"] == 1.0
    assert qc["qualityCounts"]["trusted"] == 30
    assert qc["qualityFindings"][0]["code"] == "WEAK_BUT_ANSWERABLE"


def test_benchmark_changes_qc_null_for_legacy_run(monkeypatch) -> None:
    async def empty(_rid):
        return []

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_benchmark_mutations", empty)
    monkeypatch.setattr(auto_optimize, "_delta_query", lambda *a, **k: [])
    monkeypatch.setattr(auto_optimize, "_load_latest_artifact", lambda _rid, kind: None)

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/benchmark-changes")
    assert resp.status_code == 200
    assert resp.json()["qc"] is None


# ── Phase 10 cross-review fixes (B1–B4) ─────────────────────────────────


# B1 — attempts collapse to ONE full-benchmark row per attempt_no.


def test_loop_state_dedups_to_one_full_row_per_attempt(monkeypatch) -> None:
    """Duplicate rows and non-full-benchmark probe rows for the same attempt
    must collapse to a single attempt carrying the latest full-benchmark
    accuracy — never render as phantom/duplicate attempts."""
    loop_rows = [
        # baseline iter 0 — not an attempt.
        {"iteration": 0, "eval_scope": "full", "attempt_no": None, "overall_accuracy": 70.0},
        # coverage attempt 1 at eval_scope='enrichment' (full-benchmark scored).
        {"iteration": 1, "eval_scope": "enrichment", "attempt_no": 1,
         "overall_accuracy": 68.0, "attempt_mode": "coverage"},
        # surgical attempt 2: a stale slice probe row (non-full) + the real full
        # row + an earlier duplicate full row — must collapse to the latest full.
        {"iteration": 2, "eval_scope": "slice", "attempt_no": 2, "overall_accuracy": 55.0,
         "timestamp": "2026-06-30T00:00:00Z"},
        {"iteration": 2, "eval_scope": "full", "attempt_no": 2, "overall_accuracy": 80.0,
         "attempt_mode": "surgical", "timestamp": "2026-06-30T00:01:00Z"},
        {"iteration": 3, "eval_scope": "full", "attempt_no": 2, "overall_accuracy": 88.0,
         "attempt_mode": "surgical", "timestamp": "2026-06-30T00:02:00Z"},
    ]
    monkeypatch.setattr(auto_optimize, "_select_loop_state_delta", lambda _rid: [dict(r) for r in loop_rows])

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/loop-state")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # Exactly two distinct attempts (coverage 1, surgical 2) — no duplicates,
    # no phantom from the slice row.
    assert [a["attemptNo"] for a in body["attempts"]] == [1, 2]
    assert body["loopState"]["attemptCount"] == 2
    surgical = body["attempts"][1]
    # The authoritative row is the LATEST full-benchmark row (iter 3, 88.0) —
    # not the slice probe (55.0) and not the earlier full row (80.0).
    assert surgical["evalScope"] == "full"
    assert surgical["accuracy"] == 88.0


# B3 — small targets round-trip (source-specific 0–100 → 0–1).


def test_delta_accuracy_scale_handles_small_targets() -> None:
    conv = auto_optimize._delta_accuracy_to_unit_scale
    assert conv(90.0) == 0.9
    assert conv(50.0) == 0.5
    # The footgun: a 0.01 request is stored as 1.0 on the 0–100 column; a
    # magnitude heuristic would leave it at 1.0. Unconditional /100 → 0.01.
    assert conv(1.0) == 0.01
    assert conv(None) is None


def test_loop_state_target_accuracy_small_value_round_trips(monkeypatch) -> None:
    loop_rows = [
        {"iteration": 1, "eval_scope": "full", "attempt_no": 1, "overall_accuracy": 5.0,
         "best_accuracy": 5.0, "target_accuracy": 1.0, "max_attempts": 3},
    ]
    monkeypatch.setattr(auto_optimize, "_select_loop_state_delta", lambda _rid: [dict(r) for r in loop_rows])
    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/loop-state")
    assert resp.status_code == 200
    # 0–100 column value 1.0 → 0.01 request scale (not 1.0).
    assert resp.json()["loopState"]["targetAccuracy"] == 0.01


# B4 — knobs echoed from durable run-level sources before the loop runs.


def test_status_echoes_knobs_from_run_manifest_pre_loop(monkeypatch) -> None:
    """A freshly-triggered run (00 wrote the manifest, loop hasn't committed an
    attempt) still echoes the knobs from the run_manifest artifact (0–1)."""
    async def fake_run(_rid):
        return {"run_id": _RUN, "space_id": "s", "status": "IN_PROGRESS",
                "convergence_reason": None, "job_run_id": "555"}

    async def empty(_rid):
        return []

    manifest = {"run_id": _RUN, "target_accuracy": 0.85, "max_attempts": 5}
    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_run", fake_run)
    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_stages", empty)
    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_iterations", empty)
    monkeypatch.setattr(auto_optimize, "_delta_query", lambda *a, **k: [])
    # No loop-state rows yet; manifest is present.
    monkeypatch.setattr(auto_optimize, "_load_latest_artifact",
                        lambda _rid, kind: manifest if kind == "run_manifest" else None)
    monkeypatch.setattr(auto_optimize, "_loop_state_knobs", lambda _rid: (None, None))

    client = _gso_client(monkeypatch)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN}/status")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["targetAccuracy"] == 0.85
    assert body["maxAttempts"] == 5


def test_resolve_run_knobs_falls_back_to_job_params_in_queued_window(monkeypatch) -> None:
    """Before 00 writes the manifest (QUEUED/startup), the knobs come from the
    Databricks job-run parameters the trigger set (0–1 scale)."""
    from unittest.mock import MagicMock

    # No manifest, no loop-state rows yet.
    monkeypatch.setattr(auto_optimize, "_load_latest_artifact", lambda _rid, kind: None)
    monkeypatch.setattr(auto_optimize, "_loop_state_knobs", lambda _rid: (None, None))

    sp_ws = MagicMock()
    job_run = MagicMock()
    job_run.job_parameters = [
        MagicMock(name="target_accuracy", value="0.7"),
        MagicMock(name="max_attempts", value="4"),
    ]
    # MagicMock(name=...) sets the repr, not the .name attribute — set explicitly.
    job_run.job_parameters[0].name = "target_accuracy"
    job_run.job_parameters[1].name = "max_attempts"
    sp_ws.jobs.get_run.return_value = job_run
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: sp_ws)

    ta, ma = auto_optimize._resolve_run_knobs(
        {"run_id": _RUN, "status": "IN_PROGRESS", "job_run_id": "999"}
    )
    assert ta == 0.7
    assert ma == 4
    sp_ws.jobs.get_run.assert_called_once()


def test_resolve_run_knobs_null_for_legacy_terminal_run(monkeypatch) -> None:
    """A true legacy run (no manifest, no loop-state, terminal) returns
    (None, None) and never makes a Jobs API call."""
    from unittest.mock import MagicMock

    monkeypatch.setattr(auto_optimize, "_load_latest_artifact", lambda _rid, kind: None)
    monkeypatch.setattr(auto_optimize, "_loop_state_knobs", lambda _rid: (None, None))
    sp_ws = MagicMock()
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: sp_ws)

    ta, ma = auto_optimize._resolve_run_knobs(
        {"run_id": _RUN, "status": "CONVERGED", "job_run_id": "123"}
    )
    assert ta is None and ma is None
    sp_ws.jobs.get_run.assert_not_called()


# ── /runs/{run_id}/revert ───────────────────────────────────────────────


def test_revert_run_happy_path(client, monkeypatch, mock_sp_ws, mock_user_ws) -> None:
    """A successful revert returns 200 + the integration ActionResult payload."""
    run_id = "12345678-1234-1234-1234-1234567890ab"
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: mock_sp_ws)
    fake = MagicMock(status="reverted", run_id=run_id,
                      message="Genie Space reverted to this run's champion configuration.")
    captured = {}
    def _stub(*a, **k):
        captured.update(k)
        return fake
    monkeypatch.setattr(auto_optimize, "revert_optimization", _stub)
    resp = client.post(f"/api/auto-optimize/runs/{run_id}/revert?target=champion")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "reverted"
    assert body["runId"] == run_id
    assert "reverted" in body["message"].lower()
    # The target query param is forwarded to the integration function.
    assert captured.get("target") == "champion"


def test_revert_run_defaults_target_to_champion(client, monkeypatch, mock_sp_ws, mock_user_ws) -> None:
    """Omitting ?target defaults to 'champion' (back-compat)."""
    run_id = "12345678-1234-1234-1234-1234567890ab"
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: mock_sp_ws)
    captured = {}
    def _stub(*a, **k):
        captured.update(k)
        return MagicMock(status="reverted", run_id=run_id, message="ok")
    monkeypatch.setattr(auto_optimize, "revert_optimization", _stub)
    resp = client.post(f"/api/auto-optimize/runs/{run_id}/revert")
    assert resp.status_code == 200
    assert captured.get("target") == "champion"


def test_revert_run_rejects_invalid_target(client, monkeypatch, mock_sp_ws, mock_user_ws) -> None:
    """An unknown target value is a 422 before the integration layer is touched."""
    run_id = "12345678-1234-1234-1234-1234567890ab"
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: mock_sp_ws)
    stub = MagicMock(side_effect=AssertionError("must not be called"))
    monkeypatch.setattr(auto_optimize, "revert_optimization", stub)
    resp = client.post(f"/api/auto-optimize/runs/{run_id}/revert?target=garbage")
    assert resp.status_code == 422
    assert "target" in resp.json()["detail"].lower()
    stub.assert_not_called()


def test_revert_run_returns_409_for_value_error(client, monkeypatch, mock_sp_ws, mock_user_ws) -> None:
    """Still-in-progress / missing-snapshot runs surface as a 409."""
    run_id = "12345678-1234-1234-1234-1234567890ab"
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: mock_sp_ws)
    def _raise(*a, **k):
        raise ValueError("Cannot revert to a run that is still in progress.")
    monkeypatch.setattr(auto_optimize, "revert_optimization", _raise)
    resp = client.post(f"/api/auto-optimize/runs/{run_id}/revert")
    assert resp.status_code == 409
    assert "in progress" in resp.json()["detail"].lower()


def test_revert_run_returns_403_for_permission_error(
    client, monkeypatch, mock_sp_ws, mock_user_ws,
) -> None:
    """An OBO caller without CAN_EDIT/CAN_MANAGE cannot use the SP to mutate."""
    run_id = "12345678-1234-1234-1234-1234567890ab"
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: mock_sp_ws)

    def _raise(*args, **kwargs):
        raise PermissionError("You need CAN_EDIT or CAN_MANAGE permission.")

    monkeypatch.setattr(auto_optimize, "revert_optimization", _raise)
    resp = client.post(f"/api/auto-optimize/runs/{run_id}/revert")

    assert resp.status_code == 403
    assert "can_edit" in resp.json()["detail"].lower()


def test_revert_run_returns_422_for_runtime_error(client, monkeypatch, mock_sp_ws, mock_user_ws) -> None:
    """A failed Genie PATCH rollback surfaces as a 422 (unprocessable)."""
    run_id = "12345678-1234-1234-1234-1234567890ab"
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: mock_sp_ws)
    def _raise(*a, **k):
        raise RuntimeError("Failed to apply rollback via API")
    monkeypatch.setattr(auto_optimize, "revert_optimization", _raise)
    resp = client.post(f"/api/auto-optimize/runs/{run_id}/revert")
    assert resp.status_code == 422
    assert "rollback" in resp.json()["detail"].lower()


def test_revert_run_returns_500_for_unexpected_exception(client, monkeypatch, mock_sp_ws, mock_user_ws) -> None:
    """Any unexpected error maps to a generic 500, never a stack trace."""
    run_id = "12345678-1234-1234-1234-1234567890ab"
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: mock_user_ws)
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: mock_sp_ws)
    def _raise(*a, **k):
        raise OSError("boom")
    monkeypatch.setattr(auto_optimize, "revert_optimization", _raise)
    resp = client.post(f"/api/auto-optimize/runs/{run_id}/revert")
    assert resp.status_code == 500
    assert resp.json()["detail"] == "Failed to revert the Genie Space."
