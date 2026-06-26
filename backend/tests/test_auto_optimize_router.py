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
    }
    trigger_mock.assert_called_once()
    config = trigger_mock.call_args.kwargs["config"]
    assert config.llm_model == "custom-trigger-model"


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


def test_baseline_step_summary_is_assessment_centric_no_judges() -> None:
    """The Baseline Evaluation step summary must NOT mention judges and must
    report num_correct / num_questions + the NEEDS_REVIEW count."""
    from backend.routers.auto_optimize import _build_step_summary

    defn = {"name": "Baseline Evaluation"}
    matching = [{"stage": "AG_BASELINE", "detail_json": "{}"}]
    iterations_rows = [
        {
            "iteration": 0, "eval_scope": "full", "overall_accuracy": 80.0,
            "total_questions": 10, "correct_count": 8, "num_needs_review": 2,
        }
    ]
    summary = _build_step_summary(defn, matching, iterations_rows, {})

    assert summary is not None
    # No judge-era language anywhere.
    assert "judge" not in summary.lower()
    # Official accuracy num_correct / num_questions and the review count.
    assert "80.0%" in summary
    assert "8/10 correct" in summary
    assert "2 need review" in summary


def test_iterations_official_accuracy_uses_num_questions_not_evaluated_count(monkeypatch) -> None:
    """On a PARTIAL official run (num_done < num_questions), the headline
    accuracy must be num_correct / num_questions — NOT correct / evaluated_count
    (which would inflate). Legacy rows keep their stored overall_accuracy."""
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")

    from backend.routers import auto_optimize

    delta_rows = [
        # Official, partial: 8 correct, only 8 done, but 10 total questions.
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
    assert out[0]["num_done"] == 8
    # Legacy row: stored value preserved (no official recompute).
    assert out[1]["overall_accuracy"] == 88.0
