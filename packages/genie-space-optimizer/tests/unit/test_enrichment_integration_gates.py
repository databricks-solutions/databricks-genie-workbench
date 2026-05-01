"""Integration test: the high-risk lane gates fire in the apply path.

Uses fakes for the LLM judges and the smoke-test runner to verify the
control flow — no real LLM calls.
"""
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import harness


def _candidates():
    return [
        {"question": "good q", "expected_sql": "SELECT 1"},
        {"question": "bad q", "expected_sql": "SELECT MEASURE(x) FROM main.s.dim_t"},
    ]


def _snapshot():
    return {
        "_asset_semantics": {
            "main.s.dim_t": {"asset_type": "table",
                             "columns": [{"name": "x"}]},
        },
        "_uc_foreign_keys": [],
        "instructions": {"join_specs": []},
    }


def test_deterministic_safety_drops_unsafe_candidate(monkeypatch):
    monkeypatch.setenv("GSO_EXAMPLE_SQL_TEACHING_SAFETY", "false")
    monkeypatch.setenv("GSO_EXAMPLE_SQL_SMOKE_TEST", "false")

    cands = _candidates()
    snap = _snapshot()
    survivors = harness._filter_candidates_by_teaching_safety(
        candidates=cands,
        metadata_snapshot=snap,
    )
    assert len(survivors) == 1
    assert survivors[0]["question"] == "good q"


def test_smoke_test_rejection_blocks_apply(monkeypatch):
    monkeypatch.setenv("GSO_EXAMPLE_SQL_SMOKE_TEST_ENABLED", "true")
    monkeypatch.setenv("GSO_EXAMPLE_SQL_SMOKE_REGRESSION_TOLERANCE_PP", "0.0")

    baseline_rows = [{"question_id": "q1", "arbiter/value": "both_correct",
                      "request": {"question": "q1", "expected_sql": "SELECT 1"},
                      "response": {"response": "SELECT 1"}}]

    with patch.object(
        harness, "run_pre_promotion_smoke_test",
    ) as mock_smoke:
        from genie_space_optimizer.optimization.example_smoke_test import (
            SmokeTestResult,
        )
        mock_smoke.return_value = SmokeTestResult(
            accept=False, reason="regression",
            regressions=1, sample_size=1, regression_pp=100.0,
        )
        accepted = harness._gate_candidates_with_smoke_test(
            candidates=[{"question": "x", "expected_sql": "SELECT 1"}],
            baseline_both_correct_rows=baseline_rows,
            metadata_snapshot=_snapshot(),
            staged_config_builder=lambda c: {"_marker": "staged"},
            w=MagicMock(), spark=MagicMock(),
            catalog="main", schema="s", space_id="sp",
        )
    assert accepted == []


def test_smoke_test_acceptance_passes_through(monkeypatch):
    monkeypatch.setenv("GSO_EXAMPLE_SQL_SMOKE_TEST_ENABLED", "true")

    baseline_rows = [{"question_id": "q1", "arbiter/value": "both_correct",
                      "request": {"question": "q1", "expected_sql": "SELECT 1"},
                      "response": {"response": "SELECT 1"}}]

    with patch.object(harness, "run_pre_promotion_smoke_test") as mock_smoke:
        from genie_space_optimizer.optimization.example_smoke_test import (
            SmokeTestResult,
        )
        mock_smoke.return_value = SmokeTestResult(
            accept=True, reason="ok", regressions=0,
            sample_size=1, regression_pp=0.0,
        )
        cands = [{"question": "x", "expected_sql": "SELECT 1"}]
        accepted = harness._gate_candidates_with_smoke_test(
            candidates=cands,
            baseline_both_correct_rows=baseline_rows,
            metadata_snapshot=_snapshot(),
            staged_config_builder=lambda c: {"_marker": "staged"},
            w=MagicMock(), spark=MagicMock(),
            catalog="main", schema="s", space_id="sp",
        )
    assert accepted == cands
