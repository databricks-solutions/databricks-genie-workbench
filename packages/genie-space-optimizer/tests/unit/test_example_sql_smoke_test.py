"""Pre-promotion smoke test: validate that candidate examples don't
regress baseline ``both_correct`` questions when staged."""
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.example_smoke_test import (
    SmokeTestResult,
    run_pre_promotion_smoke_test,
)


def _baseline_rows(qids, *, verdict="both_correct"):
    return [
        {"question_id": q, "arbiter/value": verdict,
         "request": {"question": f"q for {q}", "expected_sql": "SELECT 1"},
         "response": {"response": "SELECT 1"}}
        for q in qids
    ]


def test_no_baseline_rows_returns_inconclusive_passes():
    candidates = [{"question": "q1", "expected_sql": "SELECT 1"}]
    result = run_pre_promotion_smoke_test(
        candidates=candidates,
        baseline_both_correct_rows=[],
        staged_config={},
        run_eval_fn=lambda **kwargs: pytest.fail("eval should not be called"),
    )
    assert isinstance(result, SmokeTestResult)
    assert result.accept is True
    assert result.reason == "no_baseline_pool"


def test_zero_regression_accepts():
    rows = _baseline_rows(["q1", "q2", "q3"])
    candidates = [{"question": "x", "expected_sql": "SELECT 1"}]

    def fake_eval(**kwargs):
        return {
            "rows": [
                {"question_id": "q1", "arbiter/value": "both_correct"},
                {"question_id": "q2", "arbiter/value": "both_correct"},
                {"question_id": "q3", "arbiter/value": "both_correct"},
            ],
        }

    result = run_pre_promotion_smoke_test(
        candidates=candidates,
        baseline_both_correct_rows=rows,
        staged_config={"_marker": "staged"},
        run_eval_fn=fake_eval,
    )
    assert result.accept is True
    assert result.regressions == 0
    assert result.regression_pp == 0.0


def test_any_regression_at_zero_tolerance_rejects(monkeypatch):
    monkeypatch.setenv("GSO_EXAMPLE_SQL_SMOKE_REGRESSION_TOLERANCE_PP", "0.0")
    rows = _baseline_rows(["q1", "q2", "q3"])
    candidates = [{"question": "x", "expected_sql": "SELECT 1"}]

    def fake_eval(**kwargs):
        return {
            "rows": [
                {"question_id": "q1", "arbiter/value": "both_correct"},
                {"question_id": "q2", "arbiter/value": "both_wrong"},
                {"question_id": "q3", "arbiter/value": "both_correct"},
            ],
        }

    result = run_pre_promotion_smoke_test(
        candidates=candidates,
        baseline_both_correct_rows=rows,
        staged_config={"_marker": "staged"},
        run_eval_fn=fake_eval,
    )
    assert result.accept is False
    assert result.regressions == 1
    assert pytest.approx(result.regression_pp, rel=1e-6) == (1 / 3) * 100


def test_within_tolerance_accepts(monkeypatch):
    monkeypatch.setenv("GSO_EXAMPLE_SQL_SMOKE_REGRESSION_TOLERANCE_PP", "50.0")
    rows = _baseline_rows(["q1", "q2"])
    candidates = [{"question": "x", "expected_sql": "SELECT 1"}]

    def fake_eval(**kwargs):
        return {
            "rows": [
                {"question_id": "q1", "arbiter/value": "both_correct"},
                {"question_id": "q2", "arbiter/value": "both_wrong"},
            ],
        }

    result = run_pre_promotion_smoke_test(
        candidates=candidates,
        baseline_both_correct_rows=rows,
        staged_config={},
        run_eval_fn=fake_eval,
    )
    assert result.accept is True
    assert result.regressions == 1
