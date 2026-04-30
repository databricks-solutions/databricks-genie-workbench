from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from genie_space_optimizer.optimization import state


def test_update_run_status_includes_space_id_when_passed(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[dict[str, Any]] = []

    def fake_update_row_with_delta_retry(spark, catalog, schema, table, keys, updates, **kwargs):
        captured.append(
            {
                "spark": spark,
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "keys": keys,
                "updates": updates,
                "kwargs": kwargs,
            }
        )

    monkeypatch.setattr(state, "_update_row_with_delta_retry", fake_update_row_with_delta_retry)

    state.update_run_status(
        object(),
        "run-1",
        "cat",
        "sch",
        space_id="space-1",
        status="IN_PROGRESS",
    )

    assert captured[0]["table"] == state.TABLE_RUNS
    assert captured[0]["keys"] == {"run_id": "run-1", "space_id": "space-1"}
    assert captured[0]["updates"]["status"] == "IN_PROGRESS"


def test_update_run_status_looks_up_space_id_when_not_passed(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[dict[str, Any]] = []

    def fake_read_table(_spark, _catalog, _schema, table, filters=None):
        assert table == state.TABLE_RUNS
        assert filters == {"run_id": "run-1"}
        return pd.DataFrame([{"run_id": "run-1", "space_id": "space-from-row"}])

    def fake_update_row_with_delta_retry(spark, catalog, schema, table, keys, updates, **kwargs):
        captured.append({"keys": keys, "updates": updates})

    monkeypatch.setattr(state, "read_table", fake_read_table)
    monkeypatch.setattr(state, "_update_row_with_delta_retry", fake_update_row_with_delta_retry)

    state.update_run_status(
        object(),
        "run-1",
        "cat",
        "sch",
        status="FAILED",
        convergence_reason="error_in_BASELINE_EVAL",
    )

    assert captured[0]["keys"] == {"run_id": "run-1", "space_id": "space-from-row"}
    assert captured[0]["updates"]["status"] == "FAILED"
    assert captured[0]["updates"]["convergence_reason"] == "error_in_BASELINE_EVAL"
    assert "completed_at" in captured[0]["updates"]


def test_update_run_status_falls_back_to_run_id_when_lookup_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[dict[str, Any]] = []

    def fake_read_table(_spark, _catalog, _schema, _table, filters=None):
        return pd.DataFrame()

    def fake_update_row_with_delta_retry(spark, catalog, schema, table, keys, updates, **kwargs):
        captured.append({"keys": keys, "updates": updates})

    monkeypatch.setattr(state, "read_table", fake_read_table)
    monkeypatch.setattr(state, "_update_row_with_delta_retry", fake_update_row_with_delta_retry)

    state.update_run_status(object(), "run-1", "cat", "sch", status="IN_PROGRESS")

    assert captured[0]["keys"] == {"run_id": "run-1"}
    assert captured[0]["updates"]["status"] == "IN_PROGRESS"


def test_write_stage_uses_delta_write_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_sql: list[str] = []

    def fake_run_query(_spark, _sql: str):
        return pd.DataFrame([{"started_at": "2026-04-30T10:00:00+00:00"}])

    def fake_execute(_spark, sql: str, **kwargs: Any) -> None:
        captured_sql.append(sql)
        assert kwargs["operation_name"] == "write_stage"
        assert kwargs["table_name"] == "cat.sch.genie_opt_stages"

    monkeypatch.setattr(state, "run_query", fake_run_query)
    monkeypatch.setattr(state, "execute_delta_write_with_retry", fake_execute)

    state.write_stage(
        object(),
        "run-1",
        "BASELINE_EVAL",
        "COMPLETE",
        task_key="baseline_eval",
        detail={"rows": 30},
        catalog="cat",
        schema="sch",
    )

    assert len(captured_sql) == 1
    assert captured_sql[0].startswith("INSERT INTO cat.sch.genie_opt_stages")
    assert "'BASELINE_EVAL'" in captured_sql[0]
    assert "'COMPLETE'" in captured_sql[0]


def test_write_iteration_uses_delta_write_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_sql: list[str] = []

    def fake_execute(_spark, sql: str, **kwargs: Any) -> None:
        captured_sql.append(sql)
        assert kwargs["operation_name"] == "write_iteration"
        assert kwargs["table_name"] == "cat.sch.genie_opt_iterations"

    monkeypatch.setattr(state, "execute_delta_write_with_retry", fake_execute)

    state.write_iteration(
        object(),
        "run-1",
        0,
        {
            "scores": {"syntax_validity": 100.0},
            "failures": [],
            "remaining_failures": [],
            "arbiter_actions": [],
            "thresholds_met": True,
            "overall_accuracy": 100.0,
            "total_questions": 1,
            "correct_count": 1,
            "evaluated_count": 1,
            "excluded_count": 0,
        },
        catalog="cat",
        schema="sch",
    )

    assert len(captured_sql) == 1
    assert captured_sql[0].startswith("INSERT INTO cat.sch.genie_opt_iterations")
    assert "'run-1'" in captured_sql[0]


def test_update_iteration_reflection_uses_delta_write_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_sql: list[str] = []

    def fake_execute(_spark, sql: str, **kwargs: Any) -> None:
        captured_sql.append(sql)
        assert kwargs["operation_name"] == "update_iteration_reflection"
        assert kwargs["table_name"] == "cat.sch.genie_opt_iterations"

    monkeypatch.setattr(state, "execute_delta_write_with_retry", fake_execute)

    state.update_iteration_reflection(
        object(),
        "run-1",
        2,
        {"accepted": False, "reason": "regression"},
        catalog="cat",
        schema="sch",
    )

    assert len(captured_sql) == 1
    assert captured_sql[0].startswith("UPDATE cat.sch.genie_opt_iterations")
    assert "WHERE run_id = 'run-1' AND iteration = 2" in captured_sql[0]


def test_mark_patches_rolled_back_retries_both_updates(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_sql: list[str] = []

    def fake_execute(_spark, sql: str, **kwargs: Any) -> None:
        captured_sql.append(sql)
        assert kwargs["operation_name"] in {
            "mark_patches_rolled_back.patches",
            "mark_patches_rolled_back.iterations",
        }

    monkeypatch.setattr(state, "execute_delta_write_with_retry", fake_execute)

    state.mark_patches_rolled_back(
        object(),
        "run-1",
        3,
        "post_arbiter_guardrail",
        "cat",
        "sch",
    )

    assert len(captured_sql) == 2
    assert captured_sql[0].startswith("UPDATE cat.sch.genie_opt_patches")
    assert captured_sql[1].startswith("UPDATE cat.sch.genie_opt_iterations")
