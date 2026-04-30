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
