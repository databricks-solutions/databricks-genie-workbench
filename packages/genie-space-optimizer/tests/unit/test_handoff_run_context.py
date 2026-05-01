"""Tests for get_run_context() — reads preflight state with Delta fallback."""
import json
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.jobs._handoff import (
    HandoffSource,
    get_run_context,
)


def _make_dbutils(values):
    """Build a mock dbutils.jobs.taskValues.get(...) returning ``values``.

    ``values`` maps (taskKey, key) tuples to the value to return.
    Missing keys return the default kwarg.
    """
    dbu = MagicMock()
    def _get(taskKey, key, default=""):
        return values.get((taskKey, key), default)
    dbu.jobs.taskValues.get.side_effect = _get
    return dbu


def test_run_context_happy_path_uses_task_values():
    dbu = _make_dbutils({
        ("preflight", "run_id"): "run-001",
        ("preflight", "space_id"): "space-abc",
        ("preflight", "domain"): "revenue",
        ("preflight", "catalog"): "cat",
        ("preflight", "schema"): "sch",
        ("preflight", "experiment_name"): "/exp",
        ("preflight", "max_iterations"): "10",
        ("preflight", "levers"): "[1,2,3]",
        ("preflight", "apply_mode"): "genie_config",
        ("preflight", "triggered_by"): "user@x.com",
        ("preflight", "warehouse_id"): "wh-xyz",
        ("preflight", "human_corrections"): "[]",
        ("preflight", "max_benchmark_count"): "42",
    })
    spark = MagicMock()

    ctx = get_run_context(
        spark,
        run_id_widget="run-001",
        catalog_widget="cat",
        schema_widget="sch",
        dbutils=dbu,
    )

    assert ctx["run_id"].value == "run-001"
    assert ctx["run_id"].source is HandoffSource.TASK_VALUES
    assert ctx["levers"].value == [1, 2, 3]
    assert ctx["max_iterations"].value == 10
    assert ctx["max_benchmark_count"].value == 42
    assert ctx["human_corrections"].value == []


def test_run_context_falls_back_to_delta_when_task_values_empty():
    dbu = _make_dbutils({})  # everything empty / default
    spark = MagicMock()
    fake_run_row = {
        "run_id": "run-001",
        "space_id": "space-abc",
        "domain": "revenue",
        "catalog": "cat",
        "uc_schema": "cat.sch",
        "experiment_name": "/exp",
        "max_iterations": 10,
        "levers": [1, 2, 3],
        "apply_mode": "genie_config",
        "triggered_by": "user@x.com",
        "warehouse_id": "wh-xyz",
        "human_corrections_json": json.dumps([{"qid": "q1"}]),
        "max_benchmark_count": 42,
    }
    with patch(
        "genie_space_optimizer.jobs._handoff.load_run",
        return_value=fake_run_row,
    ):
        ctx = get_run_context(
            spark,
            run_id_widget="run-001",
            catalog_widget="cat",
            schema_widget="sch",
            dbutils=dbu,
        )

    assert ctx["run_id"].value == "run-001"
    assert ctx["run_id"].source is HandoffSource.DELTA_FALLBACK
    assert ctx["levers"].value == [1, 2, 3]
    assert ctx["max_iterations"].value == 10
    assert ctx["catalog"].value == "cat"
    assert ctx["schema"].value == "sch"
    assert ctx["human_corrections"].value == [{"qid": "q1"}]
    assert ctx["max_benchmark_count"].value == 42


def test_run_context_raises_when_task_values_and_delta_both_empty():
    dbu = _make_dbutils({})
    spark = MagicMock()
    with patch(
        "genie_space_optimizer.jobs._handoff.load_run",
        return_value=None,
    ):
        with pytest.raises(RuntimeError, match="run context"):
            get_run_context(
                spark,
                run_id_widget="run-001",
                catalog_widget="cat",
                schema_widget="sch",
                dbutils=dbu,
            )


def test_run_context_bootstraps_from_widgets_when_task_values_empty():
    """Repair Run case: taskValues empty, widgets carry run_id/catalog/schema,
    Delta fills the rest."""
    dbu = _make_dbutils({})
    spark = MagicMock()
    fake_run_row = {
        "run_id": "run-001",
        "space_id": "space-abc",
        "domain": "revenue",
        "catalog": "cat",
        "uc_schema": "cat.sch",
        "experiment_name": "/exp",
        "max_iterations": 10,
        "levers": [1, 2, 3],
        "apply_mode": "genie_config",
    }
    with patch(
        "genie_space_optimizer.jobs._handoff.load_run",
        return_value=fake_run_row,
    ) as load_mock:
        ctx = get_run_context(
            spark,
            run_id_widget="run-001",
            catalog_widget="cat",
            schema_widget="sch",
            dbutils=dbu,
        )

    load_mock.assert_called_once_with(spark, "run-001", "cat", "sch")
    assert ctx["run_id"].value == "run-001"
    assert ctx["space_id"].value == "space-abc"
    assert ctx["catalog"].source is HandoffSource.TASK_VALUES  # came from widget
    assert ctx["space_id"].source is HandoffSource.DELTA_FALLBACK
