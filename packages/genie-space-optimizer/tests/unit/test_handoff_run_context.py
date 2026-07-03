"""Tests for get_run_context() — Delta-only run-context read (D9).

D9 (Delta-only handoff): run-level context is read straight from
``genie_opt_runs`` keyed by the ``run_id`` job widget; ``catalog`` / ``schema``
are echoed from their widgets. No Databricks task values are consulted.
"""
import json
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.jobs._handoff import (
    HandoffSource,
    get_run_context,
)


def _hostile_dbutils():
    """dbutils whose taskValues probe raises the retired-key crash if called."""
    dbu = MagicMock()
    dbu.jobs.taskValues.get.side_effect = ValueError(
        "Task key does not exist in run: preflight"
    )
    return dbu


def test_run_context_reads_from_delta():
    spark = MagicMock()
    fake_run_row = {
        "run_id": "run-001",
        "space_id": "space-abc",
        "domain": "revenue",
        "catalog": "cat",
        "uc_schema": "cat.sch",
        "experiment_name": "/exp",
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
        )

    assert ctx["run_id"].value == "run-001"
    assert ctx["run_id"].source is HandoffSource.DELTA_FALLBACK
    assert ctx["levers"].value == [1, 2, 3]
    assert ctx["catalog"].value == "cat"
    assert ctx["schema"].value == "sch"
    assert ctx["human_corrections"].value == [{"qid": "q1"}]
    assert ctx["max_benchmark_count"].value == 42


def test_run_context_parses_json_columns_stored_as_strings():
    """``levers`` / ``human_corrections_json`` come back from load_run as JSON
    strings; the helper parses them so callers always see typed values."""
    spark = MagicMock()
    fake_run_row = {
        "run_id": "run-001",
        "space_id": "space-abc",
        "levers": "[1, 2, 3]",
        "human_corrections_json": "[{\"qid\": \"q1\"}]",
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
        )
    assert ctx["levers"].value == [1, 2, 3]
    assert ctx["human_corrections"].value == [{"qid": "q1"}]


def test_run_context_does_not_probe_task_values():
    """Regression for the v2 crash: a hostile dbutils whose taskValues probe
    raises must not propagate — context comes from Delta only."""
    spark = MagicMock()
    fake_run_row = {"run_id": "run-001", "space_id": "space-abc"}
    dbu = _hostile_dbutils()
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
    dbu.jobs.taskValues.get.assert_not_called()


def test_run_context_catalog_schema_come_from_widgets():
    """``catalog`` / ``schema`` are the bootstrap job widgets — authoritative
    direct reads (TASK_VALUES source), not reconstructed from Delta."""
    spark = MagicMock()
    fake_run_row = {"run_id": "run-001", "space_id": "space-abc"}
    with patch(
        "genie_space_optimizer.jobs._handoff.load_run",
        return_value=fake_run_row,
    ) as load_mock:
        ctx = get_run_context(
            spark,
            run_id_widget="run-001",
            catalog_widget="cat",
            schema_widget="sch",
        )

    load_mock.assert_called_once_with(spark, "run-001", "cat", "sch")
    assert ctx["catalog"].value == "cat"
    assert ctx["catalog"].source is HandoffSource.TASK_VALUES  # came from widget
    assert ctx["schema"].source is HandoffSource.TASK_VALUES
    assert ctx["space_id"].source is HandoffSource.DELTA_FALLBACK


def test_run_context_raises_when_run_id_widget_empty():
    spark = MagicMock()
    with pytest.raises(RuntimeError, match="run_id_widget is required"):
        get_run_context(
            spark,
            run_id_widget="",
            catalog_widget="cat",
            schema_widget="sch",
        )


def test_run_context_raises_when_no_delta_row():
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
            )
