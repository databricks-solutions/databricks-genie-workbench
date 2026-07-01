"""Tests for get_baseline_eval_state() — Delta-only baseline read (D9).

D9 (Delta-only handoff): v2 tasks publish NO Databricks task values, so the
baseline eval state is read straight from ``genie_opt_iterations`` (iteration=0,
eval_scope='full'). These tests lock that the helper never probes
``dbutils.jobs.taskValues`` (which, under the v2 5-task DAG, would raise
``ValueError: Task key does not exist in run: baseline_eval`` — the original
crash) and returns the Delta row shape unchanged.
"""
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.jobs._handoff import (
    HandoffSource,
    get_baseline_eval_state,
)


def _hostile_dbutils():
    """A dbutils whose taskValues probe raises the v2 crash if ever called.

    Mirrors the real Databricks behavior on the v2 DAG: the retired
    ``baseline_eval`` task key does not exist in the run, so
    ``dbutils.jobs.taskValues.get`` raises ``ValueError`` BEFORE any default can
    apply. If the Delta-only helper touches it, the test fails loudly.
    """
    dbu = MagicMock()
    dbu.jobs.taskValues.get.side_effect = ValueError(
        "Task key does not exist in run: baseline_eval"
    )
    return dbu


def test_baseline_state_reads_from_delta_iteration_zero():
    spark = MagicMock()
    fake_iter = {
        "iteration": 0,
        "eval_scope": "full",
        "scores_json": {"syntax_validity": 95.0},  # already parsed by load_*
        "overall_accuracy": 85.7,
        "thresholds_met": False,
        "model_id": "m-abc",
        "mlflow_run_id": "mr-001",
    }
    with patch(
        "genie_space_optimizer.jobs._handoff._load_baseline_iteration_row",
        return_value=fake_iter,
    ):
        state = get_baseline_eval_state(
            spark, run_id="run-001", catalog="cat", schema="sch",
        )
    assert state["scores"].value == {"syntax_validity": 95.0}
    assert state["scores"].source is HandoffSource.DELTA_FALLBACK
    assert state["overall_accuracy"].value == 85.7
    assert state["thresholds_met"].value is False
    assert state["model_id"].value == "m-abc"
    assert state["mlflow_run_id"].value == "mr-001"


def test_baseline_state_does_not_probe_task_values():
    """Regression for the v2 crash: passing a dbutils whose taskValues probe
    raises must NOT propagate — the helper reads Delta only."""
    spark = MagicMock()
    fake_iter = {
        "iteration": 0,
        "eval_scope": "full",
        "scores_json": {"syntax_validity": 95.0},
        "overall_accuracy": 85.7,
        "thresholds_met": False,
        "model_id": "m-abc",
        "mlflow_run_id": "mr-001",
    }
    dbu = _hostile_dbutils()
    with patch(
        "genie_space_optimizer.jobs._handoff._load_baseline_iteration_row",
        return_value=fake_iter,
    ):
        state = get_baseline_eval_state(
            spark, run_id="run-001", catalog="cat", schema="sch", dbutils=dbu,
        )
    assert state["overall_accuracy"].value == 85.7
    dbu.jobs.taskValues.get.assert_not_called()


def test_baseline_state_missing_fields_are_missing_not_defaulted():
    """A baseline row present but with NULL scorecard/model columns yields
    MISSING (value None) for those keys — the row shape is preserved."""
    spark = MagicMock()
    fake_iter = {
        "iteration": 0,
        "eval_scope": "full",
        "scores_json": None,
        "overall_accuracy": 0.0,
        "thresholds_met": False,
        "model_id": None,
        "mlflow_run_id": None,
    }
    with patch(
        "genie_space_optimizer.jobs._handoff._load_baseline_iteration_row",
        return_value=fake_iter,
    ):
        state = get_baseline_eval_state(
            spark, run_id="run-001", catalog="cat", schema="sch",
        )
    assert state["scores"].value is None
    assert state["scores"].source is HandoffSource.MISSING
    # overall_accuracy=0.0 is a real persisted value, not MISSING.
    assert state["overall_accuracy"].value == 0.0
    assert state["overall_accuracy"].source is HandoffSource.DELTA_FALLBACK
    assert state["model_id"].source is HandoffSource.MISSING


def test_baseline_state_raises_when_no_delta_row():
    spark = MagicMock()
    with patch(
        "genie_space_optimizer.jobs._handoff._load_baseline_iteration_row",
        return_value=None,
    ):
        with pytest.raises(RuntimeError, match="baseline"):
            get_baseline_eval_state(
                spark, run_id="run-001", catalog="cat", schema="sch",
            )
