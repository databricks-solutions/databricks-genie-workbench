"""Tests for get_lever_loop_outputs() — Delta-only optimize/loop read (D9).

D9 (Delta-only handoff): the loop outputs are reconstructed from
``genie_opt_runs`` (best-* columns) and the latest ``eval_scope='full'``
``genie_opt_iterations`` row. No Databricks task values are consulted.
"""
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.jobs._handoff import (
    HandoffSource,
    get_lever_loop_outputs,
)


def _hostile_dbutils():
    """dbutils whose taskValues probe raises the retired-key crash if called."""
    dbu = MagicMock()
    dbu.jobs.taskValues.get.side_effect = ValueError(
        "Task key does not exist in run: lever_loop"
    )
    return dbu


def _delta_fixtures():
    fake_run = {
        "best_iteration": 2,
        "best_model_id": "m-final",
        "best_accuracy": 92.5,
    }
    fake_latest = {
        "iteration": 3,
        "scores_json": {"x": 90},
        "overall_accuracy": 92.5,
        "model_id": "m-final",
        "failures_json": ["q1"],
        "mlflow_run_id": "r2",
    }
    fake_iters_df = MagicMock()
    fake_iters_df.empty = False
    fake_iters_df.get.return_value.dropna.return_value.tolist.return_value = [
        "r1", "r2",
    ]
    return fake_run, fake_latest, fake_iters_df


def test_lever_loop_outputs_read_from_delta():
    spark = MagicMock()
    fake_run, fake_latest, fake_iters_df = _delta_fixtures()

    with patch(
        "genie_space_optimizer.jobs._handoff.load_run",
        return_value=fake_run,
    ), patch(
        "genie_space_optimizer.jobs._handoff.load_latest_full_iteration",
        return_value=fake_latest,
    ), patch(
        "genie_space_optimizer.jobs._handoff.load_iterations",
        return_value=fake_iters_df,
    ):
        state = get_lever_loop_outputs(
            spark, run_id="run-001", catalog="cat", schema="sch",
        )
    assert state["scores"].value == {"x": 90}
    assert state["scores"].source is HandoffSource.DELTA_FALLBACK
    assert state["accuracy"].value == 92.5
    assert state["model_id"].value == "m-final"
    assert state["iteration_counter"].value == 3
    assert state["best_iteration"].value == 2
    assert state["skipped"].value is False
    assert state["all_eval_mlflow_run_ids"].value == ["r1", "r2"]
    assert state["all_failure_question_ids"].value == ["q1"]


def test_lever_loop_outputs_skipped_true_when_latest_is_iteration_zero():
    """``skipped`` is derived from Delta: latest full row at iteration 0 means
    the loop was skipped (no optimization iterations ran)."""
    spark = MagicMock()
    fake_run = {"best_iteration": 0, "best_model_id": "m0"}
    fake_latest = {
        "iteration": 0,
        "scores_json": {"x": 80},
        "overall_accuracy": 80.0,
        "model_id": "m0",
        "failures_json": [],
    }
    fake_iters_df = MagicMock()
    fake_iters_df.empty = True
    with patch(
        "genie_space_optimizer.jobs._handoff.load_run",
        return_value=fake_run,
    ), patch(
        "genie_space_optimizer.jobs._handoff.load_latest_full_iteration",
        return_value=fake_latest,
    ), patch(
        "genie_space_optimizer.jobs._handoff.load_iterations",
        return_value=fake_iters_df,
    ):
        state = get_lever_loop_outputs(
            spark, run_id="run-001", catalog="cat", schema="sch",
        )
    assert state["skipped"].value is True
    assert state["iteration_counter"].value == 0


def test_lever_loop_outputs_do_not_probe_task_values():
    """Regression for the v2 crash: a hostile dbutils whose taskValues probe
    raises must not propagate — loop outputs come from Delta only."""
    spark = MagicMock()
    fake_run, fake_latest, fake_iters_df = _delta_fixtures()
    dbu = _hostile_dbutils()
    with patch(
        "genie_space_optimizer.jobs._handoff.load_run",
        return_value=fake_run,
    ), patch(
        "genie_space_optimizer.jobs._handoff.load_latest_full_iteration",
        return_value=fake_latest,
    ), patch(
        "genie_space_optimizer.jobs._handoff.load_iterations",
        return_value=fake_iters_df,
    ):
        state = get_lever_loop_outputs(
            spark, run_id="run-001", catalog="cat", schema="sch", dbutils=dbu,
        )
    assert state["accuracy"].value == 92.5
    dbu.jobs.taskValues.get.assert_not_called()


def test_lever_loop_outputs_raise_when_no_state():
    spark = MagicMock()
    with patch(
        "genie_space_optimizer.jobs._handoff.load_run",
        return_value=None,
    ), patch(
        "genie_space_optimizer.jobs._handoff.load_latest_full_iteration",
        return_value=None,
    ):
        with pytest.raises(RuntimeError, match="lever_loop"):
            get_lever_loop_outputs(
                spark, run_id="run-001", catalog="cat", schema="sch",
            )
