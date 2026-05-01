"""Tests for get_lever_loop_outputs() — finalize / deploy fallback."""
import json
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.jobs._handoff import (
    HandoffSource,
    get_lever_loop_outputs,
)


def _make_dbutils(values):
    dbu = MagicMock()
    def _get(taskKey, key, default=""):
        return values.get((taskKey, key), default)
    dbu.jobs.taskValues.get.side_effect = _get
    return dbu


def test_lever_loop_outputs_happy_path_uses_task_values():
    dbu = _make_dbutils({
        ("lever_loop", "scores"): json.dumps({"x": 90}),
        ("lever_loop", "accuracy"): "92.5",
        ("lever_loop", "model_id"): "m-final",
        ("lever_loop", "iteration_counter"): "3",
        ("lever_loop", "best_iteration"): "2",
        ("lever_loop", "skipped"): "false",
        ("lever_loop", "all_eval_mlflow_run_ids"): json.dumps(["r1", "r2"]),
        ("lever_loop", "all_failure_question_ids"): json.dumps(["q1"]),
    })
    spark = MagicMock()
    state = get_lever_loop_outputs(
        spark, run_id="run-001", catalog="cat", schema="sch", dbutils=dbu,
    )
    assert state["scores"].value == {"x": 90}
    assert state["accuracy"].value == 92.5
    assert state["model_id"].value == "m-final"
    assert state["iteration_counter"].value == 3
    assert state["best_iteration"].value == 2
    assert state["skipped"].value is False
    assert state["all_eval_mlflow_run_ids"].value == ["r1", "r2"]
    assert state["all_failure_question_ids"].value == ["q1"]
    assert state["scores"].source is HandoffSource.TASK_VALUES


def test_lever_loop_outputs_fall_back_to_delta():
    dbu = _make_dbutils({})
    spark = MagicMock()
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
            dbutils=dbu,
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


def test_lever_loop_outputs_raise_when_both_empty():
    dbu = _make_dbutils({})
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
                dbutils=dbu,
            )
