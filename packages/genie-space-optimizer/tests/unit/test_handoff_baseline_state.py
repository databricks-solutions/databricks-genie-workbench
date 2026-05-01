"""Tests for get_baseline_eval_state() — reads baseline_eval state with Delta fallback."""
import json
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.jobs._handoff import (
    HandoffSource,
    get_baseline_eval_state,
)


def _make_dbutils(values):
    dbu = MagicMock()
    def _get(taskKey, key, default=""):
        return values.get((taskKey, key), default)
    dbu.jobs.taskValues.get.side_effect = _get
    return dbu


def test_baseline_state_happy_path_uses_task_values():
    dbu = _make_dbutils({
        ("baseline_eval", "scores"): json.dumps({"syntax_validity": 95.0}),
        ("baseline_eval", "overall_accuracy"): "85.7",
        ("baseline_eval", "thresholds_met"): "false",
        ("baseline_eval", "model_id"): "m-abc",
        ("baseline_eval", "mlflow_run_id"): "mr-001",
    })
    spark = MagicMock()
    state = get_baseline_eval_state(
        spark, run_id="run-001", catalog="cat", schema="sch", dbutils=dbu,
    )
    assert state["scores"].value == {"syntax_validity": 95.0}
    assert state["scores"].source is HandoffSource.TASK_VALUES
    assert state["overall_accuracy"].value == 85.7
    assert state["thresholds_met"].value is False
    assert state["model_id"].value == "m-abc"


def test_baseline_state_falls_back_to_delta_iteration_zero():
    dbu = _make_dbutils({})
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
            dbutils=dbu,
        )
    assert state["scores"].value == {"syntax_validity": 95.0}
    assert state["scores"].source is HandoffSource.DELTA_FALLBACK
    assert state["overall_accuracy"].value == 85.7
    assert state["thresholds_met"].value is False
    assert state["model_id"].value == "m-abc"


def test_baseline_state_raises_when_task_values_and_delta_both_empty():
    dbu = _make_dbutils({})
    spark = MagicMock()
    with patch(
        "genie_space_optimizer.jobs._handoff._load_baseline_iteration_row",
        return_value=None,
    ):
        with pytest.raises(RuntimeError, match="baseline"):
            get_baseline_eval_state(
                spark, run_id="run-001", catalog="cat", schema="sch",
                dbutils=dbu,
            )
