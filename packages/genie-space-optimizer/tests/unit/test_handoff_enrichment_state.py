"""Tests for get_enrichment_state()."""
import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.jobs._handoff import (
    HandoffSource,
    get_enrichment_state,
)


def _make_dbutils(values):
    dbu = MagicMock()
    def _get(taskKey, key, default=""):
        return values.get((taskKey, key), default)
    dbu.jobs.taskValues.get.side_effect = _get
    return dbu


def test_enrichment_state_happy_path_uses_task_values():
    dbu = _make_dbutils({
        ("enrichment", "enrichment_model_id"): "enr-m-001",
        ("enrichment", "enrichment_skipped"): "false",
        ("enrichment", "post_enrichment_accuracy"): "87.5",
        ("enrichment", "post_enrichment_scores"): json.dumps({"x": 1}),
        ("enrichment", "post_enrichment_model_id"): "post-m-001",
        ("enrichment", "post_enrichment_thresholds_met"): "false",
    })
    spark = MagicMock()
    state = get_enrichment_state(
        spark, run_id="run-001", catalog="cat", schema="sch", dbutils=dbu,
    )
    assert state["enrichment_model_id"].value == "enr-m-001"
    assert state["enrichment_skipped"].value is False
    assert state["post_enrichment_accuracy"].value == 87.5
    assert state["post_enrichment_scores"].value == {"x": 1}
    assert state["post_enrichment_thresholds_met"].value is False
    assert state["enrichment_model_id"].source is HandoffSource.TASK_VALUES


def test_enrichment_state_falls_back_to_delta_when_task_values_empty():
    dbu = _make_dbutils({})
    spark = MagicMock()
    fake_row = {
        "iteration": 0,
        "eval_scope": "enrichment",
        "model_id": "enr-m-001",
        "overall_accuracy": 87.5,
        "scores_json": {"x": 1},
        "thresholds_met": False,
    }
    with patch(
        "genie_space_optimizer.jobs._handoff._load_enrichment_iteration_row",
        return_value=fake_row,
    ):
        state = get_enrichment_state(
            spark, run_id="run-001", catalog="cat", schema="sch",
            dbutils=dbu,
        )
    assert state["enrichment_model_id"].value == "enr-m-001"
    assert state["enrichment_model_id"].source is HandoffSource.DELTA_FALLBACK
    assert state["enrichment_skipped"].value is False
    assert state["post_enrichment_accuracy"].value == 87.5
    assert state["post_enrichment_scores"].value == {"x": 1}


def test_enrichment_skipped_when_no_row_present():
    """Absence of an enrichment row is a VALID state — enrichment was skipped.

    This must NOT raise. enrichment_skipped=True, all post_* values are MISSING.
    """
    dbu = _make_dbutils({})
    spark = MagicMock()
    with patch(
        "genie_space_optimizer.jobs._handoff._load_enrichment_iteration_row",
        return_value=None,
    ):
        state = get_enrichment_state(
            spark, run_id="run-001", catalog="cat", schema="sch",
            dbutils=dbu,
        )
    assert state["enrichment_skipped"].value is True
    assert state["enrichment_skipped"].source is HandoffSource.DELTA_FALLBACK
    assert state["enrichment_model_id"].value is None
    assert state["enrichment_model_id"].source is HandoffSource.MISSING
    assert state["post_enrichment_accuracy"].source is HandoffSource.MISSING


def test_enrichment_state_explicit_skipped_taskvalue_wins():
    """When the operator explicitly set enrichment_skipped=true via
    taskValues, do not query Delta to "second-guess" — trust the signal."""
    dbu = _make_dbutils({
        ("enrichment", "enrichment_skipped"): "true",
    })
    spark = MagicMock()
    with patch(
        "genie_space_optimizer.jobs._handoff._load_enrichment_iteration_row",
    ) as load_mock:
        state = get_enrichment_state(
            spark, run_id="run-001", catalog="cat", schema="sch",
            dbutils=dbu,
        )
    load_mock.assert_not_called()
    assert state["enrichment_skipped"].value is True
    assert state["enrichment_skipped"].source is HandoffSource.TASK_VALUES
