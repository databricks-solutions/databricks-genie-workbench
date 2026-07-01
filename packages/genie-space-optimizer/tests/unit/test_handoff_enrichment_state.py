"""Tests for get_enrichment_state() — Delta-only enrichment read (D9).

D9 (Delta-only handoff): enrichment state is read straight from the
``eval_scope='enrichment'`` ``genie_opt_iterations`` row. Absence of that row is
a VALID state (enrichment was skipped), not an error. No task values consulted.
"""
from unittest.mock import MagicMock, patch

from genie_space_optimizer.jobs._handoff import (
    HandoffSource,
    get_enrichment_state,
)


def _hostile_dbutils():
    """dbutils whose taskValues probe raises the retired-key crash if called."""
    dbu = MagicMock()
    dbu.jobs.taskValues.get.side_effect = ValueError(
        "Task key does not exist in run: enrichment"
    )
    return dbu


def test_enrichment_state_reads_from_delta():
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
        )
    assert state["enrichment_model_id"].value == "enr-m-001"
    assert state["enrichment_model_id"].source is HandoffSource.DELTA_FALLBACK
    assert state["enrichment_skipped"].value is False
    assert state["post_enrichment_accuracy"].value == 87.5
    assert state["post_enrichment_scores"].value == {"x": 1}
    assert state["post_enrichment_thresholds_met"].value is False


def test_enrichment_skipped_when_no_row_present():
    """Absence of an enrichment row is a VALID state — enrichment was skipped.

    This must NOT raise. enrichment_skipped=True, all post_* values are MISSING.
    """
    spark = MagicMock()
    with patch(
        "genie_space_optimizer.jobs._handoff._load_enrichment_iteration_row",
        return_value=None,
    ):
        state = get_enrichment_state(
            spark, run_id="run-001", catalog="cat", schema="sch",
        )
    assert state["enrichment_skipped"].value is True
    assert state["enrichment_skipped"].source is HandoffSource.DELTA_FALLBACK
    assert state["enrichment_model_id"].value is None
    assert state["enrichment_model_id"].source is HandoffSource.MISSING
    assert state["post_enrichment_accuracy"].source is HandoffSource.MISSING


def test_enrichment_state_does_not_probe_task_values():
    """Regression for the v2 crash: a hostile dbutils whose taskValues probe
    raises must not propagate — enrichment state comes from Delta only."""
    spark = MagicMock()
    dbu = _hostile_dbutils()
    with patch(
        "genie_space_optimizer.jobs._handoff._load_enrichment_iteration_row",
        return_value=None,
    ):
        state = get_enrichment_state(
            spark, run_id="run-001", catalog="cat", schema="sch", dbutils=dbu,
        )
    assert state["enrichment_skipped"].value is True
    dbu.jobs.taskValues.get.assert_not_called()
