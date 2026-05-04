"""Productive-iteration budget accounting (Cycle 5 T1).

When an iteration applies zero patches AND the no-op cause is a typed
P4 outcome (``proposal_generation_empty``,
``structural_gate_dropped_instruction_only``, ``no_structural_candidate``),
it does not consume iteration budget by default. Guarded by
``GSO_PRODUCTIVE_ITERATION_BUDGET`` (default off).
"""
from __future__ import annotations

import os
from unittest.mock import patch


def test_flag_helper_default_off() -> None:
    from genie_space_optimizer.common.config import (
        productive_iteration_budget_enabled,
    )
    with patch.dict(os.environ, {}, clear=True):
        assert productive_iteration_budget_enabled() is False


def test_flag_helper_on_when_env_set() -> None:
    from genie_space_optimizer.common.config import (
        productive_iteration_budget_enabled,
    )
    with patch.dict(
        os.environ, {"GSO_PRODUCTIVE_ITERATION_BUDGET": "1"}, clear=True,
    ):
        assert productive_iteration_budget_enabled() is True


def test_iteration_budget_decision_type_exists() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
    )
    assert DecisionType.ITERATION_BUDGET_DECISION.value == "iteration_budget_decision"


def test_iteration_budget_reason_codes_exist() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )
    assert ReasonCode.ITERATION_BUDGET_CONSUMED.value == "iteration_budget_consumed"
    assert ReasonCode.ITERATION_BUDGET_SKIPPED_NO_OP.value == "iteration_budget_skipped_no_op"
    assert ReasonCode.ITERATION_BUDGET_STRATEGY_SWITCH.value == "iteration_budget_strategy_switch"


def test_iteration_budget_decision_record_consumed_path() -> None:
    """The behavioral path emits a typed record when the iteration
    consumed budget (default behavior, flag off)."""
    from genie_space_optimizer.optimization.decision_emitters import (
        iteration_budget_decision_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )
    rec = iteration_budget_decision_record(
        run_id="run-x",
        iteration=3,
        consumed=True,
        no_op_cause=None,
        applied_patches=2,
    )
    assert rec.decision_type == DecisionType.ITERATION_BUDGET_DECISION
    assert rec.outcome == DecisionOutcome.INFO
    assert rec.reason_code == ReasonCode.ITERATION_BUDGET_CONSUMED
    assert rec.metrics["applied_patches"] == 2


def test_iteration_budget_decision_record_skipped_path() -> None:
    """Different record when the iteration was skipped (flag on,
    deterministic no-op, no budget consumed)."""
    from genie_space_optimizer.optimization.decision_emitters import (
        iteration_budget_decision_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        ReasonCode,
    )
    rec = iteration_budget_decision_record(
        run_id="run-x",
        iteration=3,
        consumed=False,
        no_op_cause="proposal_generation_empty",
        applied_patches=0,
    )
    assert rec.outcome == DecisionOutcome.INFO
    assert rec.reason_code == ReasonCode.ITERATION_BUDGET_SKIPPED_NO_OP
    assert rec.metrics["no_op_cause"] == "proposal_generation_empty"
    assert rec.metrics["applied_patches"] == 0


def test_iteration_budget_marker_round_trip() -> None:
    from genie_space_optimizer.common.mlflow_markers import (
        iteration_budget_marker,
    )
    from genie_space_optimizer.tools.marker_parser import (
        parse_iteration_budget_marker,
    )
    line = iteration_budget_marker(
        optimization_run_id="run-x",
        iteration=3,
        consumed=False,
        no_op_cause="proposal_generation_empty",
        applied_patches=0,
        iteration_counter_after=2,
    )
    parsed = parse_iteration_budget_marker(line)
    assert parsed["consumed"] is False
    assert parsed["no_op_cause"] == "proposal_generation_empty"
    assert parsed["iteration_counter_after"] == 2


def test_classify_iteration_no_op_cause_finds_typed_outcome() -> None:
    from genie_space_optimizer.optimization.harness import (
        _classify_iteration_no_op_cause,
    )
    records = [
        {"reason_code": "rca_groundedness_dropped", "decision_type": "gate_decision"},
        {"reason_code": "structural_gate_dropped_instruction_only",
         "decision_type": "gate_decision"},
        {"reason_code": "passing_hold", "decision_type": "qid_resolution"},
    ]
    assert (
        _classify_iteration_no_op_cause(records)
        == "structural_gate_dropped_instruction_only"
    )


def test_classify_iteration_no_op_cause_returns_empty_when_no_typed() -> None:
    from genie_space_optimizer.optimization.harness import (
        _classify_iteration_no_op_cause,
    )
    assert _classify_iteration_no_op_cause([]) == ""
    assert _classify_iteration_no_op_cause([
        {"reason_code": "passing_hold", "decision_type": "qid_resolution"},
    ]) == ""
