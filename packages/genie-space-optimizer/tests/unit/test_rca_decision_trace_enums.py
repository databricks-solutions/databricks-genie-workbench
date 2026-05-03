"""Pin enum values so renames are CI-detected."""
from __future__ import annotations

from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionType,
    ReasonCode,
)


def test_decision_type_includes_ag_retired():
    assert DecisionType.AG_RETIRED.value == "ag_retired"


def test_decision_outcome_includes_retired():
    assert DecisionOutcome.RETIRED.value == "retired"


def test_reason_code_includes_ag_target_no_longer_hard():
    assert ReasonCode.AG_TARGET_NO_LONGER_HARD.value == "ag_target_no_longer_hard"
