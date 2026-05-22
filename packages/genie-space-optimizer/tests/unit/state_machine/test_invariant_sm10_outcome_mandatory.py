"""SM10: every run emits exactly one GSO_OPTIMIZER_OUTCOME_V1 marker."""
from genie_space_optimizer.optimization.state_machine.invariants_sm import (
    check_sm10_outcome_mandatory,
)


def test_sm10_clean_with_exactly_one_outcome():
    assert check_sm10_outcome_mandatory(outcome_marker_count=1) == []


def test_sm10_violation_with_zero():
    violations = check_sm10_outcome_mandatory(outcome_marker_count=0)
    assert len(violations) == 1
    assert violations[0]["invariant"] == "SM10"


def test_sm10_violation_with_multiple():
    violations = check_sm10_outcome_mandatory(outcome_marker_count=2)
    assert len(violations) == 1
    assert violations[0]["invariant"] == "SM10"
