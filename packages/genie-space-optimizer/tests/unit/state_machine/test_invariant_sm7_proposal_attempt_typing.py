"""SM7: ProposalAttempt.escalated_to_attempt_index is set iff outcome == 'escalated'."""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.invariants_sm import (
    check_sm7_proposal_attempt_typing,
)
from genie_space_optimizer.optimization.state_machine.records import ProposalAttempt


def test_sm7_clean_when_escalated_has_index():
    pa = ProposalAttempt(0, "i", "p", FunnelStage.APPLYABLE, "escalated", "r", escalated_to_attempt_index=1)
    assert check_sm7_proposal_attempt_typing(proposals=(pa,)) == []


def test_sm7_violation_when_escalated_missing_index():
    pa = ProposalAttempt(0, "i", "p", FunnelStage.APPLYABLE, "escalated", "r")
    violations = check_sm7_proposal_attempt_typing(proposals=(pa,))
    assert len(violations) == 1
    assert violations[0]["invariant"] == "SM7"


def test_sm7_violation_when_non_escalated_has_index():
    pa = ProposalAttempt(0, "i", "p", FunnelStage.APPLIED, "applied", "r", escalated_to_attempt_index=1)
    violations = check_sm7_proposal_attempt_typing(proposals=(pa,))
    assert len(violations) == 1
    assert violations[0]["invariant"] == "SM7"
