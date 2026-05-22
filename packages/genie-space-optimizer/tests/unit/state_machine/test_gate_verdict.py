"""GateVerdict captures the result of a ValidationGate predicate."""
import pytest

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ProposalAttempt,
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    GateVerdict,
)


def test_success_verdict_carries_optional_record():
    v = GateVerdict.success()
    assert v.passed is True
    assert v.success_record is None
    assert v.rejection_outcome is None


def test_reject_with_terminal_record():
    term = TerminalRecord("OPTIMIZER_STALLED_SAFE_NOOP", "r", FunnelStage.PROPOSED, "sig")
    v = GateVerdict.reject_terminal(term)
    assert v.passed is False
    assert v.rejection_outcome == term


def test_reject_with_proposal_attempt_for_escalation():
    pa = ProposalAttempt(0, "i", "p", FunnelStage.APPLYABLE, "applyability_rejected", "r")
    v = GateVerdict.reject_proposal(pa)
    assert v.passed is False
    assert v.rejection_outcome == pa


def test_cannot_construct_verdict_both_pass_and_reject():
    term = TerminalRecord("OPTIMIZER_NO_CANDIDATES", "r", FunnelStage.PROPOSED, "sig")
    with pytest.raises(ValueError, match="cannot be both"):
        GateVerdict(passed=True, success_record=None, rejection_outcome=term)
