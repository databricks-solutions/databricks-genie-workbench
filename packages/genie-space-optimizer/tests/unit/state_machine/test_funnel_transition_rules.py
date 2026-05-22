"""Funnel transition rules: forward-only with escalation cycle to PROPOSED."""
import pytest

from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
    is_legal_transition,
)


def test_forward_transitions_legal():
    assert is_legal_transition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED)
    assert is_legal_transition(FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED)
    assert is_legal_transition(FunnelStage.NORMALIZED, FunnelStage.APPLYABLE)
    assert is_legal_transition(FunnelStage.APPLYABLE, FunnelStage.APPLIED)


def test_skipping_stages_illegal():
    assert not is_legal_transition(FunnelStage.PROPOSED, FunnelStage.APPLIED)
    assert not is_legal_transition(FunnelStage.HARD_QID_SEEN, FunnelStage.PROPOSED)


def test_backward_transitions_illegal_except_escalation_to_proposed():
    # Backward to PROPOSED from any rejection stage is the escalation cycle.
    assert is_legal_transition(FunnelStage.NORMALIZED, FunnelStage.PROPOSED)
    assert is_legal_transition(FunnelStage.APPLYABLE, FunnelStage.PROPOSED)
    # Backward to any non-PROPOSED stage is illegal.
    assert not is_legal_transition(FunnelStage.APPLYABLE, FunnelStage.DIAGNOSED)
    assert not is_legal_transition(FunnelStage.ACCEPTED, FunnelStage.PROPOSED)


def test_any_stage_may_terminate():
    for source in FunnelStage:
        if source == FunnelStage.TERMINATED:
            continue
        assert is_legal_transition(source, FunnelStage.TERMINATED)


def test_terminated_is_absorbing():
    for target in FunnelStage:
        if target == FunnelStage.TERMINATED:
            continue
        assert not is_legal_transition(FunnelStage.TERMINATED, target)


def test_same_stage_decoration_allowed():
    """Decoration gates (e.g., routing at CLUSTERED) can transition to the same stage."""
    assert is_legal_transition(FunnelStage.CLUSTERED, FunnelStage.CLUSTERED)
    assert is_legal_transition(FunnelStage.PROPOSED, FunnelStage.PROPOSED)


def test_same_stage_terminated_still_blocked():
    """TERMINATED is absorbing; even same-stage transitions out of it are illegal."""
    assert not is_legal_transition(FunnelStage.TERMINATED, FunnelStage.TERMINATED)
