"""FunnelStage enum shape and ordering."""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage


def test_funnel_stage_values():
    assert FunnelStage.HARD_QID_SEEN.value == "hard_qid_seen"
    assert FunnelStage.DIAGNOSED.value == "diagnosed"
    assert FunnelStage.CLUSTERED.value == "clustered"
    assert FunnelStage.PROPOSED.value == "proposed"
    assert FunnelStage.NORMALIZED.value == "normalized"
    assert FunnelStage.APPLYABLE.value == "applyable"
    assert FunnelStage.APPLIED.value == "applied"
    assert FunnelStage.EVALUATED.value == "evaluated"
    assert FunnelStage.ACCEPTED.value == "accepted"
    assert FunnelStage.TERMINATED.value == "terminated"


def test_funnel_stage_ordering_index():
    from genie_space_optimizer.optimization.state_machine.funnel import stage_index
    assert stage_index(FunnelStage.HARD_QID_SEEN) == 0
    assert stage_index(FunnelStage.ACCEPTED) == 8
    assert stage_index(FunnelStage.TERMINATED) == 9


def test_funnel_stage_is_terminal_predicate():
    from genie_space_optimizer.optimization.state_machine.funnel import is_terminal
    assert is_terminal(FunnelStage.TERMINATED) is True
    assert is_terminal(FunnelStage.ACCEPTED) is False
    assert is_terminal(FunnelStage.PROPOSED) is False
