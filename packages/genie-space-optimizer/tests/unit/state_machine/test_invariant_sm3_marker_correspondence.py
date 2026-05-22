"""SM3: every GSO_QSTATE_TRANSITION_V1 marker matches a recorded transition; no orphans."""
from genie_space_optimizer.optimization.state_machine.invariants_sm import (
    check_sm3_marker_correspondence,
)
from genie_space_optimizer.optimization.state_machine.records import StageTransition
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage


def test_sm3_clean_when_counts_match():
    transitions = (StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "a", "llm"),)
    markers = ({"qid": "gs_009", "from_stage": "hard_qid_seen", "to_stage": "diagnosed", "at_ms": 1},)
    assert check_sm3_marker_correspondence(transitions_by_qid={"gs_009": transitions}, markers=markers) == []


def test_sm3_violation_on_orphan_marker():
    markers = ({"qid": "gs_999", "from_stage": "hard_qid_seen", "to_stage": "diagnosed", "at_ms": 1},)
    violations = check_sm3_marker_correspondence(transitions_by_qid={}, markers=markers)
    assert len(violations) == 1
    assert violations[0]["invariant"] == "SM3"
