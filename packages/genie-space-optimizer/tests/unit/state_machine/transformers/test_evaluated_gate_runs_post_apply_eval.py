"""Evaluated gate runs post-apply eval on the target QID and writes EvaluatedRecord."""
from unittest.mock import patch

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    AppliedRecord, ClusterMembershipRecord, DiagnosisRecord, HardQidSeenRecord,
    ProposalAttempt, StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import build_initial_state
from genie_space_optimizer.optimization.state_machine.transformers.evaluated_gate import (
    evaluated_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext, ValidationContext,
)


def _state_at_applied():
    s = build_initial_state(
        qid="gs_009", iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "SELECT 1", "x", 1),
    )
    for from_s, to_s, kw in (
        (FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED,
         {"diagnosed": DiagnosisRecord("plan11_stage1", "k", "s", "f", "e", "high", "r")}),
        (FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED,
         {"clustered": ClusterMembershipRecord("H1", "AG", ("gs_009",), 6, "k")}),
        (FunnelStage.CLUSTERED, FunnelStage.PROPOSED,
         {"proposals": (ProposalAttempt(0, "i", "p", FunnelStage.APPLIED, "applied", "ok"),)}),
        (FunnelStage.PROPOSED, FunnelStage.NORMALIZED, {}),
        (FunnelStage.NORMALIZED, FunnelStage.APPLYABLE, {}),
        (FunnelStage.APPLYABLE, FunnelStage.APPLIED,
         {"applied": AppliedRecord(1, "call_abc", 0, ("i",))}),
    ):
        s = s.advance(to_s, StageTransition(from_s, to_s, 1, "t", "validation_gate"), **kw)
    return s


def test_evaluated_gate_writes_pre_and_post_scores():
    s = _state_at_applied()
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.evaluated_gate._run_post_apply_eval",
        return_value=(1.0, "SELECT 2", "row_post_1"),
    ):
        s2 = evaluated_gate.transform(s, TransformerContext(1, "r", ValidationContext(1, "r", {})))
    assert s2.current_stage == FunnelStage.EVALUATED
    assert s2.evaluated is not None
    assert s2.evaluated.pre_apply_score == 0.0
    assert s2.evaluated.post_apply_score == 1.0
