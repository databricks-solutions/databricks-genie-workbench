"""target_fixed && no collateral regressions → ACCEPTED."""
from unittest.mock import patch

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    AppliedRecord, ClusterMembershipRecord, DiagnosisRecord, EvaluatedRecord,
    HardQidSeenRecord, ProposalAttempt, StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import build_initial_state
from genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate import (
    acceptance_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext, ValidationContext,
)


def _state_at_evaluated(*, post_score: float):
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
         {"applied": AppliedRecord(1, "c", 0, ("i",))}),
        (FunnelStage.APPLIED, FunnelStage.EVALUATED,
         {"evaluated": EvaluatedRecord(0.0, post_score, "SELECT 1", "SELECT 2", "rp")}),
    ):
        s = s.advance(to_s, StageTransition(from_s, to_s, 1, "t", "validation_gate"), **kw)
    return s


def test_target_fixed_no_regression_accepts():
    s = _state_at_evaluated(post_score=1.0)
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(s, TransformerContext(1, "r", ValidationContext(1, "r", {})))
    assert s2.current_stage == FunnelStage.ACCEPTED
    assert s2.accepted is not None
    assert s2.accepted.decision == "accepted"
    assert s2.accepted.target_fixed is True
    assert s2.accepted.collateral_regressions == ()
