"""Applier gate calls applier.apply_patch and writes AppliedRecord on success."""
from unittest.mock import patch

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord,
    DiagnosisRecord,
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import build_initial_state
from genie_space_optimizer.optimization.state_machine.transformers.applier_gate import (
    applier_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _state_at_applyable():
    s = build_initial_state(
        qid="gs_009", iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1),
    )
    s = s.advance(FunnelStage.DIAGNOSED,
                  StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
                  diagnosed=DiagnosisRecord("plan11_stage1", "k", "s", "f", "e", "high", "r"))
    s = s.advance(FunnelStage.CLUSTERED,
                  StageTransition(FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "batch"),
                  clustered=ClusterMembershipRecord("H001", "AG_1", ("gs_009",), 6, "k"))
    s = s.advance(FunnelStage.PROPOSED,
                  StageTransition(FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm"),
                  proposals=(ProposalAttempt(0, "intent_xyz", "add_sql_snippet_expression",
                                             FunnelStage.PROPOSED, "applied", "pending_gates"),))
    s = s.advance(FunnelStage.NORMALIZED,
                  StageTransition(FunnelStage.PROPOSED, FunnelStage.NORMALIZED, 4, "structural", "validation_gate"))
    return s.advance(FunnelStage.APPLYABLE,
                     StageTransition(FunnelStage.NORMALIZED, FunnelStage.APPLYABLE, 5, "blast", "batch"))


def test_applier_success_advances_to_applied_with_record():
    s = _state_at_applyable()
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.applier_gate._apply_via_genie_api",
        return_value=("apply_call_abc", True, ""),
    ):
        s2 = applier_gate.transform(
            s, TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    assert s2.current_stage == FunnelStage.APPLIED
    assert s2.applied is not None
    assert s2.applied.apply_call_id == "apply_call_abc"
    assert "intent_xyz" in s2.applied.applied_intent_ids
