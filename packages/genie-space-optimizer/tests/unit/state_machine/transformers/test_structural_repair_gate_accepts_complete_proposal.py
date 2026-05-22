"""Structural repair gate advances complete RepairProposal to NORMALIZED."""
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
from genie_space_optimizer.optimization.state_machine.transformers.structural_repair_gate import (
    structural_repair_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _state_at_proposed():
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
    return s.advance(FunnelStage.PROPOSED,
                     StageTransition(FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm"),
                     proposals=(ProposalAttempt(0, "intent_xyz", "add_sql_snippet_expression",
                                                FunnelStage.PROPOSED, "applied", "pending_gates"),))


def test_complete_proposal_passes_to_normalized():
    s = _state_at_proposed()
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.structural_repair_gate."
        "_proposal_passes_structural_check",
        return_value=(True, ""),
    ):
        s2 = structural_repair_gate.transform(
            s, TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    assert s2.current_stage == FunnelStage.NORMALIZED
