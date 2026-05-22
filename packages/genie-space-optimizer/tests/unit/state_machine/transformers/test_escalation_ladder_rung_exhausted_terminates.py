"""Ladder exhaustion produces OPTIMIZER_STALLED_SAFE_NOOP terminal with forbidden_signature."""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord, DiagnosisRecord, HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import build_initial_state
from genie_space_optimizer.optimization.state_machine.transformers.escalation_ladder import (
    escalation_ladder,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext, ValidationContext,
)


def test_exhaustion_terminates_with_safe_noop_and_forbidden_signature():
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
    # No proposals — ladder cannot escalate.
    s = s.advance(FunnelStage.PROPOSED,
                  StageTransition(FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm"))
    s2 = escalation_ladder.transform(s, TransformerContext(1, "r", ValidationContext(1, "r", {})))
    assert s2.current_stage == FunnelStage.TERMINATED
    assert s2.terminal.kind == "OPTIMIZER_STALLED_SAFE_NOOP"
    # forbidden_signature uses {cluster_id}|{sorted-patch-types}|{qid}.
    assert s2.terminal.forbidden_signature.startswith("H001|")
    assert "gs_009" in s2.terminal.forbidden_signature
