"""Routing gate terminates with invariant violation when evidence_kind is empty."""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord,
    DiagnosisRecord,
    HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformers.routing_gate import (
    routing_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def test_routing_gate_terminates_when_evidence_kind_empty():
    s = build_initial_state(
        qid="gs_009", iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
        diagnosed=DiagnosisRecord("plan11_stage1", "", "x", "x", "x", "high", "r"),  # empty rca_kind_label
    )
    s = s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "batch"),
        clustered=ClusterMembershipRecord("H001", "AG", ("gs_009",), 0, ""),
    )

    ctx = TransformerContext(
        iteration=1, run_id="r", validation_context=ValidationContext(1, "r", {}),
    )
    s2 = routing_gate.transform(s, ctx)
    assert s2.current_stage == FunnelStage.TERMINATED
    assert s2.terminal.reason == "routing_gate_empty_evidence_kind"
