"""Stage 3 contract failure produces a terminal state with OPTIMIZER_INVARIANT_VIOLATION."""
from dataclasses import dataclass
from unittest.mock import patch

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord,
    DiagnosisRecord,
    HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import build_initial_state
from genie_space_optimizer.optimization.state_machine.transformers.synthesize_llm import (
    plan11_stage3_synthesis,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def test_missing_target_qids_in_proposal_terminates_invariant():
    @dataclass
    class _BadProposal:
        intent_id: str = "intent_xyz"
        patch_type: str = "add_sql_snippet_expression"
        target_objects: tuple = ("flights",)
        target_qids: tuple = ()              # MISSING — contract violation
        rca_card_id: str = "rca_1"
        causal_target: str = "ROW_NUMBER"
        original_patch_body: str = "ROW_NUMBER() OVER (...)"

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

    ctx = TransformerContext(iteration=1, run_id="r", validation_context=ValidationContext(1, "r", {}))
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.synthesize_llm._invoke_stage3_llm",
        return_value=_BadProposal(),
    ):
        s2 = plan11_stage3_synthesis.transform(s, ctx)
    assert s2.current_stage == FunnelStage.TERMINATED
    assert s2.terminal.kind == "OPTIMIZER_INVARIANT_VIOLATION"
    assert "target_qids" in s2.terminal.reason
