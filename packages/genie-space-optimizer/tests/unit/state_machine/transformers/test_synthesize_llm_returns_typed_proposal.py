"""Stage 3 transformer wraps stages/synthesize.py and returns a RepairProposal."""
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


def _state_at_clustered():
    s = build_initial_state(
        qid="gs_009", iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1),
    )
    s = s.advance(FunnelStage.DIAGNOSED,
                  StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
                  diagnosed=DiagnosisRecord("plan11_stage1", "plural_top_n_collapse", "s", "f", "ROW_NUMBER", "high", "rca_1"))
    s = s.advance(FunnelStage.CLUSTERED,
                  StageTransition(FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "batch"),
                  clustered=ClusterMembershipRecord("H001", "AG_1", ("gs_009",), 6, "plural_top_n_collapse"))
    return s


def test_proposal_attempt_appended_to_state_proposals():
    @dataclass
    class _RepairProposalStub:
        intent_id: str = "intent_xyz"
        patch_type: str = "add_sql_snippet_expression"
        target_objects: tuple = ("flights",)
        target_qids: tuple = ("gs_009",)
        rca_card_id: str = "rca_1"
        causal_target: str = "ROW_NUMBER"
        original_patch_body: str = "ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank"

    s = _state_at_clustered()
    ctx = TransformerContext(iteration=1, run_id="r", validation_context=ValidationContext(1, "r", {}))
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.synthesize_llm._invoke_stage3_llm",
        return_value=_RepairProposalStub(),
    ):
        s2 = plan11_stage3_synthesis.transform(s, ctx)
    assert s2.current_stage == FunnelStage.PROPOSED
    assert len(s2.proposals) == 1
    pa = s2.proposals[0]
    assert pa.intent_id == "intent_xyz"
    assert pa.patch_type == "add_sql_snippet_expression"
    assert pa.attempt_index == 0
