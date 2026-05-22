"""Rung 4 fires only when prior rungs are exhausted (≥3 attempts) and outcome is still rejected."""
from dataclasses import dataclass
from unittest.mock import patch

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord, DiagnosisRecord, HardQidSeenRecord,
    ProposalAttempt, StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import build_initial_state
from genie_space_optimizer.optimization.state_machine.transformers.escalation_ladder import (
    escalation_ladder,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext, ValidationContext,
)


def _state_with_three_attempts_last_rejected():
    s = build_initial_state(
        qid="gs_009", iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1),
    )
    s = s.advance(FunnelStage.DIAGNOSED,
                  StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
                  diagnosed=DiagnosisRecord("plan11_stage1", "k", "s", "f", "e", "high", "r"))
    s = s.advance(FunnelStage.CLUSTERED,
                  StageTransition(FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "batch"),
                  clustered=ClusterMembershipRecord("H1", "AG", ("gs_009",), 6, "k"))
    return s.advance(FunnelStage.PROPOSED,
                     StageTransition(FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm"),
                     proposals=(
                         ProposalAttempt(0, "i_a", "add_sql_snippet_expression",
                                         FunnelStage.PROPOSED, "escalated", "x",
                                         escalated_to_attempt_index=1),
                         ProposalAttempt(1, "i_b", "add_sql_snippet_expression",
                                         FunnelStage.PROPOSED, "escalated", "x",
                                         escalated_to_attempt_index=2),
                         ProposalAttempt(2, "i_c", "add_example_sql",
                                         FunnelStage.PROPOSED, "applyability_rejected", "x"),
                     ))


def test_rung_4_chosen_at_attempt_count_three():
    s = _state_with_three_attempts_last_rejected()
    @dataclass
    class _NarrowedEx:
        intent_id: str = "intent_narrowed_example"
        patch_type: str = "add_example_sql"
        target_objects: tuple = ()
        target_qids: tuple = ("gs_009",)
        rca_card_id: str = "r"
        causal_target: str = ""
        original_patch_body: str = "SELECT ..."

    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.escalation_ladder._invoke_rung_4_narrowed_example_sql",
        return_value=_NarrowedEx(),
    ):
        s2 = escalation_ladder.transform(s, TransformerContext(1, "r", ValidationContext(1, "r", {})))
    assert s2.proposals[-1].intent_id == "intent_narrowed_example"
