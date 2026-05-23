"""Rung 1: a structural_repair_rejected proposal becomes a scoped L6 candidate."""

import pytest

# SM Cutover Phase 3 (2026-05-23): routing_gate and escalation_ladder
# transformers were quarantined to ``optimization/_legacy/`` because the
# production state machine no longer mimics the legacy lever-cascade
# escalation inside the SM. These tests are kept for archival reference
# but are excluded from default test runs.
pytestmark = pytest.mark.skip(reason="legacy: routing_gate/escalation_ladder quarantined in SM Cutover Phase 3")

from dataclasses import dataclass
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
from genie_space_optimizer.optimization._legacy.state_machine.transformers.escalation_ladder import (
    escalation_ladder,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _proposed_with_structural_rejection():
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
                  proposals=(ProposalAttempt(0, "intent_a", "add_sql_snippet_expression",
                                             FunnelStage.PROPOSED, "applied", "in_flight"),))
    rejected = ProposalAttempt(
        attempt_index=0, intent_id="intent_a", patch_type="add_sql_snippet_expression",
        deepest_stage_in_attempt=FunnelStage.PROPOSED,
        outcome="structural_repair_rejected", outcome_reason="absent_anchor",
    )
    return s.advance(
        FunnelStage.PROPOSED,  # cycle back (same-stage decoration)
        StageTransition(FunnelStage.PROPOSED, FunnelStage.PROPOSED, 4, "structural", "validation_gate"),
        proposals=s.proposals + (rejected,),
    )


def test_rung_1_produces_scoped_l6_attempt():
    s = _proposed_with_structural_rejection()
    @dataclass
    class _ScopedProposal:
        intent_id: str = "intent_a_scoped"
        patch_type: str = "add_sql_snippet_expression"
        target_objects: tuple = ("flights",)
        target_qids: tuple = ("gs_009",)
        rca_card_id: str = "r"
        causal_target: str = "ROW_NUMBER"
        original_patch_body: str = "ROW_NUMBER() OVER (PARTITION BY flights.flight_no ORDER BY COUNT(*) DESC)"

    with patch(
        "genie_space_optimizer.optimization._legacy.state_machine.transformers.escalation_ladder._invoke_rung_1_scoped_l6",
        return_value=_ScopedProposal(),
    ):
        s2 = escalation_ladder.transform(
            s, TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    assert s2.current_stage == FunnelStage.PROPOSED
    last = s2.proposals[-1]
    assert last.intent_id == "intent_a_scoped"
    assert last.attempt_index == 2  # 0=initial, 1=rejected, 2=scoped
