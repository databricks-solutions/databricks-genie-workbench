"""Rung 3: applyability_rejected → add_example_sql proposal."""

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
    ClusterMembershipRecord, DiagnosisRecord, HardQidSeenRecord,
    ProposalAttempt, StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import build_initial_state
from genie_space_optimizer.optimization._legacy.state_machine.transformers.escalation_ladder import (
    escalation_ladder,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext, ValidationContext,
)


def _proposed_with_applyability_rejection():
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
    s = s.advance(FunnelStage.PROPOSED,
                  StageTransition(FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm"),
                  proposals=(ProposalAttempt(0, "i_a", "p", FunnelStage.PROPOSED, "applied", "x"),))
    rejected = ProposalAttempt(0, "i_a", "p", FunnelStage.APPLYABLE,
                               "applyability_rejected", "apply_failure")
    # Same-stage decoration attaches the rejection attempt without
    # modelling the APPLYABLE→PROPOSED cycle (the ladder only reads the
    # latest attempt's outcome).
    return s.advance(FunnelStage.PROPOSED,
                     StageTransition(FunnelStage.PROPOSED, FunnelStage.PROPOSED, 5,
                                     "applier", "validation_gate"),
                     proposals=s.proposals + (rejected,))


def test_rung_3_selected_after_applyability_rejection():
    s = _proposed_with_applyability_rejection()
    @dataclass
    class _ExSqlProposal:
        intent_id: str = "intent_example_a"
        patch_type: str = "add_example_sql"
        target_objects: tuple = ()
        target_qids: tuple = ("gs_009",)
        rca_card_id: str = "r"
        causal_target: str = ""
        original_patch_body: str = "SELECT origin_city, COUNT(*) c, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) r FROM flights ..."

    with patch(
        "genie_space_optimizer.optimization._legacy.state_machine.transformers.escalation_ladder._invoke_rung_3_add_example_sql",
        return_value=_ExSqlProposal(),
    ):
        s2 = escalation_ladder.transform(s, TransformerContext(1, "r", ValidationContext(1, "r", {})))

    assert s2.proposals[-1].patch_type == "add_example_sql"
    assert s2.proposals[-1].intent_id == "intent_example_a"
    # Prior attempt was marked 'escalated' with pointer.
    assert s2.proposals[-2].outcome == "escalated"
    assert s2.proposals[-2].escalated_to_attempt_index == s2.proposals[-1].attempt_index
