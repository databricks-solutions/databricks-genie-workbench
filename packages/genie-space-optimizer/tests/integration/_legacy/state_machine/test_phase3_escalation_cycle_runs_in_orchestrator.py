"""When structural_repair_gate rejects, the orchestrator routes to escalation_ladder in the same step."""
import pytest

# SM Cutover Phase 3 (2026-05-23): escalation_ladder was quarantined to
# ``optimization/_legacy/``. The orchestrator no longer routes a declined
# structural_repair_gate to an in-SM escalation cascade — declined means
# terminate for the iteration. This test is kept as archival reference.
pytestmark = pytest.mark.skip(reason="legacy: escalation_ladder quarantined in SM Cutover Phase 3")

from dataclasses import dataclass
from unittest.mock import patch

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord, DiagnosisRecord, HardQidSeenRecord,
    ProposalAttempt, StageTransition,
)
from genie_space_optimizer.optimization.state_machine.registry import (
    build_production_state_machine,
)
from genie_space_optimizer.optimization.state_machine.state import build_initial_state
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext, ValidationContext,
)


def test_failed_structural_check_triggers_escalation_in_same_step():
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

    sm = build_production_state_machine()

    @dataclass
    class _Scoped:
        intent_id: str = "intent_a_scoped"
        patch_type: str = "add_sql_snippet_expression"
        target_objects: tuple = ("flights",)
        target_qids: tuple = ("gs_009",)
        rca_card_id: str = "r"
        causal_target: str = "ROW_NUMBER"
        original_patch_body: str = "ROW_NUMBER() OVER (PARTITION BY flights.flight_no ORDER BY COUNT(*) DESC)"

    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.structural_repair_gate."
        "_proposal_passes_structural_check",
        return_value=(False, "absent_anchor"),
    ), patch(
        "genie_space_optimizer.optimization._legacy.state_machine.transformers.escalation_ladder."
        "_invoke_rung_1_scoped_l6",
        return_value=_Scoped(),
    ):
        s2 = sm.step(s, TransformerContext(1, "r", ValidationContext(1, "r", {})))
    # After one orchestrator step:
    #   1) structural_repair_gate appended a structural_repair_rejected attempt (cycled to PROPOSED)
    #   2) escalation_ladder appended a new scoped L6 attempt; prior is marked 'escalated'
    assert s2.current_stage == FunnelStage.PROPOSED
    latest = s2.proposals[-1]
    assert latest.intent_id == "intent_a_scoped"
    # The structural_repair_rejected attempt should still be visible in trajectory
    # (now marked 'escalated' with a pointer to the scoped attempt).
    rej_or_escalated = [
        p for p in s2.proposals
        if p.outcome in ("structural_repair_rejected", "escalated")
    ]
    assert len(rej_or_escalated) >= 1
