"""Blast-radius batch advances target+collateral to APPLYABLE; rejection cycles target to PROPOSED."""
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
from genie_space_optimizer.optimization.state_machine.transformers.blast_radius_batch import (
    blast_radius_batch,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _state_at_normalized(qid: str):
    s = build_initial_state(
        qid=qid, iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1),
    )
    s = s.advance(FunnelStage.DIAGNOSED,
                  StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
                  diagnosed=DiagnosisRecord("plan11_stage1", "k", "s", "f", "e", "high", "r"))
    s = s.advance(FunnelStage.CLUSTERED,
                  StageTransition(FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "batch"),
                  clustered=ClusterMembershipRecord("H001", "AG_1", (qid,), 6, "k"))
    s = s.advance(FunnelStage.PROPOSED,
                  StageTransition(FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm"),
                  proposals=(ProposalAttempt(0, "intent_xyz", "add_sql_snippet_filter",
                                             FunnelStage.PROPOSED, "applied", "pending_gates"),))
    return s.advance(FunnelStage.NORMALIZED,
                     StageTransition(FunnelStage.PROPOSED, FunnelStage.NORMALIZED, 4, "structural", "validation_gate"))


def test_safe_batch_advances_target_to_applyable():
    s = _state_at_normalized("gs_009")
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.blast_radius_batch._assess_blast_radius",
        return_value=("safe", None),
    ):
        out = blast_radius_batch.transform_batch(
            (s,), TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    assert out[0].current_stage == FunnelStage.APPLYABLE


def test_rejected_batch_cycles_target_back_to_proposed_with_typed_attempt():
    s = _state_at_normalized("gs_024")
    from genie_space_optimizer.optimization.blast_radius_drop_record import (
        BlastRadiusDropRecord,
    )
    drop = BlastRadiusDropRecord(
        intent_id="intent_xyz",
        original_patch_type="add_sql_snippet_filter",
        original_patch_body={"sql_expression": "PAYMENT_CURRENCY_CD = 'USD'"},
        causal_target="PAYMENT_CURRENCY_CD",
        failing_sql_anchor="tkt_payment.payment_amt",
        target_qids=("gs_024",),
        collateral_qids=("gs_003",),
        protected_sql_by_qid={"gs_003": "SELECT ..."},
        rca_card_id="rca_24",
        cluster_id="H001",
        ag_id="AG_24",
    )
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.blast_radius_batch._assess_blast_radius",
        return_value=("reject", drop),
    ):
        out = blast_radius_batch.transform_batch(
            (s,), TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    assert out[0].current_stage == FunnelStage.PROPOSED
    last = out[0].proposals[-1]
    assert last.outcome == "blast_radius_rejected"
