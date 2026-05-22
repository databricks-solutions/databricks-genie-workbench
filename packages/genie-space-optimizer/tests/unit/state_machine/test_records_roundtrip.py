"""All stage records round-trip through JsonRoundTrip without loss."""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
    AppliedRecord,
    ClusterMembershipRecord,
    DiagnosisRecord,
    EvaluatedRecord,
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
    TerminalRecord,
)


def test_hard_qid_seen_record_roundtrip():
    rec = HardQidSeenRecord(
        eval_row_id="row_42",
        predicate="row_is_hard_failure",
        score=0.0,
        baseline_sql="SELECT 1",
        expected_shape="ROW_NUMBER over COUNT(*)",
        iteration_first_seen=1,
    )
    assert HardQidSeenRecord.from_json(rec.to_json()) == rec


def test_diagnosis_record_roundtrip():
    rec = DiagnosisRecord(
        source="plan11_stage1",
        rca_kind_label="plural_top_n_collapse",
        evidence_summary="top-N collapsed",
        observed_failure="returned 1 row instead of 3",
        expected_sql_shape="ROW_NUMBER over COUNT(*) DESC LIMIT 3",
        confidence="high",
        rca_card_id="rca_abc",
    )
    assert DiagnosisRecord.from_json(rec.to_json()) == rec


def test_cluster_membership_record_roundtrip():
    rec = ClusterMembershipRecord(
        cluster_id="H001",
        ag_id="AG_DECOMPOSED_H001",
        co_member_qids=("gs_009", "gs_026"),
        effective_target_lever=6,
        routing_evidence_kind="plural_top_n_collapse",
    )
    assert ClusterMembershipRecord.from_json(rec.to_json()) == rec


def test_proposal_attempt_roundtrip_all_outcomes():
    for outcome in (
        "applied", "accepted", "rolled_back",
        "contract_failed", "validator_rejected",
        "blast_radius_rejected", "applyability_rejected",
        "structural_repair_rejected", "escalated",
    ):
        rec = ProposalAttempt(
            attempt_index=0,
            intent_id="intent_xyz",
            patch_type="add_sql_snippet_expression",
            deepest_stage_in_attempt=FunnelStage.APPLYABLE,
            outcome=outcome,  # type: ignore[arg-type]
            outcome_reason="test",
            escalated_to_attempt_index=1 if outcome == "escalated" else None,
            patch_outcome_id="outcome_1",
        )
        assert ProposalAttempt.from_json(rec.to_json()) == rec


def test_applied_record_roundtrip():
    rec = AppliedRecord(
        applied_at_ms=1234567890,
        apply_call_id="call_1",
        proposal_attempt_index=0,
        applied_intent_ids=("intent_a", "intent_b"),
    )
    assert AppliedRecord.from_json(rec.to_json()) == rec


def test_evaluated_record_roundtrip():
    rec = EvaluatedRecord(
        pre_apply_score=0.0,
        post_apply_score=1.0,
        pre_apply_sql="SELECT 1",
        post_apply_sql="SELECT 2",
        eval_row_id_post="row_43",
    )
    assert EvaluatedRecord.from_json(rec.to_json()) == rec


def test_acceptance_record_roundtrip():
    rec = AcceptanceDecisionRecord(
        decision="accepted",
        arbiter_reason="target fixed; no regressions",
        target_fixed=True,
        collateral_regressions=(),
    )
    assert AcceptanceDecisionRecord.from_json(rec.to_json()) == rec


def test_terminal_record_roundtrip_all_kinds():
    for kind in (
        "OPTIMIZER_IMPROVED",
        "OPTIMIZER_TRIED_NO_GAIN",
        "OPTIMIZER_STALLED_NO_APPLIED_PATCHES",
        "OPTIMIZER_NO_CANDIDATES",
        "OPTIMIZER_SKIPPED_INPUT_GAP",
        "OPTIMIZER_STALLED_SAFE_NOOP",
    ):
        rec = TerminalRecord(
            kind=kind,  # type: ignore[arg-type]
            reason="test",
            deepest_stage_reached=FunnelStage.PROPOSED,
            forbidden_signature="h001|add_sql_snippet_expression|count_topN",
        )
        assert TerminalRecord.from_json(rec.to_json()) == rec


def test_stage_transition_roundtrip():
    rec = StageTransition(
        from_stage=FunnelStage.PROPOSED,
        to_stage=FunnelStage.NORMALIZED,
        at_ms=1234,
        transformer_name="structural_repair_gate",
        transition_kind="validation_gate",
        proposal_attempt_index=0,
        reason="",
    )
    assert StageTransition.from_json(rec.to_json()) == rec
