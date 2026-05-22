"""Per-gate reasoning markers fire on every transformer rejection.

v4 Task 2.3: every transformer that rejects (terminal or cycled-back
ProposalAttempt) must emit ``GSO_GATE_REASONING_V1`` carrying the
predicate inputs that produced the verdict. This test pins the
behavior at ``structural_repair_gate`` — the most common rejection
path and the one the anchor fixtures exercise.

The plan's test calls the gate as a function; in the actual code the
transformer is a ``ValidationGate`` instance, so we invoke
``.transform(...)``. The assertion intent is unchanged: the marker
must appear with ``gate=structural_repair_gate`` and ``qid=gs_009``.
"""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord,
    DiagnosisRecord,
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformers.structural_repair_gate import (
    structural_repair_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _state_at_proposed_for_gs_009():
    s = build_initial_state(
        qid="gs_009", iteration=1,
        seen=HardQidSeenRecord(
            eval_row_id="r",
            predicate="row_is_hard_failure",
            score=0.0,
            baseline_sql="S",
            expected_shape="x",
            iteration_first_seen=1,
        ),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(
            FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm",
        ),
        diagnosed=DiagnosisRecord(
            source="plan11_stage1",
            rca_kind_label="plural_top_n_collapse",
            evidence_summary="",
            observed_failure="",
            expected_sql_shape="",
            confidence="high",
            rca_card_id="rca_gs_009_v1",
        ),
    )
    s = s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(
            FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "batch",
        ),
        clustered=ClusterMembershipRecord(
            cluster_id="H001", ag_id="AG_1",
            co_member_qids=("gs_009",),
            effective_target_lever=6, routing_evidence_kind="k",
        ),
    )
    # Advance to PROPOSED with a ProposalAttempt that has no backing
    # entry in proposal_store — that drives the rejection path we want
    # to witness.
    s = s.advance(
        FunnelStage.PROPOSED,
        StageTransition(
            FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm",
        ),
        proposals=(
            ProposalAttempt(
                attempt_index=0,
                intent_id="intent_missing_in_store",
                patch_type="add_sql_snippet_expression",
                deepest_stage_in_attempt=FunnelStage.PROPOSED,
                outcome="applied",
                outcome_reason="pending_gates",
            ),
        ),
    )
    return s


def test_structural_repair_gate_emits_reasoning_on_rejection(capsys):
    state = _state_at_proposed_for_gs_009()
    ctx = TransformerContext(
        iteration=1, run_id="r1",
        validation_context=ValidationContext(1, "r1", {}),
    )
    # proposal_store is empty by default → predicate rejects with
    # ``proposal_store_miss:intent_missing_in_store``. The marker
    # must precede the GateVerdict.reject_proposal return.
    _ = structural_repair_gate.transform(state, ctx)
    out = capsys.readouterr().out
    assert "GSO_GATE_REASONING_V1" in out
    assert "gate=structural_repair_gate" in out
    assert "qid=gs_009" in out
