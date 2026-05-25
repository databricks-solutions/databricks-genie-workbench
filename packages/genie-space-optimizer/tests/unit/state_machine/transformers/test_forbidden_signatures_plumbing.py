"""Trial 16.3 — ``cluster_batch`` and ``synthesize_llm`` must forward
``ctx.forbidden_signatures`` to their respective downstream stages.

Without these two plumbing seams, even if Stage 2's ``cluster_diagnoses``
and Stage 3's ``run_plan11_synthesis_for_single_cluster`` accept the
``forbidden_signatures`` kwarg (see ``test_cluster_plan11_forbidden_signatures``
and ``test_synthesize_forbidden_signatures``), the transformers in the
SM lane never propagate the context. The result is the same regression
the analyst described: the LLM is "structurally blind" to prior-iteration
typed feedback.

These tests pin the SM lane plumbing by patching the downstream stage
functions and verifying ``forbidden_signatures`` flows through the
adapter layer with byte-identical contents.
"""
from __future__ import annotations

from unittest.mock import patch

from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
)
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord,
    DiagnosisRecord,
    HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


# ----------------------------------------------------------------------
# Stage 2 plumbing: cluster_batch._invoke_stage2_llm
# ----------------------------------------------------------------------


def _diagnosed_state(qid: str):
    """Build a state at FunnelStage.DIAGNOSED for Stage 2 batch input."""
    s = build_initial_state(
        qid=qid,
        iteration=2,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(
            from_stage=FunnelStage.HARD_QID_SEEN,
            to_stage=FunnelStage.DIAGNOSED,
            at_ms=1,
            transformer_name="t",
            transition_kind="validation_gate",
        ),
        diagnosed=DiagnosisRecord(
            source="plan11_stage1",
            rca_kind_label="missing_metadata",
            evidence_summary="",
            observed_failure="",
            expected_sql_shape="",
            confidence="high",
            rca_card_id="rca_x",
        ),
    )
    return s


def test_cluster_batch_forwards_ctx_forbidden_signatures_to_cluster_diagnoses():
    """``cluster_batch`` reads ``ctx.forbidden_signatures`` and forwards
    them to ``cluster_diagnoses`` as a kwarg — without this, the typed
    prior-iteration rejections never reach the Stage 2 LLM prompt."""
    forbidden = (
        "add_column_description:dropped_no_op:missing_table",
        "update_column_description:dropped_no_op:missing_table",
    )
    states = (_diagnosed_state("gs_013"),)
    ctx = TransformerContext(
        iteration=2,
        run_id="trial16-3",
        validation_context=ValidationContext(2, "trial16-3", {}),
        forbidden_signatures=forbidden,
        extras={},
    )

    captured: dict[str, object] = {}

    def _spy(*, diagnoses, schema_columns, optimization_run_id, iteration,
             namespace, w, forbidden_signatures=()):
        captured["forbidden_signatures"] = tuple(forbidden_signatures)
        return []  # decline → cluster_batch terminates; we only need plumbing.

    from genie_space_optimizer.optimization.state_machine.transformers import (
        cluster_batch as _cb,
    )

    with patch(
        "genie_space_optimizer.optimization.stages.cluster_plan11."
        "cluster_diagnoses",
        side_effect=_spy,
    ):
        _cb._invoke_stage2_llm(
            _cb.build_stage2_batch_input(
                states, forbidden_signatures=ctx.forbidden_signatures,
            ),
            ctx,
            states,
        )

    assert captured.get("forbidden_signatures") == forbidden, (
        "cluster_batch must forward ctx.forbidden_signatures to "
        f"cluster_diagnoses — got "
        f"{captured.get('forbidden_signatures')!r}, expected {forbidden!r}"
    )


# ----------------------------------------------------------------------
# Stage 3 plumbing: synthesize_llm._invoke_stage3_llm
# ----------------------------------------------------------------------


def _clustered_state(qid: str):
    """Build a state at FunnelStage.CLUSTERED for Stage 3 invocation."""
    s = _diagnosed_state(qid)
    s = s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(
            from_stage=FunnelStage.DIAGNOSED,
            to_stage=FunnelStage.CLUSTERED,
            at_ms=2,
            transformer_name="t",
            transition_kind="validation_gate",
        ),
        clustered=ClusterMembershipRecord(
            cluster_id="H001",
            ag_id="AG001",
            co_member_qids=(qid,),
            effective_target_lever=5,
            routing_evidence_kind="rca_card",
        ),
    )
    return s


def test_synthesize_llm_forwards_ctx_forbidden_signatures_to_synthesis_stage():
    """``synthesize_llm`` reads ``ctx.forbidden_signatures`` and forwards
    them to ``run_plan11_synthesis_for_single_cluster`` as a kwarg.

    Without this plumbing seam the lever LLM is structurally blind to
    prior-iteration patch_type rejections. Postmortem 813949510175466
    showed gs_013 re-proposing ``update_column_description`` after
    ``add_column_description`` had been rejected with the same root
    cause — exactly the loop this plumbing seam closes.
    """
    forbidden = (
        "add_column_description:dropped_no_op:missing_table",
        "update_column_description:dropped_no_op:missing_table",
    )
    state = _clustered_state("gs_013")

    captured: dict[str, object] = {}

    class _StubResult:
        proposal = None  # synthesize_llm returns None on missing proposal

    def _spy(
        cluster, schema_slice, history, *,
        member_qid_evidence=None,
        optimization_run_id,
        iteration,
        ag_id,
        w,
        forbidden_signatures=(),
    ):
        captured["forbidden_signatures"] = tuple(forbidden_signatures)
        return _StubResult()

    ctx = TransformerContext(
        iteration=2,
        run_id="trial16-3",
        validation_context=ValidationContext(2, "trial16-3", {}),
        forbidden_signatures=forbidden,
        extras={},
    )

    with patch(
        "genie_space_optimizer.optimization.stages.synthesize."
        "run_plan11_synthesis_for_single_cluster",
        side_effect=_spy,
    ):
        from genie_space_optimizer.optimization.state_machine.transformers import (
            synthesize_llm as _sl,
        )
        _sl._invoke_stage3_llm(state, ctx)

    assert captured.get("forbidden_signatures") == forbidden, (
        "synthesize_llm must forward ctx.forbidden_signatures to "
        f"run_plan11_synthesis_for_single_cluster — got "
        f"{captured.get('forbidden_signatures')!r}, expected {forbidden!r}"
    )
