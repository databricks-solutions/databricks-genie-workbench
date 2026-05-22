"""Safe-escalation ladder as a single typed transformer.

Reads the latest ProposalAttempt outcome and produces the next softer
artifact:

  rung 1 (structural_repair_rejected): scoped L6 (PARTITION BY narrower)
  rung 2 (blast_radius_rejected):      narrowed L6 via narrow_replacement
  rung 3 (applyability_rejected):      add_example_sql (question-scoped)
  rung 4 (any of the above repeated):  narrowed example_sql (target_qid only)
  rung 5 (ladder exhausted):           OPTIMIZER_STALLED_SAFE_NOOP terminal

Each rung is an LLM call. The ladder picks the rung based on the
latest typed outcome; the orchestrator's escalation cycle (PROPOSED →
PROPOSED) re-invokes the ladder until structural_repair_gate passes
or the ladder exhausts.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.markers import (
    gate_reasoning_marker,
)
from genie_space_optimizer.optimization.state_machine.records import (
    ProposalAttempt,
    StageTransition,
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
)


def _build_cluster_from_state(state):
    """Reverse-project ``state.clustered`` + ``state.diagnosed`` into
    a ``FailureCluster`` for the rung dispatchers (same shape the
    Stage 3 wire-in uses)."""
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )
    return FailureCluster(
        cluster_id=state.clustered.cluster_id if state.clustered else "",
        semantic_theme=(
            state.clustered.routing_evidence_kind if state.clustered else ""
        ),
        member_qids=(
            tuple(state.clustered.co_member_qids) if state.clustered
            else (state.qid,)
        ),
        unifying_evidence=(
            state.diagnosed.evidence_summary if state.diagnosed else ""
        ),
        repair_hypothesis=(
            state.clustered.routing_evidence_kind if state.clustered else ""
        ),
        primary_blame_set=(),
        confidence=(
            state.diagnosed.confidence if state.diagnosed else "low"
        ),
    )


def _adapter_from_dict(proposal_dict, state, ctx):
    """Hydrate ``RepairProposal.from_json(proposal_dict)``, store it in
    ``ctx.proposal_store``, and return a duck-typed adapter exposing the
    v3 ``intent_id`` / ``patch_type`` attributes the escalation_ladder
    consumes.

    Returns ``None`` when the proposal dict is missing or fails to
    hydrate — the ladder maps that to a safe-noop terminal.
    """
    if not proposal_dict:
        return None
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    try:
        typed = RepairProposal.from_json(proposal_dict)
    except Exception:
        return None
    ctx.proposal_store.remember(typed)

    @dataclass(frozen=True, slots=True)
    class _Adapter:
        intent_id: str
        patch_type: str

    return _Adapter(
        intent_id=typed.intent_id,
        patch_type=(
            typed.patch_type.value
            if hasattr(typed.patch_type, "value")
            else str(typed.patch_type)
        ),
    )


def _dispatch_via_synth(state, ctx, rung_hint_value: str):
    """Common path for rungs 1/3/4 — call the unified dispatcher."""
    if not state.proposals:
        return None
    latest = state.proposals[-1]
    failed = ctx.proposal_store.lookup(latest.intent_id)
    if failed is None:
        return None

    from genie_space_optimizer.optimization.stages.synthesize import (
        EscalationRungHint, synthesize_escalation_for_state,
    )
    cluster = _build_cluster_from_state(state)
    result = synthesize_escalation_for_state(
        rung_hint=EscalationRungHint(rung_hint_value),
        failed_proposal=failed,
        failure_reason=latest.outcome_reason,
        cluster=cluster,
        schema_slice=dict(ctx.schema_slice),
        history=[dict(h) for h in ctx.history],
        optimization_run_id=ctx.run_id,
        iteration=ctx.iteration,
        ag_id=state.clustered.ag_id if state.clustered else "",
        w=ctx.w,
    )
    return _adapter_from_dict(getattr(result, "proposal", None), state, ctx)


def _invoke_rung_1_scoped_l6(state, ctx) -> Any:
    """Rung 1: scoped L6 narrower than the rejected one. Routes through
    the unified ``synthesize_escalation_for_state`` with the
    ``SCOPED_L6`` rung hint."""
    return _dispatch_via_synth(state, ctx, "scoped_l6")


def _invoke_rung_2_narrowed_l6(state, ctx) -> Any:
    """Rung 2: narrow replacement built off the blast-radius rejection.
    Routes through the existing ``narrow_replacement_with_llm`` (the
    v3 plan's "rung 2 uses existing entry point" decision)."""
    if not state.proposals:
        return None
    latest = state.proposals[-1]
    failed = ctx.proposal_store.lookup(latest.intent_id)
    if failed is None:
        return None

    cluster = _build_cluster_from_state(state)
    # ``collateral_qids`` for narrow-replacement: the QIDs the blast
    # gate flagged as outside-target. Latest ProposalAttempt's
    # outcome_reason carries them in the string form
    # ``reason=... collateral=('q_other',) drop_record_id=...`` —
    # parse defensively, fall back to empty when absent.
    collateral_qids: tuple[str, ...] = ()
    # protected_sql: pre-apply SQL for non-target QIDs that pass — for
    # v3 iteration 1, leave empty; the narrow-replacement loop handles
    # absence by working from the failed_proposal alone.
    protected_sql: dict[str, str] = {}

    from genie_space_optimizer.optimization.stages.narrow_replacement import (
        narrow_replacement_with_llm,
    )
    narrowed = narrow_replacement_with_llm(
        failed,
        collateral_qids=collateral_qids,
        protected_sql=protected_sql,
        cluster=cluster,
        w=ctx.w,
        optimization_run_id=ctx.run_id,
        iteration=ctx.iteration,
        ag_id=state.clustered.ag_id if state.clustered else "",
    )
    if narrowed is None:
        return None
    ctx.proposal_store.remember(narrowed)

    @dataclass(frozen=True, slots=True)
    class _Adapter:
        intent_id: str
        patch_type: str

    return _Adapter(
        intent_id=narrowed.intent_id,
        patch_type=(
            narrowed.patch_type.value
            if hasattr(narrowed.patch_type, "value")
            else str(narrowed.patch_type)
        ),
    )


def _invoke_rung_3_add_example_sql(state, ctx) -> Any:
    """Rung 3: question-scoped add_example_sql teaching artifact via
    the unified dispatcher with ``ADD_EXAMPLE_SQL`` rung hint."""
    return _dispatch_via_synth(state, ctx, "add_example_sql")


def _invoke_rung_4_narrowed_example_sql(state, ctx) -> Any:
    """Rung 4: narrowed example_sql (single-QID, no collateral exposure)
    via the unified dispatcher with ``NARROWED_EXAMPLE_SQL`` rung hint."""
    return _dispatch_via_synth(state, ctx, "narrowed_example_sql")


def _terminate_safe_noop(
    state: QuestionStateInIteration, name: str,
    *, predicate_inputs: dict | None = None, reason: str = "all_escalation_rungs_exhausted",
) -> QuestionStateInIteration:
    print(
        gate_reasoning_marker(
            gate=name,
            qid=state.qid,
            verdict="rejected",
            predicate_inputs=predicate_inputs or {
                "proposal_count": len(state.proposals),
                "latest_outcome": (
                    state.proposals[-1].outcome if state.proposals else ""
                ),
            },
            reason=reason,
        ),
        flush=True,
    )
    return state.terminate(
        transition=StageTransition(
            from_stage=state.current_stage, to_stage=FunnelStage.TERMINATED,
            at_ms=int(time.time() * 1000),
            transformer_name=name, transition_kind="llm",
            reason=reason,
        ),
        terminal=TerminalRecord(
            kind="OPTIMIZER_STALLED_SAFE_NOOP",
            reason=reason,
            deepest_stage_reached=state.deepest_stage_reached,
            forbidden_signature=_compute_forbidden_signature(state),
        ),
    )


def _compute_forbidden_signature(state: QuestionStateInIteration) -> str:
    """``{cluster_id}|{sorted-patch-types}|{qid}`` — Phase 4 forbidden-set carrier."""
    cluster_id = state.clustered.cluster_id if state.clustered else ""
    patch_types = "|".join(sorted({p.patch_type for p in state.proposals}))
    return f"{cluster_id}|{patch_types}|{state.qid}"


def _choose_rung(latest: ProposalAttempt, state: QuestionStateInIteration):
    """Return (rung_invoke_callable, rung_label) or (None, None) if exhausted.

    Rung selection precedence: outcome → attempt count. The ``attempt_count
    >= 3`` rung 4 branch comes BEFORE the rung 1/3 branches so a repeated
    rejection after multiple attempts always escalates to the narrowest
    artifact rather than re-trying the same rung.
    """
    attempt_count = len(state.proposals)
    # Rung 4 takes precedence after attempt 3+: ladder is near exhaustion.
    if attempt_count >= 3 and latest.outcome in (
        "structural_repair_rejected", "applyability_rejected",
    ):
        return (_invoke_rung_4_narrowed_example_sql, "4_narrowed_example_sql")
    if latest.outcome == "structural_repair_rejected":
        return (_invoke_rung_1_scoped_l6, "1_scoped_l6")
    if latest.outcome == "blast_radius_rejected":
        return (_invoke_rung_2_narrowed_l6, "2_narrowed_l6")
    if latest.outcome == "applyability_rejected":
        return (_invoke_rung_3_add_example_sql, "3_add_example_sql")
    return (None, None)


@dataclass(frozen=True, slots=True)
class _EscalationLadder:
    name: str = "escalation_ladder"
    from_stage: FunnelStage = FunnelStage.PROPOSED
    to_stage_on_success: FunnelStage = FunnelStage.PROPOSED  # cycle in place
    to_stage_on_reject: FunnelStage = FunnelStage.TERMINATED

    def transform(
        self,
        state: QuestionStateInIteration,
        ctx: TransformerContext,
    ) -> QuestionStateInIteration:
        if not state.proposals:
            # No proposal to escalate from; orchestrator routed here in error
            # OR Stage 3 didn't produce one. Either way: stall safely.
            return _terminate_safe_noop(state, self.name)
        latest = state.proposals[-1]
        rung_invoke, rung_label = _choose_rung(latest, state)
        if rung_invoke is None:
            return _terminate_safe_noop(state, self.name)

        proposal = rung_invoke(state, ctx)
        if proposal is None:
            return _terminate_safe_noop(state, self.name)

        # Build the new escalated attempt; mark prior as 'escalated' with
        # a pointer (SM7 requires escalated_to_attempt_index iff
        # outcome=='escalated').
        new_attempt = ProposalAttempt(
            attempt_index=len(state.proposals),
            intent_id=str(proposal.intent_id),
            patch_type=str(proposal.patch_type),
            deepest_stage_in_attempt=FunnelStage.PROPOSED,
            outcome="applied",  # in-flight sentinel; downstream gates overwrite
            outcome_reason=f"escalated_rung_{rung_label}",
        )
        old = state.proposals[-1]
        escalated_old = ProposalAttempt(
            attempt_index=old.attempt_index,
            intent_id=old.intent_id,
            patch_type=old.patch_type,
            deepest_stage_in_attempt=old.deepest_stage_in_attempt,
            outcome="escalated",
            outcome_reason=f"escalated_to_rung_{rung_label}",
            escalated_to_attempt_index=new_attempt.attempt_index,
            patch_outcome_id=old.patch_outcome_id,
        )
        new_proposals = state.proposals[:-1] + (escalated_old, new_attempt)

        return state.advance(
            to_stage=self.to_stage_on_success,
            transition=StageTransition(
                from_stage=self.from_stage,
                to_stage=self.to_stage_on_success,
                at_ms=int(time.time() * 1000),
                transformer_name=self.name,
                transition_kind="llm",
                proposal_attempt_index=new_attempt.attempt_index,
                reason=f"rung_{rung_label}",
            ),
            proposals=new_proposals,
        )


escalation_ladder = _EscalationLadder()
