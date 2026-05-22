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


def _invoke_rung_1_scoped_l6(state, ctx) -> Any:
    """Rung 1: ask LLM for a scoped L6 narrower than the rejected one.

    Patchable for tests. Production wire-in calls into ``stages.synthesize``
    (a new ``synthesize_scoped_l6_escalation`` entry point that Phase 4
    will add). Until then this is a test-only seam.
    """
    raise NotImplementedError(
        "Rung 1 scoped-L6 LLM dispatch not wired to production yet. "
        "Tests must monkeypatch _invoke_rung_1_scoped_l6."
    )


def _invoke_rung_2_narrowed_l6(state, ctx) -> Any:
    """Rung 2: build narrow replacement from the BlastRadiusDropRecord."""
    raise NotImplementedError(
        "Rung 2 narrow-replacement LLM dispatch not wired to production yet. "
        "Tests must monkeypatch _invoke_rung_2_narrowed_l6."
    )


def _invoke_rung_3_add_example_sql(state, ctx) -> Any:
    """Rung 3: question-scoped add_example_sql teaching artifact."""
    raise NotImplementedError(
        "Rung 3 add_example_sql LLM dispatch not wired to production yet. "
        "Tests must monkeypatch _invoke_rung_3_add_example_sql."
    )


def _invoke_rung_4_narrowed_example_sql(state, ctx) -> Any:
    """Rung 4: narrowed example_sql (target_qid only, no collateral exposure)."""
    raise NotImplementedError(
        "Rung 4 narrowed-example_sql LLM dispatch not wired to production yet. "
        "Tests must monkeypatch _invoke_rung_4_narrowed_example_sql."
    )


def _terminate_safe_noop(state: QuestionStateInIteration, name: str) -> QuestionStateInIteration:
    return state.terminate(
        transition=StageTransition(
            from_stage=state.current_stage, to_stage=FunnelStage.TERMINATED,
            at_ms=int(time.time() * 1000),
            transformer_name=name, transition_kind="llm",
            reason="all_escalation_rungs_exhausted",
        ),
        terminal=TerminalRecord(
            kind="OPTIMIZER_STALLED_SAFE_NOOP",
            reason="all_escalation_rungs_exhausted",
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
