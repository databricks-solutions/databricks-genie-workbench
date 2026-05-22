"""Acceptance gate: EVALUATED → ACCEPTED, or roll back to TERMINATED.

Reads ``state.evaluated.{pre,post}_apply_score`` plus the collateral
assessment to decide. Acceptance requires both:
  * target_fixed (post > pre), AND
  * no collateral regressions.

Failure of either → ``OPTIMIZER_TRIED_NO_GAIN`` terminal, which the
postmortem renderers + run-outcome classifier treat as
"tried, applied, eval did not improve — clean rollback".
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord, TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.transformer import ValidationGate
from genie_space_optimizer.optimization.state_machine.verdict import (
    GateVerdict, TransformerContext,
)


def _assess_collateral(
    state: QuestionStateInIteration, ctx: TransformerContext,
) -> tuple[str, ...]:
    """Wrap legacy arbiter collateral check.

    Returns tuple of regressed QIDs. Empty tuple means "no regressions
    detected". Patchable for tests; production wire-in lands alongside
    the deployed-smoke gate that the user deferred.
    """
    raise NotImplementedError(
        "Collateral assessment not wired to production yet. "
        "Tests must monkeypatch _assess_collateral."
    )


def _predicate(state: QuestionStateInIteration, ctx: TransformerContext) -> GateVerdict:
    assert state.evaluated is not None, "acceptance_gate requires evaluated record"
    target_fixed = state.evaluated.post_apply_score > state.evaluated.pre_apply_score
    collateral = _assess_collateral(state, ctx)

    if target_fixed and not collateral:
        return GateVerdict.success(record=AcceptanceDecisionRecord(
            decision="accepted",
            arbiter_reason="target_fixed_no_regression",
            target_fixed=True,
            collateral_regressions=(),
        ))

    if not target_fixed:
        reason = "target_unchanged: post_score <= pre_score"
    else:
        reason = f"collateral_regressions={list(collateral)}"
    return GateVerdict.reject_terminal(TerminalRecord(
        kind="OPTIMIZER_TRIED_NO_GAIN",
        reason=reason,
        deepest_stage_reached=state.deepest_stage_reached,
        forbidden_signature="",
    ))


acceptance_gate = ValidationGate(
    name="acceptance_gate",
    from_stage=FunnelStage.EVALUATED,
    to_stage_on_success=FunnelStage.ACCEPTED,
    to_stage_on_reject=FunnelStage.TERMINATED,
    predicate=_predicate,
)
