"""LLM-driven narrow-replacement gate.

Replaces the deterministic-then-maybe-narrow two-step (blast_radius drops,
then auto_narrow_replacement maybe runs). The LLM sees the dropped patch,
its passing dependents' SQL, and the failure anchor; it returns one of
four typed decisions. The state machine consumes the decision directly.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal, Mapping

from genie_space_optimizer.optimization.blast_radius_drop_record import (
    BlastRadiusDropRecord,
)
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.markers import (
    gate_reasoning_marker,
)
from genie_space_optimizer.optimization.state_machine.records import (
    ProposalAttempt,
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.transformer import (
    ValidationGate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    GateVerdict,
    TransformerContext,
)


NarrowDecision = Literal[
    "accept", "narrow_to", "pivot_to_example_sql", "reject_unfixable",
]


@dataclass(frozen=True, slots=True)
class NarrowReplacementVerdict:
    decision: NarrowDecision
    scoped_patch: Mapping[str, Any] | None
    rationale: str


def run_narrow_replacement(
    *,
    drop: BlastRadiusDropRecord,
    llm_call: Callable[..., Mapping[str, Any]],
) -> NarrowReplacementVerdict:
    """Invoke the LLM with the dropped patch + collateral context.

    The LLM is expected to return a dict with keys:
      decision: one of NarrowDecision
      narrowed_patch: required when decision == "narrow_to"
      example_sql: required when decision == "pivot_to_example_sql"
      rationale: required always
    """
    response = llm_call(
        original_patch=dict(drop.original_patch_body),
        target_qids=list(drop.target_qids),
        collateral_qids=list(drop.collateral_qids),
        protected_sql_by_qid=dict(drop.protected_sql_by_qid),
        causal_target=drop.causal_target,
        failing_sql_anchor=drop.failing_sql_anchor,
        rca_card_id=drop.rca_card_id,
    )
    decision = response.get("decision", "reject_unfixable")
    rationale = str(response.get("rationale") or "")

    if decision == "narrow_to":
        scoped = dict(response.get("narrowed_patch") or {})
        if not scoped or not scoped.get("patch_type"):
            return NarrowReplacementVerdict(
                decision="reject_unfixable", scoped_patch=None,
                rationale=f"narrow_to missing narrowed_patch; {rationale}",
            )
        return NarrowReplacementVerdict(
            decision="narrow_to", scoped_patch=scoped, rationale=rationale,
        )
    if decision == "pivot_to_example_sql":
        scoped = dict(response.get("example_sql") or {})
        if not scoped or scoped.get("patch_type") != "add_example_sql":
            return NarrowReplacementVerdict(
                decision="reject_unfixable", scoped_patch=None,
                rationale=f"pivot missing example_sql; {rationale}",
            )
        return NarrowReplacementVerdict(
            decision="pivot_to_example_sql", scoped_patch=scoped,
            rationale=rationale,
        )
    if decision == "accept":
        return NarrowReplacementVerdict(
            decision="accept", scoped_patch=dict(drop.original_patch_body),
            rationale=rationale,
        )
    return NarrowReplacementVerdict(
        decision="reject_unfixable", scoped_patch=None, rationale=rationale,
    )


# ---------------------------------------------------------------------------
# Orchestrator-compatible adapter for the StateMachine registry.
# Runs at NORMALIZED after ``blast_radius_batch``. Reads the drop record
# associated with the current QID from ``ctx.extras`` and projects the
# LLM verdict back onto the funnel:
#   * accept / narrow_to / pivot_to_example_sql → success ProposalAttempt
#     (the scoped patch is stashed in ``ctx.extras`` for downstream
#     synthesis-style consumption; the typed proposal_store seam stays
#     reserved for ``RepairProposal`` objects until Phase 3 wires the
#     full LLM-driven flow);
#   * reject_unfixable                         → terminal record.
# When no drop is present for the QID (the common safe path), the gate
# no-ops and lets the state continue forward.
# ---------------------------------------------------------------------------


def _drop_for_state(
    state: QuestionStateInIteration, ctx: TransformerContext,
) -> BlastRadiusDropRecord | None:
    """Look up the blast-radius drop record for ``state.qid`` in ``ctx.extras``.

    ``blast_radius_batch`` (or the harness that fronts it) is responsible
    for populating ``ctx.extras["blast_radius_drop_by_qid"]`` with a typed
    :class:`BlastRadiusDropRecord` per QID it rejected. Until that wiring
    lands, ``_drop_for_state`` returns ``None`` and the gate passes through.
    """
    by_qid = ctx.extras.get("blast_radius_drop_by_qid", {}) or {}
    drop = by_qid.get(state.qid)
    if isinstance(drop, BlastRadiusDropRecord):
        return drop
    return None


def _project_verdict_to_gate(
    *,
    verdict: NarrowReplacementVerdict,
    state: QuestionStateInIteration,
    ctx: TransformerContext,
) -> GateVerdict:
    """Translate a :class:`NarrowReplacementVerdict` into a :class:`GateVerdict`."""
    if verdict.decision in ("narrow_to", "pivot_to_example_sql", "accept"):
        scoped = dict(verdict.scoped_patch or {})
        attempt_index = len(state.proposals)
        intent_id = f"narrow_{state.qid}_v{attempt_index}"
        # Stash the scoped patch on ctx.extras so a downstream synthesizer
        # can reconstruct a typed RepairProposal in Phase 3. proposal_store
        # is intentionally not touched here — it remains the RepairProposal
        # bridge.
        scoped_patches = ctx.extras.setdefault(
            "narrow_replacement_scoped_patches", {},
        )
        scoped_patches[intent_id] = scoped
        attempt = ProposalAttempt(
            attempt_index=attempt_index,
            intent_id=intent_id,
            patch_type=str(scoped.get("patch_type") or ""),
            deepest_stage_in_attempt=FunnelStage.APPLYABLE,
            outcome="escalated",
            outcome_reason=verdict.rationale,
        )
        return GateVerdict.success(record=attempt)
    terminal = TerminalRecord(
        kind="OPTIMIZER_NO_CANDIDATES",
        reason=verdict.rationale or "narrow_replacement_unfixable",
        deepest_stage_reached=state.deepest_stage_reached,
        forbidden_signature="",
    )
    print(
        gate_reasoning_marker(
            gate="narrow_replacement_gate",
            qid=state.qid,
            verdict="rejected",
            predicate_inputs={
                "llm_decision": verdict.decision,
                "scoped_patch_present": verdict.scoped_patch is not None,
            },
            reason=verdict.rationale or "narrow_replacement_unfixable",
        ),
        flush=True,
    )
    return GateVerdict.reject_terminal(terminal)


def _predicate(
    state: QuestionStateInIteration, ctx: TransformerContext,
) -> GateVerdict:
    """ValidationGate predicate: runs after ``blast_radius_batch`` at NORMALIZED.

    No-op (passed, no record) when the state has no blast-radius drop
    record in ``ctx.extras`` — this covers the safe path where
    ``blast_radius_batch`` advanced the state to APPLYABLE already.
    No-op when the LLM seam (``ctx.extras["narrow_replacement_llm"]``)
    is not wired — Phase 3 lights it up; the registry entry can land
    independently.
    """
    drop = _drop_for_state(state, ctx)
    if drop is None:
        return GateVerdict.success(record=None)
    llm_fn = ctx.extras.get("narrow_replacement_llm")
    if llm_fn is None:
        return GateVerdict.success(record=None)
    verdict = run_narrow_replacement(drop=drop, llm_call=llm_fn)
    return _project_verdict_to_gate(verdict=verdict, state=state, ctx=ctx)


narrow_replacement_gate = ValidationGate(
    name="narrow_replacement_gate",
    from_stage=FunnelStage.NORMALIZED,
    # On a pass-through (no drop), the state's current_stage is already
    # NORMALIZED (orchestrator only entered this transformer because
    # blast_radius_batch decided NOT to advance — i.e. the state cycled
    # back to PROPOSED, OR the state genuinely stayed at NORMALIZED).
    # When the gate is a true no-op, advancing to APPLYABLE matches the
    # blast_radius safe path; on rejection it terminates. The success
    # record (ProposalAttempt) is dropped by ValidationGate for non-mapped
    # target stages — see ``_record_updates_for_stage``.
    to_stage_on_success=FunnelStage.APPLYABLE,
    to_stage_on_reject=FunnelStage.PROPOSED,
    predicate=_predicate,
)
