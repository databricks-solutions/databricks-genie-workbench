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
