"""Phase 3 Action 3.1 — canonical IterationFeedback carry-over object.

Built at the end of each iteration in
``optimization.harness._finalize_iteration_summary`` from the canonical
``ControlPlaneAcceptance`` + ``TierVerdict`` and the per-iteration AG
history. Threaded into the next iteration's strategist call via the new
``iteration_feedback`` kwarg on
``optimization.optimizer._call_llm_for_adaptive_strategy``.

The object MUST carry:

* ``acceptance_class`` (one of the four ``AcceptedClass`` values).
* ``regression_debt_classification`` for all non-strict outcomes.
* The per-target AG-shape history so Section B's pre-strategy gate
  can refuse to repeat the same shape on the same target.
* The typed ``NearMissReflection`` payloads emitted for
  ``DIAGNOSTIC_HOLD`` and ``NET_WIN_WITH_DEBT`` outcomes.

All fields are frozen; downstream consumers read but do not mutate.
The legacy ``reflection_buffer`` and ``verdict_history`` channels in
the harness remain populated unchanged when
``GSO_ITERATION_FEEDBACK=0``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from genie_space_optimizer.optimization.acceptance_policy import AcceptedClass

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.near_miss_reflection import (
        AGShapeSignature,
        NearMissReflection,
    )


@dataclass(frozen=True)
class IterationFeedback:
    """Phase 3 Action 3.1 — typed carry-over object."""

    iteration: int
    acceptance_class: AcceptedClass
    accept: bool
    delta_pp: float
    target_qids: tuple[str, ...]
    target_fixed_qids: tuple[str, ...]
    target_still_hard_qids: tuple[str, ...]
    regression_debt_classification: dict[str, list[str]]
    tried_ag_shapes_by_target: dict[tuple[str, ...], tuple["AGShapeSignature", ...]]
    near_miss_reflections: tuple["NearMissReflection", ...]


# ---------------------------------------------------------------------------
# Phase 3 Action 3.1 — pure builder.
# ---------------------------------------------------------------------------

from typing import Mapping, Optional

from genie_space_optimizer.optimization.acceptance_policy import TierVerdict
from genie_space_optimizer.optimization.control_plane import ControlPlaneAcceptance


def _classify_regression_debt(
    decision: ControlPlaneAcceptance,
) -> dict[str, list[str]]:
    """Project the canonical decision's typed regression buckets into
    a stable map suitable for the strategist prompt and postmortem
    aggregation. Empty buckets are omitted."""
    out: dict[str, list[str]] = {}
    if decision.passing_to_hard_regressed_qids:
        out["passing_to_hard"] = list(decision.passing_to_hard_regressed_qids)
    if decision.soft_to_hard_regressed_qids:
        out["soft_to_hard"] = list(decision.soft_to_hard_regressed_qids)
    if decision.unknown_to_hard_regressed_qids:
        out["unknown_to_hard"] = list(decision.unknown_to_hard_regressed_qids)
    if decision.protected_regressed_qids:
        out["protected"] = list(decision.protected_regressed_qids)
    return out


def _merge_tried_shapes(
    *,
    prior: "Mapping[tuple[str, ...], tuple[AGShapeSignature, ...]] | None",
    attempted: "Mapping[tuple[str, ...], tuple[AGShapeSignature, ...]]",
) -> "dict[tuple[str, ...], tuple[AGShapeSignature, ...]]":
    """Append this iteration's attempted AG shapes per target onto the
    prior history. Order is preserved so the gate can report the
    chronological sequence of attempts in postmortem."""
    merged: dict[tuple[str, ...], tuple] = {}
    if prior:
        for key, shapes in prior.items():
            merged[key] = tuple(shapes)
    for key, shapes in attempted.items():
        merged[key] = merged.get(key, ()) + tuple(shapes)
    return merged


def build_iteration_feedback(
    *,
    iteration: int,
    decision: ControlPlaneAcceptance,
    verdict: TierVerdict,
    attempted_ag_shapes_by_target: "Mapping[tuple[str, ...], tuple[AGShapeSignature, ...]]",
    near_miss_reflections: "tuple[NearMissReflection, ...]",
    prior_iteration_feedback: "Optional[IterationFeedback]",
) -> "IterationFeedback":
    """Phase 3 Action 3.1 — pure builder.

    Produces a typed ``IterationFeedback`` for the iteration that just
    settled. Caller (``harness._finalize_iteration_summary``) is
    responsible for emitting the ``ITERATION_FEEDBACK_BUILT`` decision
    record and stashing the result on
    ``_iter_summaries[iteration]["iteration_feedback"]``.

    The ``regression_debt_classification`` map is built from the
    canonical bucket fields on ``decision``, NOT recomputed.
    """
    return IterationFeedback(
        iteration=iteration,
        acceptance_class=verdict.accepted_class,
        accept=verdict.accept,
        delta_pp=float(decision.delta_pp),
        target_qids=tuple(decision.target_qids),
        target_fixed_qids=tuple(decision.target_fixed_qids),
        target_still_hard_qids=tuple(decision.target_still_hard_qids),
        regression_debt_classification=_classify_regression_debt(decision),
        tried_ag_shapes_by_target=_merge_tried_shapes(
            prior=(
                prior_iteration_feedback.tried_ag_shapes_by_target
                if prior_iteration_feedback is not None
                else None
            ),
            attempted=attempted_ag_shapes_by_target,
        ),
        near_miss_reflections=tuple(near_miss_reflections),
    )
