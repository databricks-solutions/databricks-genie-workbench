"""Phase 3 Action 3.1 — IterationFeedback dataclass shape + freeze."""

from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.acceptance_policy import AcceptedClass
from genie_space_optimizer.optimization.iteration_feedback import (
    IterationFeedback,
)


def _empty(iteration: int = 1) -> IterationFeedback:
    return IterationFeedback(
        iteration=iteration,
        acceptance_class=AcceptedClass.STRICT_WIN,
        accept=True,
        delta_pp=0.0,
        target_qids=(),
        target_fixed_qids=(),
        target_still_hard_qids=(),
        regression_debt_classification={},
        tried_ag_shapes_by_target={},
        near_miss_reflections=(),
    )


def test_iteration_feedback_fields_present() -> None:
    fb = _empty()
    assert fb.iteration == 1
    assert fb.acceptance_class == AcceptedClass.STRICT_WIN
    assert fb.accept is True
    assert fb.delta_pp == 0.0
    assert fb.target_qids == ()
    assert fb.target_fixed_qids == ()
    assert fb.target_still_hard_qids == ()
    assert fb.regression_debt_classification == {}
    assert fb.tried_ag_shapes_by_target == {}
    assert fb.near_miss_reflections == ()


def test_iteration_feedback_is_frozen() -> None:
    fb = _empty()
    try:
        fb.delta_pp = 5.0  # type: ignore[misc]
        raised = False
    except dataclasses.FrozenInstanceError:
        raised = True
    assert raised, "IterationFeedback must be frozen"


def test_iteration_feedback_accepts_diagnostic_hold() -> None:
    fb = IterationFeedback(
        iteration=2,
        acceptance_class=AcceptedClass.DIAGNOSTIC_HOLD,
        accept=False,
        delta_pp=4.3,
        target_qids=("gs_026",),
        target_fixed_qids=(),
        target_still_hard_qids=("gs_026",),
        regression_debt_classification={"unknown_to_hard": ["gs_012"]},
        tried_ag_shapes_by_target={},
        near_miss_reflections=(),
    )
    assert fb.acceptance_class == AcceptedClass.DIAGNOSTIC_HOLD
    assert fb.accept is False
    assert fb.regression_debt_classification == {"unknown_to_hard": ["gs_012"]}
