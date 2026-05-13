"""Phase 3 Action 3.1 — build_iteration_feedback tests."""

from __future__ import annotations

from genie_space_optimizer.optimization.acceptance_policy import (
    AcceptedClass,
    TierVerdict,
)
from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
)
from genie_space_optimizer.optimization.iteration_feedback import (
    IterationFeedback,
    build_iteration_feedback,
)


def _decision(
    *,
    delta_pp: float,
    target_qids: tuple[str, ...] = (),
    target_fixed_qids: tuple[str, ...] = (),
    target_still_hard_qids: tuple[str, ...] = (),
    out_of_target_regressed_qids: tuple[str, ...] = (),
    passing_to_hard_regressed_qids: tuple[str, ...] = (),
    soft_to_hard_regressed_qids: tuple[str, ...] = (),
    unknown_to_hard_regressed_qids: tuple[str, ...] = (),
    protected_regressed_qids: tuple[str, ...] = (),
) -> ControlPlaneAcceptance:
    return ControlPlaneAcceptance(
        accepted=False,
        reason_code="ignored_for_test",
        baseline_accuracy=0.0,
        candidate_accuracy=0.0,
        delta_pp=delta_pp,
        target_qids=target_qids,
        target_fixed_qids=target_fixed_qids,
        target_still_hard_qids=target_still_hard_qids,
        out_of_target_regressed_qids=out_of_target_regressed_qids,
        passing_to_hard_regressed_qids=passing_to_hard_regressed_qids,
        soft_to_hard_regressed_qids=soft_to_hard_regressed_qids,
        unknown_to_hard_regressed_qids=unknown_to_hard_regressed_qids,
        protected_regressed_qids=protected_regressed_qids,
    )


def _verdict(cls: AcceptedClass, accept: bool) -> TierVerdict:
    return TierVerdict(
        accepted_class=cls, accept=accept, debt_classification={},
        reflection_payload={},
    )


def test_builder_strict_win_carries_empty_debt_and_reflections() -> None:
    fb = build_iteration_feedback(
        iteration=1,
        decision=_decision(
            delta_pp=+5.0,
            target_qids=("gs_026",),
            target_fixed_qids=("gs_026",),
        ),
        verdict=_verdict(AcceptedClass.STRICT_WIN, accept=True),
        attempted_ag_shapes_by_target={},
        near_miss_reflections=(),
        prior_iteration_feedback=None,
    )
    assert isinstance(fb, IterationFeedback)
    assert fb.acceptance_class == AcceptedClass.STRICT_WIN
    assert fb.accept is True
    assert fb.regression_debt_classification == {}
    assert fb.near_miss_reflections == ()
    assert fb.tried_ag_shapes_by_target == {}


def test_builder_diagnostic_hold_populates_debt_classification() -> None:
    """ccf1d60d iter-1 worked example: +4.3pp, target gs_026 stayed
    hard, gs_012 regressed unknown→hard."""
    decision = _decision(
        delta_pp=+4.3,
        target_qids=("gs_026",),
        target_still_hard_qids=("gs_026",),
        out_of_target_regressed_qids=("gs_012",),
        unknown_to_hard_regressed_qids=("gs_012",),
    )
    fb = build_iteration_feedback(
        iteration=1,
        decision=decision,
        verdict=_verdict(AcceptedClass.DIAGNOSTIC_HOLD, accept=False),
        attempted_ag_shapes_by_target={},
        near_miss_reflections=(),
        prior_iteration_feedback=None,
    )
    assert fb.acceptance_class == AcceptedClass.DIAGNOSTIC_HOLD
    assert fb.accept is False
    assert fb.regression_debt_classification == {
        "unknown_to_hard": ["gs_012"],
    }
    assert fb.target_still_hard_qids == ("gs_026",)


def test_builder_chains_tried_ag_shapes_from_prior() -> None:
    """Per-target shape history must accumulate across iterations so
    Section B's gate sees every prior attempt."""
    from genie_space_optimizer.optimization.near_miss_reflection import (
        AGShapeSignature,
    )
    from genie_space_optimizer.optimization.target_scope import TargetScope

    prior_shape = AGShapeSignature(
        repair_archetype="default_time_window_filter",
        target_scope=TargetScope.SINGLE_QID,
        primary_cluster_id="cluster_h002",
        target_qids=("gs_021",),
    )
    prior = IterationFeedback(
        iteration=1,
        acceptance_class=AcceptedClass.DIAGNOSTIC_HOLD,
        accept=False,
        delta_pp=4.3,
        target_qids=("gs_021",),
        target_fixed_qids=(),
        target_still_hard_qids=("gs_021",),
        regression_debt_classification={"unknown_to_hard": ["gs_012"]},
        tried_ag_shapes_by_target={("gs_021",): (prior_shape,)},
        near_miss_reflections=(),
    )
    new_shape = AGShapeSignature(
        repair_archetype="default_time_window_filter",
        target_scope=TargetScope.CLUSTER_SCOPED,
        primary_cluster_id="cluster_h002",
        target_qids=("gs_021",),
    )
    fb = build_iteration_feedback(
        iteration=2,
        decision=_decision(delta_pp=+1.0, target_qids=("gs_021",)),
        verdict=_verdict(AcceptedClass.LOSS, accept=False),
        attempted_ag_shapes_by_target={("gs_021",): (new_shape,)},
        near_miss_reflections=(),
        prior_iteration_feedback=prior,
    )
    assert fb.tried_ag_shapes_by_target == {
        ("gs_021",): (prior_shape, new_shape),
    }


def test_builder_loss_has_empty_reflections_but_carries_debt() -> None:
    decision = _decision(
        delta_pp=-1.0,
        target_qids=("gs_026",),
        passing_to_hard_regressed_qids=("gs_001",),
        out_of_target_regressed_qids=("gs_001",),
    )
    fb = build_iteration_feedback(
        iteration=3,
        decision=decision,
        verdict=_verdict(AcceptedClass.LOSS, accept=False),
        attempted_ag_shapes_by_target={},
        near_miss_reflections=(),
        prior_iteration_feedback=None,
    )
    assert fb.acceptance_class == AcceptedClass.LOSS
    assert fb.regression_debt_classification == {
        "passing_to_hard": ["gs_001"],
    }
