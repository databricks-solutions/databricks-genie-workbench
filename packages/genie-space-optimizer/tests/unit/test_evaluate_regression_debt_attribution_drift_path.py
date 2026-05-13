"""Phase 1 Task 5 — evaluate_regression_debt under the new attribution-drift policy.

The existing pure helper ``evaluate_regression_debt`` is unchanged.
What changes is the policy passed in. These tests pin three concrete
shapes the new policy must accept or reject correctly. The mapping
from verdict.reason_code to the new ``accepted_with_attribution_drift_and_debt``
production reason code happens at the ``decide_control_plane_acceptance``
layer (Task 6) — at the policy layer, the verdict still reads as
``accepted_with_partial_harvest_debt`` because the helper is policy-agnostic.

Fixture shapes are derived from the actual captured fixtures at
``tests/replay/fixtures/policy_replay/{ccf1d60d_iter1,3b050ec5_iter1}.json``.
"""

from __future__ import annotations


def _build_decision(
    *,
    delta_pp: float,
    target_fixed: tuple[str, ...],
    target_still_hard: tuple[str, ...],
    out_of_target_regressed: tuple[str, ...],
    unknown_to_hard: tuple[str, ...],
    soft_to_hard: tuple[str, ...] = (),
    passing_to_hard: tuple[str, ...] = (),
):
    """Construct a ControlPlaneAcceptance for policy evaluation.

    Sets ``accepted=False`` to mirror the synthesised state
    ``decide_control_plane_acceptance`` creates before delegating to
    ``evaluate_regression_debt`` (control_plane.py:1643-1660).
    """
    from genie_space_optimizer.optimization.control_plane import (
        ControlPlaneAcceptance,
        DeltaState,
    )

    target_qids = target_fixed + target_still_hard
    delta_states = tuple(
        sorted(
            [
                (q, DeltaState.FIXED.value)
                for q in target_fixed
            ]
            + [
                (q, DeltaState.STILL_HARD.value)
                for q in target_still_hard
            ]
        )
    )
    return ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_qids_not_improved",
        baseline_accuracy=87.0,
        candidate_accuracy=round(87.0 + delta_pp, 1),
        delta_pp=round(float(delta_pp), 1),
        target_qids=target_qids,
        target_fixed_qids=target_fixed,
        target_still_hard_qids=target_still_hard,
        out_of_target_regressed_qids=out_of_target_regressed,
        regression_debt_qids=(),
        protected_regressed_qids=(),
        soft_to_hard_regressed_qids=soft_to_hard,
        passing_to_hard_regressed_qids=passing_to_hard,
        unknown_to_hard_regressed_qids=unknown_to_hard,
        target_delta_states=delta_states,
    )


def test_ccf1d60d_shape_under_attribution_drift_policy_is_accepted_with_debt() -> None:
    """+4.3pp net, target gs_026 unfixed, 1 unknown_to_hard
    (gs_012). Under the new attribution-drift policy this is
    under_policy AND has debt → verdict says accept with debt."""
    from genie_space_optimizer.optimization.acceptance_policy import (
        attribution_drift_policy_pilot_default,
    )
    from genie_space_optimizer.optimization.control_plane import (
        evaluate_regression_debt,
    )

    decision = _build_decision(
        delta_pp=4.3,
        target_fixed=(),
        target_still_hard=("7now_delivery_analytics_space_gs_026",),
        out_of_target_regressed=("7now_delivery_analytics_space_gs_012",),
        unknown_to_hard=("7now_delivery_analytics_space_gs_012",),
    )
    verdict = evaluate_regression_debt(
        decision=decision,
        policy=attribution_drift_policy_pilot_default(),
        cumulative_debt=0,
        threshold_pass_rate=1.0,
    )
    assert verdict.under_policy is True
    assert verdict.reason_code == "accepted_with_partial_harvest_debt"
    assert verdict.debt_qids == ("7now_delivery_analytics_space_gs_012",)


def test_3b050ec5_shape_under_attribution_drift_policy_has_no_debt_to_harvest() -> None:
    """+8.1pp net, target unfixed, zero debt. The policy clears every
    gate but has no debt to harvest, so verdict short-circuits to
    no_debt_present (control_plane.py:1865-1874). In production the
    legacy zero-debt ``accepted_with_attribution_drift`` branch
    handles this case (control_plane.py:1567); the new branch is
    correctly silent."""
    from genie_space_optimizer.optimization.acceptance_policy import (
        attribution_drift_policy_pilot_default,
    )
    from genie_space_optimizer.optimization.control_plane import (
        evaluate_regression_debt,
    )

    decision = _build_decision(
        delta_pp=8.1,
        target_fixed=(),
        target_still_hard=("3b050ec5_target_qid",),
        out_of_target_regressed=(),
        unknown_to_hard=(),
    )
    verdict = evaluate_regression_debt(
        decision=decision,
        policy=attribution_drift_policy_pilot_default(),
    )
    assert verdict.under_policy is True
    assert verdict.reason_code == "no_debt_present"
    assert verdict.debt_qids == ()


def test_attribution_drift_policy_rejects_below_aggregate_floor() -> None:
    """+1.0pp net is below the new policy's 4.0pp floor — rejected."""
    from genie_space_optimizer.optimization.acceptance_policy import (
        attribution_drift_policy_pilot_default,
    )
    from genie_space_optimizer.optimization.control_plane import (
        evaluate_regression_debt,
    )

    decision = _build_decision(
        delta_pp=1.0,
        target_fixed=(),
        target_still_hard=("ccf1d60d_target_qid",),
        out_of_target_regressed=("ccf1d60d_debt_qid",),
        unknown_to_hard=("ccf1d60d_debt_qid",),
    )
    verdict = evaluate_regression_debt(
        decision=decision,
        policy=attribution_drift_policy_pilot_default(),
    )
    assert verdict.under_policy is False
    assert verdict.reason_code == "aggregate_gain_below_floor"


def test_attribution_drift_policy_rejects_passing_to_hard_debt() -> None:
    """A passing_to_hard debt qid maps to REGRESSED_TO_UNKNOWN
    (control_plane.py:1838), which the new policy does not include in
    allowed_debt_buckets. The verdict must reject."""
    from genie_space_optimizer.optimization.acceptance_policy import (
        attribution_drift_policy_pilot_default,
    )
    from genie_space_optimizer.optimization.control_plane import (
        evaluate_regression_debt,
    )

    decision = _build_decision(
        delta_pp=4.5,
        target_fixed=(),
        target_still_hard=("ccf1d60d_target_qid",),
        out_of_target_regressed=("ccf1d60d_debt_qid",),
        unknown_to_hard=(),
        passing_to_hard=("ccf1d60d_debt_qid",),
    )
    verdict = evaluate_regression_debt(
        decision=decision,
        policy=attribution_drift_policy_pilot_default(),
    )
    assert verdict.under_policy is False
    assert verdict.reason_code == "debt_bucket_disallowed"
