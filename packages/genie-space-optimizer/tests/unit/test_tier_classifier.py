from __future__ import annotations

from genie_space_optimizer.optimization.acceptance_policy import (
    AcceptedClass,
    classify_acceptance_tier,
    tier_acceptance_policy_pilot_default,
)
from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
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


def test_strict_win_target_fixed_no_regressions() -> None:
    decision = _decision(
        delta_pp=+5.0,
        target_qids=("gs_026",),
        target_fixed_qids=("gs_026",),
    )
    verdict = classify_acceptance_tier(
        decision=decision, policy=tier_acceptance_policy_pilot_default(),
    )
    assert verdict.accepted_class == AcceptedClass.STRICT_WIN
    assert verdict.accept is True


def test_loss_when_passing_to_hard_regression_present() -> None:
    decision = _decision(
        delta_pp=+10.0,
        target_qids=("gs_026",),
        target_fixed_qids=("gs_026",),
        passing_to_hard_regressed_qids=("gs_001",),
        out_of_target_regressed_qids=("gs_001",),
    )
    verdict = classify_acceptance_tier(
        decision=decision, policy=tier_acceptance_policy_pilot_default(),
    )
    assert verdict.accepted_class == AcceptedClass.LOSS
    assert verdict.accept is False


def test_loss_when_protected_regression_present() -> None:
    decision = _decision(
        delta_pp=+10.0,
        target_qids=("gs_026",),
        target_fixed_qids=("gs_026",),
        protected_regressed_qids=("gs_protected",),
    )
    verdict = classify_acceptance_tier(
        decision=decision, policy=tier_acceptance_policy_pilot_default(),
    )
    assert verdict.accepted_class == AcceptedClass.LOSS


def test_loss_when_delta_pp_zero() -> None:
    decision = _decision(delta_pp=0.0, target_qids=("gs_026",))
    verdict = classify_acceptance_tier(
        decision=decision, policy=tier_acceptance_policy_pilot_default(),
    )
    assert verdict.accepted_class == AcceptedClass.LOSS


def test_net_win_with_debt_when_within_bounds() -> None:
    """Synthetic case: delta +4.0pp, no passing/soft to hard, 1
    unknown-to-hard, fixes_count=4 ≥ regressions_count + 2 = 3."""
    decision = _decision(
        delta_pp=+4.0,
        target_qids=("gs_a", "gs_b", "gs_c", "gs_d"),
        target_fixed_qids=("gs_a", "gs_b", "gs_c", "gs_d"),
        out_of_target_regressed_qids=("gs_z",),
        unknown_to_hard_regressed_qids=("gs_z",),
    )
    verdict = classify_acceptance_tier(
        decision=decision, policy=tier_acceptance_policy_pilot_default(),
    )
    assert verdict.accepted_class == AcceptedClass.NET_WIN_WITH_DEBT
    assert verdict.accept is True
    assert verdict.debt_classification == {"unknown_to_hard": ["gs_z"]}


def test_diagnostic_hold_when_fixes_vs_regressions_too_thin() -> None:
    """ccf1d60d iter-1 shape: delta +4.3, no passing/soft → hard, 1
    unknown_to_hard, target not fixed → diagnostic_hold."""
    decision_with_fixes = _decision(
        delta_pp=+4.3,
        target_qids=("gs_026",),
        target_still_hard_qids=("gs_026",),
        out_of_target_regressed_qids=("gs_012",),
        unknown_to_hard_regressed_qids=("gs_012",),
    )
    verdict = classify_acceptance_tier(
        decision=decision_with_fixes,
        policy=tier_acceptance_policy_pilot_default(),
    )
    assert verdict.accepted_class == AcceptedClass.DIAGNOSTIC_HOLD
    assert verdict.accept is False
    # Reflection payload populated for diagnostic_hold.
    assert "fixes_vs_regressions" in verdict.reflection_payload
