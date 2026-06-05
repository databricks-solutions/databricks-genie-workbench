"""Trial 23 W2 — target-honest acceptance.

The e943 postmortem (``FULL_EVAL_ACCEPTED_WITH_UNRESOLVED_TARGET_DEBT``)
showed a +8.3pp global gain accepted as deployable while the named
target QID stayed hard. With the W2 demotion enabled, a net win whose
target debt is unresolved is classified ``NET_WIN_NON_DEPLOYABLE`` with
``accept=False``.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.acceptance_policy import (
    AcceptedClass,
    classify_acceptance_tier,
    tier_acceptance_policy_pilot_default,
)
from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
)


def _decision(**kw) -> ControlPlaneAcceptance:
    base = dict(
        accepted=False,
        reason_code="ignored_for_test",
        baseline_accuracy=0.0,
        candidate_accuracy=0.0,
        delta_pp=0.0,
        target_qids=(),
        target_fixed_qids=(),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=(),
        passing_to_hard_regressed_qids=(),
        soft_to_hard_regressed_qids=(),
        unknown_to_hard_regressed_qids=(),
        protected_regressed_qids=(),
    )
    base.update(kw)
    return ControlPlaneAcceptance(**base)


def _net_win_unresolved_target():
    # +8.3pp global gain, >=2 target fixes (clears the net-win fixes
    # margin) but the named target gs_009 stays hard -> unresolved debt.
    return _decision(
        delta_pp=8.3,
        target_qids=("gs_009", "gs_010", "gs_011"),
        target_fixed_qids=("gs_010", "gs_011"),
        target_still_hard_qids=("gs_009",),
    )


def test_net_win_with_unresolved_target_is_deployable_when_flag_off():
    """Default (demote off) preserves the prior NET_WIN_WITH_DEBT accept."""
    verdict = classify_acceptance_tier(
        decision=_net_win_unresolved_target(),
        policy=tier_acceptance_policy_pilot_default(),
        demote_on_unresolved_target_debt=False,
    )
    assert verdict.accepted_class == AcceptedClass.NET_WIN_WITH_DEBT
    assert verdict.accept is True


def test_net_win_with_unresolved_target_demoted_when_flag_on():
    verdict = classify_acceptance_tier(
        decision=_net_win_unresolved_target(),
        policy=tier_acceptance_policy_pilot_default(),
        demote_on_unresolved_target_debt=True,
    )
    assert verdict.accepted_class == AcceptedClass.NET_WIN_NON_DEPLOYABLE
    assert verdict.accept is False
    assert verdict.reflection_payload["unresolved_target_debt_qids"] == ["gs_009"]


def test_strict_win_not_demoted_even_with_flag_on():
    """A genuine target fix is never demoted."""
    verdict = classify_acceptance_tier(
        decision=_decision(
            delta_pp=5.0,
            target_qids=("gs_009",),
            target_fixed_qids=("gs_009",),
        ),
        policy=tier_acceptance_policy_pilot_default(),
        demote_on_unresolved_target_debt=True,
    )
    assert verdict.accepted_class == AcceptedClass.STRICT_WIN
    assert verdict.accept is True
