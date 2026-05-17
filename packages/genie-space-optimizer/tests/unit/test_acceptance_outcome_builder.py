"""Phase 1 — Acceptance Unification.

Pure-function unit tests for ``build_acceptance_outcome``. The builder
wraps a strict-gate ``GainGateDecision`` + a canonical
``ControlPlaneAcceptance`` into a single ``AcceptanceOutcome`` that
downstream surfaces consume.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.acceptance_outcome import (
    AcceptanceOutcome,
    acceptance_decision_dict,
    build_acceptance_outcome,
    derive_accepted_label,
)
from genie_space_optimizer.optimization.acceptance_policy import (
    GainGateDecision,
)
from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
)


def _gain_gate(*, accepted: bool, reason: str, delta: float = 0.0) -> GainGateDecision:
    return GainGateDecision(
        accepted=accepted,
        post_arbiter_candidate=25.0,
        post_arbiter_baseline=25.0,
        delta_pp=delta,
        min_gain_pp=2.0,
        reason_code=reason,
    )


def _control_plane(
    *,
    accepted: bool,
    reason: str,
    target_fixed: tuple[str, ...] = (),
    out_of_target_regressed: tuple[str, ...] = (),
) -> ControlPlaneAcceptance:
    return ControlPlaneAcceptance(
        accepted=accepted,
        reason_code=reason,
        baseline_accuracy=25.0,
        candidate_accuracy=25.0,
        delta_pp=0.0,
        target_qids=("gs_013",),
        target_fixed_qids=target_fixed,
        target_still_hard_qids=(),
        out_of_target_regressed_qids=out_of_target_regressed,
    )


def test_attribution_drift_accept_yields_single_canonical_verdict():
    """Run A iter 1 anchor — control-plane accepts, gain gate rejects."""
    strict = _gain_gate(accepted=False, reason="rejected_insufficient_gain")
    canonical = _control_plane(
        accepted=True,
        reason="accepted_with_attribution_drift",
        target_fixed=("gs_013",),
    )

    outcome = build_acceptance_outcome(
        strict_decision=strict,
        control_plane_decision=canonical,
        enable_control_plane_acceptance=True,
    )

    assert outcome.accepted is True
    assert outcome.reason_code == "accepted_with_attribution_drift"
    assert outcome.gain_gate_failed is True
    assert outcome.control_plane_failed is False
    assert outcome.target_fixed_qids == ("gs_013",)
    assert outcome.accepted_label == "PASS"


def test_strict_win_yields_single_canonical_verdict():
    strict = _gain_gate(accepted=True, reason="accepted", delta=4.5)
    canonical = _control_plane(
        accepted=True,
        reason="accepted",
        target_fixed=("gs_013",),
    )

    outcome = build_acceptance_outcome(
        strict_decision=strict,
        control_plane_decision=canonical,
        enable_control_plane_acceptance=True,
    )

    assert outcome.accepted is True
    assert outcome.reason_code == "accepted"
    assert outcome.gain_gate_failed is False
    assert outcome.control_plane_failed is False
    assert outcome.accepted_label == "PASS"


def test_both_reject_yields_rollback():
    strict = _gain_gate(accepted=False, reason="rejected_regression", delta=-3.0)
    canonical = _control_plane(
        accepted=False,
        reason="target_fixed_offset_by_regression",
        out_of_target_regressed=("gs_002", "gs_007"),
    )

    outcome = build_acceptance_outcome(
        strict_decision=strict,
        control_plane_decision=canonical,
        enable_control_plane_acceptance=True,
    )

    assert outcome.accepted is False
    assert outcome.gain_gate_failed is True
    assert outcome.control_plane_failed is True
    assert outcome.accepted_label == "FAIL (REGRESSION)"
    judges = sorted(e["judge"] for e in outcome.regression_attribution)
    assert judges == [
        "acceptance_gate (rejected_regression)",
        "control_plane_acceptance",
    ]


def test_control_plane_only_reject_yields_rollback():
    strict = _gain_gate(accepted=True, reason="accepted", delta=3.0)
    canonical = _control_plane(
        accepted=False,
        reason="target_fixed_offset_by_regression",
        out_of_target_regressed=("gs_005",),
    )

    outcome = build_acceptance_outcome(
        strict_decision=strict,
        control_plane_decision=canonical,
        enable_control_plane_acceptance=True,
    )

    assert outcome.accepted is False
    assert outcome.gain_gate_failed is False
    assert outcome.control_plane_failed is True
    assert outcome.accepted_label == "FAIL (REGRESSION)"
    assert any(
        e["judge"] == "control_plane_acceptance"
        for e in outcome.regression_attribution
    )


def test_control_plane_acceptance_flag_off_drops_canonical_path():
    """When ``enable_control_plane_acceptance`` is False, no regression
    entry is appended from the control plane."""
    strict = _gain_gate(accepted=False, reason="rejected_regression", delta=-1.0)
    canonical = _control_plane(
        accepted=False,
        reason="target_fixed_offset_by_regression",
        out_of_target_regressed=("gs_005",),
    )

    outcome = build_acceptance_outcome(
        strict_decision=strict,
        control_plane_decision=canonical,
        enable_control_plane_acceptance=False,
    )

    assert outcome.accepted is False  # canonical owner says reject
    judges = [e["judge"] for e in outcome.regression_attribution]
    assert judges == ["acceptance_gate (rejected_regression)"]


def test_acceptance_decision_dict_pass_path_carries_canonical_and_accepted():
    strict = _gain_gate(accepted=True, reason="accepted", delta=4.5)
    canonical = _control_plane(
        accepted=True, reason="accepted", target_fixed=("gs_013",)
    )
    outcome = build_acceptance_outcome(
        strict_decision=strict,
        control_plane_decision=canonical,
        enable_control_plane_acceptance=True,
    )

    decision = acceptance_decision_dict(outcome)

    assert decision["accepted"] is True
    assert decision["reason"] == "accepted"
    assert decision["target_qids"] == ["gs_013"]
    assert decision["target_fixed_qids"] == ["gs_013"]
    assert decision["_canonical"] is canonical


def test_acceptance_decision_dict_rollback_path_carries_canonical_and_accepted():
    strict = _gain_gate(accepted=False, reason="rejected_regression", delta=-1.0)
    canonical = _control_plane(
        accepted=False,
        reason="target_fixed_offset_by_regression",
        out_of_target_regressed=("gs_005",),
    )
    outcome = build_acceptance_outcome(
        strict_decision=strict,
        control_plane_decision=canonical,
        enable_control_plane_acceptance=True,
    )

    decision = acceptance_decision_dict(outcome)

    assert decision["accepted"] is False
    assert decision["reason"] == "target_fixed_offset_by_regression"
    assert decision["_canonical"] is canonical


def test_derive_accepted_label_known_codes():
    assert derive_accepted_label("accepted") == "PASS"
    assert derive_accepted_label("accepted_with_attribution_drift") == "PASS"
    assert derive_accepted_label("accepted_with_regression_debt") == "PASS WITH DEBT"
    assert derive_accepted_label("accepted_with_partial_harvest_debt") == "PASS WITH DEBT"
    assert (
        derive_accepted_label("accepted_with_attribution_drift_and_debt")
        == "PASS WITH DEBT"
    )
    assert derive_accepted_label("rejected_insufficient_gain") == "FAIL (REGRESSION)"
    assert derive_accepted_label("rejected_regression") == "FAIL (REGRESSION)"
    assert (
        derive_accepted_label("target_fixed_offset_by_regression")
        == "FAIL (REGRESSION)"
    )


def test_acceptance_outcome_is_frozen():
    strict = _gain_gate(accepted=True, reason="accepted", delta=4.5)
    canonical = _control_plane(
        accepted=True, reason="accepted", target_fixed=("gs_013",)
    )
    outcome = build_acceptance_outcome(
        strict_decision=strict,
        control_plane_decision=canonical,
        enable_control_plane_acceptance=True,
    )

    with pytest.raises(Exception):  # dataclasses.FrozenInstanceError
        outcome.accepted = False  # type: ignore[misc]
