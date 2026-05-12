"""Unit tests for the canonical acceptance-decision renderer (Plan P-C)."""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.control_plane import (
    AcceptanceDecisionRendering,
    ControlPlaneAcceptance,
    DeltaState,
    render_acceptance_decision,
)


def _decision_ccf1d60d_iter1() -> ControlPlaneAcceptance:
    """Reconstructs the iter-1 decision from anchor run ccf1d60d.

    GSO_FULL_EVAL_V1 payload showed:
      reason_code="target_qids_not_improved"
      target_qids=["gs_026"]
      target_delta_states=[["gs_026", "STILL_HARD"]]
      out_of_target_regressed_qids=["gs_012"]
    """
    return ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_qids_not_improved",
        baseline_accuracy=0.870,
        candidate_accuracy=0.870,
        delta_pp=0.0,
        target_qids=("gs_026",),
        target_fixed_qids=(),
        # Legacy field intentionally EMPTY — the bug is that callers
        # which read this field directly produced "(none)" while
        # callers reading target_delta_states produced ["gs_026"].
        target_still_hard_qids=(),
        out_of_target_regressed_qids=("gs_012",),
        target_delta_states=(("gs_026", DeltaState.STILL_HARD.value),),
    )


def test_rendering_has_required_fields():
    decision = _decision_ccf1d60d_iter1()
    rendering = render_acceptance_decision(
        decision, ag_id="ag1", iteration=1, accepted_label="FAIL (REGRESSION)",
    )
    assert isinstance(rendering, AcceptanceDecisionRendering)
    assert rendering.reason_code == "target_qids_not_improved"
    assert rendering.target_still_hard_qids == ("gs_026",)
    assert rendering.out_of_target_regressed_qids == ("gs_012",)
    assert rendering.target_fixed_qids == ()


def test_rendering_falls_back_to_legacy_fields_when_delta_states_empty():
    """Pre-T0 fixtures without target_delta_states must render the
    legacy tuple fields verbatim — preserves byte-stable replay."""
    decision = ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_qids_not_improved",
        baseline_accuracy=0.5,
        candidate_accuracy=0.5,
        delta_pp=0.0,
        target_qids=("q1",),
        target_fixed_qids=(),
        target_still_hard_qids=("q1",),
        out_of_target_regressed_qids=("q2",),
        target_delta_states=(),
    )
    rendering = render_acceptance_decision(
        decision, ag_id="ag1", iteration=1, accepted_label="FAIL",
    )
    assert rendering.target_still_hard_qids == ("q1",)
    assert rendering.target_fixed_qids == ()
    assert rendering.out_of_target_regressed_qids == ("q2",)


def test_rendering_unknown_to_hard_subtracts_target_qids():
    """When target_delta_states is populated, target QIDs must be
    subtracted from unknown_to_hard_regressed_qids — closes airline
    anchor 833709971504406 gs_016 mis-classification."""
    decision = ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_qids_not_improved",
        baseline_accuracy=0.5,
        candidate_accuracy=0.5,
        delta_pp=0.0,
        target_qids=("gs_016",),
        target_fixed_qids=(),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=(),
        target_delta_states=(("gs_016", DeltaState.STILL_HARD.value),),
        unknown_to_hard_regressed_qids=("gs_016", "gs_017"),
    )
    rendering = render_acceptance_decision(
        decision, ag_id="ag1", iteration=1, accepted_label="FAIL",
    )
    assert rendering.unknown_to_hard_regressed_qids == ("gs_017",)
    assert "gs_016" in rendering.target_still_hard_qids


def test_rendering_is_pure_same_input_byte_equal_output():
    decision = _decision_ccf1d60d_iter1()
    a = render_acceptance_decision(
        decision, ag_id="ag1", iteration=1, accepted_label="FAIL",
    )
    b = render_acceptance_decision(
        decision, ag_id="ag1", iteration=1, accepted_label="FAIL",
    )
    assert a == b
