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
