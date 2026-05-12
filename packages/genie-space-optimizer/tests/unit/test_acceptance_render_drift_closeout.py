"""Plan P-C — cross-emitter byte-equality tests for the acceptance
render path. Reproduces the ccf1d60d drift and asserts the closeout."""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    DeltaState,
    format_control_plane_acceptance_detail,
    format_full_eval_marker_payload,
    render_acceptance_decision,
)


def _decision_ccf1d60d_iter1() -> ControlPlaneAcceptance:
    return ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_qids_not_improved",
        baseline_accuracy=0.870,
        candidate_accuracy=0.870,
        delta_pp=0.0,
        target_qids=("gs_026",),
        target_fixed_qids=(),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=("gs_012",),
        target_delta_states=(("gs_026", DeltaState.STILL_HARD.value),),
    )


def test_full_eval_payload_uses_render_acceptance_decision():
    decision = _decision_ccf1d60d_iter1()
    payload = format_full_eval_marker_payload(
        decision, ag_id="ag1", iteration=1, accepted_label="FAIL (REGRESSION)",
    )
    rendering = render_acceptance_decision(
        decision, ag_id="ag1", iteration=1, accepted_label="FAIL (REGRESSION)",
    )
    for key in (
        "reason_code",
        "target_qids",
        "target_fixed_qids",
        "target_still_hard_qids",
        "out_of_target_regressed_qids",
        "regression_debt_qids",
        "target_delta_states",
    ):
        rhs = getattr(rendering, key)
        if isinstance(rhs, tuple):
            if rhs and isinstance(rhs[0], tuple):
                rhs_serialised = [list(p) for p in rhs]
            else:
                rhs_serialised = list(rhs)
        else:
            rhs_serialised = rhs
        assert payload[key] == rhs_serialised, (
            f"{key}: marker={payload[key]!r} vs rendering={rhs_serialised!r}"
        )
