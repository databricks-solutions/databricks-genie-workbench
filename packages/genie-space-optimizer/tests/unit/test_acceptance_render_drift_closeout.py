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


def test_acceptance_detail_string_is_not_stale_when_delta_states_populated():
    """Plan P-C anchor — ccf1d60d iter 1 reproduction.

    Today format_control_plane_acceptance_detail reads
    decision.target_still_hard_qids directly, which is empty,
    producing 'target_still_hard_qids=(none)' in the operator
    transcript. The marker payload (which derives from
    target_delta_states) shows 'gs_026'. After Plan P-C, both
    surfaces must show 'gs_026'.
    """
    decision = _decision_ccf1d60d_iter1()
    detail = format_control_plane_acceptance_detail(decision)
    assert "target_still_hard_qids=gs_026" in detail, (
        f"Stale render: {detail!r}"
    )
    assert "target_still_hard_qids=(none)" not in detail
    assert "out_of_target_regressed_qids=gs_012" in detail


def test_three_way_consistency_marker_decision_record_transcript():
    """Plan P-C charter — assert all THREE render surfaces show
    gs_026 / gs_012 consistently for the reconstructed ccf1d60d
    decision. No full lever loop required."""
    from genie_space_optimizer.optimization.decision_emitters import (
        ag_outcome_decision_record,
    )

    decision = _decision_ccf1d60d_iter1()
    ag = {"id": "ag1", "target_qids": ["gs_026"]}

    payload = format_full_eval_marker_payload(
        decision, ag_id="ag1", iteration=1, accepted_label="FAIL (REGRESSION)",
    )

    record = ag_outcome_decision_record(
        run_id="run-1",
        iteration=1,
        ag=ag,
        outcome="rolled_back",
        rca_id_by_cluster={},
        regression_qids=("gs_012",),
        acceptance_detail=decision,
    )
    assert record is not None
    metrics = dict(record.metrics or {})

    detail = format_control_plane_acceptance_detail(decision)

    assert payload["target_still_hard_qids"] == ["gs_026"]
    assert payload["out_of_target_regressed_qids"] == ["gs_012"]

    assert list(metrics.get("target_still_hard_qids") or []) == ["gs_026"], (
        f"DecisionRecord.metrics drift: {metrics!r}"
    )
    assert list(metrics.get("out_of_target_regressed_qids") or []) == ["gs_012"]

    assert "target_still_hard_qids=gs_026" in detail
    assert "out_of_target_regressed_qids=gs_012" in detail
    assert detail == record.reason_detail, (
        f"reason_detail drift between transcript renderer and "
        f"DecisionRecord: transcript={detail!r} vs "
        f"record={record.reason_detail!r}"
    )

    assert payload["reason_code"] == "target_qids_not_improved"
    assert record.reason_code.value.lower() in {
        "target_qids_not_improved",
    } or "target_qids_not_improved" in str(record.reason_code), (
        f"DecisionRecord.reason_code drift: {record.reason_code!r}"
    )
    assert "reason=target_qids_not_improved" in detail
