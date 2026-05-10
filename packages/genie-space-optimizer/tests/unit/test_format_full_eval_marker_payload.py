"""Cycle 14-T2 — format_full_eval_marker_payload canonical render.

The helper is the only render path for ControlPlaneAcceptance after
T2 ships. Both the typed stdout marker GSO_FULL_EVAL_V1 and the
replay acceptance_decided DecisionRecord read fields off this
helper's output. T2's binary success criterion is byte-equality
between the two surfaces; this test suite locks the helper's output
shape so that criterion can be verified.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    DeltaState,
    format_full_eval_marker_payload,
)


def _accepted_decision() -> ControlPlaneAcceptance:
    return ControlPlaneAcceptance(
        accepted=True,
        reason_code="accepted",
        baseline_accuracy=83.3,
        candidate_accuracy=100.0,
        delta_pp=16.7,
        target_qids=("gs_024",),
        target_fixed_qids=("gs_024",),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=(),
        target_delta_states=(("gs_024", DeltaState.FIXED.value),),
    )


def _rolled_back_lookup_failed_decision() -> ControlPlaneAcceptance:
    return ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_resolution_failed",
        baseline_accuracy=78.3,
        candidate_accuracy=78.3,
        delta_pp=0.0,
        target_qids=("gs_026",),
        target_fixed_qids=(),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=(),
        target_delta_states=(("gs_026", DeltaState.LOOKUP_FAILED.value),),
    )


def test_helper_returns_dict_with_stable_keys() -> None:
    payload = format_full_eval_marker_payload(
        _accepted_decision(),
        ag_id="AG_DECOMPOSED_H004",
        iteration=1,
        accepted_label="PASS -- ACCEPTED",
    )
    assert isinstance(payload, dict)
    expected_keys = {
        "iteration",
        "ag_id",
        "accepted",
        "reason_code",
        "accepted_label",
        "baseline_accuracy",
        "candidate_accuracy",
        "delta_pp",
        "target_qids",
        "target_fixed_qids",
        "target_still_hard_qids",
        # Cycle 14-W T1: SOFT_PASSING targets get a first-class
        # bucket field.
        "target_soft_passing_qids",
        "target_delta_states",
        "out_of_target_regressed_qids",
        "regression_debt_qids",
        "soft_to_hard_regressed_qids",
        "passing_to_hard_regressed_qids",
        "unknown_to_hard_regressed_qids",
        # Cycle 14-C T4: reattribution accounting fields. Empty
        # lists for non-drift decisions; populated only when the
        # accepted_with_attribution_drift branch fires.
        "accidentally_improved_qids",
        "unresolved_target_debt_qids",
        "reason_detail",
    }
    assert set(payload.keys()) == expected_keys


def test_helper_renders_accepted_decision() -> None:
    payload = format_full_eval_marker_payload(
        _accepted_decision(),
        ag_id="AG_DECOMPOSED_H004",
        iteration=1,
        accepted_label="PASS -- ACCEPTED",
    )
    assert payload["accepted"] is True
    assert payload["reason_code"] == "accepted"
    assert payload["accepted_label"] == "PASS -- ACCEPTED"
    assert payload["target_qids"] == ["gs_024"]
    assert payload["target_fixed_qids"] == ["gs_024"]
    assert payload["target_delta_states"] == [["gs_024", "fixed"]]


def test_helper_renders_target_resolution_failed() -> None:
    """Anchor: 7Now run 76457773587391 attempt 7 F2 — gs_026 in
    LOOKUP_FAILED state must surface as reason_code=
    target_resolution_failed (T0's invariant) and the helper's
    target_delta_states output captures the typed bucket."""
    payload = format_full_eval_marker_payload(
        _rolled_back_lookup_failed_decision(),
        ag_id="AG1",
        iteration=1,
        accepted_label="FAIL (REGRESSION)",
    )
    assert payload["accepted"] is False
    assert payload["reason_code"] == "target_resolution_failed"
    assert payload["target_delta_states"] == [["gs_026", "lookup_failed"]]
    # Out-of-target buckets default to empty when no regressions.
    assert payload["out_of_target_regressed_qids"] == []
    assert payload["regression_debt_qids"] == []


def test_helper_falls_through_when_target_delta_states_empty() -> None:
    """Pre-T0 fixtures (target_delta_states=()) still render. The
    helper falls back to the dataclass tuples for target_fixed /
    target_still."""
    decision = ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_qids_not_improved",
        baseline_accuracy=78.3,
        candidate_accuracy=78.3,
        delta_pp=0.0,
        target_qids=("gs_026",),
        target_fixed_qids=(),
        target_still_hard_qids=("gs_026",),
        out_of_target_regressed_qids=(),
        target_delta_states=(),
    )
    payload = format_full_eval_marker_payload(
        decision,
        ag_id="AG1",
        iteration=1,
        accepted_label="FAIL (REGRESSION)",
    )
    assert payload["target_delta_states"] == []
    assert payload["target_still_hard_qids"] == ["gs_026"]
    assert payload["reason_code"] == "target_qids_not_improved"


def test_helper_propagates_partial_harvest_buckets() -> None:
    """C14B-T2 acceptance with debt: out_of_target / regression_debt /
    soft_to_hard buckets must flow through unchanged."""
    decision = ControlPlaneAcceptance(
        accepted=True,
        reason_code="accepted_with_partial_harvest_debt",
        baseline_accuracy=78.3,
        candidate_accuracy=87.0,
        delta_pp=8.7,
        target_qids=("gs_026",),
        target_fixed_qids=("gs_026",),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=("gs_018",),
        regression_debt_qids=("gs_018",),
        soft_to_hard_regressed_qids=("gs_018",),
        target_delta_states=(("gs_026", DeltaState.FIXED.value),),
    )
    payload = format_full_eval_marker_payload(
        decision,
        ag_id="AG1",
        iteration=1,
        accepted_label="PASS -- ACCEPT WITH PARTIAL HARVEST",
    )
    assert payload["regression_debt_qids"] == ["gs_018"]
    assert payload["soft_to_hard_regressed_qids"] == ["gs_018"]
    assert payload["out_of_target_regressed_qids"] == ["gs_018"]
    assert payload["reason_code"] == "accepted_with_partial_harvest_debt"


def test_helper_reason_detail_is_format_control_plane_acceptance_detail() -> None:
    """Continuity: the reason_detail slot is the same human-readable
    string today's `format_control_plane_acceptance_detail` produces.
    Operators reading transcripts pre-T2 and post-T2 see the same
    free-form text in the FULL EVAL block."""
    from genie_space_optimizer.optimization.control_plane import (
        format_control_plane_acceptance_detail,
    )
    decision = _rolled_back_lookup_failed_decision()
    payload = format_full_eval_marker_payload(
        decision,
        ag_id="AG1",
        iteration=1,
        accepted_label="FAIL (REGRESSION)",
    )
    assert payload["reason_detail"] == format_control_plane_acceptance_detail(decision)


def test_helper_output_is_json_serializable() -> None:
    """T2's binary criterion includes byte-equality between the typed
    marker payload (which becomes JSON) and the replay record's
    metrics dict. Helper output must therefore be JSON-clean."""
    import json

    payload = format_full_eval_marker_payload(
        _accepted_decision(),
        ag_id="AG_DECOMPOSED_H004",
        iteration=1,
        accepted_label="PASS -- ACCEPTED",
    )
    serialized = json.dumps(payload, sort_keys=True)
    # Round-trip is identical (no NaN, no tuples leaking through).
    assert json.loads(serialized) == payload
