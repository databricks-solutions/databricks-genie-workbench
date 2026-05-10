"""Cycle 14-W T1 — `target_soft_passing_qids` derivation.

Anchor: 7Now run 960148942255012 F2 — `gs_026=soft_passing` in
`target_delta_states` but absent from every bucket field.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    DeltaState,
    _detect_render_contradictions,
    format_full_eval_marker_payload,
)


def _decision_with_soft_passing_target() -> ControlPlaneAcceptance:
    return ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_qids_not_improved",
        baseline_accuracy=78.3,
        candidate_accuracy=86.4,
        delta_pp=8.1,
        target_qids=("gs_026",),
        target_fixed_qids=(),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=(),
        target_delta_states=(("gs_026", DeltaState.SOFT_PASSING.value),),
    )


def test_soft_passing_target_appears_in_target_soft_passing_qids() -> None:
    payload = format_full_eval_marker_payload(
        _decision_with_soft_passing_target(),
        ag_id="AG1",
        iteration=1,
        accepted_label="ROLLBACK",
    )
    assert payload["target_soft_passing_qids"] == ["gs_026"]


def test_soft_passing_target_excluded_from_fixed_and_still_hard() -> None:
    payload = format_full_eval_marker_payload(
        _decision_with_soft_passing_target(),
        ag_id="AG1",
        iteration=1,
        accepted_label="ROLLBACK",
    )
    assert payload["target_fixed_qids"] == []
    assert payload["target_still_hard_qids"] == []


def test_target_qid_in_two_state_buckets_is_a_contradiction() -> None:
    """Defensive: an internally-inconsistent payload (qid in
    soft_passing AND still_hard) must be flagged by the rail."""
    payload = {
        "target_delta_states": [["gs_026", "soft_passing"]],
        "target_fixed_qids": [],
        "target_still_hard_qids": ["gs_026"],
        "target_soft_passing_qids": ["gs_026"],
        "out_of_target_regressed_qids": [],
        "unknown_to_hard_regressed_qids": [],
    }
    violations = _detect_render_contradictions(payload)
    assert any(
        v.get("class") == "qid_in_multiple_state_buckets"
        and "gs_026" in (v.get("qids") or ())
        for v in violations
    ), violations


def test_legacy_fall_through_when_target_delta_states_empty() -> None:
    """Pre-T0 fixtures (empty target_delta_states) → empty soft_passing
    list (no derivation source); no false positive."""
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
        decision, ag_id="AG1", iteration=1, accepted_label="ROLLBACK",
    )
    assert payload["target_soft_passing_qids"] == []


def test_full_payload_keyset_includes_target_soft_passing_qids() -> None:
    """The new field is part of the canonical key set."""
    payload = format_full_eval_marker_payload(
        _decision_with_soft_passing_target(),
        ag_id="AG1",
        iteration=1,
        accepted_label="ROLLBACK",
    )
    assert "target_soft_passing_qids" in payload
