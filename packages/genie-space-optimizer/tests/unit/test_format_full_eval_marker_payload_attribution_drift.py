"""Cycle 14-C T4 — full-eval marker payload renders the two new
reattribution fields.

Anchor: airline run 1105451933925748 iter 1 — payload key set
must include `accidentally_improved_qids` and
`unresolved_target_debt_qids`."""
from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    format_full_eval_marker_payload,
)


def _attribution_drift_decision() -> ControlPlaneAcceptance:
    return ControlPlaneAcceptance(
        accepted=True,
        reason_code="accepted_with_attribution_drift",
        baseline_accuracy=83.3,
        candidate_accuracy=95.8,
        delta_pp=12.5,
        target_qids=("gs_024",),
        target_fixed_qids=(),
        target_still_hard_qids=("gs_024",),
        out_of_target_regressed_qids=(),
        accidentally_improved_qids=("gs_007", "gs_009", "gs_013"),
        unresolved_target_debt_qids=("gs_024",),
    )


def test_payload_includes_accidentally_improved_qids() -> None:
    payload = format_full_eval_marker_payload(
        _attribution_drift_decision(),
        ag_id="AG_DECOMPOSED_H004",
        iteration=1,
        accepted_label="PASS -- ACCEPTED",
    )
    assert payload["accidentally_improved_qids"] == [
        "gs_007", "gs_009", "gs_013",
    ]


def test_payload_includes_unresolved_target_debt_qids() -> None:
    payload = format_full_eval_marker_payload(
        _attribution_drift_decision(),
        ag_id="AG_DECOMPOSED_H004",
        iteration=1,
        accepted_label="PASS -- ACCEPTED",
    )
    assert payload["unresolved_target_debt_qids"] == ["gs_024"]


def test_legacy_decision_renders_empty_lists_for_new_keys() -> None:
    """Decisions that did NOT fire the drift branch render
    empty lists for the new keys so the payload schema is
    stable for every reason code."""
    decision = ControlPlaneAcceptance(
        accepted=True, reason_code="accepted",
        baseline_accuracy=80.0, candidate_accuracy=85.0, delta_pp=5.0,
        target_qids=("gs_001",), target_fixed_qids=("gs_001",),
        target_still_hard_qids=(), out_of_target_regressed_qids=(),
    )
    payload = format_full_eval_marker_payload(
        decision, ag_id="AG1", iteration=1, accepted_label="PASS -- ACCEPTED",
    )
    assert payload["accidentally_improved_qids"] == []
    assert payload["unresolved_target_debt_qids"] == []
    assert payload["target_fixed_qids"] == ["gs_001"]


def test_payload_keyset_is_complete() -> None:
    """The new keys are part of the canonical key set so
    JSON-schema validators in downstream tooling don't drop
    them."""
    payload = format_full_eval_marker_payload(
        _attribution_drift_decision(),
        ag_id="AG1", iteration=1, accepted_label="PASS -- ACCEPTED",
    )
    expected_keys = {
        "iteration", "ag_id", "accepted", "reason_code", "accepted_label",
        "baseline_accuracy", "candidate_accuracy", "delta_pp",
        "target_qids", "target_fixed_qids", "target_still_hard_qids",
        "target_soft_passing_qids", "target_delta_states",
        "out_of_target_regressed_qids", "regression_debt_qids",
        "soft_to_hard_regressed_qids", "passing_to_hard_regressed_qids",
        "unknown_to_hard_regressed_qids",
        "accidentally_improved_qids", "unresolved_target_debt_qids",
    }
    assert expected_keys.issubset(set(payload.keys()))
