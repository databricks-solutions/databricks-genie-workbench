"""Cycle 16-T3 — canonical render payload pins.

Today the canonical render is produced by
``format_full_eval_marker_payload(decision, ag_id, iteration, accepted_label)``
in ``optimization/control_plane.py``. Two consumers read it: the
``GSO_FULL_EVAL_V1`` typed stdout marker and the ``acceptance_decided``
``DecisionRecord``. C16-T3 adds a new key
``existing_hard_still_hard_outside_target_qids`` to the payload; this
suite pins:

1. The key is always present in the rendered payload (defaults to ``[]``).
2. Populated buckets are rendered as a list (not a tuple) for JSON
   compatibility.
3. The render-contradiction self-check fires when the new bucket
   overlaps any of {target_fixed, target_still_hard, soft_passing}.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    _detect_render_contradictions,
    format_full_eval_marker_payload,
)


def _build_decision(**overrides) -> ControlPlaneAcceptance:
    defaults = dict(
        accepted=True,
        reason_code="accepted",
        baseline_accuracy=80.0,
        candidate_accuracy=90.0,
        delta_pp=10.0,
        target_qids=("target_qid",),
        target_fixed_qids=("target_qid",),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=(),
        regression_debt_qids=(),
        protected_regressed_qids=(),
        soft_to_hard_regressed_qids=(),
        passing_to_hard_regressed_qids=(),
        unknown_to_hard_regressed_qids=(),
        target_delta_states=(),
        target_soft_passing_qids=(),
        accidentally_improved_qids=(),
        unresolved_target_debt_qids=(),
        existing_hard_still_hard_outside_target_qids=(),
    )
    defaults.update(overrides)
    return ControlPlaneAcceptance(**defaults)


def test_canonical_payload_always_includes_existing_hard_bucket_key() -> None:
    """Even when the bucket is empty, the payload key must exist so
    downstream consumers can rely on the schema.
    """
    decision = _build_decision()
    payload = format_full_eval_marker_payload(
        decision, ag_id="AG1", iteration=1, accepted_label="ACCEPTED",
    )
    assert "existing_hard_still_hard_outside_target_qids" in payload
    assert payload["existing_hard_still_hard_outside_target_qids"] == []


def test_canonical_payload_renders_populated_existing_hard_bucket_as_list() -> None:
    """Populated bucket is rendered as a JSON-compatible list."""
    decision = _build_decision(
        existing_hard_still_hard_outside_target_qids=("non_target_a", "non_target_b"),
    )
    payload = format_full_eval_marker_payload(
        decision, ag_id="AG1", iteration=1, accepted_label="ACCEPTED",
    )
    assert payload["existing_hard_still_hard_outside_target_qids"] == [
        "non_target_a", "non_target_b",
    ]


def test_render_contradiction_detector_flags_overlap_with_target_fixed() -> None:
    """A QID in both target_fixed_qids and the new existing-hard
    bucket is a math error — they're disjoint by construction.
    The render self-check must surface it.
    """
    payload = {
        "target_fixed_qids": ["q_a"],
        "target_still_hard_qids": [],
        "target_soft_passing_qids": [],
        "target_delta_states": [],
        "out_of_target_regressed_qids": [],
        "existing_hard_still_hard_outside_target_qids": ["q_a"],
    }
    violations = _detect_render_contradictions(payload)
    classes = {v["class"] for v in violations}
    assert "existing_hard_overlaps_target_bucket" in classes
    overlap = [
        v for v in violations
        if v["class"] == "existing_hard_overlaps_target_bucket"
    ][0]
    assert "q_a" in overlap["qids"]


def test_render_contradiction_detector_silent_when_buckets_disjoint() -> None:
    """Clean payload — no overlap. Detector is silent."""
    payload = {
        "target_fixed_qids": ["target_q"],
        "target_still_hard_qids": [],
        "target_soft_passing_qids": [],
        "target_delta_states": [],
        "out_of_target_regressed_qids": [],
        "existing_hard_still_hard_outside_target_qids": ["non_target_q"],
    }
    overlap_classes = {
        v["class"]
        for v in _detect_render_contradictions(payload)
        if v["class"] == "existing_hard_overlaps_target_bucket"
    }
    assert overlap_classes == set()
