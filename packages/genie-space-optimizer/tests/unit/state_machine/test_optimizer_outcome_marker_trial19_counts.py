"""Trial 19 C4 — ``GSO_OPTIMIZER_OUTCOME_V1`` counter shape.

Pins:

* ``hard_qids_already_correct_count`` and ``pending_gt_review_count``
  are additive optional fields with default 0 (back-compat with
  pre-Trial-19 tape fixtures + dashboards).
* When provided, both counts round-trip into the marker payload.
* ``outcome`` remains the legacy classifier key (unchanged).
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.state_machine.markers import (
    optimizer_outcome_marker,
)


def _parse_payload(line: str) -> dict:
    assert line.startswith("GSO_OPTIMIZER_OUTCOME_V1 ")
    return json.loads(line.split(" ", 1)[1])


def test_outcome_marker_defaults_counters_to_zero():
    line = optimizer_outcome_marker(
        run_id="r1",
        outcome="OPTIMIZER_IMPROVED",
        hard_qids_count=3,
        deepest_stage_by_qid={"qid1": "ACCEPTED"},
    )
    payload = _parse_payload(line)
    assert payload["hard_qids_already_correct_count"] == 0
    assert payload["pending_gt_review_count"] == 0


def test_outcome_marker_round_trips_trial19_counters():
    line = optimizer_outcome_marker(
        run_id="r1",
        outcome="OPTIMIZER_TRIED_INSUFFICIENT_GAIN",
        hard_qids_count=5,
        deepest_stage_by_qid={"qid1": "ACCEPTED"},
        hard_qids_already_correct_count=2,
        pending_gt_review_count=3,
    )
    payload = _parse_payload(line)
    assert payload["hard_qids_already_correct_count"] == 2
    assert payload["pending_gt_review_count"] == 3
    # Legacy fields untouched.
    assert payload["outcome"] == "OPTIMIZER_TRIED_INSUFFICIENT_GAIN"
    assert payload["hard_qids_count"] == 5
