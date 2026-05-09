"""Cycle 14-V Task 7 — integration test for both anchor shadow emissions.

Anchor 1 (7Now run 338386531912450, attempt 10): iter 2-5 each
emitted ``Proposals (0 total)`` for AG1. With Cycle 14-V T1 in
place AND ``GSO_FORBIDDEN_AG_ADMITS_NO_ACTION`` off, the shadow
marker ``GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1`` fires per
NO_ACTION reflection with ``suppressed_by_admit_no_action_off=True``.

Anchor 2 (airline run 833709971504406, attempt 12): AG3 iter 3+4
rolled back with ``reason_code=target_fixed_offset_by_regression``.
With Cycle 14-V T2 in place AND ``GSO_PATCH_SUBSET_ISOLATION`` off,
the shadow marker ``GSO_PATCH_ISOLATION_OBSERVE_V1`` fires with
``suppressed_by_isolation_flag_off=True``.

Both anchors: zero ``GSO_CANONICAL_RENDER_INVARIANT_V1`` emissions
on clean replay (T3+T4 silent regression rail).
"""

from __future__ import annotations

import json
import re

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    DeltaState,
    format_full_eval_marker_payload,
)
from genie_space_optimizer.optimization.harness import (
    _compute_forbidden_ag_set,
    _maybe_run_patch_isolation_orchestrator,
)


def _anchor1_no_action_reflections() -> list[dict]:
    """Iter 2-5 of 7Now run 338386531912450: same AG1 signature
    (root_cause=plural_top_n_collapse, blame=mv_esr_dim_location.zone_vp_name,
    levers=[1, 5]) re-emitted four times with zero proposals.
    Each iteration produces one NO_ACTION reflection."""
    return [
        {
            "iteration": it,
            "rollback_class": "no_action",
            "rollback_reason": "no_proposals",
            "accepted": False,
            "escalation_handled": False,
            "root_cause": "plural_top_n_collapse",
            "blame_set": ("mv_esr_dim_location.zone_vp_name",),
            "lever_set": [1, 5],
        }
        for it in (2, 3, 4, 5)
    ]


def _anchor2_offset_by_regression_decision() -> ControlPlaneAcceptance:
    """Airline run 833709971504406 AG3 shape: target gs_024 fixed
    but soft→hard regression on gs_018 → legacy code rejects with
    target_fixed_offset_by_regression.
    """
    return ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_fixed_offset_by_regression",
        baseline_accuracy=78.3,
        candidate_accuracy=87.0,
        delta_pp=8.7,
        target_qids=("gs_024",),
        target_fixed_qids=("gs_024",),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=("gs_018",),
        soft_to_hard_regressed_qids=("gs_018",),
        target_delta_states=(("gs_024", DeltaState.FIXED.value),),
    )


def test_anchor_1_emits_observe_marker_per_iteration_with_flag_off(
    monkeypatch, capsys,
) -> None:
    """Anchor 1: iters 2-5 each emit one shadow marker with
    suppressed_by_admit_no_action_off=True; the actual forbidden
    set stays empty under the flag-off behavior."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMISSION_OBSERVE", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "0")

    forbidden = _compute_forbidden_ag_set(_anchor1_no_action_reflections())
    out = capsys.readouterr().out

    observe_lines = re.findall(
        r"GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1\s+(\{.*\})", out
    )
    assert len(observe_lines) == 4
    for line in observe_lines:
        payload = json.loads(line)
        assert payload["rollback_class"] == "no_action"
        assert payload["would_admit_with_admit_no_action_on"] is True
        assert payload["behavior_flag_on"] is False
        assert payload["suppressed_by_admit_no_action_off"] is True

    # Behavior unchanged: forbidden set empty, AG1 keeps re-emitting
    # in real lever-loop replay.
    assert forbidden == set()


def test_anchor_2_emits_isolation_observe_with_flag_off(monkeypatch) -> None:
    """Anchor 2: AG3 reject with target_fixed_offset_by_regression →
    shadow marker emits with attribution_status='single_patch' and
    suppressed_by_isolation_flag_off=True. The diagnostic marker
    stays silent (behavior flag off)."""
    monkeypatch.setenv("GSO_PATCH_ISOLATION_OBSERVE", "1")
    monkeypatch.setenv("GSO_PATCH_SUBSET_ISOLATION", "0")

    applied = (
        {
            "patch_id": "p-good",
            "expanded_patch_id": "L5:p-good#0",
            "cluster_id": "H001",
            "affected_qids": ["gs_024"],
        },
        {
            "patch_id": "p-bad",
            "expanded_patch_id": "L4:p-bad#0",
            "cluster_id": "H002",
            "affected_qids": ["gs_018"],
        },
    )
    emitted: list[str] = []
    _maybe_run_patch_isolation_orchestrator(
        decision=_anchor2_offset_by_regression_decision(),
        iteration=3,
        ag_id="AG3",
        applied_patches=applied,
        clusters=[],
        run_id="run-airline-833709971504406",
        emit_marker=lambda line: emitted.append(line),
    )

    observe_lines = [l for l in emitted if "GSO_PATCH_ISOLATION_OBSERVE_V1" in l]
    diagnostic_lines = [
        l for l in emitted if "GSO_PATCH_ISOLATION_DIAGNOSTIC_V1" in l
    ]
    assert len(observe_lines) == 1
    assert len(diagnostic_lines) == 0
    payload = json.loads(re.search(r"\s+(\{.*\})", observe_lines[0]).group(1))
    assert payload["reason_code"] == "target_fixed_offset_by_regression"
    assert payload["attribution_status"] == "single_patch"
    assert payload["expanded_patch_id"] == "L4:p-bad#0"
    assert payload["suppressed_by_isolation_flag_off"] is True


def test_both_anchors_emit_zero_canonical_render_invariant_markers(
    monkeypatch, capsys,
) -> None:
    """T3+T4 regression rail: clean rendered payloads for both
    anchors must produce zero ``GSO_CANONICAL_RENDER_INVARIANT_V1``
    emissions. Failure means a regression of T3 introduced a new
    contradiction."""
    monkeypatch.setenv("GSO_CANONICAL_RENDER_INVARIANT", "1")

    # Render the airline (anchor 2) accepted decision and the 7Now
    # (anchor 1) rolled-back decision. Both should be silent post-T3.
    accepted = ControlPlaneAcceptance(
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
    rolled_back = ControlPlaneAcceptance(
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
    format_full_eval_marker_payload(
        accepted, ag_id="AG_DECOMPOSED_H004", iteration=1,
        accepted_label="PASS -- ACCEPTED",
    )
    format_full_eval_marker_payload(
        rolled_back, ag_id="AG1", iteration=1,
        accepted_label="FAIL (REGRESSION)",
    )
    out = capsys.readouterr().out
    assert "GSO_CANONICAL_RENDER_INVARIANT_V1" not in out
