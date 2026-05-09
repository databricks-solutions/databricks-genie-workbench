"""Cycle 14-V Task 2 — shadow-mode observability for the C14B-T3
patch-isolation orchestrator.

Anchor: airline run 833709971504406 AG3 iter 3+4 — both candidates
rolled back with reason_code=target_fixed_offset_by_regression but
GSO_PATCH_SUBSET_ISOLATION was off so no diagnostic markers were
emitted. After T2, GSO_PATCH_ISOLATION_OBSERVE_V1 fires regardless
of the behavior flag, recording attribution evidence on the
canonical triggers.
"""

from __future__ import annotations

import json
import re

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    DeltaState,
)
from genie_space_optimizer.optimization.harness import (
    _maybe_run_patch_isolation_orchestrator,
)


def _patch(patch_id: str, expanded_id: str, cluster_id: str, qids=()) -> dict:
    return {
        "patch_id": patch_id,
        "expanded_patch_id": expanded_id,
        "cluster_id": cluster_id,
        "affected_qids": list(qids),
    }


def _decision_offset_by_regression() -> ControlPlaneAcceptance:
    """Anchor 2 AG3 shape: target gs_024 fixed but soft→hard
    regression on gs_018; legacy code rejects with
    target_fixed_offset_by_regression."""
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


def _decision_unbounded_collateral() -> ControlPlaneAcceptance:
    return ControlPlaneAcceptance(
        accepted=False,
        reason_code="rejected_unbounded_collateral",
        baseline_accuracy=78.3,
        candidate_accuracy=82.0,
        delta_pp=3.7,
        target_qids=("gs_026",),
        target_fixed_qids=("gs_026",),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=("gs_018", "gs_004"),
        soft_to_hard_regressed_qids=("gs_018",),
        passing_to_hard_regressed_qids=("gs_004",),
    )


def _decision_accepted() -> ControlPlaneAcceptance:
    return ControlPlaneAcceptance(
        accepted=True,
        reason_code="accepted",
        baseline_accuracy=78.3,
        candidate_accuracy=95.0,
        delta_pp=16.7,
        target_qids=("gs_024",),
        target_fixed_qids=("gs_024",),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=(),
    )


def _decision_target_qids_not_improved() -> ControlPlaneAcceptance:
    return ControlPlaneAcceptance(
        accepted=False,
        reason_code="target_qids_not_improved",
        baseline_accuracy=78.3,
        candidate_accuracy=78.3,
        delta_pp=0.0,
        target_qids=("gs_026",),
        target_fixed_qids=(),
        target_still_hard_qids=("gs_026",),
        out_of_target_regressed_qids=(),
    )


def _capture_marker(emitted: list) -> callable:
    def _emit(line: str) -> None:
        emitted.append(line)
    return _emit


def test_observe_emits_on_target_fixed_offset_when_observe_on_isolation_off(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_PATCH_ISOLATION_OBSERVE", "1")
    monkeypatch.setenv("GSO_PATCH_SUBSET_ISOLATION", "0")

    applied = (
        _patch("p-good", "L5:p-good#0", "H001", qids=("gs_024",)),
        _patch("p-bad", "L4:p-bad#0", "H002", qids=("gs_018",)),
    )
    emitted: list[str] = []
    _maybe_run_patch_isolation_orchestrator(
        decision=_decision_offset_by_regression(),
        iteration=3,
        ag_id="AG3",
        applied_patches=applied,
        clusters=[],
        run_id="run-airline",
        emit_marker=_capture_marker(emitted),
    )

    observe_lines = [l for l in emitted if "GSO_PATCH_ISOLATION_OBSERVE_V1" in l]
    diagnostic_lines = [
        l for l in emitted if "GSO_PATCH_ISOLATION_DIAGNOSTIC_V1" in l
    ]
    assert len(observe_lines) == 1
    assert len(diagnostic_lines) == 0
    payload = json.loads(re.search(r"\s+(\{.*\})", observe_lines[0]).group(1))
    assert payload["reason_code"] == "target_fixed_offset_by_regression"
    assert payload["regressed_qid"] == "gs_018"
    assert payload["attribution_status"] == "single_patch"
    assert payload["expanded_patch_id"] == "L4:p-bad#0"
    assert payload["behavior_flag_on"] is False
    assert payload["suppressed_by_isolation_flag_off"] is True


def test_observe_emits_on_rejected_unbounded_collateral(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PATCH_ISOLATION_OBSERVE", "1")
    monkeypatch.setenv("GSO_PATCH_SUBSET_ISOLATION", "0")

    emitted: list[str] = []
    _maybe_run_patch_isolation_orchestrator(
        decision=_decision_unbounded_collateral(),
        iteration=2,
        ag_id="AG2",
        applied_patches=(),
        clusters=[],
        run_id="run-x",
        emit_marker=_capture_marker(emitted),
    )

    assert any("GSO_PATCH_ISOLATION_OBSERVE_V1" in l for l in emitted)


def test_observe_does_not_emit_when_observe_off(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PATCH_ISOLATION_OBSERVE", "0")
    monkeypatch.setenv("GSO_PATCH_SUBSET_ISOLATION", "0")

    emitted: list[str] = []
    _maybe_run_patch_isolation_orchestrator(
        decision=_decision_offset_by_regression(),
        iteration=1,
        ag_id="AG1",
        applied_patches=(),
        clusters=[],
        run_id="run-x",
        emit_marker=_capture_marker(emitted),
    )

    assert emitted == []


def test_observe_does_not_emit_when_decision_accepted(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PATCH_ISOLATION_OBSERVE", "1")
    monkeypatch.setenv("GSO_PATCH_SUBSET_ISOLATION", "0")

    emitted: list[str] = []
    _maybe_run_patch_isolation_orchestrator(
        decision=_decision_accepted(),
        iteration=1,
        ag_id="AG1",
        applied_patches=(),
        clusters=[],
        run_id="run-x",
        emit_marker=_capture_marker(emitted),
    )

    assert emitted == []


def test_observe_does_not_emit_for_non_canonical_reason_code(
    monkeypatch,
) -> None:
    """target_qids_not_improved is NOT an isolation trigger; the
    orchestrator must short-circuit before any marker emit."""
    monkeypatch.setenv("GSO_PATCH_ISOLATION_OBSERVE", "1")
    monkeypatch.setenv("GSO_PATCH_SUBSET_ISOLATION", "0")

    emitted: list[str] = []
    _maybe_run_patch_isolation_orchestrator(
        decision=_decision_target_qids_not_improved(),
        iteration=1,
        ag_id="AG1",
        applied_patches=(),
        clusters=[],
        run_id="run-x",
        emit_marker=_capture_marker(emitted),
    )

    assert emitted == []


def test_observe_and_diagnostic_both_emit_when_both_flags_on(
    monkeypatch,
) -> None:
    """When both the observe flag AND the behavior flag are on, the
    shadow marker fires AND the existing diagnostic marker fires —
    the existing diagnostic surface is not silenced."""
    monkeypatch.setenv("GSO_PATCH_ISOLATION_OBSERVE", "1")
    monkeypatch.setenv("GSO_PATCH_SUBSET_ISOLATION", "1")

    applied = (
        _patch("p-bad", "L4:p-bad#0", "H002", qids=("gs_018",)),
    )
    emitted: list[str] = []
    _maybe_run_patch_isolation_orchestrator(
        decision=_decision_offset_by_regression(),
        iteration=3,
        ag_id="AG3",
        applied_patches=applied,
        clusters=[],
        run_id="run-airline",
        emit_marker=_capture_marker(emitted),
    )

    assert any("GSO_PATCH_ISOLATION_OBSERVE_V1" in l for l in emitted)
    assert any("GSO_PATCH_ISOLATION_DIAGNOSTIC_V1" in l for l in emitted)
    # Observe payload reports behavior_flag_on=True now.
    observe = next(
        l for l in emitted if "GSO_PATCH_ISOLATION_OBSERVE_V1" in l
    )
    payload = json.loads(re.search(r"\s+(\{.*\})", observe).group(1))
    assert payload["behavior_flag_on"] is True
    assert payload["suppressed_by_isolation_flag_off"] is False
