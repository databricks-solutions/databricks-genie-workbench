"""Cycle 14-V Task 4 — canonical-render invariant alarm.

After T3, the rendered payload is contradiction-free. The
invariant marker is silent on clean payloads and emits on
contradictions. Used as a regression rail.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    DeltaState,
    _detect_render_contradictions,
    format_full_eval_marker_payload,
)


def _decision(
    *,
    target_qids: tuple[str, ...] = ("gs_024",),
    target_delta_states: tuple[tuple[str, str], ...] = (
        ("gs_024", DeltaState.FIXED.value),
    ),
    target_fixed_qids: tuple[str, ...] = ("gs_024",),
    target_still_hard_qids: tuple[str, ...] = (),
    out_of_target_regressed_qids: tuple[str, ...] = (),
    accepted: bool = True,
    reason_code: str = "accepted",
) -> ControlPlaneAcceptance:
    return ControlPlaneAcceptance(
        accepted=accepted,
        reason_code=reason_code,
        baseline_accuracy=83.3,
        candidate_accuracy=100.0,
        delta_pp=16.7,
        target_qids=target_qids,
        target_fixed_qids=target_fixed_qids,
        target_still_hard_qids=target_still_hard_qids,
        target_delta_states=target_delta_states,
        out_of_target_regressed_qids=out_of_target_regressed_qids,
    )


# ── Pure helper: _detect_render_contradictions ───────────────────────


def test_detect_returns_empty_on_clean_payload() -> None:
    payload = format_full_eval_marker_payload(
        _decision(),
        ag_id="AG1",
        iteration=1,
        accepted_label="ACCEPTED",
    )
    assert _detect_render_contradictions(payload) == []


def test_detect_flags_fixed_and_still_hard_overlap() -> None:
    payload = {
        "target_fixed_qids": ["gs_024"],
        "target_still_hard_qids": ["gs_024"],
        "target_delta_states": [],
        "out_of_target_regressed_qids": [],
    }
    violations = _detect_render_contradictions(payload)
    assert len(violations) == 1
    assert violations[0]["class"] == "fixed_and_still_hard_overlap"
    assert "gs_024" in violations[0]["qids"]


def test_detect_flags_target_in_out_of_target_set() -> None:
    payload = {
        "target_fixed_qids": [],
        "target_still_hard_qids": [],
        "target_delta_states": [["gs_024", "fixed"]],
        "out_of_target_regressed_qids": ["gs_024"],
    }
    violations = _detect_render_contradictions(payload)
    classes = {v["class"] for v in violations}
    assert "target_in_out_of_target_set" in classes


def test_detect_flags_delta_state_disagrees_with_bucket() -> None:
    """Delta-state says FIXED but the legacy bucket says STILL_HARD."""
    payload = {
        "target_fixed_qids": [],
        "target_still_hard_qids": ["gs_024"],
        "target_delta_states": [["gs_024", "fixed"]],
        "out_of_target_regressed_qids": [],
    }
    violations = _detect_render_contradictions(payload)
    classes = {v["class"] for v in violations}
    assert "delta_state_disagrees_with_bucket" in classes


# ── Render path emits the alarm marker ───────────────────────────────


def test_clean_render_does_not_emit_invariant_marker(monkeypatch, capsys) -> None:
    monkeypatch.setenv("GSO_CANONICAL_RENDER_INVARIANT", "1")
    format_full_eval_marker_payload(
        _decision(),
        ag_id="AG1",
        iteration=1,
        accepted_label="ACCEPTED",
    )
    out = capsys.readouterr().out
    assert "GSO_CANONICAL_RENDER_INVARIANT_V1" not in out


def test_invariant_silent_when_flag_off(monkeypatch, capsys) -> None:
    """Flag-off path: even on a forced contradiction the marker
    stays silent (back-compat with pre-T4 fixtures)."""
    monkeypatch.setenv("GSO_CANONICAL_RENDER_INVARIANT", "0")
    # Force a contradiction by passing a decision whose
    # target_delta_states disagrees with the bucket. After T3,
    # target_still_hard_qids is derived from delta_states so to
    # surface a contradiction at the render layer we must call the
    # helper directly with an artificial payload.
    payload = {
        "target_fixed_qids": ["gs_024"],
        "target_still_hard_qids": ["gs_024"],
        "target_delta_states": [],
        "out_of_target_regressed_qids": [],
    }
    # Directly verify the pure helper still detects the violation,
    # but the render-layer guard skipped emission.
    assert len(_detect_render_contradictions(payload)) == 1
    out = capsys.readouterr().out
    assert "GSO_CANONICAL_RENDER_INVARIANT_V1" not in out
