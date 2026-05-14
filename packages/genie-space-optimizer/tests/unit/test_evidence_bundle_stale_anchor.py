"""Phase 0.1 — STALE_ANCHOR MissingPiece classification.

The evidence bundle must fail closed when no Phase H artifact's
``lever_loop_task_run_id`` matches the resolved lever_loop task,
rather than silently anchoring on a stale Phase H run (the airline
ff71c000 defect).
"""
from __future__ import annotations

from genie_space_optimizer.tools.evidence_layout import (
    MissingPiece,
    MissingPieceKind,
)
from genie_space_optimizer.tools.evidence_bundle import (
    detect_stale_phase_h_anchor,
)


def test_stale_anchor_enum_value_exists():
    """MissingPieceKind.STALE_ANCHOR must be a valid enum value."""
    assert MissingPieceKind.STALE_ANCHOR.value == "stale_anchor"


def test_detect_stale_phase_h_anchor_returns_none_when_matched():
    """When at least one Phase H sibling carries the same
    lever_loop_task_run_id as the chosen task, NO stale-anchor
    sentinel is emitted."""
    result = detect_stale_phase_h_anchor(
        chosen_task_run_id="9999",
        phase_h_sibling_task_run_ids=("9999", "8888"),
    )
    assert result is None


def test_detect_stale_phase_h_anchor_emits_when_no_match():
    """When NONE of the Phase H siblings match, emit STALE_ANCHOR
    with a diagnosis naming the chosen task and the candidates that
    did not match."""
    result = detect_stale_phase_h_anchor(
        chosen_task_run_id="9999",
        phase_h_sibling_task_run_ids=("1111", "2222"),
    )
    assert isinstance(result, MissingPiece)
    assert result.kind == MissingPieceKind.STALE_ANCHOR
    assert "9999" in result.diagnosis
    assert "1111" in result.diagnosis or "2222" in result.diagnosis


def test_detect_stale_phase_h_anchor_emits_when_no_siblings():
    """Empty sibling list also yields STALE_ANCHOR — the bundle
    cannot anchor on Phase H artifacts that don't exist."""
    result = detect_stale_phase_h_anchor(
        chosen_task_run_id="9999",
        phase_h_sibling_task_run_ids=(),
    )
    assert isinstance(result, MissingPiece)
    assert result.kind == MissingPieceKind.STALE_ANCHOR
