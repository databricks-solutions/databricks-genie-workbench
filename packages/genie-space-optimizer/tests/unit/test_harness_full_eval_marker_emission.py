"""Cycle 14-T2 — harness emits GSO_FULL_EVAL_V1 alongside the FULL
EVAL text block.

Static-source tests pin the two FULL EVAL print sites in harness.py
so future refactors keep both legacy text and typed marker emission
in lock-step. Behavioural emission is exercised by the integration
replay in
``tests/integration/test_cycle_14_t2_canonical_acceptance_render.py``.
"""

from __future__ import annotations

from pathlib import Path


def _read_harness_source() -> str:
    root = Path(__file__).resolve().parents[2]
    return (
        root / "src" / "genie_space_optimizer" / "optimization" / "harness.py"
    ).read_text()


def test_harness_imports_full_eval_marker() -> None:
    src = _read_harness_source()
    assert "from genie_space_optimizer.optimization.run_analysis_contract import" in src
    assert "full_eval_marker" in src


def test_harness_imports_format_full_eval_marker_payload() -> None:
    src = _read_harness_source()
    assert "format_full_eval_marker_payload" in src


def test_harness_consults_canonical_acceptance_render_flag() -> None:
    src = _read_harness_source()
    assert "canonical_acceptance_render_enabled()" in src


def test_harness_emits_full_eval_marker_on_pass_path() -> None:
    """The PASS -- ACCEPTED print site emits the typed marker behind
    the flag (after the print)."""
    src = _read_harness_source()
    pass_idx = src.find('PASS -- ACCEPTED')
    assert pass_idx >= 0
    marker_idx = src.find('full_eval_marker(', pass_idx)
    assert marker_idx >= 0


def test_harness_emits_full_eval_marker_on_regression_path() -> None:
    """The FAIL (REGRESSION) print site emits the typed marker behind
    the flag (after the print)."""
    src = _read_harness_source()
    fail_idx = src.find('FAIL (REGRESSION)')
    assert fail_idx >= 0
    marker_idx = src.find('full_eval_marker(', fail_idx)
    assert marker_idx >= 0
