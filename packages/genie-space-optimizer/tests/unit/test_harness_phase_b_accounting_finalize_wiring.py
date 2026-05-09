"""Cycle 14-T1 — _finalize_iteration_summary calls record_phase_b_iter_accounting.

Static-source assertions only; the behavioural wiring is tested by
the integration replay in
``tests/integration/test_cycle_14_t1_phase_b_aggregator_total.py``.
"""

from __future__ import annotations

from pathlib import Path


def _read_harness_source() -> str:
    root = Path(__file__).resolve().parents[2]
    return (
        root / "src" / "genie_space_optimizer" / "optimization" / "harness.py"
    ).read_text()


def test_finalize_iteration_summary_imports_phase_b_helper() -> None:
    src = _read_harness_source()
    assert "from genie_space_optimizer.optimization.phase_b_accounting import" in src
    assert "record_phase_b_iter_accounting" in src


def test_finalize_iteration_summary_calls_helper_behind_flag() -> None:
    """The flag accessor is consulted; on flag-on the helper is called.
    On flag-off the legacy in-body block remains the producer."""
    src = _read_harness_source()
    assert "phase_b_aggregator_in_finalize_enabled()" in src
    # The helper invocation MUST appear inside _finalize_iteration_summary,
    # not at module level. Split the source at the first def
    # _finalize_iteration_summary header and assert the call lives in
    # that function's body.
    marker = "def _finalize_iteration_summary("
    body_start = src.index(marker)
    body_end = src.index("\ndef ", body_start + len(marker))
    body = src[body_start:body_end]
    assert "record_phase_b_iter_accounting(" in body


def test_legacy_in_body_block_still_present() -> None:
    """T1 ramp: the legacy in-body block at the iteration body's
    happy path is preserved verbatim. The helper's idempotency guard
    makes the second call a no-op."""
    src = _read_harness_source()
    # Sentinel comment from the legacy in-body block.
    assert "Phase B observability follow-up — per-iteration accounting" in src
