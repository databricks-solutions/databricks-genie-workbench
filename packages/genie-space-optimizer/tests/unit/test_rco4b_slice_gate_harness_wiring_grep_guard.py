"""RCO-4b Phase B Task 5 — grep-guard that the slice-gate pure path
is wired and the legacy path is preserved.

This guards three invariants:
  1. ``gate_checks_slice_pure_enabled()`` is imported in harness.
  2. All three pure helpers are reachable from harness.
  3. The legacy ``slice_benchmarks = filter_benchmarks_by_scope(...)``
     call and the ``_audit_emit(... gate_name="slice_gate" ...)`` audit
     row are still present (the legacy path is not deleted).
"""
from __future__ import annotations

import pathlib


HARNESS = pathlib.Path(
    "src/genie_space_optimizer/optimization/harness.py"
).read_text(encoding="utf-8")


def test_pure_flag_accessor_imported() -> None:
    assert "gate_checks_slice_pure_enabled" in HARNESS


def test_all_three_pure_helpers_referenced() -> None:
    assert "decide_slice_gate_should_run" in HARNESS
    assert "compute_slice_gate_effective_tolerance" in HARNESS
    assert "decide_slice_gate_post_eval" in HARNESS


def test_legacy_filter_benchmarks_call_preserved() -> None:
    """The legacy ``slice_benchmarks = filter_benchmarks_by_scope(...)``
    call must appear at least twice — once in each flag branch. (The
    legacy path needs its own copy; the pure path also calls it because
    the helper consumes only the count, not the benchmarks themselves.)"""
    assert HARNESS.count("filter_benchmarks_by_scope(") >= 2


def test_audit_emit_for_slice_gate_preserved() -> None:
    """The harness must still emit ``gate_name="slice_gate"`` on
    rollback. The pure helper does NOT emit; the harness owns audit."""
    assert 'gate_name="slice_gate"' in HARNESS


def test_legacy_slice_gate_disabled_print_preserved() -> None:
    """The ``SLICE GATE [...]: DISABLED`` print must appear in both
    branches so transcripts stay byte-stable across flag flips."""
    assert HARNESS.count("SLICE GATE [{ag_id}]: DISABLED") >= 2
