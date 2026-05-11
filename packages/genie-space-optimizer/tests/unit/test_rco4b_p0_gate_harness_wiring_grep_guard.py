"""RCO-4b Phase C Task 4 — grep-guard that the P0-gate pure path
is wired and the legacy path is preserved.

This guards five invariants:
  1. ``gate_checks_p0_pure_enabled()`` is imported in harness.
  2. Both pure helpers are reachable from harness.
  3. The legacy ``filter_benchmarks_by_scope(benchmarks, "p0")`` call
     appears in BOTH branches (helper-on rebuilds the count; legacy
     reads the result directly).
  4. The ``_audit_emit(gate_name="p0_gate")`` call exists in both
     branches (only one fires per run; the parity test verifies
     identical behavior).
  5. ``mlflow.end_run()`` lives outside the flag dispatcher.
"""
from __future__ import annotations

import pathlib


HARNESS = pathlib.Path(
    "src/genie_space_optimizer/optimization/harness.py"
).read_text(encoding="utf-8")


def test_pure_flag_accessor_imported() -> None:
    assert "gate_checks_p0_pure_enabled" in HARNESS


def test_both_pure_helpers_referenced() -> None:
    assert "decide_p0_gate_should_run" in HARNESS
    assert "decide_p0_gate_post_eval" in HARNESS


def test_legacy_filter_benchmarks_call_preserved() -> None:
    """The ``filter_benchmarks_by_scope(benchmarks, "p0")`` call must
    appear at least twice — once in each flag branch."""
    assert HARNESS.count('filter_benchmarks_by_scope(benchmarks, "p0")') >= 2


def test_audit_emit_for_p0_gate_appears_in_both_branches() -> None:
    """``gate_name="p0_gate"`` audit must be wired in both flag
    branches; the parity test confirms only one fires per run."""
    assert HARNESS.count('gate_name="p0_gate"') == 2


def test_p0_gate_pass_print_preserved() -> None:
    """The ``P0 GATE [...]: PASS`` print must appear in both
    branches so transcripts stay byte-stable across flag flips."""
    assert HARNESS.count("P0 GATE [{ag_id}]: PASS") >= 2


def test_mlflow_end_run_lives_outside_dispatcher() -> None:
    """``mlflow.end_run()`` must fire unconditionally before the
    flag dispatcher (the helper does NOT own MLflow lifecycle)."""
    sep_idx = HARNESS.find("# ── P0 gate")
    dispatch_idx = HARNESS.find("gate_checks_p0_pure_enabled", sep_idx)
    end_run_idx = HARNESS.find("mlflow.end_run()", sep_idx)
    assert sep_idx != -1
    assert dispatch_idx != -1
    assert end_run_idx != -1
    assert sep_idx < end_run_idx < dispatch_idx, (
        "mlflow.end_run() must fire between the P0 separator and the "
        "flag dispatcher — outside the if/else branches"
    )
