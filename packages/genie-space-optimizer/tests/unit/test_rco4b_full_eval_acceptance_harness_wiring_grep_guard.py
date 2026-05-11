"""RCO-4b Phase E Task 4 — grep-guard that the full-eval-acceptance
pure path is wired at all three audit-emission sites and the legacy
paths are preserved.

Guards seven invariants:
  1. ``gate_checks_full_eval_acceptance_pure_enabled`` is imported.
  2. ``decide_full_eval_acceptance`` is referenced.
  3. The legacy ``decide_acceptance(...)`` call is preserved (it runs
     BEFORE the dispatcher; the helper consumes its result).
  4. ``gate_name="full_eval_acceptance"`` appears EXACTLY 6 times
     (3 sites × 2 branches each).
  5. The legacy ``regressions[0]['judge']`` format string survives
     in the legacy rollback branch.
  6. The legacy diagnostic regressions list-comp survives in both
     legacy emission branches.
  7. The Phase E outcome variable ``_rco4b_full_eval_out`` appears
     at all three audit-emission sites in the helper-on path
     (>=4 references — 3 helper calls + multiple field reads).
"""
from __future__ import annotations

import pathlib


HARNESS = pathlib.Path(
    "src/genie_space_optimizer/optimization/harness.py"
).read_text(encoding="utf-8")


def test_pure_flag_accessor_imported() -> None:
    assert "gate_checks_full_eval_acceptance_pure_enabled" in HARNESS


def test_pure_helper_referenced() -> None:
    assert "decide_full_eval_acceptance" in HARNESS


def test_legacy_decide_acceptance_call_preserved() -> None:
    """``decide_acceptance(...)`` runs UPSTREAM of the dispatcher on
    both branches — the helper consumes its result. The call site
    is unconditional; both branches see it."""
    assert HARNESS.count("decide_acceptance(") >= 1


def test_full_eval_audit_emission_appears_six_times() -> None:
    """Three sites × two branches = six occurrences of
    ``gate_name="full_eval_acceptance"``."""
    assert HARNESS.count('gate_name="full_eval_acceptance"') == 6


def test_legacy_regression_judge_format_preserved() -> None:
    """The legacy ``f"full_eval: {regressions[0]['judge']}"``
    format string must survive in the legacy rollback branch as
    proof the legacy body is byte-stable."""
    assert "regressions[0]['judge']" in HARNESS


def test_legacy_diagnostic_regressions_format_preserved() -> None:
    """The legacy diagnostic regressions list-comp must survive in
    both legacy emission branches (rollback + accept)."""
    assert HARNESS.count('r.get("judge") for r in _diagnostic_regressions') >= 2


def test_rco4b_full_eval_out_threaded_to_all_three_sites() -> None:
    """The Phase E outcome variable ``_rco4b_full_eval_out`` must
    appear at all three audit-emission sites in the helper-on path.
    Count >=4 — 3 helper-call assignments + multiple field reads."""
    assert HARNESS.count("_rco4b_full_eval_out") >= 4
