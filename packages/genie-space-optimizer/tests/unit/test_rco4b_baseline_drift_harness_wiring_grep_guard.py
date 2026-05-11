"""RCO-4b Phase D Task 6 — grep-guard for baseline-drift wiring."""
from __future__ import annotations

import pathlib


HARNESS = pathlib.Path(
    "src/genie_space_optimizer/optimization/harness.py"
).read_text(encoding="utf-8")


def test_baseline_drift_pure_flag_accessor_imported() -> None:
    assert "gate_checks_baseline_drift_pure_enabled" in HARNESS


def test_build_baseline_drift_diagnostic_referenced() -> None:
    assert "build_baseline_drift_diagnostic" in HARNESS


def test_legacy_decide_baseline_drift_call_preserved() -> None:
    """The legacy ``decide_baseline_drift(...)`` call must appear in
    the else branch as proof the legacy body is byte-stable."""
    assert HARNESS.count("decide_baseline_drift(") >= 1


def test_audit_emit_for_baseline_drift_appears_in_both_branches() -> None:
    assert HARNESS.count('gate_name="baseline_drift_diagnostic"') == 2


def test_baseline_drift_log_format_string_preserved() -> None:
    """The legacy logger.info line uses ``%s/%.1f%%`` format. It must
    still appear (in the else branch). Phase D pre-renders the same
    string inside ``build_baseline_drift_diagnostic.log_line``, but
    the harness's else branch still constructs it inline."""
    assert "BASELINE DRIFT [%s]: iter %d post-arbiter %.1f%%" in HARNESS
