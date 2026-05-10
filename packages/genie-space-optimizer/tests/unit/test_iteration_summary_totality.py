"""Cycle 14-W T5 — iteration summary totality invariant.

Anchor: airline run 1105451933925748 F7 — 3-iteration optimizer
run emitted 4 ``iter_record_counts`` buckets and only 1
``GSO_ITERATION_SUMMARY_V1``. The new invariant alarm
``GSO_ITERATION_SUMMARY_TOTALITY_V1`` fires when any of the
three cardinalities disagree.
"""

from __future__ import annotations

import json
import re

from genie_space_optimizer.optimization.run_analysis_contract import (
    check_iteration_summary_totality,
    iteration_summary_totality_marker,
)


# ── Pure helper: check_iteration_summary_totality ────────────────────


def test_all_three_equal_returns_none() -> None:
    """Clean run: counter == summary count == record-count length."""
    assert check_iteration_summary_totality(
        iteration_counter=3,
        iteration_summary_count=3,
        phase_b_iter_record_counts_length=3,
    ) is None


def test_summary_count_lags_counter_returns_violation() -> None:
    """Anchor 13 shape: counter=3, summary=1, record-length=4."""
    violation = check_iteration_summary_totality(
        iteration_counter=3,
        iteration_summary_count=1,
        phase_b_iter_record_counts_length=4,
    )
    assert violation is not None
    assert violation["iteration_counter"] == 3
    assert violation["iteration_summary_count"] == 1
    assert violation["phase_b_iter_record_counts_length"] == 4


def test_record_count_length_drift_returns_violation() -> None:
    """Counter and summary agree, but Phase B record-counts have
    one extra bucket."""
    violation = check_iteration_summary_totality(
        iteration_counter=3,
        iteration_summary_count=3,
        phase_b_iter_record_counts_length=4,
    )
    assert violation is not None


def test_zero_iterations_is_clean() -> None:
    """Empty run: all three are 0 → no violation."""
    assert check_iteration_summary_totality(
        iteration_counter=0,
        iteration_summary_count=0,
        phase_b_iter_record_counts_length=0,
    ) is None


# ── Marker constructor: iteration_summary_totality_marker ────────────


def test_marker_emits_v1_with_canonical_payload() -> None:
    line = iteration_summary_totality_marker(
        optimization_run_id="run-anchor",
        iteration_counter=3,
        iteration_summary_count=1,
        phase_b_iter_record_counts_length=4,
    )
    assert line.startswith("GSO_ITERATION_SUMMARY_TOTALITY_V1 ")
    body = re.search(r"\s+(\{.*\})", line).group(1)
    payload = json.loads(body)
    assert payload["iteration_counter"] == 3
    assert payload["iteration_summary_count"] == 1
    assert payload["phase_b_iter_record_counts_length"] == 4
    assert "expected_equality" in payload


def test_marker_carries_optimization_run_id() -> None:
    line = iteration_summary_totality_marker(
        optimization_run_id="run-7now",
        iteration_counter=5,
        iteration_summary_count=5,
        phase_b_iter_record_counts_length=5,
    )
    body = re.search(r"\s+(\{.*\})", line).group(1)
    payload = json.loads(body)
    assert payload["optimization_run_id"] == "run-7now"


# ── Flag accessor ────────────────────────────────────────────────────


def test_iteration_summary_totality_default_on(monkeypatch) -> None:
    monkeypatch.delenv("GSO_ITERATION_SUMMARY_TOTALITY", raising=False)
    from genie_space_optimizer.common.config import (
        iteration_summary_totality_enabled,
    )
    assert iteration_summary_totality_enabled() is True


def test_iteration_summary_totality_disabled_via_env(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ITERATION_SUMMARY_TOTALITY", "0")
    from genie_space_optimizer.common.config import (
        iteration_summary_totality_enabled,
    )
    assert iteration_summary_totality_enabled() is False
