"""Pin the harness's Phase H baseline-overview / iteration-summary helpers.

The pretty-print transcript was rendering with two visible defects:

1. ``Overall accuracy: 8947.0%`` — the harness was passing
   ``prev_accuracy`` (already a 0-100 percentage) into
   ``render_run_overview``, which then multiplied by 100 again. The
   baseline-overview builder must convert percent → fraction (0-1) so
   ``render_run_overview``'s ``* 100`` formatter produces the right
   value.

2. Empty per-iteration sections — ``_iter_traces`` and
   ``_iter_summaries`` were declared but never populated, so
   ``render_full_transcript`` saw an empty list of iteration
   transcripts and produced only the run overview header. The
   iteration-summary builder must produce a stable, non-empty dict
   from the per-iteration counters the harness already tracks.

Both are tested at the helper boundary so the contract is pinned even
though the surrounding ``_run_lever_loop`` body is too large to drive
end-to-end from a unit test.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.harness import (
    _build_baseline_overview_dict,
    _build_iteration_summary_dict,
)


# ── Baseline overview ──────────────────────────────────────────────


def test_baseline_overview_converts_percent_input_to_fraction() -> None:
    """The renderer multiplies overall_accuracy by 100. The harness
    therefore must hand it a fraction (0-1)."""
    overview = _build_baseline_overview_dict(
        prev_accuracy_percent=89.47,
        prev_scores={"j1": 88.0, "j2": 91.0},
        hard_failure_count=0,
        soft_signal_count=0,
    )
    assert overview["overall_accuracy"] == pytest.approx(0.8947, rel=1e-3)
    # all_judge_pass_rate is approximated from the minimum per-judge
    # pass rate (still in fraction form) so it is at least directional.
    assert overview["all_judge_pass_rate"] == pytest.approx(0.88, rel=1e-3)
    assert overview["hard_failures"] == 0
    assert overview["soft_signals"] == 0


def test_baseline_overview_handles_empty_scores_without_zero_division() -> None:
    overview = _build_baseline_overview_dict(
        prev_accuracy_percent=0.0,
        prev_scores={},
        hard_failure_count=3,
        soft_signal_count=8,
    )
    assert overview["overall_accuracy"] == 0.0
    assert overview["all_judge_pass_rate"] == 0.0
    assert overview["hard_failures"] == 3
    assert overview["soft_signals"] == 8


def test_baseline_overview_clamps_percent_to_zero_one_range() -> None:
    """Defensive: if a caller hands an out-of-range value, the helper
    must not silently emit something the renderer will display as
    >100% (the original 8947% bug)."""
    overview = _build_baseline_overview_dict(
        prev_accuracy_percent=120.0,
        prev_scores={"j1": 200.0},
        hard_failure_count=0,
        soft_signal_count=0,
    )
    assert 0.0 <= overview["overall_accuracy"] <= 1.0
    assert 0.0 <= overview["all_judge_pass_rate"] <= 1.0


# ── Iteration summary ──────────────────────────────────────────────


def test_iteration_summary_dict_has_stable_counter_keys() -> None:
    summary = _build_iteration_summary_dict(
        iteration=2,
        accepted_count=1,
        rolled_back_count=0,
        skipped_count=0,
        gate_drop_count=0,
        decision_record_count=12,
        journey_violation_count=0,
        iteration_accuracy_percent=92.5,
    )
    assert summary["iteration"] == 2
    assert summary["accepted_count"] == 1
    assert summary["rolled_back_count"] == 0
    assert summary["skipped_count"] == 0
    assert summary["gate_drop_count"] == 0
    assert summary["decision_record_count"] == 12
    assert summary["journey_violation_count"] == 0
    # Iteration accuracy is rendered as a percent string so humans
    # can read it directly in the transcript.
    assert summary["iteration_accuracy"] == "92.5%"


def test_iteration_summary_dict_omits_accuracy_when_unknown() -> None:
    summary = _build_iteration_summary_dict(
        iteration=1,
        accepted_count=0,
        rolled_back_count=1,
        skipped_count=0,
        gate_drop_count=0,
        decision_record_count=0,
        journey_violation_count=0,
        iteration_accuracy_percent=None,
    )
    assert "iteration_accuracy" not in summary
    assert summary["iteration"] == 1
    assert summary["rolled_back_count"] == 1


def test_iteration_summary_dict_is_sorted_friendly() -> None:
    """The renderer iterates over ``sorted(iteration_summary.items())``
    so every value must be representable as a string without raising."""
    summary = _build_iteration_summary_dict(
        iteration=3,
        accepted_count=2,
        rolled_back_count=0,
        skipped_count=1,
        gate_drop_count=2,
        decision_record_count=5,
        journey_violation_count=1,
        iteration_accuracy_percent=88.0,
    )
    for k, v in sorted(summary.items()):
        assert isinstance(k, str)
        # Render path uses f"- {k}: {v}" which calls str() on v;
        # any value that raises during str() would crash the
        # transcript.
        str(v)
