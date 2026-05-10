"""C15 Phase 1 Task 1.11 — D-7 closure: iteration-summary totality
emits one GSO_ITERATION_SUMMARY_V1 per attempted iteration sourced
from LearningOutput.iteration_summaries.

Anchor: airline run 1105451933925748 — 3 iterations attempted,
1 GSO_ITERATION_SUMMARY_V1 emitted (pre-fix), iter_record_counts
has 4 buckets. The typed LearningOutput path must emit 3 summaries
(one per IterationSummary in iteration_summaries).
"""

from __future__ import annotations

import json

import pytest


def _parse_gsov1_markers(captured: str) -> list[dict]:
    """Extract GSO_ITERATION_SUMMARY_V1 payloads from captured stdout."""
    markers = []
    for line in captured.splitlines():
        if line.startswith("GSO_ITERATION_SUMMARY_V1 "):
            markers.append(json.loads(line[len("GSO_ITERATION_SUMMARY_V1 "):]))
    return markers


def _parse_totality_markers(captured: str) -> list[dict]:
    markers = []
    for line in captured.splitlines():
        if line.startswith("GSO_ITERATION_SUMMARY_TOTALITY_V1 "):
            markers.append(json.loads(line[len("GSO_ITERATION_SUMMARY_TOTALITY_V1 "):]))
    return markers


def test_three_attempted_iters_emit_three_summaries(capsys, monkeypatch) -> None:
    """D-7 closure: LearningOutput with 3 summaries → 3 V1 markers + 1 totality.

    With learning_output provided, the function must emit one
    GSO_ITERATION_SUMMARY_V1 per IterationSummary (not one per accepted
    iteration — that's the D-7 defect).
    """
    monkeypatch.setenv("GSO_ITERATION_SUMMARY_TOTALITY", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_iteration_summary_totality_at_terminate,
    )
    from genie_space_optimizer.optimization.stages.learning import (
        AcceptanceVerdict,
        IterationSummary,
        LearningOutput,
    )

    learn = LearningOutput(
        iteration_summaries=(
            IterationSummary(
                iteration=1,
                attempted=True,
                verdict=AcceptanceVerdict.ACCEPTED_WITH_ATTRIBUTION_DRIFT,
                candidate_accuracy=95.83,
                baseline_accuracy=83.33,
            ),
            IterationSummary(
                iteration=2,
                attempted=True,
                verdict=AcceptanceVerdict.ROLLED_BACK,
                candidate_accuracy=91.67,
                baseline_accuracy=95.83,
            ),
            IterationSummary(
                iteration=3,
                attempted=True,
                verdict=AcceptanceVerdict.ROLLED_BACK,
                candidate_accuracy=91.67,
                baseline_accuracy=95.83,
            ),
        ),
        terminate=True,
        terminate_reason="plateau_unresolved_hard_failures_quarantined",
    )

    _emit_iteration_summary_totality_at_terminate(learning_output=learn)
    captured = capsys.readouterr().out

    summary_markers = _parse_gsov1_markers(captured)
    assert len(summary_markers) == 3, (
        f"expected 3 GSO_ITERATION_SUMMARY_V1 markers (one per attempted iter), "
        f"got {len(summary_markers)}"
    )

    # Verdicts round-trip correctly
    assert summary_markers[0]["verdict"] == "accepted_with_attribution_drift"
    assert summary_markers[1]["verdict"] == "rolled_back"
    assert summary_markers[2]["verdict"] == "rolled_back"

    # Iteration numbers are threaded through
    assert summary_markers[0]["iteration"] == 1
    assert summary_markers[1]["iteration"] == 2
    assert summary_markers[2]["iteration"] == 3

    # Exactly one totality marker
    totality_markers = _parse_totality_markers(captured)
    assert len(totality_markers) == 1, (
        f"expected 1 GSO_ITERATION_SUMMARY_TOTALITY_V1, got {len(totality_markers)}"
    )
    totality = totality_markers[0]
    assert totality["iteration_counter"] == 3
    assert totality["summary_count"] == 3


def test_legacy_kwargs_still_work(monkeypatch) -> None:
    """Backward-compat: existing callers that pass legacy kwargs must still
    get the C14-W behaviour (alarm on disagreement only)."""
    monkeypatch.setenv("GSO_ITERATION_SUMMARY_TOTALITY", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_iteration_summary_totality_at_terminate,
    )
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        # Clean run: counter==summary==record_counts → no alarm
        _emit_iteration_summary_totality_at_terminate(
            run_id="legacy_clean",
            iteration_counter=3,
            iteration_summary_count=3,
            phase_b_iter_record_counts_length=3,
        )
    assert "GSO_ITERATION_SUMMARY_TOTALITY_V1" not in buf.getvalue()

    buf2 = io.StringIO()
    with redirect_stdout(buf2):
        # Anchor 13 shape: disagreement → alarm
        _emit_iteration_summary_totality_at_terminate(
            run_id="anchor_13",
            iteration_counter=3,
            iteration_summary_count=1,
            phase_b_iter_record_counts_length=4,
        )
    assert "GSO_ITERATION_SUMMARY_TOTALITY_V1" in buf2.getvalue()
