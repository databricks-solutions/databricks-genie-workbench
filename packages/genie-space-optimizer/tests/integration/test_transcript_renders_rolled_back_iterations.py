"""Composition regression: pre-stamp + rolled-back finalise + render
together must produce a transcript with per-iteration sections and
exit_path=rolled_back. Mirrors run-11110002-0000-4000-8000-000000000002
where the lever loop rolled back twice and the rendered transcript was
567 bytes (header only).
"""

from __future__ import annotations

from typing import Any


def _make_decision_record_dict(
    *, decision_type: str, outcome: str, qids: tuple[str, ...], iteration: int,
) -> dict[str, Any]:
    """Minimal DecisionRecord-compatible dict for from_dict()."""
    return {
        "decision_type": decision_type,
        "outcome": outcome,
        "target_qids": list(qids),
        "reason_code": "none",
        "iteration": iteration,
        "ag_id": "AG_DECOMPOSED_H004",
    }


def test_rolled_back_run_renders_complete_transcript() -> None:
    """Compose pre-stamp + rolled-back finalise + render and assert the
    transcript shows both iterations with ``exit_path: rolled_back``."""
    from genie_space_optimizer.optimization.harness import (
        _build_baseline_overview_dict,
        _finalize_iteration_summary,
        _stamp_iteration_stub,
    )
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_full_transcript,
        render_iteration_transcript,
        render_run_overview,
    )

    iter_traces: dict[int, Any] = {}
    iter_summaries: dict[int, dict[str, Any]] = {}

    # Iteration 1: rolled back via content regression.
    _stamp_iteration_stub(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=1,
    )
    _finalize_iteration_summary(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=1,
        current_iter_inputs={
            "decision_records": [
                _make_decision_record_dict(
                    decision_type="patch_applied",
                    outcome="applied",
                    qids=("airline_ticketing_and_fare_analysis_gs_024",),
                    iteration=1,
                ),
                _make_decision_record_dict(
                    decision_type="acceptance_decided",
                    outcome="rolled_back",
                    qids=("airline_ticketing_and_fare_analysis_gs_024",),
                    iteration=1,
                ),
            ],
        },
        journey_events=[],
        journey_report=None,
        accepted_count=0,
        rolled_back_count=1,
        skipped_count=0,
        gate_drop_count=0,
        iteration_accuracy_percent=91.7,
        exit_path="rolled_back",
    )

    # Iteration 2: rolled back again on the same AG.
    _stamp_iteration_stub(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=2,
    )
    _finalize_iteration_summary(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=2,
        current_iter_inputs={
            "decision_records": [
                _make_decision_record_dict(
                    decision_type="acceptance_decided",
                    outcome="rolled_back",
                    qids=("airline_ticketing_and_fare_analysis_gs_024",),
                    iteration=2,
                ),
            ],
        },
        journey_events=[],
        journey_report=None,
        accepted_count=0,
        rolled_back_count=1,
        skipped_count=0,
        gate_drop_count=0,
        iteration_accuracy_percent=91.3,
        exit_path="rolled_back",
    )

    overview = render_run_overview(
        run_id="11110002-0000-4000-8000-000000000002",
        space_id="01f10000000000000000000000000001",
        domain="airline_ticketing",
        max_iters=8,
        baseline=_build_baseline_overview_dict(
            prev_accuracy_percent=83.3,
            prev_scores={"j1": 80.0, "j2": 86.0},
            hard_failure_count=4,
            soft_signal_count=0,
        ),
        hard_failures=[
            ("airline_ticketing_and_fare_analysis_gs_024", "missing_filter", "currency"),
        ],
    )
    iter_transcripts = [
        render_iteration_transcript(
            iteration=i,
            trace=iter_traces[i],
            iteration_summary=iter_summaries[i],
        )
        for i in (1, 2)
    ]
    transcript = render_full_transcript(
        run_overview=overview,
        iteration_transcripts=iter_transcripts,
    )

    assert "## Iteration 1" in transcript, (
        "Iteration 1 section missing — Task 4 (pre-stamp) or Task 7 "
        "(filter loosening) regression. Transcript head:\n" + transcript[:1500]
    )
    assert "## Iteration 2" in transcript, (
        "Iteration 2 section missing — Task 6.10 (rolled_back finalise) "
        "regression."
    )
    assert transcript.count("exit_path: rolled_back") >= 2, (
        "Expected ``- exit_path: rolled_back`` on both iterations. "
        "Transcript:\n" + transcript
    )
    # Run-overview numerical sanity: 83.3% baseline must NOT render as
    # 8330.0% (the original double-multiplication bug pinned in T1).
    assert "Overall accuracy:        83.3%" in transcript


def test_pre_stamp_alone_renders_in_progress_iteration() -> None:
    """If a future regression skips the finalise call entirely, the
    pre-stamp stub must still render with ``exit_path: in_progress`` so
    the iteration is visible in the transcript instead of disappearing."""
    from genie_space_optimizer.optimization.harness import (
        _build_baseline_overview_dict,
        _stamp_iteration_stub,
    )
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_full_transcript,
        render_iteration_transcript,
        render_run_overview,
    )

    iter_traces: dict[int, Any] = {}
    iter_summaries: dict[int, dict[str, Any]] = {}
    _stamp_iteration_stub(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=1,
    )

    overview = render_run_overview(
        run_id="r",
        space_id="s",
        domain="d",
        max_iters=1,
        baseline=_build_baseline_overview_dict(
            prev_accuracy_percent=50.0,
            prev_scores={"j1": 50.0},
            hard_failure_count=0,
            soft_signal_count=0,
        ),
        hard_failures=[],
    )
    transcript = render_full_transcript(
        run_overview=overview,
        iteration_transcripts=[
            render_iteration_transcript(
                iteration=1,
                trace=iter_traces[1],
                iteration_summary=iter_summaries[1],
            ),
        ],
    )

    assert "## Iteration 1" in transcript
    assert "exit_path: in_progress" in transcript
