"""Phase H Task 6: operator_process_transcript renderer."""

from __future__ import annotations

from genie_space_optimizer.optimization.rca_decision_trace import (
    OptimizationTrace,
)


def test_render_iteration_includes_all_eleven_stage_headers() -> None:
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_iteration_transcript,
    )
    from genie_space_optimizer.optimization.run_output_contract import (
        PROCESS_STAGE_ORDER,
    )

    trace = OptimizationTrace(
        journey_events=(),
        decision_records=(),
    )

    rendered = render_iteration_transcript(
        iteration=1, trace=trace,
        iteration_summary={"hard_failure_count": 3},
    )

    for stage in PROCESS_STAGE_ORDER:
        assert stage.title in rendered, f"missing stage header: {stage.title}"


def test_render_iteration_includes_why_blurbs() -> None:
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_iteration_transcript,
    )
    from genie_space_optimizer.optimization.run_output_contract import (
        PROCESS_STAGE_ORDER,
    )

    trace = OptimizationTrace(journey_events=(), decision_records=())
    rendered = render_iteration_transcript(
        iteration=1, trace=trace, iteration_summary={},
    )
    for stage in PROCESS_STAGE_ORDER:
        first_sentence = stage.why.split(". ")[0]
        assert first_sentence in rendered


def test_render_run_overview_shows_run_metadata() -> None:
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_run_overview,
    )
    rendered = render_run_overview(
        run_id="abc-123",
        space_id="s1",
        domain="airline",
        max_iters=5,
        baseline={
            "overall_accuracy": 0.875,
            "all_judge_pass_rate": 0.5,
            "hard_failures": 3,
            "soft_signals": 8,
        },
        hard_failures=[
            ("gs_009", "wrong_join_spec", "top-N returned wrong rows"),
        ],
    )
    assert "abc-123" in rendered
    assert "airline" in rendered
    assert "87.5%" in rendered
    assert "gs_009" in rendered


def test_render_full_transcript_concatenates_overview_and_iterations() -> None:
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_full_transcript,
    )
    rendered = render_full_transcript(
        run_overview="OVERVIEW_HEADER",
        iteration_transcripts=["ITER_1_BLOCK", "ITER_2_BLOCK"],
    )
    assert "OVERVIEW_HEADER" in rendered
    assert "ITER_1_BLOCK" in rendered
    assert "ITER_2_BLOCK" in rendered
    assert rendered.index("OVERVIEW_HEADER") < rendered.index("ITER_1_BLOCK")
