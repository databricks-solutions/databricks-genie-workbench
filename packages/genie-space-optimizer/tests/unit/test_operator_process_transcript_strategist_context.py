"""Plan P-G — operator transcript renders Stage 4 + Stage 5 boundary records.

Both evidence runs (ccf1d60d, 31ecd96f) show
"(no decisions emitted for this stage in this iteration)" for Stage 4
in every iteration. This test pins the post-fix rendering so the next
deployed run carries the assembled/consumed records visibly.
"""

from __future__ import annotations


def test_strategist_context_stage_renders_assembled_record() -> None:
    """Stage 4 (strategist_context) shows the ASSEMBLED record, not the
    placeholder."""
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_context_assembled_record,
    )
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_iteration_transcript,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        OptimizationTrace,
    )
    from genie_space_optimizer.optimization.stages.strategist_context import (
        StrategistContextOutput,
    )

    out = StrategistContextOutput(iteration=2, baseline_accuracy=0.0)
    record = strategist_context_assembled_record(
        run_id="r", iteration=2, assembled_output=out,
    )
    trace = OptimizationTrace(decision_records=(record,))

    text = render_iteration_transcript(
        iteration=2, trace=trace, iteration_summary={},
    )

    assert "### 4. Strategist Context" in text
    assert "strategist_context_assembled" in text
    # The placeholder must NOT appear within the Strategist Context section.
    section = text.split("### 4. Strategist Context", 1)[1].split(
        "### 5.", 1
    )[0]
    assert "(no decisions emitted for this stage in this iteration)" not in (
        section
    )


def test_action_group_selection_stage_renders_consumed_record() -> None:
    """Stage 5 (action_group_selection) shows the CONSUMED record alongside
    any existing STRATEGIST_AG_EMITTED records."""
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_context_consumed_record,
    )
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_iteration_transcript,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        OptimizationTrace,
    )

    record = strategist_context_consumed_record(
        run_id="r",
        iteration=2,
        consumed_payload={"a": 1},
        assembled_hash="sha256:" + "a" * 64,
    )
    trace = OptimizationTrace(decision_records=(record,))

    text = render_iteration_transcript(
        iteration=2, trace=trace, iteration_summary={},
    )

    section = text.split("### 5. Action Group Selection", 1)[1].split(
        "### 6.", 1
    )[0]
    assert "strategist_context_consumed" in section
    assert (
        "(no decisions emitted for this stage in this iteration)" not in (
            section
        )
    )
