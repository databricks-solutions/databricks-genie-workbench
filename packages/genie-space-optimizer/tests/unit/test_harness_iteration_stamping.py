"""Pin the harness's iteration pre-stamp / finalise contract.

Run ``11110002-0000-4000-8000-000000000002`` rolled back twice via the
content-regression continue at ``harness.py:19749``. Because
``_iter_traces[N]`` was only populated at the end-of-iteration body, the
rendered ``operator_transcript.md`` was 567 bytes — only the run-overview
header. The pre-stamp ensures every iteration that started has at least
a stub trace to render, and the finalise overwrite carries the rich data
when it is available.
"""

from __future__ import annotations

from typing import Any

from genie_space_optimizer.optimization.harness import (
    _compute_iteration_counters,
    _stamp_iteration_stub,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    OptimizationTrace,
)


def test_stamp_iteration_stub_populates_empty_trace_and_in_progress_summary() -> None:
    iter_traces: dict[int, Any] = {}
    iter_summaries: dict[int, dict[str, Any]] = {}
    _stamp_iteration_stub(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=1,
    )
    assert isinstance(iter_traces[1], OptimizationTrace)
    assert iter_traces[1].journey_events == ()
    assert iter_traces[1].decision_records == ()
    assert iter_summaries[1]["iteration"] == 1
    assert iter_summaries[1]["exit_path"] == "in_progress"
    assert iter_summaries[1]["decision_record_count"] == 0


def test_stamp_iteration_stub_is_idempotent_when_called_twice() -> None:
    """Defensive: if a future caller stamps twice, the second call must
    not raise and must leave the dict in the same shape (the rich finalise
    overwrite is the only intended re-write path)."""
    iter_traces: dict[int, Any] = {}
    iter_summaries: dict[int, dict[str, Any]] = {}
    _stamp_iteration_stub(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=3,
    )
    _stamp_iteration_stub(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=3,
    )
    assert iter_summaries[3]["exit_path"] == "in_progress"


def test_finalize_iteration_summary_overwrites_stub_with_rich_data() -> None:
    from genie_space_optimizer.optimization.harness import (
        _finalize_iteration_summary,
        _stamp_iteration_stub,
    )

    iter_traces: dict[int, Any] = {}
    iter_summaries: dict[int, dict[str, Any]] = {}
    _stamp_iteration_stub(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=2,
    )
    assert iter_summaries[2]["exit_path"] == "in_progress"

    current_iter_inputs: dict[str, Any] = {
        "decision_records": [
            {
                "decision_type": "patch_applied",
                "outcome": "applied",
                "target_qids": ("gs_001",),
                "reason_code": "none",
                "iteration": 2,
                "ag_id": "AG_DECOMPOSED_H004",
            },
        ],
    }

    _finalize_iteration_summary(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=2,
        current_iter_inputs=current_iter_inputs,
        journey_events=[],
        journey_report=None,
        accepted_count=0,
        rolled_back_count=1,
        skipped_count=0,
        gate_drop_count=0,
        iteration_accuracy_percent=91.7,
        exit_path="rolled_back",
    )

    assert iter_summaries[2]["exit_path"] == "rolled_back"
    assert iter_summaries[2]["rolled_back_count"] == 1
    assert iter_summaries[2]["iteration_accuracy"] == "91.7%"
    assert iter_traces[2] is not None


def test_finalize_iteration_summary_handles_unparseable_records_gracefully() -> None:
    from genie_space_optimizer.optimization.harness import (
        _finalize_iteration_summary,
        _stamp_iteration_stub,
    )

    iter_traces: dict[int, Any] = {}
    iter_summaries: dict[int, dict[str, Any]] = {}
    _stamp_iteration_stub(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=1,
    )
    current_iter_inputs: dict[str, Any] = {
        "decision_records": [
            {"this_is_not_a_decision_record": True},
            None,
        ],
    }

    _finalize_iteration_summary(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=1,
        current_iter_inputs=current_iter_inputs,
        journey_events=[],
        journey_report=None,
        accepted_count=0,
        rolled_back_count=0,
        skipped_count=0,
        gate_drop_count=0,
        iteration_accuracy_percent=None,
        exit_path="completed",
    )

    assert iter_summaries[1]["exit_path"] == "completed"
    # Phase H Fidelity Task 4: every iteration finalise emits one
    # ``ITERATION_BUDGET_DECISION`` learning record so Stage 10 is never
    # empty. The unparseable records must still be dropped (they would
    # crash the renderer otherwise), so the total count is exactly 1
    # — the auto-emitted learning record.
    assert iter_summaries[1]["decision_record_count"] == 1
    # The trace must contain only typed records — the two malformed
    # entries are silently filtered out.
    typed_records = list(iter_traces[1].decision_records)
    assert len(typed_records) == 1
    assert (
        typed_records[0].decision_type.value == "iteration_budget_decision"
    )


def test_render_filter_includes_stub_iterations() -> None:
    """A stub iteration (only the pre-stamp ran, no finalise) must still
    appear in the rendered transcript so the operator can see that the
    iteration started but did not complete its body. This pins the
    Task-7 filter loosening."""
    from genie_space_optimizer.optimization.harness import (
        _stamp_iteration_stub,
    )
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_iteration_transcript,
    )

    iter_traces: dict[int, Any] = {}
    iter_summaries: dict[int, dict[str, Any]] = {}
    _stamp_iteration_stub(
        iter_traces=iter_traces,
        iter_summaries=iter_summaries,
        iteration=1,
    )
    rendered = render_iteration_transcript(
        iteration=1,
        trace=iter_traces[1],
        iteration_summary=iter_summaries[1],
    )
    assert "## Iteration 1" in rendered
    assert "exit_path: in_progress" in rendered


# ── Phase H Fidelity Task 2 — gate-drop counting ──────────────────


def test_compute_iteration_counters_counts_gate_drop_decision_records() -> None:
    """Blast-radius / safety gates emit ``GATE_DECISION`` records with
    ``outcome == "dropped"``. ``ag_outcomes`` may not surface every drop
    (the AG can survive even when individual patches are dropped), so the
    iteration summary must derive ``gate_drop_count`` from records too.
    """
    current_iter_inputs: dict[str, Any] = {
        "ag_outcomes": {},  # AG-level outcomes show no drops
        "decision_records": [
            {"decision_type": "gate_decision", "outcome": "dropped"},
            {"decision_type": "gate_decision", "outcome": "dropped"},
            {"decision_type": "gate_decision", "outcome": "dropped"},
            {"decision_type": "gate_decision", "outcome": "passed"},
            {"decision_type": "eval_classified", "outcome": "info"},
        ],
    }
    accepted, rolled_back, skipped, gate_drop = _compute_iteration_counters(
        current_iter_inputs,
    )
    assert (accepted, rolled_back, skipped) == (0, 0, 0)
    assert gate_drop == 3


def test_compute_iteration_counters_takes_max_of_outcomes_and_records() -> None:
    """When ``ag_outcomes`` already counts a drop AND a typed
    ``GATE_DECISION`` record exists for it, we must not double-count.
    Returning ``max(outcome_count, record_count)`` is conservative and
    safe across both early-exit and end-of-body paths."""
    current_iter_inputs: dict[str, Any] = {
        "ag_outcomes": {"AG1": "gate_drop_blast_radius"},
        "decision_records": [
            {"decision_type": "gate_decision", "outcome": "dropped"},
            {"decision_type": "gate_decision", "outcome": "dropped"},
        ],
    }
    _, _, _, gate_drop = _compute_iteration_counters(current_iter_inputs)
    assert gate_drop == 2


def test_compute_iteration_counters_handles_missing_decision_records() -> None:
    """No ``decision_records`` key → fall back to outcome-only counting."""
    current_iter_inputs: dict[str, Any] = {
        "ag_outcomes": {"AG1": "gate_drop_blast_radius"},
    }
    _, _, _, gate_drop = _compute_iteration_counters(current_iter_inputs)
    assert gate_drop == 1


def test_compute_iteration_counters_tolerates_malformed_records() -> None:
    """Records that are not dicts or lack the expected keys must not crash
    the counter helper."""
    current_iter_inputs: dict[str, Any] = {
        "ag_outcomes": {},
        "decision_records": [
            None,
            "not a dict",
            {"decision_type": "gate_decision", "outcome": "dropped"},
            {"decision_type": "gate_decision"},  # missing outcome
            {"outcome": "dropped"},  # missing decision_type
        ],
    }
    _, _, _, gate_drop = _compute_iteration_counters(current_iter_inputs)
    assert gate_drop == 1
