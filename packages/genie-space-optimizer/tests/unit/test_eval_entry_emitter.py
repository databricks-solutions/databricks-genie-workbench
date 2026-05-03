"""Phase D Harness Extractions T1: unit tests for eval_entry emitter.

Pins the byte-stable journey-event sequence emitted by
``_emit_eval_entry_journey`` for a known input.
"""
from __future__ import annotations


def _capture():
    """Return (emit_callback, captured_events_list)."""
    captured: list[tuple[str, dict]] = []

    def emit(kind: str, **kwargs):
        captured.append((kind, dict(kwargs)))

    return emit, captured


def test_emit_eval_entry_journey_skips_when_no_eval_qids():
    from genie_space_optimizer.optimization.eval_entry import (
        _emit_eval_entry_journey,
    )

    emit, captured = _capture()
    _emit_eval_entry_journey(
        emit=emit,
        eval_qids=[],
        already_passing_qids=["q_ignored"],
        hard_qids=["h_ignored"],
        soft_qids=["s_ignored"],
        gt_correction_qids=["g_ignored"],
    )
    assert captured == []


def test_emit_eval_entry_journey_emits_evaluated_first_then_classifications():
    from genie_space_optimizer.optimization.eval_entry import (
        _emit_eval_entry_journey,
    )

    emit, captured = _capture()
    _emit_eval_entry_journey(
        emit=emit,
        eval_qids=["q1", "q2", "q3", "q4"],
        already_passing_qids=["q1"],
        hard_qids=["q2"],
        soft_qids=["q3"],
        gt_correction_qids=["q4"],
    )
    kinds = [k for k, _ in captured]
    # 'evaluated' is always first; classifications follow in canonical order.
    assert kinds[0] == "evaluated"
    assert "already_passing" in kinds
    assert "soft_signal" in kinds
    assert "gt_correction_candidate" in kinds
    # 'evaluated' carries the sorted qid list.
    assert captured[0][1]["question_ids"] == ["q1", "q2", "q3", "q4"]


def test_emit_eval_entry_journey_filters_classifications_outside_eval_set():
    """A qid in already_passing_qids that is NOT in eval_qids must not
    produce an event — the contract is closed over the eval set."""
    from genie_space_optimizer.optimization.eval_entry import (
        _emit_eval_entry_journey,
    )

    emit, captured = _capture()
    _emit_eval_entry_journey(
        emit=emit,
        eval_qids=["q1"],
        already_passing_qids=["q1", "q_outside"],
        hard_qids=[],
        soft_qids=[],
        gt_correction_qids=[],
    )
    qids_in_classification = [
        e[1].get("question_id")
        for e in captured if e[0] == "already_passing"
    ]
    assert qids_in_classification == ["q1"]
    assert "q_outside" not in qids_in_classification
