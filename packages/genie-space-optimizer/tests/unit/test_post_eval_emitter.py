"""Phase D Harness Extractions T3: unit tests for post_eval emitter.

Pins the byte-stable transition-classification table: each qid in the
eval set produces exactly one ``post_eval`` event with a transition of
``hold_pass`` / ``fail_to_pass`` / ``pass_to_fail`` / ``hold_fail``.
"""
from __future__ import annotations


def _capture():
    captured: list[tuple[str, dict]] = []

    def emit(kind: str, **kwargs):
        captured.append((kind, dict(kwargs)))

    return emit, captured


def test_emit_post_eval_journey_emits_nothing_for_empty_eval_set():
    from genie_space_optimizer.optimization.post_eval import (
        _emit_post_eval_journey,
    )

    emit, captured = _capture()
    _emit_post_eval_journey(
        emit=emit,
        eval_qids=[],
        was_passing_qids=["q_old"],
        is_passing_qids=["q_new"],
    )
    assert captured == []


def test_emit_post_eval_journey_classifies_four_transitions():
    from genie_space_optimizer.optimization.post_eval import (
        _emit_post_eval_journey,
    )

    emit, captured = _capture()
    _emit_post_eval_journey(
        emit=emit,
        eval_qids=["q_hold_pass", "q_fail_to_pass", "q_pass_to_fail", "q_hold_fail"],
        was_passing_qids=["q_hold_pass", "q_pass_to_fail"],
        is_passing_qids=["q_hold_pass", "q_fail_to_pass"],
    )
    assert len(captured) == 4
    transitions = {
        kwargs["question_id"]: kwargs["transition"]
        for kind, kwargs in captured if kind == "post_eval"
    }
    assert transitions == {
        "q_hold_pass": "hold_pass",
        "q_fail_to_pass": "fail_to_pass",
        "q_pass_to_fail": "pass_to_fail",
        "q_hold_fail": "hold_fail",
    }


def test_emit_post_eval_journey_carries_was_and_is_passing_flags():
    from genie_space_optimizer.optimization.post_eval import (
        _emit_post_eval_journey,
    )

    emit, captured = _capture()
    _emit_post_eval_journey(
        emit=emit,
        eval_qids=["q1"],
        was_passing_qids=["q1"],
        is_passing_qids=[],
    )
    assert len(captured) == 1
    kind, kwargs = captured[0]
    assert kind == "post_eval"
    assert kwargs["was_passing"] is True
    assert kwargs["is_passing"] is False
    assert kwargs["transition"] == "pass_to_fail"


def test_emit_post_eval_journey_skips_empty_string_qids_in_eval_set():
    """The contract: empty-string qids in eval_qids do not produce events.

    Note: the verbatim relocation preserves the original harness behaviour
    of ``str(q)`` then ``if not qid: continue`` — that filters ``""`` but
    not ``None`` (which str()-coerces to the truthy string ``"None"``).
    Adjusting either filter would be a contract change, not a relocation.
    """
    from genie_space_optimizer.optimization.post_eval import (
        _emit_post_eval_journey,
    )

    emit, captured = _capture()
    _emit_post_eval_journey(
        emit=emit,
        eval_qids=["q1", "", "q2"],
        was_passing_qids=[],
        is_passing_qids=[],
    )
    qids = [kwargs["question_id"] for _, kwargs in captured]
    assert qids == ["q1", "q2"]
