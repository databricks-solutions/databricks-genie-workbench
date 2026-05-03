"""Phase D Harness Extractions T4: byte-stable journey-event snapshots.

Each of the three relocated emit helpers gets a frozen-output snapshot
test for a known input. Future refactors that change ordering, casing,
sorted-ness, or filter logic will fail these tests immediately.

Extending the assertions: when an event-ordering contract change is
deliberate, update the expected tuples here AND the corresponding
section in the journey-contract validator. Both should land in the
same commit.
"""
from __future__ import annotations


def _capture():
    captured: list[tuple[str, dict]] = []

    def emit(kind: str, **kwargs):
        captured.append((kind, dict(kwargs)))

    return emit, captured


# eval_entry — golden snapshot


def test_eval_entry_emits_evaluated_then_classifications_in_canonical_order():
    """Golden snapshot for _emit_eval_entry_journey.

    Input represents one iteration with:
      eval_qids   = [q1..q5]
      already_pass = [q1]
      hard         = [q2]   (NOT classified at entry — gets 'clustered' later)
      soft         = [q3]
      gt_correction = [q4]
      (q5 is in eval_qids but not in any classification set)
    """
    from genie_space_optimizer.optimization.eval_entry import (
        _emit_eval_entry_journey,
    )

    emit, captured = _capture()
    _emit_eval_entry_journey(
        emit=emit,
        eval_qids=["q5", "q3", "q1", "q2", "q4"],  # unsorted on purpose
        already_passing_qids=["q1"],
        hard_qids=["q2"],
        soft_qids=["q3"],
        gt_correction_qids=["q4"],
    )
    expected = [
        ("evaluated", {"question_ids": ["q1", "q2", "q3", "q4", "q5"]}),
        ("already_passing", {"question_id": "q1"}),
        ("soft_signal", {"question_id": "q3"}),
        ("gt_correction_candidate", {"question_id": "q4"}),
    ]
    assert captured == expected


# ag_outcome — golden snapshot


def test_ag_outcome_emits_one_event_with_full_qid_list():
    """Golden snapshot for _emit_ag_outcome_journey on the rolled_back path."""
    from genie_space_optimizer.optimization.ag_outcome import (
        _emit_ag_outcome_journey,
    )

    emit, captured = _capture()
    _emit_ag_outcome_journey(
        emit=emit,
        ag_id="AG_DECOMPOSED_H001",
        outcome="rolled_back",
        affected_qids=["q1", "q2", "q3"],
    )
    expected = [
        (
            "rolled_back",
            {"question_ids": ["q1", "q2", "q3"], "ag_id": "AG_DECOMPOSED_H001"},
        )
    ]
    assert captured == expected


# post_eval — golden snapshot


def test_post_eval_classifies_all_four_transition_types_in_eval_qid_order():
    """Golden snapshot for _emit_post_eval_journey covering every transition.

    Order of events follows the order of ``eval_qids`` (no implicit sort).
    """
    from genie_space_optimizer.optimization.post_eval import (
        _emit_post_eval_journey,
    )

    emit, captured = _capture()
    _emit_post_eval_journey(
        emit=emit,
        eval_qids=["q_hf", "q_hp", "q_pf", "q_fp"],
        was_passing_qids=["q_hp", "q_pf"],
        is_passing_qids=["q_hp", "q_fp"],
    )
    expected = [
        (
            "post_eval",
            {
                "question_id": "q_hf",
                "was_passing": False,
                "is_passing": False,
                "transition": "hold_fail",
            },
        ),
        (
            "post_eval",
            {
                "question_id": "q_hp",
                "was_passing": True,
                "is_passing": True,
                "transition": "hold_pass",
            },
        ),
        (
            "post_eval",
            {
                "question_id": "q_pf",
                "was_passing": True,
                "is_passing": False,
                "transition": "pass_to_fail",
            },
        ),
        (
            "post_eval",
            {
                "question_id": "q_fp",
                "was_passing": False,
                "is_passing": True,
                "transition": "fail_to_pass",
            },
        ),
    ]
    assert captured == expected


# Cross-helper invariant


def test_three_helpers_can_be_chained_into_one_iteration_event_log():
    """Sanity composite: chaining the three helpers in canonical order
    produces a journey-event log that the journey-contract validator
    would consider well-formed for one happy-path qid.

    This pins the invariant that the three helpers compose; if a future
    extraction breaks the composition, this test catches it.
    """
    from genie_space_optimizer.optimization.eval_entry import (
        _emit_eval_entry_journey,
    )
    from genie_space_optimizer.optimization.ag_outcome import (
        _emit_ag_outcome_journey,
    )
    from genie_space_optimizer.optimization.post_eval import (
        _emit_post_eval_journey,
    )

    emit, captured = _capture()
    _emit_eval_entry_journey(
        emit=emit,
        eval_qids=["q1"],
        already_passing_qids=[],
        hard_qids=["q1"],
        soft_qids=[],
        gt_correction_qids=[],
    )
    _emit_ag_outcome_journey(
        emit=emit, ag_id="AG_001", outcome="accepted", affected_qids=["q1"],
    )
    _emit_post_eval_journey(
        emit=emit,
        eval_qids=["q1"],
        was_passing_qids=[],
        is_passing_qids=["q1"],
    )
    kinds = [k for k, _ in captured]
    assert kinds == ["evaluated", "accepted", "post_eval"]
    # The terminal post_eval transition should be fail_to_pass for q1.
    assert captured[-1][1]["transition"] == "fail_to_pass"
