"""Phase D Harness Extractions T2: unit tests for ag_outcome emitter.

Pins the byte-stable journey-event behaviour: one event per AG outcome,
emitted only for the three valid outcome strings, with the AG's
``affected_qids`` carried through.
"""
from __future__ import annotations


def _capture():
    captured: list[tuple[str, dict]] = []

    def emit(kind: str, **kwargs):
        captured.append((kind, dict(kwargs)))

    return emit, captured


def test_emit_ag_outcome_journey_skips_unknown_outcome():
    from genie_space_optimizer.optimization.ag_outcome import (
        _emit_ag_outcome_journey,
    )

    emit, captured = _capture()
    _emit_ag_outcome_journey(
        emit=emit,
        ag_id="AG_001",
        outcome="banana",  # not in the valid set
        affected_qids=["q1"],
    )
    assert captured == []


def test_emit_ag_outcome_journey_skips_when_no_affected_qids():
    from genie_space_optimizer.optimization.ag_outcome import (
        _emit_ag_outcome_journey,
    )

    emit, captured = _capture()
    _emit_ag_outcome_journey(
        emit=emit,
        ag_id="AG_001",
        outcome="accepted",
        affected_qids=[],
    )
    assert captured == []


def test_emit_ag_outcome_journey_emits_one_event_with_qids_and_ag_id():
    from genie_space_optimizer.optimization.ag_outcome import (
        _emit_ag_outcome_journey,
    )

    emit, captured = _capture()
    _emit_ag_outcome_journey(
        emit=emit,
        ag_id="AG_001",
        outcome="rolled_back",
        affected_qids=["q1", "q2"],
    )
    assert len(captured) == 1
    kind, kwargs = captured[0]
    assert kind == "rolled_back"
    assert kwargs["ag_id"] == "AG_001"
    assert kwargs["question_ids"] == ["q1", "q2"]


def test_emit_ag_outcome_journey_accepts_three_valid_outcomes():
    from genie_space_optimizer.optimization.ag_outcome import (
        _emit_ag_outcome_journey,
    )

    valid = ["accepted", "accepted_with_regression_debt", "rolled_back"]
    for outcome in valid:
        emit, captured = _capture()
        _emit_ag_outcome_journey(
            emit=emit, ag_id="A", outcome=outcome, affected_qids=["q"],
        )
        assert len(captured) == 1, (
            f"outcome {outcome!r} should produce one event"
        )
        assert captured[0][0] == outcome
