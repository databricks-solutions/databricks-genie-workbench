"""F-7 — diagnostic_ag trunk emit + classifier tightening for
RCA-regeneration-exhausted hard qids (gs_021 case).

Triage of run 833969815458299: the user's "clustered -> soft_signal"
hypothesis for gs_021 was disproven (gs_021 has no soft_signal trunk
event in this run). The actual misclassification is gs_021 ->
``terminal_unactionable`` when the correct state is
``hard_failure_unresolved`` (T3 RCA regen was triggered and exhausted).
"""
from __future__ import annotations


def _trunk_event(qid: str, stage: str):
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    return QuestionJourneyEvent(question_id=qid, stage=stage)


def test_classifier_prefers_hard_failure_unresolved_after_rca_exhausted() -> None:
    """A qid with `clustered`, `diagnostic_ag`, and `rca_exhausted`
    trunk events must classify as HARD_FAILURE_UNRESOLVED, not
    TERMINAL_UNACTIONABLE."""
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyTerminalState,
        _classify_terminal_state,
    )
    events = [
        _trunk_event("gs_021", "clustered"),
        _trunk_event("gs_021", "diagnostic_ag"),
        _trunk_event("gs_021", "rca_exhausted"),
    ]
    state = _classify_terminal_state(events=events)
    assert state == JourneyTerminalState.HARD_FAILURE_UNRESOLVED


def test_classifier_keeps_terminal_unactionable_when_no_diagnostic_ag() -> None:
    """Backward compat: `clustered` alone (no diagnostic_ag) still
    classifies as TERMINAL_UNACTIONABLE."""
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyTerminalState,
        _classify_terminal_state,
    )
    events = [_trunk_event("gs_x", "clustered")]
    state = _classify_terminal_state(events=events)
    assert state == JourneyTerminalState.TERMINAL_UNACTIONABLE


def test_classifier_returns_hard_failure_unresolved_for_diagnostic_ag_without_rca_exhaust() -> None:
    """`clustered + diagnostic_ag` without rca_exhausted (legacy
    AG-1-F success-path) classifies as HARD_FAILURE_UNRESOLVED via
    the trailing fall-through (no regression)."""
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyTerminalState,
        _classify_terminal_state,
    )
    events = [
        _trunk_event("gs_x", "clustered"),
        _trunk_event("gs_x", "diagnostic_ag"),
    ]
    state = _classify_terminal_state(events=events)
    assert state == JourneyTerminalState.HARD_FAILURE_UNRESOLVED


def test_diagnostic_ag_trunk_event_emitted_when_t3_regen_runs() -> None:
    """When _regenerate_rca_for_cluster runs, the harness emits a
    `diagnostic_ag` trunk event for every cluster qid via
    _emit_diagnostic_ag_trunk_events."""
    from genie_space_optimizer.optimization.harness import (
        _emit_diagnostic_ag_trunk_events,
    )
    emitted: list[dict] = []

    def fake_emit(stage: str, **fields):
        emitted.append({"stage": stage, **fields})

    _emit_diagnostic_ag_trunk_events(
        journey_emit=fake_emit,
        cluster_qids=("gs_021",),
        cluster_id="H002",
    )
    assert any(
        ev["stage"] == "diagnostic_ag" and ev.get("question_id") == "gs_021"
        for ev in emitted
    )


def test_diagnostic_ag_trunk_emit_is_no_op_for_empty_qids() -> None:
    """Empty cluster_qids tuple emits nothing (defensive)."""
    from genie_space_optimizer.optimization.harness import (
        _emit_diagnostic_ag_trunk_events,
    )
    emitted: list[dict] = []

    def fake_emit(stage: str, **fields):
        emitted.append({"stage": stage, **fields})

    _emit_diagnostic_ag_trunk_events(
        journey_emit=fake_emit,
        cluster_qids=(),
        cluster_id="H002",
    )
    assert emitted == []
