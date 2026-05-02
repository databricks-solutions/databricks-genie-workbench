"""Pin the typed contract for question-journey events."""

from __future__ import annotations

import pytest


def test_journey_stage_enum_covers_existing_stage_order() -> None:
    """Every stage in question_journey._STAGE_ORDER must have a JourneyStage member."""
    from genie_space_optimizer.optimization.question_journey import _STAGE_ORDER
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyStage,
    )

    legal = {s.value for s in JourneyStage}
    missing = [s for s in _STAGE_ORDER if s not in legal]
    assert not missing, (
        f"JourneyStage enum is missing stages emitted by the existing harness: {missing}. "
        "Update the enum in question_journey_contract.py."
    )


def test_terminal_state_enum_lists_seven_legal_outcomes() -> None:
    """The terminal-state enum is the closed set of allowed end-of-iteration outcomes."""
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyTerminalState,
    )

    expected = {
        "already_passing",
        "hard_failure_resolved",
        "hard_failure_unresolved",
        "soft_signal_only",
        "gt_correction_candidate",
        "terminal_unactionable",
        "rolled_back_no_progress",
    }
    actual = {s.value for s in JourneyTerminalState}
    assert actual == expected, (
        f"JourneyTerminalState drift: expected {expected}, got {actual}. "
        "Adding/removing terminal states requires updating validate_question_journeys."
    )


def test_legal_transitions_map_rejects_proposed_after_already_passing() -> None:
    """A passing question must not have a proposed event after the already_passing terminal."""
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyStage,
        is_legal_next_stage,
    )

    assert not is_legal_next_stage(
        prev=JourneyStage.ALREADY_PASSING,
        nxt=JourneyStage.PROPOSED,
    )


def test_legal_transitions_map_allows_clustered_after_evaluated() -> None:
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyStage,
        is_legal_next_stage,
    )

    assert is_legal_next_stage(
        prev=JourneyStage.EVALUATED,
        nxt=JourneyStage.CLUSTERED,
    )


def test_legal_transitions_map_allows_dropped_at_cap_after_proposed() -> None:
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyStage,
        is_legal_next_stage,
    )

    assert is_legal_next_stage(
        prev=JourneyStage.PROPOSED,
        nxt=JourneyStage.DROPPED_AT_CAP,
    )


def test_validate_reports_missing_qids() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        validate_question_journeys,
    )

    events = [
        QuestionJourneyEvent(question_id="gs_001", stage="evaluated"),
        QuestionJourneyEvent(question_id="gs_001", stage="already_passing"),
        QuestionJourneyEvent(question_id="gs_001", stage="post_eval"),
    ]
    report = validate_question_journeys(
        events=events,
        eval_qids={"gs_001", "gs_002"},
    )

    assert report.is_valid is False
    assert "gs_002" in report.missing_qids
    assert "gs_001" not in report.missing_qids


def test_validate_reports_illegal_transition() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        validate_question_journeys,
    )

    events = [
        QuestionJourneyEvent(question_id="gs_001", stage="evaluated"),
        QuestionJourneyEvent(question_id="gs_001", stage="applied"),  # illegal
    ]
    report = validate_question_journeys(
        events=events,
        eval_qids={"gs_001"},
    )

    assert report.is_valid is False
    assert any(
        v.question_id == "gs_001" and v.kind == "illegal_transition"
        for v in report.violations
    )


def test_validate_reports_no_terminal_state() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        validate_question_journeys,
    )

    events = [
        QuestionJourneyEvent(question_id="gs_001", stage="evaluated"),
        QuestionJourneyEvent(question_id="gs_001", stage="clustered"),
        # no post_eval, no terminal
    ]
    report = validate_question_journeys(
        events=events,
        eval_qids={"gs_001"},
    )

    assert report.is_valid is False
    assert any(
        v.question_id == "gs_001" and v.kind == "no_terminal_state"
        for v in report.violations
    )


def test_validate_passes_for_complete_journey() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        validate_question_journeys,
    )

    events = [
        QuestionJourneyEvent(question_id="gs_001", stage="evaluated"),
        QuestionJourneyEvent(question_id="gs_001", stage="clustered",
                             cluster_id="H001", root_cause="missing_filter"),
        QuestionJourneyEvent(question_id="gs_001", stage="ag_assigned",
                             ag_id="AG1"),
        QuestionJourneyEvent(question_id="gs_001", stage="proposed",
                             proposal_id="P1", patch_type="add_sql_snippet"),
        QuestionJourneyEvent(question_id="gs_001", stage="applied",
                             proposal_id="P1"),
        QuestionJourneyEvent(question_id="gs_001", stage="accepted"),
        QuestionJourneyEvent(question_id="gs_001", stage="post_eval",
                             was_passing=False, is_passing=True,
                             transition="fail_to_pass"),
    ]
    report = validate_question_journeys(
        events=events,
        eval_qids={"gs_001"},
    )

    assert report.is_valid is True
    assert report.violations == []
    assert report.terminal_state_by_qid["gs_001"].value == "hard_failure_resolved"


def test_canonical_journey_strips_extra_dict_and_sorts_events() -> None:
    import json
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        canonical_journey_json,
    )

    events_a = [
        QuestionJourneyEvent(
            question_id="gs_002", stage="evaluated",
            extra={"timestamp": 12345, "duration_ms": 7},
        ),
        QuestionJourneyEvent(
            question_id="gs_001", stage="evaluated",
            extra={"timestamp": 99999, "duration_ms": 1},
        ),
    ]
    events_b = list(reversed(events_a))  # different insertion order

    a = canonical_journey_json(events=events_a)
    b = canonical_journey_json(events=events_b)

    assert a == b
    parsed = json.loads(a)
    assert [ev["question_id"] for ev in parsed] == ["gs_001", "gs_002"]
    # extra is stripped (volatile by definition).
    assert "extra" not in parsed[0]


def test_canonical_journey_is_stable_across_runs() -> None:
    """Two identical event lists must produce the same canonical bytes."""
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        canonical_journey_json,
    )

    events = [
        QuestionJourneyEvent(question_id="gs_001", stage="evaluated"),
        QuestionJourneyEvent(question_id="gs_001", stage="clustered",
                             cluster_id="H001"),
    ]
    a = canonical_journey_json(events=events)
    b = canonical_journey_json(events=list(events))
    assert a == b


def test_journey_validation_report_to_dict_round_trip() -> None:
    """to_dict() must produce a JSON-safe, stable dict; enum values render
    as their .value strings so the output is byte-stable."""
    import json

    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyContractViolation,
        JourneyTerminalState,
        JourneyValidationReport,
    )

    report = JourneyValidationReport(
        is_valid=False,
        missing_qids=("q_missing",),
        violations=[
            JourneyContractViolation(
                question_id="q1",
                kind="illegal_transition",
                detail="evaluated -> post_eval",
            ),
        ],
        terminal_state_by_qid={
            "q1": JourneyTerminalState.HARD_FAILURE_UNRESOLVED,
            "q2": JourneyTerminalState.ALREADY_PASSING,
        },
    )

    d = report.to_dict()
    assert json.loads(json.dumps(d, sort_keys=True, separators=(",", ":"))) == d
    assert d["is_valid"] is False
    assert d["missing_qids"] == ["q_missing"]
    assert d["violations"] == [
        {
            "question_id": "q1",
            "kind": "illegal_transition",
            "detail": "evaluated -> post_eval",
        },
    ]
    assert d["terminal_state_by_qid"] == {
        "q1": "hard_failure_unresolved",
        "q2": "already_passing",
    }
