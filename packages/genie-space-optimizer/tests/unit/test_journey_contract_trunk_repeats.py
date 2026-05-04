"""Plan N1 Task 1 — pin the trunk-repeat journey-validation defect.

Inspiration: run 2afb0be2-88b6-4832-99aa-c7e78fbc90f7 retry attempt
993610879088298. ``GSO_ITERATION_SUMMARY_V1.journey_violation_count``
was 12 / 8 across the two iterations; 7 + 5 of those were
``soft_signal -> soft_signal`` self-transitions caused by the producer
emitting one ``soft_signal`` per (cluster, qid) pair instead of one
per qid.

These tests pin the producer-side fix (Task 2 ``emit_cluster_
membership_events`` helper) and the contract's existing rejection
of the ``SOFT_SIGNAL -> SOFT_SIGNAL`` self-transition so future
contract relaxations cannot silently re-admit the regression.
"""
from __future__ import annotations


def test_qid_in_two_soft_clusters_emits_one_soft_signal() -> None:
    """``emit_cluster_membership_events`` must emit exactly one
    ``soft_signal`` event per qid even when the qid appears in
    multiple soft clusters. Multi-cluster membership is preserved
    on ``extra.additional_cluster_ids`` so auditors retain the
    information without polluting the journey timeline.
    """
    from genie_space_optimizer.optimization.question_journey import (
        emit_cluster_membership_events,
    )

    soft_clusters = [
        {
            "cluster_id": "S001",
            "root_cause": "format_difference",
            "question_ids": ["qid_x", "qid_y"],
        },
        {
            "cluster_id": "S002",
            "root_cause": "rounding_difference",
            "question_ids": ["qid_x", "qid_z"],
        },
    ]

    captured: list[dict] = []

    def fake_emit(stage: str, **fields) -> None:
        captured.append({"stage": stage, **fields})

    emit_cluster_membership_events(
        journey_emit=fake_emit,
        hard_clusters=[],
        soft_clusters=soft_clusters,
    )

    soft_emits = [
        e for e in captured
        if e["stage"] == "soft_signal"
        and "qid_x" in (e.get("question_ids") or [])
    ]
    assert len(soft_emits) == 1, (
        f"qid_x must produce exactly one soft_signal event across "
        f"all clusters; got {len(soft_emits)}: {soft_emits}"
    )
    additional = (soft_emits[0].get("extra") or {}).get(
        "additional_cluster_ids", []
    )
    assert "S002" in additional, (
        "second cluster id must appear in extra.additional_cluster_ids "
        f"so audit retains full membership; got extra={soft_emits[0].get('extra')}"
    )


def test_validate_question_journeys_rejects_soft_signal_self_transition() -> None:
    """Lock in the contract's existing rejection of
    ``SOFT_SIGNAL -> SOFT_SIGNAL``. If a future change ever relaxes
    ``_LEGAL_NEXT[SOFT_SIGNAL]`` to admit the self-loop, this test
    fails and forces a deliberate review.
    """
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        validate_question_journeys,
    )

    events = [
        QuestionJourneyEvent(question_id="qid_x", stage="evaluated"),
        QuestionJourneyEvent(
            question_id="qid_x", stage="soft_signal", cluster_id="S001",
        ),
        QuestionJourneyEvent(
            question_id="qid_x", stage="soft_signal", cluster_id="S002",
        ),
        QuestionJourneyEvent(question_id="qid_x", stage="post_eval"),
    ]
    report = validate_question_journeys(
        events=events, eval_qids=["qid_x"],
    )
    illegal = [
        v for v in report.violations
        if v.kind == "illegal_transition"
        and "soft_signal -> soft_signal" in v.detail
    ]
    assert illegal, (
        "validator must report soft_signal -> soft_signal as an illegal "
        "transition; got violations: "
        + "; ".join(f"{v.kind}:{v.detail}" for v in report.violations)
    )
