"""F-5 — consecutive identical soft_signal trunk events dedup at producer.

Run 833969815458299 emitted 13 ``soft_signal -> soft_signal`` trunk
transitions because the soft-pile classifier and the cluster-formation
pass both append a soft_signal event for the same qid. N1's plan added
a producer-side dedup; only the contract validator and lane-keys
landed. This task closes the producer-side gap.
"""
from __future__ import annotations


def test_consecutive_identical_soft_signal_events_collapse() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
        dedupe_consecutive_trunk_events,
    )
    events = [
        QuestionJourneyEvent(
            question_id="gs_001",
            stage="soft_signal",
            cluster_id="S001",
        ),
        QuestionJourneyEvent(
            question_id="gs_001",
            stage="soft_signal",
            cluster_id="S001",
        ),
    ]
    result = dedupe_consecutive_trunk_events(events)
    assert len(result) == 1
    assert result[0].stage == "soft_signal"
    assert result[0].question_id == "gs_001"


def test_dedup_preserves_distinct_qids() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
        dedupe_consecutive_trunk_events,
    )
    events = [
        QuestionJourneyEvent(
            question_id="gs_001",
            stage="soft_signal",
        ),
        QuestionJourneyEvent(
            question_id="gs_002",
            stage="soft_signal",
        ),
    ]
    result = dedupe_consecutive_trunk_events(events)
    assert len(result) == 2


def test_dedup_does_not_collapse_lane_events() -> None:
    """Lane events (proposal_id != '') are intentionally per-patch;
    dedup applies to trunk events only."""
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
        dedupe_consecutive_trunk_events,
    )
    events = [
        QuestionJourneyEvent(
            question_id="gs_026",
            stage="applied_targeted",
            proposal_id="L1:P001#1",
            parent_proposal_id="P001",
        ),
        QuestionJourneyEvent(
            question_id="gs_026",
            stage="applied_targeted",
            proposal_id="L1:P001#2",
            parent_proposal_id="P001",
        ),
    ]
    result = dedupe_consecutive_trunk_events(events)
    assert len(result) == 2  # distinct lanes by proposal_id


def test_dedup_does_not_collapse_distinct_consecutive_stages() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
        dedupe_consecutive_trunk_events,
    )
    events = [
        QuestionJourneyEvent(
            question_id="gs_001",
            stage="soft_signal",
        ),
        QuestionJourneyEvent(
            question_id="gs_001",
            stage="clustered",
        ),
    ]
    result = dedupe_consecutive_trunk_events(events)
    assert len(result) == 2  # different stages, both kept


def test_dedup_handles_empty_input() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        dedupe_consecutive_trunk_events,
    )
    assert dedupe_consecutive_trunk_events([]) == []


def test_dedup_lane_event_resets_trunk_tracker() -> None:
    """A lane event (proposal_id != '') resets the trunk tracker so a
    later trunk emit with the same stage is allowed (legitimate cycle).
    """
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
        dedupe_consecutive_trunk_events,
    )
    events = [
        QuestionJourneyEvent(question_id="gs_001", stage="soft_signal"),
        QuestionJourneyEvent(
            question_id="gs_001",
            stage="proposed",
            proposal_id="P001",
        ),
        QuestionJourneyEvent(question_id="gs_001", stage="soft_signal"),
    ]
    result = dedupe_consecutive_trunk_events(events)
    assert len(result) == 3  # both soft_signal events kept, separated by lane
