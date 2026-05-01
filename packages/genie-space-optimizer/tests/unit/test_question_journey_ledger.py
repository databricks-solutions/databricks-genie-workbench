"""Pin the per-question journey ledger output."""

from __future__ import annotations

from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
    build_question_journey_ledger,
)


def _ev(qid: str, stage: str, **kwargs) -> QuestionJourneyEvent:
    return QuestionJourneyEvent(question_id=qid, stage=stage, **kwargs)


def test_ledger_groups_events_by_qid_and_orders_by_stage() -> None:
    events = [
        _ev("gs_017", "clustered", cluster_id="H002", root_cause="missing_filter"),
        _ev("gs_017", "proposed",  proposal_id="P016", patch_type="add_sql_snippet_filter"),
        _ev("gs_017", "applied",   proposal_id="P016"),
        _ev("gs_017", "post_eval", was_passing=False, is_passing=True, transition="fail_to_pass"),
        _ev("gs_026", "clustered", cluster_id="H001", root_cause="plural_top_n_collapse"),
        _ev("gs_026", "diagnostic_ag", ag_id="AG_COVERAGE_H001"),
        _ev("gs_026", "post_eval", was_passing=False, is_passing=False, transition="hold_fail"),
    ]
    ledger = build_question_journey_ledger(events=events, iteration=2)
    assert "QUESTION JOURNEY LEDGER" in ledger
    assert "gs_017" in ledger and "gs_026" in ledger
    assert "fail_to_pass" in ledger
    assert "AG_COVERAGE_H001" in ledger
    assert ledger.index("clustered") < ledger.index("proposed")


def test_ledger_renders_dropped_proposals_with_drop_reason() -> None:
    events = [
        _ev("gs_017", "clustered", cluster_id="H002"),
        _ev("gs_017", "proposed", proposal_id="P016", patch_type="add_sql_snippet_filter"),
        _ev("gs_017", "dropped_at_cap",
            proposal_id="P016", reason="not_in_active_cluster"),
        _ev("gs_017", "post_eval", was_passing=False, is_passing=False, transition="hold_fail"),
    ]
    ledger = build_question_journey_ledger(events=events, iteration=1)
    assert "dropped_at_cap" in ledger
    assert "not_in_active_cluster" in ledger


def test_ledger_is_empty_when_no_events() -> None:
    assert build_question_journey_ledger(events=[], iteration=1) == ""
