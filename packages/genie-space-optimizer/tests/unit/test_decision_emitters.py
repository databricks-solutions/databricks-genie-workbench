"""Per-producer unit tests for decision_emitters.py.

Each producer is exercised against golden-input fixtures that pin:
- the right ``DecisionType`` / ``DecisionOutcome`` / ``ReasonCode`` mapping
- RCA-grounding fields populated where applicable
- cross-checker compatibility (running ``validate_decisions_against_journey``
  against the producer's output yields no rca-required violations modulo
  the documented exemptions)

See `docs/2026-05-02-unified-trace-and-operator-transcript-plan.md` and the
postmortem at
`docs/runid_analysis/1036606061019898_894992655057610_analysis.md`.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# eval_classification_records
# ---------------------------------------------------------------------------


def test_eval_classification_records_one_per_qid_with_partition_reason() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        eval_classification_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    records = eval_classification_records(
        run_id="run_1",
        iteration=1,
        eval_qids=["q1", "q2", "q3", "q4"],
        classification={
            "q1": "already_passing",
            "q2": "hard",
            "q3": "soft",
            "q4": "gt_correction",
        },
        cluster_by_qid={"q2": "H001"},
    )

    assert len(records) == 4
    assert all(r.decision_type == DecisionType.EVAL_CLASSIFIED for r in records)
    assert all(r.outcome == DecisionOutcome.INFO for r in records)
    by_qid = {r.question_id: r for r in records}
    assert by_qid["q1"].reason_code == ReasonCode.ALREADY_PASSING
    assert by_qid["q2"].reason_code == ReasonCode.HARD_FAILURE
    assert by_qid["q2"].cluster_id == "H001"  # carried through
    assert by_qid["q3"].reason_code == ReasonCode.SOFT_SIGNAL
    assert by_qid["q4"].reason_code == ReasonCode.GT_CORRECTION
    # All carry evidence_refs and target_qids
    for rec in records:
        assert rec.evidence_refs == (f"eval:{rec.question_id}",)
        assert rec.target_qids == (rec.question_id,)


def test_eval_classification_records_skips_unmapped_qids() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        eval_classification_records,
    )

    records = eval_classification_records(
        run_id="run_1",
        iteration=1,
        eval_qids=["q1", "q2", ""],
        classification={"q1": "hard"},  # q2 not in classification
    )

    assert len(records) == 1
    assert records[0].question_id == "q1"


def test_eval_classification_passes_cross_checker_without_rca_id() -> None:
    """EVAL_CLASSIFIED is exempt from rca_id/root_cause requirements."""
    from genie_space_optimizer.optimization.decision_emitters import (
        eval_classification_records,
    )
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    records = eval_classification_records(
        run_id="run_1",
        iteration=1,
        eval_qids=["q1"],
        classification={"q1": "hard"},
    )
    events = [QuestionJourneyEvent(question_id="q1", stage="evaluated")]

    violations = validate_decisions_against_journey(records=records, events=events)
    assert violations == []


# ---------------------------------------------------------------------------
# cluster_records
# ---------------------------------------------------------------------------


def test_cluster_records_one_per_hard_cluster_with_evidence_and_target_qids() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        cluster_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        ReasonCode,
    )

    records = cluster_records(
        run_id="run_1",
        iteration=1,
        clusters=[
            {
                "cluster_id": "H001",
                "question_ids": ["q1", "q2"],
                "root_cause": "missing_filter",
            },
            {
                "cluster_id": "H002",
                "question_ids": ["q3"],
                "root_cause": "wrong_column",
            },
        ],
        rca_id_by_cluster={"H001": "rca_h001"},
    )

    assert len(records) == 2
    assert records[0].decision_type == DecisionType.CLUSTER_SELECTED
    assert records[0].reason_code == ReasonCode.CLUSTERED
    assert records[0].cluster_id == "H001"
    assert records[0].rca_id == "rca_h001"
    assert records[0].root_cause == "missing_filter"
    assert records[0].evidence_refs == ("cluster:H001",)
    assert records[0].target_qids == ("q1", "q2")
    assert records[1].rca_id == ""  # H002 not in rca_id_by_cluster


# ---------------------------------------------------------------------------
# strategist_ag_records
# ---------------------------------------------------------------------------


def test_strategist_ag_records_carries_target_qids_when_directives_provide_them() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_ag_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        ReasonCode,
    )

    records = strategist_ag_records(
        run_id="run_1",
        iteration=1,
        action_groups=[
            {
                "id": "AG1",
                "affected_questions": ["q1", "q2"],
                "source_cluster_ids": ["H001"],
                "lever_directives": {
                    "5": {"target_qids": ["q1"]},
                    "6": {"target_qids": ["q2"]},
                },
            }
        ],
        source_clusters_by_id={
            "H001": {"cluster_id": "H001", "root_cause": "missing_filter"},
        },
        rca_id_by_cluster={"H001": "rca_h001"},
    )

    assert len(records) == 1
    rec = records[0]
    assert rec.decision_type == DecisionType.STRATEGIST_AG_EMITTED
    assert rec.reason_code == ReasonCode.STRATEGIST_SELECTED
    assert rec.ag_id == "AG1"
    assert rec.rca_id == "rca_h001"
    assert rec.root_cause == "missing_filter"
    assert rec.affected_qids == ("q1", "q2")
    assert set(rec.target_qids) == {"q1", "q2"}
    assert rec.source_cluster_ids == ("H001",)


def test_strategist_ag_records_flags_missing_target_qids_with_reason_code() -> None:
    """Cycle-8-Bug-1 signal: when an AG has no target_qids, mark
    reason_code=MISSING_TARGET_QIDS so the cross-checker exempts it from
    target_qids requirement."""
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_ag_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )

    records = strategist_ag_records(
        run_id="run_1",
        iteration=1,
        action_groups=[
            {
                "id": "AG_BROKEN",
                "affected_questions": [],  # no scope!
                "source_cluster_ids": [],
                "lever_directives": {"5": {"target_qids": []}},
            }
        ],
    )

    assert len(records) == 1
    assert records[0].reason_code == ReasonCode.MISSING_TARGET_QIDS
    assert records[0].target_qids == ()


def test_strategist_ag_records_falls_back_to_affected_questions_when_directives_empty() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_ag_records,
    )

    records = strategist_ag_records(
        run_id="run_1",
        iteration=1,
        action_groups=[
            {
                "id": "AG2",
                "affected_questions": ["q5"],
                "source_cluster_ids": ["H002"],
                "lever_directives": {},
            }
        ],
    )

    assert records[0].target_qids == ("q5",)


# ---------------------------------------------------------------------------
# ag_outcome_decision_record
# ---------------------------------------------------------------------------


def test_ag_outcome_decision_record_maps_each_outcome_string() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        ag_outcome_decision_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    ag = {
        "id": "AG1",
        "affected_questions": ["q1"],
        "source_cluster_ids": ["H001"],
    }
    cases = {
        "accepted": (DecisionOutcome.ACCEPTED, ReasonCode.PATCH_APPLIED),
        "accepted_with_regression_debt": (
            DecisionOutcome.ACCEPTED,
            ReasonCode.PATCH_APPLIED,
        ),
        "rolled_back": (DecisionOutcome.ROLLED_BACK, ReasonCode.PATCH_SKIPPED),
        "skipped_no_applied_patches": (
            DecisionOutcome.SKIPPED,
            ReasonCode.NO_APPLIED_PATCHES,
        ),
        "skipped_dead_on_arrival": (
            DecisionOutcome.SKIPPED,
            ReasonCode.NO_APPLIED_PATCHES,
        ),
        "skipped_pre_ag_snapshot_failed": (
            DecisionOutcome.SKIPPED,
            ReasonCode.NONE,
        ),
    }
    for outcome, (expected_outcome, expected_reason) in cases.items():
        rec = ag_outcome_decision_record(
            run_id="run_1",
            iteration=1,
            ag=ag,
            outcome=outcome,
        )
        assert rec is not None, f"outcome={outcome}"
        assert rec.decision_type == DecisionType.ACCEPTANCE_DECIDED
        assert rec.outcome == expected_outcome, f"outcome={outcome}"
        assert rec.reason_code == expected_reason, f"outcome={outcome}"
        assert rec.observed_effect, f"outcome={outcome} should have observed_effect"
        assert rec.next_action, f"outcome={outcome} should have next_action"


def test_ag_outcome_decision_record_returns_none_for_unknown_outcome() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        ag_outcome_decision_record,
    )

    rec = ag_outcome_decision_record(
        run_id="run_1",
        iteration=1,
        ag={"id": "AG1", "affected_questions": []},
        outcome="not_a_real_outcome_string",
    )

    assert rec is None


def test_ag_outcome_decision_record_carries_regression_qids_when_supplied() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        ag_outcome_decision_record,
    )

    rec = ag_outcome_decision_record(
        run_id="run_1",
        iteration=1,
        ag={"id": "AG1", "affected_questions": ["q1"], "source_cluster_ids": []},
        outcome="accepted_with_regression_debt",
        regression_qids=["q9", "q10"],
    )

    assert rec is not None
    assert rec.regression_qids == ("q9", "q10")


# ---------------------------------------------------------------------------
# post_eval_resolution_records
# ---------------------------------------------------------------------------


def test_post_eval_resolution_classifies_each_transition_correctly() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        post_eval_resolution_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    records = post_eval_resolution_records(
        run_id="run_1",
        iteration=1,
        eval_qids=["hold_pass", "fail_to_pass", "hold_fail", "pass_to_fail"],
        prior_passing_qids=["hold_pass", "pass_to_fail"],
        post_passing_qids=["hold_pass", "fail_to_pass"],
        cluster_by_qid={"fail_to_pass": "H001", "hold_fail": "H001"},
        rca_id_by_cluster={"H001": "rca_h001"},
    )

    assert len(records) == 4
    assert all(r.decision_type == DecisionType.QID_RESOLUTION for r in records)
    by_qid = {r.question_id: r for r in records}
    assert by_qid["hold_pass"].outcome == DecisionOutcome.RESOLVED
    assert by_qid["hold_pass"].reason_code == ReasonCode.POST_EVAL_HOLD_PASS
    # Held-pass qids carry no rca_id (never clustered).
    assert by_qid["hold_pass"].rca_id == ""
    # Held-pass qids carry no target_qids (the cross-checker exemption uses
    # the reason code, but we additionally set target_qids=() so the
    # contract is honest about scope).
    assert by_qid["hold_pass"].target_qids == ()
    assert by_qid["fail_to_pass"].outcome == DecisionOutcome.RESOLVED
    assert by_qid["fail_to_pass"].reason_code == ReasonCode.POST_EVAL_FAIL_TO_PASS
    assert by_qid["fail_to_pass"].rca_id == "rca_h001"
    assert by_qid["hold_fail"].outcome == DecisionOutcome.UNRESOLVED
    assert by_qid["hold_fail"].reason_code == ReasonCode.POST_EVAL_HOLD_FAIL
    assert by_qid["hold_fail"].rca_id == "rca_h001"
    assert by_qid["pass_to_fail"].outcome == DecisionOutcome.UNRESOLVED
    assert by_qid["pass_to_fail"].reason_code == ReasonCode.POST_EVAL_PASS_TO_FAIL


def test_post_eval_resolution_passes_cross_checker_for_held_pass_without_rca() -> None:
    """The POST_EVAL_HOLD_PASS exemption added in Task 2 lets held-pass
    records pass without rca_id/root_cause/target_qids."""
    from genie_space_optimizer.optimization.decision_emitters import (
        post_eval_resolution_records,
    )
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    records = post_eval_resolution_records(
        run_id="run_1",
        iteration=1,
        eval_qids=["q1"],
        prior_passing_qids={"q1"},
        post_passing_qids={"q1"},
    )
    events = [
        QuestionJourneyEvent(question_id="q1", stage="evaluated"),
        QuestionJourneyEvent(question_id="q1", stage="post_eval", is_passing=True),
    ]

    violations = validate_decisions_against_journey(records=records, events=events)
    assert violations == []


# ---------------------------------------------------------------------------
# classify_no_records_reason
# ---------------------------------------------------------------------------


def test_classify_no_records_reason_prioritises_producer_exception() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        NoRecordsReason,
        classify_no_records_reason,
    )

    reason = classify_no_records_reason(
        iteration_inputs={
            "clusters": [{"cluster_id": "H001"}],
            "strategist_response": {"action_groups": [{"id": "AG1"}]},
        },
        producer_exceptions={"eval_classification": 1},
    )

    assert reason == NoRecordsReason.PRODUCER_EXCEPTION


def test_classify_no_records_reason_no_clusters() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        NoRecordsReason,
        classify_no_records_reason,
    )

    reason = classify_no_records_reason(
        iteration_inputs={"clusters": []},
        producer_exceptions={},
    )

    assert reason == NoRecordsReason.NO_CLUSTERS


def test_classify_no_records_reason_no_ags() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        NoRecordsReason,
        classify_no_records_reason,
    )

    reason = classify_no_records_reason(
        iteration_inputs={
            "clusters": [{"cluster_id": "H001"}],
            "strategist_response": {"action_groups": []},
        },
        producer_exceptions={},
    )

    assert reason == NoRecordsReason.NO_AGS_EMITTED


def test_classify_no_records_reason_all_ags_dropped_at_grounding() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        NoRecordsReason,
        classify_no_records_reason,
    )

    reason = classify_no_records_reason(
        iteration_inputs={
            "clusters": [{"cluster_id": "H001"}],
            "strategist_response": {"action_groups": [{"id": "AG1"}, {"id": "AG2"}]},
            "ag_outcomes": {
                "AG1": "skipped_no_applied_patches",
                "AG2": "skipped_dead_on_arrival",
            },
        },
        producer_exceptions={},
    )

    assert reason == NoRecordsReason.ALL_AGS_DROPPED_AT_GROUNDING


def test_classify_no_records_reason_patch_cap_did_not_fire() -> None:
    """Mixed outcomes (some non-skipped) → fall back to PATCH_CAP_DID_NOT_FIRE."""
    from genie_space_optimizer.optimization.decision_emitters import (
        NoRecordsReason,
        classify_no_records_reason,
    )

    reason = classify_no_records_reason(
        iteration_inputs={
            "clusters": [{"cluster_id": "H001"}],
            "strategist_response": {"action_groups": [{"id": "AG1"}]},
            "ag_outcomes": {"AG1": "accepted"},  # non-skipped
        },
        producer_exceptions={},
    )

    assert reason == NoRecordsReason.PATCH_CAP_DID_NOT_FIRE


# ---------------------------------------------------------------------------
# is_strict_mode (env var helper)
# ---------------------------------------------------------------------------


def test_is_strict_mode_reads_env_var(monkeypatch) -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        is_strict_mode,
    )

    monkeypatch.delenv("GSO_DECISION_EMITTER_STRICT", raising=False)
    assert is_strict_mode() is False
    monkeypatch.setenv("GSO_DECISION_EMITTER_STRICT", "1")
    assert is_strict_mode() is True
    monkeypatch.setenv("GSO_DECISION_EMITTER_STRICT", "0")
    assert is_strict_mode() is False
    monkeypatch.setenv("GSO_DECISION_EMITTER_STRICT", "true")
    assert is_strict_mode() is True


# ---------------------------------------------------------------------------
# Plan N4 — invariant warn-and-degrade reason codes + emitters
# ---------------------------------------------------------------------------


def test_reason_codes_for_invariant_policy_exist() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode

    assert ReasonCode.QID_RELEASED_FROM_QUARANTINE.value == (
        "qid_released_from_quarantine"
    )
    assert ReasonCode.REGRESSION_DEBT_PARTITION_INCOMPLETE.value == (
        "regression_debt_partition_incomplete"
    )
    assert ReasonCode.CAP_CONSERVATION_REPAIRED.value == (
        "cap_conservation_repaired"
    )
    assert ReasonCode.NON_CANONICAL_JUDGE_ROW.value == (
        "non_canonical_judge_row"
    )
    assert ReasonCode.INVARIANT_VIOLATION_DOWNGRADED.value == (
        "invariant_violation_downgraded"
    )


def test_qid_released_from_quarantine_record_has_typed_fields() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        qid_released_from_quarantine_record,
    )

    rec = qid_released_from_quarantine_record(
        run_id="r1", iteration=2, qids=("gs_009",),
        cause="recovered_post_eval",
    )
    assert rec.iteration == 2
    assert rec.target_qids == ("gs_009",)
    assert rec.reason_code.value == "qid_released_from_quarantine"
    assert "gs_009" in (rec.next_action or "")


def test_regression_debt_partition_incomplete_record_has_typed_fields() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        regression_debt_partition_incomplete_record,
    )

    rec = regression_debt_partition_incomplete_record(
        run_id="r1", iteration=3, missing_qids=("gs_021", "gs_013"),
    )
    assert rec.iteration == 3
    assert rec.reason_code.value == "regression_debt_partition_incomplete"
    assert rec.target_qids == ("gs_013", "gs_021")  # sorted


def test_cap_conservation_repaired_record_has_typed_fields() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        cap_conservation_repaired_record,
    )

    rec = cap_conservation_repaired_record(
        run_id="r1", iteration=4, func_name="select_under_cap",
        decisions_in=3, decisions_out=2, input_count=2,
    )
    assert rec.gate == "select_under_cap"
    assert rec.reason_code.value == "cap_conservation_repaired"
    assert rec.metrics["decisions_in"] == 3
    assert rec.metrics["decisions_out"] == 2
    assert rec.metrics["input_count"] == 2


def test_non_canonical_judge_row_record_has_typed_fields() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        non_canonical_judge_row_record,
    )

    rec = non_canonical_judge_row_record(
        run_id="r1", iteration=5, judge="result_correctness",
        detail="missing rationale",
    )
    assert rec.gate == "result_correctness"
    assert rec.reason_code.value == "non_canonical_judge_row"
    assert "missing rationale" in (rec.reason_detail or "")


# Cycle 10 W3/W4/W8 — typed emitters
def test_lever6_force_llm_declined_record_shape():
    from genie_space_optimizer.optimization.decision_emitters import (
        lever6_force_llm_declined_record,
    )
    rec = lever6_force_llm_declined_record(
        run_id="r1", iteration=2, ag_id="AG_DECOMPOSED_H004",
        cluster_id="H004", root_cause="missing_filter",
        target_qids=("gs_024",),
    )
    d = rec.to_dict()
    assert d["reason_code"] == "lever6_force_llm_declined"
    assert d["decision_type"] in {"proposal_generated", "strategist_ag_emitted"}


def test_lever6_force_raised_record_shape():
    from genie_space_optimizer.optimization.decision_emitters import (
        lever6_force_raised_record,
    )
    rec = lever6_force_raised_record(
        run_id="r1", iteration=2, ag_id="AG_DECOMPOSED_H004",
        cluster_id="H004", root_cause="missing_filter",
        exception_repr="ValueError('synthesis failed')",
    )
    d = rec.to_dict()
    assert d["reason_code"] == "lever6_force_raised"


def test_narrow_not_applicable_record_shape():
    from genie_space_optimizer.optimization.decision_emitters import (
        narrow_not_applicable_record,
    )
    rec = narrow_not_applicable_record(
        run_id="r1", iteration=3, ag_id="AG_X",
        cluster_id="H001", root_cause="missing_filter",
        original_patch_type="add_sql_snippet_measure",
        reason="patch_type_lacks_where_predicate",
    )
    d = rec.to_dict()
    assert d["reason_code"] == "narrow_not_applicable"


def test_ag_levers_unioned_record_shape():
    from genie_space_optimizer.optimization.decision_emitters import (
        ag_levers_unioned_record,
    )
    rec = ag_levers_unioned_record(
        run_id="r1", iteration=2, ag_id="AG_DECOMPOSED_H001",
        cluster_id="H001",
        levers_before=("5",),
        levers_after=("3", "5", "6"),
    )
    d = rec.to_dict()
    assert d["reason_code"] == "ag_levers_unioned"


def test_cluster_blocked_no_rca_record_produces_typed_record():
    from genie_space_optimizer.optimization.decision_emitters import (
        cluster_blocked_no_rca_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        DecisionOutcome,
        ReasonCode,
    )

    rec = cluster_blocked_no_rca_record(
        run_id="run-123",
        iteration=2,
        cluster_id="H001",
        rca_id="",
        affected_qids=["airline_ticketing_and_fare_analysis_gs_009"],
        root_cause="wrong_aggregation",
    )

    assert rec.decision_type == DecisionType.CLUSTER_BLOCKED_NO_RCA
    assert rec.outcome == DecisionOutcome.SKIPPED
    assert rec.reason_code == ReasonCode.RCA_UNGROUNDED
    assert rec.iteration == 2
    assert rec.run_id == "run-123"
    payload = rec.to_dict()
    assert payload["decision_type"] == "cluster_blocked_no_rca"
    assert payload["cluster_id"] == "H001"
    assert payload["root_cause"] == "wrong_aggregation"
    assert payload["target_qids"] == [
        "airline_ticketing_and_fare_analysis_gs_009"
    ]


def test_cluster_blocked_no_rca_record_handles_empty_optional_fields():
    from genie_space_optimizer.optimization.decision_emitters import (
        cluster_blocked_no_rca_record,
    )

    rec = cluster_blocked_no_rca_record(
        run_id="run-x",
        iteration=0,
        cluster_id="H999",
        rca_id=None,
        affected_qids=None,
        root_cause=None,
    )

    # Dataclass-level coercion (None -> ""): the to_dict shape drops
    # empty strings, so check the field directly for the empty cases.
    assert rec.cluster_id == "H999"
    assert rec.rca_id == ""
    assert rec.root_cause == ""
    payload = rec.to_dict()
    assert payload["cluster_id"] == "H999"
    # rca_id / root_cause are omitted from to_dict when empty — that's
    # the existing DecisionRecord contract, not a defect-1 concern.
    assert "rca_id" not in payload
    assert "root_cause" not in payload


def test_rca_classified_ungrounded_record_carries_typed_reason():
    from genie_space_optimizer.optimization.decision_emitters import (
        rca_classified_ungrounded_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType, DecisionOutcome, ReasonCode, RcaUngroundedReason,
    )

    rec = rca_classified_ungrounded_record(
        run_id="r",
        iteration=1,
        cluster_id="H001",
        target_qids=("airline_gs_009",),
        ungrounded_reason=RcaUngroundedReason.NO_FINDINGS,
        policy_max_attempts=1,
    )
    assert rec.decision_type == DecisionType.RCA_FORMED
    assert rec.outcome == DecisionOutcome.INFO
    assert rec.reason_code == ReasonCode.RCA_CLASSIFIED_UNGROUNDED
    payload = rec.to_dict()
    assert payload["metrics"]["ungrounded_reason"] == "no_findings"
    assert payload["metrics"]["policy_max_attempts"] == 1


def test_rca_classified_ungrounded_record_handles_non_retryable_reason():
    from genie_space_optimizer.optimization.decision_emitters import (
        rca_classified_ungrounded_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        RcaUngroundedReason,
    )

    rec = rca_classified_ungrounded_record(
        run_id="r",
        iteration=1,
        cluster_id="H001",
        target_qids=("airline_gs_009",),
        ungrounded_reason=RcaUngroundedReason.MISSING_TARGET_QIDS,
        policy_max_attempts=0,
    )
    payload = rec.to_dict()
    assert payload["metrics"]["ungrounded_reason"] == "missing_target_qids"
    assert payload["metrics"]["policy_max_attempts"] == 0
    assert "policy refused" in payload["next_action"].lower()


def test_rca_regeneration_succeeded_record_produces_typed_record():
    from genie_space_optimizer.optimization.decision_emitters import (
        rca_regeneration_succeeded_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType, DecisionOutcome, ReasonCode, RcaUngroundedReason,
    )

    rec = rca_regeneration_succeeded_record(
        run_id="r",
        iteration=2,
        cluster_id="H001",
        rca_id="rca-9af3",
        target_qids=("airline_gs_009",),
        attempt_number=1,
        attempted_evidence_sources=("failure_buckets",),
        ungrounded_reason=RcaUngroundedReason.NO_FINDINGS,
    )
    assert rec.decision_type == DecisionType.RCA_FORMED
    assert rec.outcome == DecisionOutcome.INFO
    assert rec.reason_code == ReasonCode.RCA_REGENERATION_SUCCEEDED
    payload = rec.to_dict()
    assert payload["metrics"]["attempt_number"] == 1
    assert payload["metrics"]["attempted_evidence_sources"] == [
        "failure_buckets",
    ]
    assert payload["metrics"]["ungrounded_reason"] == "no_findings"
