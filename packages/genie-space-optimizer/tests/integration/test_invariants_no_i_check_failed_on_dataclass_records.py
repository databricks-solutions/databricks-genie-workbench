"""B2 Task 5 — integration: run_invariants on dataclass-only decision_records.

Asserts the 2314bb2c shape (dataclass DecisionRecord instances mixed with
dict-shaped records) produces ZERO ``I_CHECK_FAILED`` violations after the
projection-boundary normalization. Before B2, this iteration would have
fired 2 I_CHECK_FAILED rows (one each from check_i7_rca_grounding and
check_i14_l6_decline_dedup).
"""

from __future__ import annotations


def _mk_dataclass_record(
    *,
    decision_type: str,
    reason_code: str = "none",
    cluster_id: str = "",
    ag_id: str = "",
    root_cause: str = "",
    evidence_refs: tuple[str, ...] = (),
    metrics: dict | None = None,
):
    """Real DecisionRecord, the shape 2314bb2c emit sites produced."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
    )

    return DecisionRecord(
        run_id="2314bb2c-95a1-4d60-8226-09e5155aee2a",
        iteration=1,
        decision_type=DecisionType(decision_type),
        outcome=DecisionOutcome.APPLIED,
        reason_code=ReasonCode(reason_code),
        cluster_id=cluster_id,
        ag_id=ag_id,
        root_cause=root_cause,
        evidence_refs=evidence_refs,
        metrics=metrics or {},
    )


def test_dataclass_only_iteration_produces_no_i_check_failed_rows() -> None:
    from genie_space_optimizer.optimization.invariant_projection import (
        project_iter_evidence,
    )
    from genie_space_optimizer.optimization.invariants import (
        run_invariants,
    )

    iter_inputs = {
        "clusters": [
            {
                "cluster_id": "c-1",
                "recommended_levers": [5],
                "qids": ["gs_001"],
            },
        ],
        "open_hard_cluster_ids": ["c-1"],
        "rca_cards_present": {"c-1": True},
        "strategist_response": {
            "action_groups": [
                {
                    "id": "AG1",
                    "Levers": [5],
                    "source_cluster_ids": ["c-1"],
                    "lever_directives": {"5": {}},
                    "root_cause": "missing_filter",
                },
            ],
        },
        "decision_records": [
            _mk_dataclass_record(
                decision_type="strategist_ag_emitted",
                ag_id="AG1",
                cluster_id="c-1",
                root_cause="missing_filter",
            ),
            _mk_dataclass_record(
                decision_type="cluster_blocked_no_rca",
                cluster_id="c-99",
                root_cause="ungrounded",
            ),
        ],
    }
    evidence = project_iter_evidence(
        current_iter_inputs=iter_inputs,
        iteration=1,
        run_id="2314bb2c",
        iter_producer_exceptions={},
        prior_iter_evidence=None,
    )
    violations = run_invariants(evidence)
    i_check_failed = [
        v for v in violations
        if str(v.get("invariant_id") or "") == "I_CHECK_FAILED"
    ]
    assert i_check_failed == [], (
        f"expected zero I_CHECK_FAILED rows; got: {i_check_failed}"
    )


def test_l6_decline_dedup_runs_on_dataclass_records() -> None:
    """The specific 2314bb2c failure mode: check_i14_l6_decline_dedup
    must process L6-declined dataclass records without raising."""
    from genie_space_optimizer.optimization.invariant_projection import (
        project_iter_evidence,
    )
    from genie_space_optimizer.optimization.invariants import (
        run_invariants,
    )

    iter_inputs = {
        "clusters": [],
        "strategist_response": {"action_groups": []},
        "rca_cards_present": {},
        "decision_records": [
            _mk_dataclass_record(
                decision_type="proposal_failure_decided",
                reason_code="lever6_force_llm_declined",
                root_cause="missing_filter",
                evidence_refs=("signature:c1_missing_filter",),
                metrics={"cached": False},
            ),
        ],
    }
    evidence = project_iter_evidence(
        current_iter_inputs=iter_inputs,
        iteration=1,
        run_id="2314bb2c",
        iter_producer_exceptions={},
        prior_iter_evidence=None,
    )
    violations = run_invariants(evidence)
    i_check_failed = [
        v for v in violations
        if str(v.get("invariant_id") or "") == "I_CHECK_FAILED"
    ]
    assert i_check_failed == [], i_check_failed


def test_mixed_dataclass_and_dict_records_normalize_uniformly() -> None:
    """Half dataclass, half dict — both must reach the invariants."""
    from genie_space_optimizer.optimization.invariant_projection import (
        project_iter_evidence,
    )
    from genie_space_optimizer.optimization.invariants import (
        run_invariants,
    )

    dataclass_record = _mk_dataclass_record(
        decision_type="cluster_blocked_no_rca",
        cluster_id="c-blocked",
        root_cause="ungrounded",
    )
    iter_inputs = {
        "clusters": [],
        "open_hard_cluster_ids": ["c-blocked"],
        "rca_cards_present": {"c-blocked": False},
        "strategist_response": {"action_groups": []},
        "decision_records": [
            dataclass_record,
            {
                "decision_type": "strategist_ag_emitted",
                "ag_id": "AG2",
                "cluster_id": "c-blocked",
                "root_cause": "ungrounded",
            },
        ],
    }
    evidence = project_iter_evidence(
        current_iter_inputs=iter_inputs,
        iteration=1,
        run_id="2314bb2c",
        iter_producer_exceptions={},
        prior_iter_evidence=None,
    )
    violations = run_invariants(evidence)
    i_check_failed = [
        v for v in violations
        if str(v.get("invariant_id") or "") == "I_CHECK_FAILED"
    ]
    assert i_check_failed == [], i_check_failed
    # And critically, I7 must have observed the blocked-record fact, so the
    # ungrounded-cluster qid does NOT register as an I7 violation:
    i7_violations = [
        v for v in violations
        if str(v.get("invariant_id") or "") == "I7"
    ]
    assert i7_violations == [], (
        f"I7 fired despite cluster_blocked_no_rca record being present "
        f"as a dataclass; B2 wiring is incomplete. Violations: {i7_violations}"
    )
