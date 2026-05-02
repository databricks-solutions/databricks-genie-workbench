from __future__ import annotations


def test_patch_inventory_summary_includes_causal_fields() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        summarize_patch_for_trace,
    )

    patch = {
        "proposal_id": "P015",
        "lever": 5,
        "patch_type": "update_instruction_section",
        "section_name": "FUNCTION ROUTING",
        "rca_id": "rca_q028_function_routing",
        "patch_family": "function_routing_guidance",
        "target_qids": ["q028"],
        "relevance_score": 1.0,
    }

    summary = summarize_patch_for_trace(patch)

    assert summary == {
        "proposal_id": "P015",
        "parent_proposal_id": "",
        "expanded_patch_id": "",
        "lever": 5,
        "patch_type": "update_instruction_section",
        "target": "FUNCTION ROUTING",
        "rca_id": "rca_q028_function_routing",
        "patch_family": "function_routing_guidance",
        "target_qids": ["q028"],
        "relevance_score": 1.0,
    }


def test_patch_cap_decision_rows_are_queryable_by_gate_and_proposal_id() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        patch_cap_decision_rows,
    )

    decisions = [
        {
            "proposal_id": "P015",
            "decision": "selected",
            "selection_reason": "highest_causal_relevance",
            "rank": 1,
            "relevance_score": 1.0,
            "lever": 5,
            "patch_type": "update_instruction_section",
            "rca_id": "rca_q028_function_routing",
            "target_qids": ["q028"],
        },
        {
            "proposal_id": "P001",
            "decision": "dropped",
            "selection_reason": "lower_causal_rank",
            "rank": None,
            "relevance_score": 0.55,
            "lever": 5,
            "patch_type": "update_instruction_section",
            "rca_id": "rca_q012_store_count",
            "target_qids": ["q012"],
        },
    ]

    rows = patch_cap_decision_rows(
        run_id="run_1",
        iteration=2,
        ag_id="AG1",
        decisions=decisions,
    )

    assert [r["gate_name"] for r in rows] == ["patch_cap", "patch_cap"]
    assert [r["decision"] for r in rows] == ["accepted", "dropped"]
    assert rows[0]["proposal_ids"] == ["P015"]
    assert rows[0]["metrics"]["selection_reason"] == "highest_causal_relevance"
    assert rows[1]["reason_code"] == "lower_causal_rank"


def test_patch_cap_decision_records_use_phase_b_contract() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
        patch_cap_decision_records,
    )

    records = patch_cap_decision_records(
        run_id="run_1",
        iteration=2,
        ag_id="AG1",
        decisions=[
            {
                "proposal_id": "P015",
                "decision": "selected",
                "selection_reason": "highest_causal_relevance",
                "rank": 1,
                "relevance_score": 1.0,
                "lever": 5,
                "patch_type": "update_instruction_section",
                "rca_id": "rca_q028_function_routing",
                "target_qids": ["q028"],
            },
            {
                "proposal_id": "P001",
                "decision": "dropped",
                "selection_reason": "lower_causal_rank",
                "rank": 2,
                "relevance_score": 0.55,
                "lever": 5,
                "patch_type": "update_instruction_section",
                "rca_id": "rca_q012_store_count",
                "target_qids": ["q012"],
            },
        ],
    )

    assert [r.decision_type for r in records] == [
        DecisionType.GATE_DECISION,
        DecisionType.GATE_DECISION,
    ]
    assert [r.outcome for r in records] == [
        DecisionOutcome.ACCEPTED,
        DecisionOutcome.DROPPED,
    ]
    assert records[0].reason_code == ReasonCode.PATCH_CAP_SELECTED
    assert records[1].reason_code == ReasonCode.PATCH_CAP_DROPPED
    assert records[0].affected_qids == ("q028",)
    assert records[1].proposal_id == "P001"
    assert records[1].gate == "patch_cap"
