from genie_space_optimizer.optimization.rca_contract_harness import (
    evaluate_frozen_rca_contract,
)


def test_contract_harness_top_n_collapse_reaches_grounded_patchable_state() -> None:
    rows = [
        {
            "inputs/question_id": "q_topn",
            "inputs/question": "Which zone VPs stores have the highest total CY sales?",
            "outputs/response": "SELECT zone_vp_name, SUM(cy_sales) FROM mv GROUP BY zone_vp_name ORDER BY SUM(cy_sales) DESC LIMIT 1",
            "schema_accuracy/metadata": {
                "failure_type": "plural_top_n_collapse",
                "blame_set": ["zone_vp_name", "cy_sales"],
                "counterfactual_fix": "Plural wording should preserve all grouped zone VPs ordered by CY sales.",
                "rca_kind": "top_n_cardinality_collapse",
                "recommended_levers": [1, 5, 6],
            },
        }
    ]
    source_clusters = [
        {
            "cluster_id": "cluster_topn",
            "question_ids": ["q_topn"],
            "root_cause": "plural_top_n_collapse",
            "asi_blame_set": ["zone_vp_name", "cy_sales"],
            "asi_counterfactual_fixes": [
                "Plural wording should preserve all grouped zone VPs ordered by CY sales."
            ],
        }
    ]
    action_group = {
        "id": "AG_topn",
        "source_cluster_ids": ["cluster_topn"],
        "affected_questions": ["Which zone VPs stores have the highest total CY sales?"],
    }

    result = evaluate_frozen_rca_contract(
        rows=rows,
        source_clusters=source_clusters,
        action_group=action_group,
        post_arbiter_accuracy=75.0,
        iteration_counter=1,
        max_iterations=5,
    )

    assert result["target_qids"] == ["q_topn"]
    assert result["finding_count"] >= 1
    assert result["execution_plan_count"] >= 1
    assert set(result["required_levers"]) >= {1, 5, 6}
    assert result["grounding"]["scoped_row_count"] == 1
    assert result["grounding"]["score"] >= 1.0
    assert result["terminal"]["status"] == "patchable_in_progress"


def test_contract_harness_time_window_failure_forces_lever_6_and_grounds() -> None:
    rows = [
        {
            "inputs/question_id": "q_time",
            "outputs/response": "SELECT zone_vp_name, time_window, SUM(py_sales) FROM mv GROUP BY zone_vp_name, time_window",
            "schema_accuracy/metadata": {
                "failure_type": "time_window_logic_mismatch",
                "blame_set": ["time_window", "py_sales"],
                "counterfactual_fix": "Day and MTD should be pivoted into separate joined columns.",
                "rca_kind": "time_window_logic_mismatch",
                "recommended_levers": [2, 5, 6],
            },
        }
    ]
    source_clusters = [
        {
            "cluster_id": "cluster_time",
            "question_ids": ["q_time"],
            "root_cause": "time_window_pivot",
            "asi_blame_set": ["time_window", "py_sales"],
            "asi_counterfactual_fixes": [
                "Day and MTD should be pivoted into separate joined columns."
            ],
        }
    ]
    action_group = {
        "id": "AG_time",
        "source_cluster_ids": ["cluster_time"],
        "affected_questions": ["Show day vs MTD sales by zone VP"],
    }

    result = evaluate_frozen_rca_contract(
        rows=rows,
        source_clusters=source_clusters,
        action_group=action_group,
        post_arbiter_accuracy=70.0,
        iteration_counter=1,
        max_iterations=5,
    )

    assert result["target_qids"] == ["q_time"]
    assert 6 in result["required_levers"]
    assert result["grounding"]["failure_category"] == "grounded"


def test_contract_harness_budget_exhaustion_returns_terminal_state() -> None:
    result = evaluate_frozen_rca_contract(
        rows=[],
        source_clusters=[],
        action_group={"id": "AG_none", "affected_questions": []},
        post_arbiter_accuracy=80.0,
        iteration_counter=5,
        max_iterations=5,
    )

    assert result["terminal"]["status"] == "exhausted_budget"
    assert result["terminal"]["should_continue"] is False
