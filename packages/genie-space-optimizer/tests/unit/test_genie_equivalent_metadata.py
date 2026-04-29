from genie_space_optimizer.optimization.genie_eval_taxonomy import (
    with_genie_equivalent_eval,
)
from genie_space_optimizer.optimization.scorers.asset_routing import (
    asset_routing_scorer,
)
from genie_space_optimizer.optimization.scorers.repeatability import (
    repeatability_scorer,
)
from genie_space_optimizer.optimization.scorers.result_correctness import (
    result_correctness_scorer,
)


def test_asset_routing_failure_adds_genie_equivalent_metadata() -> None:
    feedback = asset_routing_scorer(
        inputs={"question_id": "q1"},
        outputs={"response": "SELECT * FROM sales", "comparison": {"match": False}},
        expectations={
            "expected_asset": "TVF",
            "expected_response": "SELECT * FROM TABLE(fn_sales())",
        },
    )

    assert feedback.value == "no"
    assert feedback.metadata["failure_type"] == "asset_routing_error"
    assert (
        feedback.metadata["genie_equivalent_eval"]["primary_assessment_reason"]
        == "LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE"
    )


def test_result_correctness_failure_adds_deterministic_genie_metadata() -> None:
    feedback = result_correctness_scorer(
        inputs={"question_id": "q1"},
        outputs={
            "response": "SELECT country, SUM(revenue) FROM sales GROUP BY country",
            "comparison": {
                "match": False,
                "gt_rows": 5,
                "genie_rows": 3,
                "gt_columns": ["country", "revenue"],
                "genie_columns": ["country", "revenue"],
                "gt_hash": "aaaa",
                "genie_hash": "bbbb",
                "error": None,
            },
        },
        expectations={},
    )

    assert feedback.value == "no"
    assert feedback.metadata["failure_type"] == "wrong_aggregation"
    assert (
        feedback.metadata["genie_equivalent_eval"]["primary_assessment_reason"]
        == "RESULT_MISSING_ROWS"
    )
    assert "Genie equivalent eval: BAD / RESULT_MISSING_ROWS" in feedback.rationale


def test_repeatability_failure_maps_to_llm_judge_other() -> None:
    feedback = repeatability_scorer(
        inputs={"question_id": "q1"},
        outputs={"response": "SELECT * FROM sales_v2", "comparison": {}},
        expectations={"previous_sql": "SELECT * FROM sales_v1"},
    )

    assert feedback.value == "no"
    assert feedback.metadata["failure_type"] == "repeatability_issue"
    assert (
        feedback.metadata["genie_equivalent_eval"]["primary_assessment_reason"]
        == "LLM_JUDGE_OTHER"
    )


def test_llm_judge_metadata_preserves_existing_failure_type() -> None:
    metadata = with_genie_equivalent_eval(
        {
            "failure_type": "wrong_filter",
            "severity": "major",
            "confidence": 0.95,
            "wrong_clause": "WHERE order_date >= '2024-01-01'",
            "blame_set": ["order_date"],
            "counterfactual_fix": "Clarify the required date filter.",
            "rca_kind": "time_window_logic_mismatch",
            "patch_family": "time_window_logic_guidance",
            "recommended_levers": [2, 5, 6],
        },
        judge_name="logical_accuracy",
        value="no",
    )

    assert metadata["failure_type"] == "wrong_filter"
    assert metadata["rca_kind"] == "time_window_logic_mismatch"
    assert metadata["patch_family"] == "time_window_logic_guidance"
    assert metadata["recommended_levers"] == [2, 5, 6]
    assert (
        metadata["genie_equivalent_eval"]["primary_assessment_reason"]
        == "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER"
    )


def test_instruction_compliance_signal_maps_without_new_judge() -> None:
    metadata = with_genie_equivalent_eval(
        {
            "failure_type": "missing_instruction",
            "severity": "major",
            "confidence": 0.9,
        },
        judge_name="logical_accuracy",
        value="no",
    )

    assert (
        metadata["genie_equivalent_eval"]["primary_assessment_reason"]
        == "LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC"
    )


def test_misinterpretation_signal_maps_without_new_judge() -> None:
    metadata = with_genie_equivalent_eval(
        {
            "failure_type": "different_scope",
            "severity": "major",
            "confidence": 0.9,
        },
        judge_name="semantic_equivalence",
        value="no",
    )

    assert (
        metadata["genie_equivalent_eval"]["primary_assessment_reason"]
        == "LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST"
    )
