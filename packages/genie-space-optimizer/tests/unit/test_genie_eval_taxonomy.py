from genie_space_optimizer.optimization.genie_eval_taxonomy import (
    build_genie_equivalent_eval,
    format_genie_eval_summary,
)


def test_logical_filter_maps_to_genie_filter_reason() -> None:
    result = build_genie_equivalent_eval(
        judge_name="logical_accuracy",
        value="no",
        failure_type="wrong_filter",
        confidence=0.95,
    )

    assert result["assessment"] == "BAD"
    assert result["primary_assessment_reason"] == "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER"
    assert result["assessment_reasons"] == ["LLM_JUDGE_MISSING_OR_INCORRECT_FILTER"]
    assert result["reason_family"] == "llm_judge"
    assert result["mapped_from"] == {
        "judge": "logical_accuracy",
        "failure_type": "wrong_filter",
    }
    assert result["mapping_version"] == "gso_genie_eval_taxonomy_v1"


def test_semantic_metric_maps_to_metric_calculation() -> None:
    result = build_genie_equivalent_eval(
        judge_name="semantic_equivalence",
        value="no",
        failure_type="different_metric",
        confidence=0.9,
    )

    assert result["assessment"] == "BAD"
    assert result["primary_assessment_reason"] == "LLM_JUDGE_INCORRECT_METRIC_CALCULATION"


def test_repeatability_maps_to_other() -> None:
    result = build_genie_equivalent_eval(
        judge_name="repeatability",
        value="no",
        failure_type="repeatability_issue",
        confidence=0.85,
    )

    assert result["assessment"] == "BAD"
    assert result["primary_assessment_reason"] == "LLM_JUDGE_OTHER"
    assert result["mapped_from"]["failure_type"] == "repeatability_issue"


def test_unknown_low_confidence_maps_to_needs_review() -> None:
    result = build_genie_equivalent_eval(
        judge_name="logical_accuracy",
        value="no",
        failure_type="brand_new_failure",
        confidence=0.2,
    )

    assert result["assessment"] == "NEEDS_REVIEW"
    assert result["primary_assessment_reason"] == "LLM_JUDGE_OTHER"
    assert result["mapping_confidence"] == 0.2
    assert result["unmapped"] is True


def test_pass_maps_to_good_with_no_reasons() -> None:
    result = build_genie_equivalent_eval(
        judge_name="schema_accuracy",
        value="yes",
        failure_type="",
        confidence=1.0,
    )

    assert result["assessment"] == "GOOD"
    assert result["assessment_reasons"] == []
    assert result["primary_assessment_reason"] is None


def test_summary_line_is_mlflow_friendly() -> None:
    result = build_genie_equivalent_eval(
        judge_name="logical_accuracy",
        value="no",
        failure_type="missing_filter",
        confidence=0.9,
    )

    assert (
        format_genie_eval_summary(result)
        == "Genie equivalent eval: BAD / LLM_JUDGE_MISSING_OR_INCORRECT_FILTER"
    )


def test_result_correctness_missing_rows_maps_to_deterministic_reason() -> None:
    result = build_genie_equivalent_eval(
        judge_name="result_correctness",
        value="no",
        failure_type="wrong_aggregation",
        confidence=0.8,
        comparison={
            "gt_rows": 10,
            "genie_rows": 4,
            "gt_columns": ["country", "revenue"],
            "genie_columns": ["country", "revenue"],
        },
    )

    assert result["assessment"] == "BAD"
    assert result["primary_assessment_reason"] == "RESULT_MISSING_ROWS"
    assert result["reason_family"] == "deterministic"


def test_result_correctness_extra_columns_maps_to_deterministic_reason() -> None:
    result = build_genie_equivalent_eval(
        judge_name="result_correctness",
        value="no",
        failure_type="wrong_aggregation",
        confidence=0.8,
        comparison={
            "gt_rows": 10,
            "genie_rows": 10,
            "gt_columns": ["country", "revenue"],
            "genie_columns": ["country", "region", "revenue"],
        },
    )

    assert result["primary_assessment_reason"] == "RESULT_EXTRA_COLUMNS"


def test_result_correctness_single_cell_maps_to_single_cell_difference() -> None:
    result = build_genie_equivalent_eval(
        judge_name="result_correctness",
        value="no",
        failure_type="wrong_aggregation",
        confidence=0.8,
        comparison={
            "gt_rows": 1,
            "genie_rows": 1,
            "gt_columns": ["total"],
            "genie_columns": ["total"],
        },
    )

    assert result["primary_assessment_reason"] == "SINGLE_CELL_DIFFERENCE"


def test_result_correctness_both_empty_maps_to_good() -> None:
    result = build_genie_equivalent_eval(
        judge_name="result_correctness",
        value="yes",
        failure_type="",
        confidence=1.0,
        comparison={
            "match_type": "both_empty",
            "gt_rows": 0,
            "genie_rows": 0,
            "gt_columns": ["customer_id"],
            "genie_columns": ["customer_id"],
        },
    )

    assert result["assessment"] == "GOOD"
    assert result["assessment_reasons"] == []
    assert result["reason_family"] == "deterministic"


def test_format_asi_markdown_includes_genie_summary_and_json() -> None:
    from genie_space_optimizer.optimization.evaluation import format_asi_markdown

    metadata = {
        "failure_type": "wrong_filter",
        "confidence": 0.9,
        "genie_equivalent_eval": build_genie_equivalent_eval(
            judge_name="logical_accuracy",
            value="no",
            failure_type="wrong_filter",
            confidence=0.9,
        ),
    }

    rendered = format_asi_markdown(
        judge_name="logical_accuracy",
        value="no",
        rationale="Filter condition is missing.",
        metadata=metadata,
    )

    assert "Genie equivalent eval: BAD / LLM_JUDGE_MISSING_OR_INCORRECT_FILTER" in rendered
    assert '"genie_equivalent_eval"' in rendered
    assert '"failure_type": "wrong_filter"' in rendered


def test_result_correctness_column_type_difference_takes_precedence_after_shape() -> None:
    result = build_genie_equivalent_eval(
        judge_name="result_correctness",
        value="no",
        failure_type="wrong_aggregation",
        confidence=0.8,
        comparison={
            "gt_rows": 3,
            "genie_rows": 3,
            "gt_columns": ["customer_id"],
            "genie_columns": ["customer_id"],
            "column_type_difference": True,
            "gt_column_types": {"customer_id": "int64"},
            "genie_column_types": {"customer_id": "object"},
        },
    )

    assert result["primary_assessment_reason"] == "COLUMN_TYPE_DIFFERENCE"


def test_function_usage_gap_maps_from_existing_judges() -> None:
    for judge_name in ("syntax_validity", "schema_accuracy", "logical_accuracy", "asset_routing"):
        result = build_genie_equivalent_eval(
            judge_name=judge_name,
            value="no",
            failure_type="incorrect_function_usage",
            confidence=0.9,
        )
        assert result["primary_assessment_reason"] == "LLM_JUDGE_INCORRECT_FUNCTION_USAGE"


def test_formatting_gap_maps_from_existing_judges() -> None:
    for judge_name in ("logical_accuracy", "response_quality", "arbiter"):
        result = build_genie_equivalent_eval(
            judge_name=judge_name,
            value="no",
            failure_type="formatting_error",
            confidence=0.9,
        )
        assert result["primary_assessment_reason"] == "LLM_JUDGE_FORMATTING_ERROR"


def test_instruction_compliance_gap_maps_from_existing_judges() -> None:
    for judge_name in ("logical_accuracy", "completeness", "arbiter"):
        result = build_genie_equivalent_eval(
            judge_name=judge_name,
            value="no",
            failure_type="missing_instruction",
            confidence=0.9,
        )
        assert (
            result["primary_assessment_reason"]
            == "LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC"
        )


def test_user_request_misinterpretation_gap_maps_from_existing_judges() -> None:
    for judge_name, failure_type in (
        ("semantic_equivalence", "different_scope"),
        ("response_quality", "misleading_summary"),
        ("arbiter", "misinterpreted_request"),
    ):
        result = build_genie_equivalent_eval(
            judge_name=judge_name,
            value="no",
            failure_type=failure_type,
            confidence=0.9,
        )
        assert (
            result["primary_assessment_reason"]
            == "LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST"
        )
