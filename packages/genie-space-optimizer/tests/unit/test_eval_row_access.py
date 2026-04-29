from __future__ import annotations

import json

from genie_space_optimizer.optimization.eval_row_access import (
    extract_failure_surface,
    iter_asi_metadata,
    row_expected_sql,
    row_generated_sql,
    row_qid,
    row_question,
    rows_for_qids,
)


def test_reads_slash_style_mlflow_eval_row() -> None:
    row = {
        "inputs/question_id": "7now_delivery_analytics_space_gs_025",
        "inputs/question": "Which zone VPs stores have the highest total CY sales?",
        "inputs/expected_response": (
            "SELECT zone_vp_name, SUM(cy_sales) AS total_cy_sales "
            "FROM mv_7now_fact_sales GROUP BY zone_vp_name "
            "ORDER BY total_cy_sales DESC"
        ),
        "outputs/response": (
            "SELECT zone_vp_name, total_cy_sales FROM ranked "
            "WHERE rank = 1"
        ),
        "schema_accuracy/metadata": {
            "failure_type": "wrong_column",
            "blame_set": ["RANK()", "rank_filter"],
            "counterfactual_fix": (
                "Remove WHERE rank = 1 and return all zone VPs ordered by "
                "total_cy_sales DESC."
            ),
        },
    }

    assert row_qid(row) == "7now_delivery_analytics_space_gs_025"
    assert row_question(row).startswith("Which zone VPs")
    assert "ORDER BY total_cy_sales DESC" in row_expected_sql(row)
    assert "WHERE rank = 1" in row_generated_sql(row)

    surface = extract_failure_surface(row)
    assert {"zone_vp_name", "total_cy_sales", "rank", "rank_filter"}.issubset(surface)
    assert "highest" in surface


def test_reads_dotted_flat_and_nested_row_shapes() -> None:
    dotted = {
        "inputs.question_id": "q_dot",
        "inputs.question": "Compare day vs MTD by zone",
        "inputs.expected_sql": "SELECT zone, day_sales, mtd_sales FROM pivoted",
        "outputs.predictions.sql": "SELECT zone, time_window, sales FROM long_form",
    }
    nested = {
        "inputs": {
            "question_id": "q_nested",
            "question": "Show region sales",
            "expected_sql": "SELECT region, SUM(sales) FROM mv GROUP BY region",
        },
        "outputs": {
            "predictions": {
                "sql": "SELECT region_name, SUM(sales) FROM mv GROUP BY region_name"
            }
        },
    }

    assert row_qid(dotted) == "q_dot"
    assert "day vs MTD" in row_question(dotted)
    assert "pivoted" in row_expected_sql(dotted)
    assert "long_form" in row_generated_sql(dotted)

    assert row_qid(nested) == "q_nested"
    assert "Show region sales" == row_question(nested)
    assert "GROUP BY region" in row_expected_sql(nested)
    assert "region_name" in row_generated_sql(nested)


def test_reads_request_kwargs_from_dict_and_json_string() -> None:
    row_with_dict_request = {
        "request": {
            "kwargs": {
                "question_id": "q_request",
                "question": "Which stores have highest sales?",
                "expected_sql": "SELECT store, SUM(sales) FROM mv GROUP BY store",
            }
        },
        "response": {"response": "SELECT store FROM ranked WHERE rank = 1"},
    }
    row_with_json_request = {
        "request": json.dumps({
            "kwargs": {
                "question_id": "q_json",
                "question": "Show day and MTD sales",
                "expected_sql": "SELECT day_sales, mtd_sales FROM joined",
            }
        }),
        "response": {"sql": "SELECT time_window, sales FROM grouped"},
    }

    assert row_qid(row_with_dict_request) == "q_request"
    assert row_question(row_with_dict_request) == "Which stores have highest sales?"
    assert "GROUP BY store" in row_expected_sql(row_with_dict_request)
    assert "WHERE rank = 1" in row_generated_sql(row_with_dict_request)

    assert row_qid(row_with_json_request) == "q_json"
    assert row_question(row_with_json_request) == "Show day and MTD sales"
    assert "joined" in row_expected_sql(row_with_json_request)
    assert "grouped" in row_generated_sql(row_with_json_request)


def test_iter_asi_metadata_supports_slash_and_dot_keys_and_skips_non_dict() -> None:
    row = {
        "schema_accuracy/metadata": {"failure_type": "wrong_column"},
        "logical_accuracy.metadata": {"failure_type": "wrong_filter_condition"},
        "response_quality/metadata": "not-a-dict",
    }

    assert list(iter_asi_metadata(row)) == [
        ("schema_accuracy", {"failure_type": "wrong_column"}),
        ("logical_accuracy", {"failure_type": "wrong_filter_condition"}),
    ]


def test_rows_for_qids_uses_canonical_qid_for_slash_rows() -> None:
    rows = [
        {"inputs/question_id": "q1", "inputs/question": "one"},
        {"inputs.question_id": "q2", "inputs.question": "two"},
        {"inputs": {"question_id": "q3", "question": "three"}},
    ]

    assert [row_qid(r) for r in rows_for_qids(rows, ["q3", "q1"])] == ["q3", "q1"]
