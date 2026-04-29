from __future__ import annotations


def test_failure_context_includes_genie_sql_and_judge_feedback_but_not_benchmark_fields() -> None:
    from genie_space_optimizer.optimization.rca_failure_context import (
        failure_context_from_row,
    )

    row = {
        "inputs.question_id": "q_001",
        "inputs.question": "BENCHMARK QUESTION MUST NOT APPEAR",
        "inputs.expected_sql": "SELECT secret_expected_sql FROM benchmark_table",
        "outputs.predictions.sql": (
            "SELECT store_id, SUM(cy_sales) AS sales "
            "FROM cat.sch.mv_sales GROUP BY store_id"
        ),
        "schema_accuracy/metadata": {
            "failure_type": "wrong_grouping",
            "blame_set": ["cat.sch.mv_sales", "zone_vp_name"],
            "counterfactual_fix": "Group by zone_vp_name instead of store_id.",
            "rationale": "The SQL grouped at store grain instead of zone VP grain.",
        },
        "feedback/arbiter/value": "ground_truth_correct",
    }

    ctx = failure_context_from_row(row)

    assert ctx is not None
    payload = ctx.as_prompt_dict()
    rendered = str(payload)
    assert payload["question_id"] == "q_001"
    assert "SUM(cy_sales)" in payload["generated_sql"]
    assert "Group by zone_vp_name" in rendered
    assert "BENCHMARK QUESTION MUST NOT APPEAR" not in rendered
    assert "secret_expected_sql" not in rendered
    assert "expected_sql" not in rendered


def test_failure_contexts_are_indexed_by_qid() -> None:
    from genie_space_optimizer.optimization.rca_failure_context import (
        failure_contexts_by_qid,
    )

    rows = [
        {
            "question_id": "q_one",
            "generated_sql": "SELECT COUNT(*) FROM cat.sch.orders",
            "answer_correctness/metadata": {
                "failure_type": "missing_filter",
                "counterfactual_fix": "Filter to active orders.",
            },
        },
        {
            "question_id": "q_two",
            "outputs/predictions/sql": "SELECT SUM(amount) FROM cat.sch.orders",
            "schema_accuracy/metadata": {
                "failure_type": "wrong_measure",
                "blame_set": ["amount"],
            },
        },
    ]

    indexed = failure_contexts_by_qid(rows)

    assert sorted(indexed) == ["q_one", "q_two"]
    assert indexed["q_one"][0]["root_cause"] == "missing_filter"
    assert indexed["q_two"][0]["generated_sql"].startswith("SELECT SUM")


def test_contexts_for_target_qids_preserves_order_and_dedupes() -> None:
    from genie_space_optimizer.optimization.rca_failure_context import (
        contexts_for_target_qids,
    )

    indexed = {
        "q1": [{"question_id": "q1", "generated_sql": "SELECT 1"}],
        "q2": [{"question_id": "q2", "generated_sql": "SELECT 2"}],
    }

    contexts = contexts_for_target_qids(indexed, ["q2", "q1", "q2"])

    assert [c["question_id"] for c in contexts] == ["q2", "q1"]


def test_normalize_teaching_kit_accepts_example_instruction_synonym_and_snippet() -> None:
    from genie_space_optimizer.optimization.teaching_kit import normalize_teaching_kit

    raw = {
        "kit_summary": "Teach zone VP grain and MTD function routing.",
        "example_sql": {
            "example_question": "Which zone VPs have the highest month-to-date sales?",
            "example_sql": (
                "SELECT zone_vp_name, SUM(cy_sales) AS total_sales "
                "FROM cat.sch.mv_sales GROUP BY zone_vp_name "
                "ORDER BY total_sales DESC LIMIT 10"
            ),
            "usage_guidance": "Use for zone VP aggregation examples.",
        },
        "supporting_changes": [
            {
                "patch_type": "add_instruction",
                "section_name": "QUERY CONSTRUCTION",
                "new_text": "For plural top-N questions, return all ranked rows unless the user asks for one.",
            },
            {
                "patch_type": "add_column_synonym",
                "table": "cat.sch.mv_sales",
                "column": "zone_vp_name",
                "synonyms": ["zone vp", "zone vice president"],
            },
            {
                "patch_type": "add_sql_snippet_filter",
                "snippet_type": "filter",
                "display_name": "Current Month To Date",
                "sql": "business_date >= DATE_TRUNC('MONTH', CURRENT_DATE())",
                "instruction": "Use for current month-to-date filtering.",
                "target_table": "cat.sch.mv_sales",
                "synonyms": ["mtd", "month to date"],
            },
        ],
    }

    kit = normalize_teaching_kit(raw, kit_id="kit_001", target_qids=["q_001"], rca_id="rca_001")

    assert kit.primary["patch_type"] == "add_example_sql"
    assert kit.primary["kit_id"] == "kit_001"
    assert kit.primary["target_qids"] == ["q_001"]
    assert len(kit.supporting) == 3
    assert {p["patch_type"] for p in kit.supporting} == {
        "add_instruction",
        "add_column_synonym",
        "add_sql_snippet_filter",
    }
    assert all(p["kit_id"] == "kit_001" for p in kit.supporting)


def test_normalize_teaching_kit_rejects_unsupported_supporting_patch_type() -> None:
    from genie_space_optimizer.optimization.teaching_kit import normalize_teaching_kit

    raw = {
        "example_sql": {
            "example_question": "Show sales by region",
            "example_sql": "SELECT region, SUM(amount) FROM cat.sch.sales GROUP BY region",
        },
        "supporting_changes": [
            {"patch_type": "rewrite_instruction", "new_text": "Rewrite everything."},
        ],
    }

    kit = normalize_teaching_kit(raw, kit_id="kit_002", target_qids=["q_002"], rca_id="rca_002")

    assert kit.primary["patch_type"] == "add_example_sql"
    assert kit.supporting == []


def test_normalize_teaching_kit_supports_legacy_flat_example_sql_shape() -> None:
    from genie_space_optimizer.optimization.teaching_kit import normalize_teaching_kit

    raw = {
        "example_question": "Show sales by category",
        "example_sql": "SELECT category, SUM(amount) FROM cat.sch.sales GROUP BY category",
        "usage_guidance": "Use for category aggregations.",
    }

    kit = normalize_teaching_kit(raw, kit_id="kit_003", target_qids=["q_003"], rca_id="")

    assert kit.primary["example_question"] == "Show sales by category"
    assert kit.primary["example_sql"].startswith("SELECT category")
    assert kit.supporting == []
