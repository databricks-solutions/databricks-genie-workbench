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
