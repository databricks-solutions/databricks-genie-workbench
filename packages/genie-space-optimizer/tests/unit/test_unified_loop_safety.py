from __future__ import annotations

from genie_space_optimizer.optimization.unified_loop import (
    _ALLOWED_PATCH_TYPES,
    _normalize_llm_patches,
    _preapply_safety_screen,
)


def _config() -> dict:
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.orders",
                    "column_configs": [
                        {"column_name": "amount", "data_type": "DOUBLE"},
                        {"column_name": "region", "data_type": "STRING"},
                    ],
                }
            ]
        },
        "instructions": {},
    }


def _eval_result() -> dict:
    return {
        "rows": [
            {
                "question_id": "q1",
                "assessment": "BAD",
                "question": "What is total revenue by region?",
                "expected_sql": "SELECT region, SUM(amount) FROM orders GROUP BY region",
                "generated_sql": "SELECT region FROM orders",
            }
        ]
    }


def test_benchmark_question_copy_in_instruction_patch_is_dropped() -> None:
    patches = [
        {
            "type": "add_instruction",
            "new_text": "When asked: What is total revenue by region? use the region column.",
        },
        {
            "type": "update_column_description",
            "table": "cat.sch.orders",
            "column": "region",
            "new_text": "Sales territory.",
        },
    ]

    kept, dropped = _preapply_safety_screen(
        patches,
        current_config=_config(),
        benchmarks=[
            {
                "question": "What is total revenue by region?",
                "expected_sql": "SELECT region, SUM(amount) FROM orders GROUP BY region",
            }
        ],
        eval_result=_eval_result(),
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert [p["type"] for p in kept] == ["update_column_description"]
    assert dropped[0]["drop_reason"] == "benchmark_prose_leak"


def test_model_supplied_validation_passed_is_ignored_for_sql_snippet(monkeypatch) -> None:
    def fail_validation(*args, **kwargs):
        return False, "warehouse rejected snippet", args[0]

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.benchmarks.validate_sql_snippet",
        fail_validation,
    )

    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "add_sql_snippet_filter",
                "sql": "region = 'West'",
                "display_name": "West region",
                "instruction": "Use for West region filtering.",
                "synonyms": ["west"],
                "target_table": "cat.sch.orders",
                "snippet_type": "filter",
                "validation_passed": True,
            }
        ],
        current_config=_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert kept == []
    assert dropped[0]["drop_reason"] == "snippet_validation_failed"
    assert dropped[0]["drop_detail"] == "warehouse rejected snippet"


def test_valid_sql_snippet_is_validated_stamped_and_materialized(monkeypatch) -> None:
    def pass_validation(sql, snippet_type, metadata_snapshot, **kwargs):
        return True, "", "orders.region = 'West'"

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.benchmarks.validate_sql_snippet",
        pass_validation,
    )

    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "add_sql_snippet_filter",
                "sql": "region = 'West'",
                "display_name": "West region",
                "instruction": "Use for West region filtering.",
                "synonyms": ["west"],
                "target_table": "cat.sch.orders",
                "snippet_type": "filter",
                "validation_passed": False,
            }
        ],
        current_config=_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert dropped == []
    assert kept[0]["validation_passed"] is True
    assert kept[0]["sql"] == "orders.region = 'West'"
    assert kept[0]["target"] == "cat.sch.orders"
    assert kept[0]["sql_snippet"]["sql"] == ["orders.region = 'West'"]
    assert kept[0]["sql_snippet"]["instruction"] == ["Use for West region filtering."]


def test_invalid_sql_snippet_is_dropped_without_dropping_other_patch(monkeypatch) -> None:
    def fail_validation(*args, **kwargs):
        return False, "bad filter", args[0]

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.benchmarks.validate_sql_snippet",
        fail_validation,
    )

    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "add_sql_snippet_filter",
                "sql": "1=1",
                "display_name": "Everything",
                "instruction": "Bad filter.",
                "synonyms": [],
                "target_table": "cat.sch.orders",
                "snippet_type": "filter",
            },
            {
                "type": "update_column_description",
                "table": "cat.sch.orders",
                "column": "amount",
                "new_text": "Order amount.",
            },
        ],
        current_config=_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert [p["type"] for p in kept] == ["update_column_description"]
    assert dropped[0]["drop_reason"] == "snippet_validation_failed"


def test_add_example_sql_is_allowed_and_canonicalized_to_instruction_lever() -> None:
    assert "add_example_sql" in _ALLOWED_PATCH_TYPES

    proposal_lever, rationale, patches = _normalize_llm_patches(
        {
            "lever": 3,
            "rationale": "choose structured patches",
            "patches": [
                {
                    "type": "add_instruction",
                    "new_text": "Ask for a time range when customer performance is ambiguous.",
                    "lever": 3,
                },
                {
                    "type": "add_sql_snippet_filter",
                    "sql": "region = 'West'",
                    "display_name": "West region",
                    "instruction": "Use for West region filtering.",
                    "synonyms": ["west"],
                    "target_table": "cat.sch.orders",
                    "snippet_type": "filter",
                    "lever": 3,
                },
                {
                    "type": "add_example_sql",
                    "example_question": "How much sales came from each territory last month?",
                    "example_sql": "SELECT region, SUM(amount) FROM orders GROUP BY region",
                    "lever": 3,
                },
            ],
        },
        allowed_levers=[1, 3, 4, 5, 6],
    )

    assert proposal_lever == 3
    assert rationale == "choose structured patches"
    assert [p["lever"] for p in patches] == [5, 6, 5]


def test_add_example_sql_missing_required_provenance_is_dropped() -> None:
    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "add_example_sql",
                "example_question": "How much sales came from each territory last month?",
                "example_sql": "SELECT region, SUM(amount) FROM orders GROUP BY region",
            }
        ],
        current_config=_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert kept == []
    assert dropped[0]["drop_reason"] == "example_sql_contract_failed"
    assert "usage_guidance" in dropped[0]["drop_detail"]


def test_add_example_sql_copying_benchmark_qa_is_dropped() -> None:
    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "add_example_sql",
                "example_question": "What is total revenue by region?",
                "example_sql": "SELECT region, SUM(amount) FROM orders GROUP BY region",
                "usage_guidance": "Use for generalized regional aggregation examples.",
                "source_failure_pattern": "regional aggregation",
                "affected_qids": ["q1"],
                "semantic_delta_from_benchmark": "claimed to be generalized",
                "why_not_benchmark_copy": "claimed not copied",
            }
        ],
        current_config=_config(),
        benchmarks=[
            {
                "id": "q1",
                "question": "What is total revenue by region?",
                "expected_sql": "SELECT region, SUM(amount) FROM orders GROUP BY region",
            }
        ],
        eval_result=_eval_result(),
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert kept == []
    assert dropped[0]["drop_reason"] in {
        "benchmark_prose_leak",
        "benchmark_example_sql_leak",
    }


def test_novel_add_example_sql_with_required_provenance_survives() -> None:
    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "add_example_sql",
                "example_question": "Which territories had the highest sales last month?",
                "example_sql": (
                    "SELECT region, SUM(amount) AS sales_amount "
                    "FROM orders GROUP BY region ORDER BY sales_amount DESC LIMIT 5"
                ),
                "usage_guidance": "Use for top-N aggregation examples by sales territory.",
                "source_failure_pattern": "top-N aggregation over a known dimension",
                "affected_qids": ["q2"],
                "semantic_delta_from_benchmark": (
                    "Uses a top-N ranking example instead of the benchmark's by-region total."
                ),
                "why_not_benchmark_copy": (
                    "The question intent and SQL shape differ from scored benchmark rows."
                ),
            }
        ],
        current_config=_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert dropped == []
    assert [p["type"] for p in kept] == ["add_example_sql"]


def test_instruction_patch_without_rejected_patch_types_is_dropped() -> None:
    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "update_instruction_section",
                "section": "DISAMBIGUATION",
                "new_text": "Ask for a time range when customer performance is ambiguous.",
            }
        ],
        current_config=_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert kept == []
    assert dropped[0]["drop_reason"] == "instruction_fallback_unjustified"


def test_instruction_patch_with_rejected_patch_types_survives() -> None:
    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "update_instruction_section",
                "section": "DISAMBIGUATION",
                "new_text": "Ask for a time range when customer performance is ambiguous.",
                "rejected_patch_types": [
                    {
                        "type": "metadata/synonyms",
                        "reason": "No table or column terminology is ambiguous.",
                    },
                    {
                        "type": "structured_behavior",
                        "reason": "This is a cross-cutting clarification rule.",
                    },
                ],
            }
        ],
        current_config=_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert dropped == []
    assert [p["type"] for p in kept] == ["update_instruction_section"]
