from __future__ import annotations

from genie_space_optimizer.optimization.unified_loop import _preapply_safety_screen


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
