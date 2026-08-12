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


def _profiled_config() -> dict:
    config = _config()
    config["_proposal_data_profile"] = {
        "cat.sch.orders": {
            "row_count": 250,
            "columns": {
                "status": {
                    "cardinality": 2,
                    "distinct_values": ["ACTIVE", "CLOSED"],
                },
                "amount": {"min": "1.0", "max": "500.0"},
            },
        }
    }
    return config


def test_profile_gate_drops_formula_like_unsupported_action_bundle() -> None:
    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "update_column_description",
                "table": "cat.sch.orders",
                "column": "amount",
                "new_text": "Use amount to identify the customer's preferred order.",
            },
            {
                "type": "add_instruction",
                "new_text": "Treat priorityId = 3 as an urgent order.",
                "routing_evidence": [
                    {
                        "type": "structured_behavior",
                        "reason": "This is a cross-cutting filter rule.",
                    }
                ],
            },
            {
                "type": "add_join_spec",
                "join_spec": {
                    "left": {"identifier": "cat.sch.orders"},
                    "right": {"identifier": "cat.sch.customers"},
                    "sql": ["orders.customer_id = customers.customer_id"],
                },
            },
            {
                "type": "add_example_sql",
                "example_question": "Which customers placed the largest orders?",
                "example_sql": (
                    "SELECT customer_id, MAX(amount) FROM orders GROUP BY customer_id"
                ),
                "usage_guidance": "Use for customer-level maximum order analysis.",
                "source_failure_pattern": "customer order aggregation",
                "affected_qids": ["q2"],
                "semantic_delta_from_benchmark": "Uses a different aggregation shape.",
                "why_not_benchmark_copy": "This is a novel generalized example.",
            },
        ],
        current_config=_profiled_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert kept == []
    assert [patch["drop_reason"] for patch in dropped] == [
        "profile_evidence_unsupported",
        "profile_evidence_unsupported",
        "profile_action_group_unsupported",
        "profile_action_group_unsupported",
    ]


def test_profile_gate_keeps_directly_supported_categorical_guidance() -> None:
    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "add_instruction",
                "new_text": (
                    "Filter orders.status with the exact stored values: "
                    "ACTIVE for open orders and CLOSED for closed orders."
                ),
                "routing_evidence": [
                    {
                        "type": "structured_behavior",
                        "reason": "The same coded values apply across order questions.",
                    }
                ],
            },
            {
                "type": "update_column_description",
                "table": "cat.sch.orders",
                "column": "status",
                "new_text": "Stored values are ACTIVE and CLOSED.",
            },
        ],
        current_config=_profiled_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert dropped == []
    assert [patch["type"] for patch in kept] == [
        "add_instruction",
        "update_column_description",
    ]


def test_profile_gate_rejects_substring_and_inexact_range_evidence() -> None:
    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "update_column_description",
                "table": "cat.sch.orders",
                "column": "status",
                "new_text": "Use INACTIVE for disabled orders.",
            },
            {
                "type": "update_column_description",
                "table": "cat.sch.orders",
                "column": "amount",
                "new_text": "Observed amount range is 10.0 to 5000.0.",
            },
        ],
        current_config=_profiled_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert kept == []
    assert [patch["drop_reason"] for patch in dropped] == [
        "profile_evidence_unsupported",
        "profile_evidence_unsupported",
    ]


def test_profile_gate_checks_filter_sql_not_explanatory_text() -> None:
    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "add_sql_snippet_filter",
                "sql": "orders.status = 'INACTIVE'",
                "instruction": "ACTIVE is an observed status value.",
                "display_name": "Disabled orders",
                "synonyms": ["disabled"],
                "target_table": "cat.sch.orders",
                "snippet_type": "filter",
            }
        ],
        current_config=_profiled_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert kept == []
    assert dropped[0]["drop_reason"] == "profile_evidence_unsupported"


def test_profile_gate_rejects_filter_literal_with_wrong_case(monkeypatch) -> None:
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.benchmarks.validate_sql_snippet",
        lambda sql, *_args, **_kwargs: (True, "", sql),
    )
    kept, dropped = _preapply_safety_screen(
        [{
            "type": "add_sql_snippet_filter",
            "sql": "orders.status = 'active'",
            "instruction": "Filter to active orders.",
            "display_name": "Active orders",
            "synonyms": ["active"],
            "target_table": "cat.sch.orders",
            "snippet_type": "filter",
        }],
        current_config=_profiled_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert kept == []
    assert dropped[0]["drop_reason"] == "profile_evidence_unsupported"


def test_profile_gate_rejects_filter_with_any_unobserved_literal(monkeypatch) -> None:
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.benchmarks.validate_sql_snippet",
        lambda sql, *_args, **_kwargs: (True, "", sql),
    )
    kept, dropped = _preapply_safety_screen(
        [{
            "type": "add_sql_snippet_filter",
            "sql": "orders.status IN ('ACTIVE', 'BOGUS')",
            "instruction": "Filter to selected order statuses.",
            "display_name": "Selected statuses",
            "synonyms": ["selected statuses"],
            "target_table": "cat.sch.orders",
            "snippet_type": "filter",
        }],
        current_config=_profiled_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert kept == []
    assert dropped[0]["drop_reason"] == "profile_evidence_unsupported"


def test_profile_gate_keeps_filter_when_every_literal_is_observed(monkeypatch) -> None:
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.benchmarks.validate_sql_snippet",
        lambda sql, *_args, **_kwargs: (True, "", sql),
    )
    kept, dropped = _preapply_safety_screen(
        [{
            "type": "add_sql_snippet_filter",
            "sql": "orders.status IN ('ACTIVE', 'CLOSED')",
            "instruction": "Filter to active or closed orders.",
            "display_name": "Active or closed orders",
            "synonyms": ["active or closed"],
            "target_table": "cat.sch.orders",
            "snippet_type": "filter",
        }],
        current_config=_profiled_config(),
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert dropped == []
    assert kept[0]["sql"] == "orders.status IN ('ACTIVE', 'CLOSED')"


def test_profile_gate_requires_symbolic_values_to_be_quoted() -> None:
    config = _profiled_config()
    config["_proposal_data_profile"]["cat.sch.orders"]["columns"]["status"] = {
        "cardinality": 1,
        "distinct_values": ["="],
    }

    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "update_column_description",
                "table": "cat.sch.orders",
                "column": "status",
                "new_text": "Use status = ACTIVE for active orders.",
            },
            {
                "type": "update_column_description",
                "table": "cat.sch.orders",
                "column": "status",
                "new_text": "The observed stored status is `=`.",
            },
        ],
        current_config=config,
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert [patch["new_text"] for patch in kept] == [
        "The observed stored status is `=`."
    ]
    assert dropped[0]["drop_reason"] == "profile_evidence_unsupported"


def test_profile_gate_keeps_validated_toxicology_symbol_mapping() -> None:
    config = _profiled_config()
    config["_proposal_data_profile"] = {
        "cat.sch.bond": {
            "row_count": 100,
            "columns": {
                "bond_type": {
                    "cardinality": 3,
                    "distinct_values": ["-", "=", "#"],
                }
            },
        }
    }
    instruction = (
        "bond.bond_type stores symbolic codes: single bond is '-', "
        "double bond is '=', and triple bond is '#'."
    )

    kept, dropped = _preapply_safety_screen(
        [{
            "type": "add_instruction",
            "new_text": instruction,
            "routing_evidence": [{
                "type": "structured_behavior",
                "reason": "The same stored codes apply across bond questions.",
            }],
        }],
        current_config=config,
        benchmarks=[],
        eval_result={"rows": []},
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert dropped == []
    assert kept[0]["new_text"] == instruction


def test_profile_gate_is_inactive_without_proposal_profile() -> None:
    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "add_join_spec",
                "join_spec": {
                    "left": {"identifier": "cat.sch.orders"},
                    "right": {"identifier": "cat.sch.customers"},
                    "sql": ["orders.customer_id = customers.customer_id"],
                },
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
    assert [patch["type"] for patch in kept] == ["add_join_spec"]


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


def test_measure_and_expression_snippets_do_not_synthesize_alias(monkeypatch) -> None:
    def pass_validation(sql, snippet_type, metadata_snapshot, **kwargs):
        return True, "", sql

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.benchmarks.validate_sql_snippet",
        pass_validation,
    )

    for snippet_type, patch_type, sql in (
        ("measure", "add_sql_snippet_measure", "SUM(orders.amount)"),
        ("expression", "add_sql_snippet_expression", "YEAR(orders.order_date)"),
    ):
        kept, dropped = _preapply_safety_screen(
            [{
                "type": patch_type,
                "sql": sql,
                "display_name": f"Test {snippet_type}",
                "instruction": f"Use this {snippet_type} when applicable.",
                "synonyms": [],
                "target_table": "cat.sch.orders",
                "snippet_type": snippet_type,
            }],
            current_config=_config(),
            benchmarks=[],
            eval_result={"rows": []},
            spark=None,
            catalog="cat",
            schema="sch",
            w=None,
        )

        assert dropped == []
        assert "alias" not in kept[0]["sql_snippet"]


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


def test_add_example_sql_missing_question_or_sql_is_dropped() -> None:
    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "add_example_sql",
                "example_question": "How much sales came from each territory last month?",
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
    assert "example_sql" in dropped[0]["drop_detail"]


def test_add_example_sql_missing_provenance_is_repaired() -> None:
    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "add_example_sql",
                "example_question": "How much sales came from each territory last month?",
                "example_sql": (
                    "SELECT region, SUM(amount) AS sales_amount "
                    "FROM orders GROUP BY region ORDER BY sales_amount DESC LIMIT 5"
                ),
            }
        ],
        current_config=_config(),
        benchmarks=[],
        eval_result=_eval_result(),
        spark=None,
        catalog="cat",
        schema="sch",
        w=None,
    )

    assert dropped == []
    assert kept[0]["provenance_repaired"] is True
    assert kept[0]["affected_qids"] == ["q1"]
    assert "usage_guidance" in kept[0]


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


def test_instruction_patch_without_routing_evidence_is_dropped() -> None:
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
    assert dropped[0]["drop_reason"] == "instruction_routing_unjustified"


def test_instruction_patch_with_routing_evidence_survives() -> None:
    kept, dropped = _preapply_safety_screen(
        [
            {
                "type": "update_instruction_section",
                "section": "DISAMBIGUATION",
                "new_text": "Ask for a time range when customer performance is ambiguous.",
                "routing_evidence": [
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
    assert kept[0]["routing_evidence"][0]["type"] == "metadata/synonyms"
