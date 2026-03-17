"""Unit tests for genie_space_optimizer.common.genie_schema — config validation."""

from __future__ import annotations

import json
import os
import re

import pytest

from genie_space_optimizer.common.genie_schema import (
    BenchmarkQuestion,
    ColumnConfig,
    ExampleQuestionSql,
    JoinSpec,
    MAX_INSTRUCTION_SLOTS,
    SerializedSpace,
    SqlSnippetExpression,
    SqlSnippetFilter,
    SqlSnippetMeasure,
    SqlSnippets,
    TextInstruction,
    _rewrite_predicate_aliases,
    count_instruction_slots,
    ensure_join_spec_fields,
    generate_genie_id,
    normalize_join_spec_sql,
    validate_serialized_space,
)


# ── Helpers ──────────────────────────────────────────────────────────

_ID_A = "a" * 32
_ID_B = "b" * 32
_ID_C = "c" * 32
_ID_D = "d" * 32
_ID_E = "e" * 32
_ID_F = "f" * 32


def _minimal_valid_config() -> dict:
    """Smallest config that passes lenient validation."""
    return {"data_sources": {"tables": []}}


def _minimal_strict_config() -> dict:
    """Smallest config that passes strict validation."""
    return {
        "version": 2,
        "data_sources": {"tables": []},
    }


def _full_strict_config() -> dict:
    """A comprehensive config that passes strict validation."""
    return {
        "version": 2,
        "config": {
            "sample_questions": [
                {"id": _ID_A, "question": ["How much revenue?"]},
                {"id": _ID_B, "question": ["Show top customers"]},
            ]
        },
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.customers",
                    "description": ["Customer data"],
                    "column_configs": [
                        {"column_name": "customer_id", "description": ["Unique ID"]},
                        {"column_name": "name", "enable_entity_matching": True},
                    ],
                },
                {
                    "identifier": "cat.sch.orders",
                    "column_configs": [
                        {"column_name": "amount"},
                        {"column_name": "order_date", "enable_format_assistance": True},
                    ],
                },
            ],
            "metric_views": [
                {"identifier": "cat.sch.revenue_mv"},
            ],
        },
        "instructions": {
            "text_instructions": [
                {"id": _ID_C, "content": ["Use net revenue always."]}
            ],
            "example_question_sqls": [
                {
                    "id": _ID_D,
                    "question": ["Top 10 stores?"],
                    "sql": ["SELECT * FROM cat.sch.orders LIMIT 10"],
                }
            ],
            "join_specs": [
                {
                    "id": _ID_E,
                    "left": {"identifier": "cat.sch.orders", "alias": "o"},
                    "right": {"identifier": "cat.sch.customers", "alias": "c"},
                    "sql": ["o.customer_id = c.customer_id"],
                }
            ],
            "sql_snippets": {
                "filters": [
                    {
                        "id": _ID_F,
                        "sql": ["amount > 1000"],
                        "display_name": "high value",
                    }
                ],
            },
        },
    }


def _reference_config() -> dict | None:
    """Load the reference config from docs/ if available."""
    path = os.path.join(
        os.path.dirname(__file__), "..", "..", "docs", "genie_space_config.json"
    )
    if not os.path.exists(path):
        return None
    with open(path) as f:
        full = json.load(f)
    return full.get("serialized_space", full)


# ═══════════════════════════════════════════════════════════════════════
# validate_serialized_space — lenient mode (default)
# ═══════════════════════════════════════════════════════════════════════


class TestValidateSerializedSpace:
    def test_minimal_valid(self):
        ok, errors = validate_serialized_space(_minimal_valid_config())
        assert ok is True
        assert errors == []

    def test_reference_config_passes(self):
        ref = _reference_config()
        if ref is None:
            pytest.skip("Reference config not found at docs/genie_space_config.json")
        ok, errors = validate_serialized_space(ref)
        assert ok is True, f"Reference config failed: {errors}"

    def test_not_a_dict(self):
        ok, errors = validate_serialized_space("a string")
        assert ok is False
        assert any("dict" in e.lower() or "str" in e.lower() for e in errors)

    def test_missing_data_sources(self):
        ok, errors = validate_serialized_space({"version": 2})
        assert ok is False
        assert any("data_sources" in e for e in errors)

    def test_extra_unknown_fields_allowed(self):
        config = _minimal_valid_config()
        config["unknown_future_field"] = True
        config["another_field"] = {"nested": "value"}
        ok, errors = validate_serialized_space(config)
        assert ok is True

    def test_empty_data_sources(self):
        ok, errors = validate_serialized_space({"data_sources": {}})
        assert ok is True

    def test_full_structure_with_all_sections(self):
        """Lenient mode accepts short IDs and unsorted arrays."""
        config = {
            "version": 2,
            "config": {
                "sample_questions": [
                    {"id": "q1", "question": ["How much revenue?"]}
                ]
            },
            "data_sources": {
                "tables": [
                    {
                        "identifier": "catalog.schema.table",
                        "column_configs": [
                            {
                                "column_name": "revenue",
                                "description": ["Total revenue"],
                                "synonyms": ["sales"],
                                "enable_format_assistance": True,
                            }
                        ],
                    }
                ],
                "metric_views": [
                    {"identifier": "catalog.schema.mv1", "column_configs": []}
                ],
            },
            "instructions": {
                "text_instructions": [
                    {"id": "ti1", "content": ["Use net revenue."]}
                ],
                "example_question_sqls": [
                    {
                        "id": "eq1",
                        "question": ["Top 10 stores?"],
                        "sql": ["SELECT * FROM t LIMIT 10"],
                        "parameters": [
                            {
                                "name": "limit_val",
                                "type_hint": "INTEGER",
                                "default_value": {"values": ["10"]},
                            }
                        ],
                        "usage_guidance": ["Use for top-N queries"],
                    }
                ],
                "sql_functions": [
                    {"id": "sf1", "identifier": "catalog.schema.get_trend"}
                ],
                "join_specs": [
                    {
                        "id": "js1",
                        "left": {"identifier": "catalog.schema.fact", "alias": "fact"},
                        "right": {"identifier": "catalog.schema.dim", "alias": "dim"},
                        "sql": ["`fact`.`key` = `dim`.`key`"],
                    }
                ],
                "sql_snippets": {
                    "filters": [
                        {"id": "f1", "sql": ["x > 1"], "display_name": "big"}
                    ],
                    "expressions": [
                        {"id": "e1", "alias": "yr", "sql": ["YEAR(d)"], "display_name": "year"}
                    ],
                    "measures": [
                        {"id": "m1", "alias": "rev", "sql": ["SUM(a)"], "display_name": "revenue"}
                    ],
                },
            },
        }
        ok, errors = validate_serialized_space(config)
        assert ok is True, f"Full config failed: {errors}"


# ═══════════════════════════════════════════════════════════════════════
# Column configs
# ═══════════════════════════════════════════════════════════════════════


class TestColumnConfig:
    def test_valid(self):
        cc = ColumnConfig(column_name="revenue")
        assert cc.column_name == "revenue"

    def test_missing_column_name(self):
        with pytest.raises(Exception):
            ColumnConfig.model_validate({"description": ["test"]})

    def test_description_str_coerced_to_list(self):
        cc = ColumnConfig.model_validate(
            {"column_name": "x", "description": "plain string"}
        )
        assert cc.description == ["plain string"]

    def test_synonyms_str_coerced_to_list(self):
        cc = ColumnConfig.model_validate(
            {"column_name": "x", "synonyms": "alias"}
        )
        assert cc.synonyms == ["alias"]

    def test_boolean_flags(self):
        cc = ColumnConfig.model_validate(
            {
                "column_name": "x",
                "enable_format_assistance": True,
                "enable_entity_matching": False,
                "exclude": True,
            }
        )
        assert cc.enable_format_assistance is True
        assert cc.enable_entity_matching is False
        assert cc.exclude is True


# ═══════════════════════════════════════════════════════════════════════
# Join specs
# ═══════════════════════════════════════════════════════════════════════


class TestJoinSpec:
    def test_valid(self):
        js = JoinSpec.model_validate(
            {
                "left": {"identifier": "a.b.c", "alias": "c"},
                "right": {"identifier": "a.b.d", "alias": "d"},
                "sql": ["`c`.`id` = `d`.`id`"],
            }
        )
        assert js.left.identifier == "a.b.c"

    def test_missing_left(self):
        with pytest.raises(Exception):
            JoinSpec.model_validate(
                {
                    "right": {"identifier": "a.b.d", "alias": "d"},
                    "sql": ["`c`.`id` = `d`.`id`"],
                }
            )

    def test_missing_right(self):
        with pytest.raises(Exception):
            JoinSpec.model_validate(
                {
                    "left": {"identifier": "a.b.c", "alias": "c"},
                    "sql": ["`c`.`id` = `d`.`id`"],
                }
            )

    def test_missing_sql(self):
        with pytest.raises(Exception):
            JoinSpec.model_validate(
                {
                    "left": {"identifier": "a.b.c", "alias": "c"},
                    "right": {"identifier": "a.b.d", "alias": "d"},
                }
            )

    def test_sql_str_coerced_to_list(self):
        js = JoinSpec.model_validate(
            {
                "left": {"identifier": "a.b.c", "alias": "c"},
                "right": {"identifier": "a.b.d", "alias": "d"},
                "sql": "c.id = d.id",
            }
        )
        assert js.sql == ["c.id = d.id"]

    def test_id_optional_in_lenient(self):
        js = JoinSpec.model_validate(
            {
                "left": {"identifier": "a.b.c", "alias": "c"},
                "right": {"identifier": "a.b.d", "alias": "d"},
                "sql": ["c.id = d.id"],
            }
        )
        assert js.id is None

    def test_left_missing_alias(self):
        with pytest.raises(Exception):
            JoinSpec.model_validate(
                {
                    "left": {"identifier": "a.b.c"},
                    "right": {"identifier": "a.b.d", "alias": "d"},
                    "sql": ["c.id = d.id"],
                }
            )

    def test_comment_and_instruction(self):
        js = JoinSpec.model_validate(
            {
                "id": _ID_A,
                "left": {"identifier": "a.b.c", "alias": "c"},
                "right": {"identifier": "a.b.d", "alias": "d"},
                "sql": ["c.id = d.id"],
                "comment": "Join on id",
                "instruction": "Use for detail queries",
            }
        )
        assert js.comment == ["Join on id"]
        assert js.instruction == ["Use for detail queries"]


# ═══════════════════════════════════════════════════════════════════════
# Join spec SQL normalisation
# ═══════════════════════════════════════════════════════════════════════


class TestRewritePredicateAliases:
    """Unit tests for _rewrite_predicate_aliases."""

    def test_short_aliases_rewritten(self):
        pred = "`jt`.`workspace_id` = `j`.`workspace_id` AND `jt`.`job_id` = `j`.`job_id`"
        result = _rewrite_predicate_aliases(pred, "dim_job_task", "dim_job")
        assert "`dim_job_task`.`workspace_id`" in result
        assert "`dim_job`.`workspace_id`" in result
        assert "`dim_job_task`.`job_id`" in result
        assert "`dim_job`.`job_id`" in result
        assert "`jt`" not in result
        assert "`j`" not in result

    def test_already_canonical_unchanged(self):
        pred = "`dim_job_task`.`workspace_id` = `dim_job`.`workspace_id`"
        result = _rewrite_predicate_aliases(pred, "dim_job_task", "dim_job")
        assert result == "`dim_job_task`.`workspace_id` = `dim_job`.`workspace_id`"

    def test_no_backticks_added(self):
        pred = "jt.workspace_id = j.workspace_id"
        result = _rewrite_predicate_aliases(pred, "dim_job_task", "dim_job")
        assert "`dim_job_task`.`workspace_id`" in result
        assert "`dim_job`.`workspace_id`" in result

    def test_empty_string_passthrough(self):
        assert _rewrite_predicate_aliases("", "left", "right") == ""

    def test_no_table_refs_passthrough(self):
        assert _rewrite_predicate_aliases("1 = 1", "left", "right") == "1 = 1"

    def test_single_alias_passthrough(self):
        pred = "`t`.`a` = `t`.`b`"
        result = _rewrite_predicate_aliases(pred, "left", "right")
        assert result == pred


class TestNormalizeJoinSpecSql:
    """Unit tests for normalize_join_spec_sql."""

    def test_rewrites_short_aliases(self):
        spec = {
            "left": {"identifier": "cat.sch.dim_job_task", "alias": "dim_job_task"},
            "right": {"identifier": "cat.sch.dim_job", "alias": "dim_job"},
            "sql": [
                "`jt`.`workspace_id` = `j`.`workspace_id` AND `jt`.`job_id` = `j`.`job_id`",
                "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
            ],
        }
        result = normalize_join_spec_sql(spec)
        assert result["sql"][0].startswith("`dim_job_task`.")
        assert "`jt`" not in result["sql"][0]
        assert "`j`" not in result["sql"][0]
        assert "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--" in result["sql"]

    def test_preserves_rt_marker(self):
        spec = {
            "left": {"identifier": "cat.sch.orders", "alias": "orders"},
            "right": {"identifier": "cat.sch.customers", "alias": "customers"},
            "sql": [
                "`orders`.`customer_id` = `customers`.`customer_id`",
                "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
            ],
        }
        result = normalize_join_spec_sql(spec)
        assert result["sql"][1] == "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"

    def test_already_correct_passthrough(self):
        spec = {
            "left": {"identifier": "cat.sch.orders", "alias": "orders"},
            "right": {"identifier": "cat.sch.customers", "alias": "customers"},
            "sql": ["`orders`.`customer_id` = `customers`.`customer_id`"],
        }
        result = normalize_join_spec_sql(spec)
        assert result["sql"] == ["`orders`.`customer_id` = `customers`.`customer_id`"]

    def test_missing_alias_no_crash(self):
        spec = {
            "left": {"identifier": "cat.sch.orders"},
            "right": {"identifier": "cat.sch.customers"},
            "sql": ["`o`.`id` = `c`.`id`"],
        }
        result = normalize_join_spec_sql(spec)
        assert result["sql"] == ["`o`.`id` = `c`.`id`"]

    def test_empty_sql_no_crash(self):
        spec = {
            "left": {"identifier": "cat.sch.orders", "alias": "orders"},
            "right": {"identifier": "cat.sch.customers", "alias": "customers"},
            "sql": [],
        }
        result = normalize_join_spec_sql(spec)
        assert result["sql"] == []


class TestEnsureJoinSpecFieldsNormalization:
    """Verify that ensure_join_spec_fields auto-normalises SQL aliases."""

    def test_derives_alias_and_normalises(self):
        spec = {
            "left": {"identifier": "cat.sch.dim_job_task"},
            "right": {"identifier": "cat.sch.dim_job"},
            "sql": ["`jt`.`id` = `j`.`id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"],
        }
        result = ensure_join_spec_fields(spec)
        assert result["left"]["alias"] == "dim_job_task"
        assert result["right"]["alias"] == "dim_job"
        assert "`dim_job_task`.`id`" in result["sql"][0]
        assert "`dim_job`.`id`" in result["sql"][0]
        assert result["sql"][1] == "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"
        assert result.get("id")


# ═══════════════════════════════════════════════════════════════════════
# Example question SQLs
# ═══════════════════════════════════════════════════════════════════════


class TestExampleQuestionSql:
    def test_valid_minimal(self):
        eq = ExampleQuestionSql.model_validate(
            {"id": "eq1", "question": ["Top stores?"], "sql": ["SELECT * FROM t"]}
        )
        assert eq.id == "eq1"
        assert eq.parameters is None

    def test_missing_id(self):
        with pytest.raises(Exception):
            ExampleQuestionSql.model_validate(
                {"question": ["Q?"], "sql": ["SELECT 1"]}
            )

    def test_missing_question(self):
        with pytest.raises(Exception):
            ExampleQuestionSql.model_validate(
                {"id": "eq1", "sql": ["SELECT 1"]}
            )

    def test_missing_sql(self):
        with pytest.raises(Exception):
            ExampleQuestionSql.model_validate(
                {"id": "eq1", "question": ["Q?"]}
            )

    def test_str_coercion(self):
        eq = ExampleQuestionSql.model_validate(
            {"id": "eq1", "question": "bare string", "sql": "SELECT 1"}
        )
        assert eq.question == ["bare string"]
        assert eq.sql == ["SELECT 1"]

    def test_with_parameters(self):
        eq = ExampleQuestionSql.model_validate(
            {
                "id": "eq1",
                "question": ["Q?"],
                "sql": ["SELECT * WHERE x = :v"],
                "parameters": [
                    {
                        "name": "v",
                        "type_hint": "INTEGER",
                        "default_value": {"values": ["10"]},
                    }
                ],
            }
        )
        assert len(eq.parameters) == 1
        assert eq.parameters[0].name == "v"
        assert eq.parameters[0].default_value.values == ["10"]

    def test_parameter_with_description(self):
        eq = ExampleQuestionSql.model_validate(
            {
                "id": "eq1",
                "question": ["Q?"],
                "sql": ["SELECT * WHERE x = :v"],
                "parameters": [
                    {
                        "name": "v",
                        "description": "The region to filter by",
                    }
                ],
            }
        )
        assert eq.parameters[0].description == ["The region to filter by"]

    def test_usage_guidance_str_coerced(self):
        eq = ExampleQuestionSql.model_validate(
            {
                "id": "eq1",
                "question": ["Q?"],
                "sql": ["SELECT 1"],
                "usage_guidance": "when to use this",
            }
        )
        assert eq.usage_guidance == ["when to use this"]


# ═══════════════════════════════════════════════════════════════════════
# Text instructions
# ═══════════════════════════════════════════════════════════════════════


class TestTextInstruction:
    def test_valid(self):
        ti = TextInstruction.model_validate(
            {"id": "ti1", "content": ["Instruction text"]}
        )
        assert ti.content == ["Instruction text"]

    def test_missing_id(self):
        with pytest.raises(Exception):
            TextInstruction.model_validate({"content": ["text"]})

    def test_missing_content(self):
        with pytest.raises(Exception):
            TextInstruction.model_validate({"id": "ti1"})

    def test_content_str_coerced(self):
        ti = TextInstruction.model_validate(
            {"id": "ti1", "content": "plain string"}
        )
        assert ti.content == ["plain string"]


# ═══════════════════════════════════════════════════════════════════════
# SQL Snippets
# ═══════════════════════════════════════════════════════════════════════


class TestSqlSnippets:
    def test_filter_valid(self):
        f = SqlSnippetFilter.model_validate(
            {"id": "f1", "sql": ["x > 1"], "display_name": "big"}
        )
        assert f.id == "f1"
        assert f.sql == ["x > 1"]

    def test_expression_valid(self):
        e = SqlSnippetExpression.model_validate(
            {"id": "e1", "alias": "yr", "sql": ["YEAR(d)"], "display_name": "year"}
        )
        assert e.alias == "yr"

    def test_measure_valid(self):
        m = SqlSnippetMeasure.model_validate(
            {"id": "m1", "alias": "rev", "sql": ["SUM(a)"], "display_name": "revenue"}
        )
        assert m.alias == "rev"

    def test_filter_missing_sql(self):
        with pytest.raises(Exception):
            SqlSnippetFilter.model_validate({"id": "f1"})

    def test_expression_missing_alias(self):
        with pytest.raises(Exception):
            SqlSnippetExpression.model_validate({"id": "e1", "sql": ["YEAR(d)"]})

    def test_measure_missing_alias(self):
        with pytest.raises(Exception):
            SqlSnippetMeasure.model_validate({"id": "m1", "sql": ["SUM(a)"]})

    def test_snippets_container(self):
        s = SqlSnippets.model_validate(
            {
                "filters": [{"id": "f1", "sql": ["x > 1"]}],
                "expressions": [],
                "measures": [],
            }
        )
        assert len(s.filters) == 1
        assert s.expressions == []

    def test_str_coercion(self):
        f = SqlSnippetFilter.model_validate(
            {"id": "f1", "sql": "x > 1", "synonyms": "big", "comment": "filter large"}
        )
        assert f.sql == ["x > 1"]
        assert f.synonyms == ["big"]
        assert f.comment == ["filter large"]

    def test_in_full_config(self):
        config = {
            "data_sources": {"tables": []},
            "instructions": {
                "sql_snippets": {
                    "filters": [{"id": "f1", "sql": ["x > 1"]}],
                    "expressions": [{"id": "e1", "alias": "yr", "sql": ["YEAR(d)"]}],
                    "measures": [{"id": "m1", "alias": "rev", "sql": ["SUM(a)"]}],
                }
            },
        }
        ok, errors = validate_serialized_space(config)
        assert ok is True, f"Config with sql_snippets failed: {errors}"


# ═══════════════════════════════════════════════════════════════════════
# Benchmark questions
# ═══════════════════════════════════════════════════════════════════════


class TestBenchmarkQuestion:
    def test_valid_with_id(self):
        bq = BenchmarkQuestion.model_validate(
            {
                "id": _ID_A,
                "question": ["What is avg order?"],
                "answer": [{"format": "SQL", "content": ["SELECT AVG(a) FROM t"]}],
            }
        )
        assert bq.id == _ID_A

    def test_id_optional_in_lenient(self):
        bq = BenchmarkQuestion.model_validate(
            {"question": ["What is avg order?"]}
        )
        assert bq.id is None

    def test_question_str_coerced(self):
        bq = BenchmarkQuestion.model_validate(
            {"id": _ID_A, "question": "bare string"}
        )
        assert bq.question == ["bare string"]


# ═══════════════════════════════════════════════════════════════════════
# Type mismatches caught at config level
# ═══════════════════════════════════════════════════════════════════════


class TestTypeMismatches:
    def test_column_config_wrong_column_name_type(self):
        ok, errors = validate_serialized_space(
            {
                "data_sources": {
                    "tables": [
                        {
                            "identifier": "t",
                            "column_configs": [{"column_name": 123}],
                        }
                    ]
                }
            }
        )
        assert ok is False

    def test_table_missing_identifier(self):
        ok, errors = validate_serialized_space(
            {"data_sources": {"tables": [{"column_configs": []}]}}
        )
        assert ok is False

    def test_join_spec_left_not_dict(self):
        ok, errors = validate_serialized_space(
            {
                "data_sources": {"tables": []},
                "instructions": {
                    "join_specs": [
                        {"left": "not_a_dict", "right": {"identifier": "x", "alias": "x"}, "sql": ["a"]}
                    ]
                },
            }
        )
        assert ok is False

    def test_sql_function_missing_identifier(self):
        ok, errors = validate_serialized_space(
            {
                "data_sources": {"tables": []},
                "instructions": {"sql_functions": [{"id": "sf1"}]},
            }
        )
        assert ok is False


# ═══════════════════════════════════════════════════════════════════════
# generate_genie_id
# ═══════════════════════════════════════════════════════════════════════


class TestGenerateGenieId:
    def test_format(self):
        gid = generate_genie_id()
        assert re.fullmatch(r"[0-9a-f]{32}", gid), f"Bad ID: {gid}"

    def test_uniqueness(self):
        ids = {generate_genie_id() for _ in range(100)}
        assert len(ids) == 100

    def test_time_prefix_nondecreasing(self):
        """The first 16 chars (time component) should be non-decreasing."""
        ids = [generate_genie_id() for _ in range(50)]
        prefixes = [gid[:16] for gid in ids]
        assert prefixes == sorted(prefixes)


# ═══════════════════════════════════════════════════════════════════════
# Strict mode — full config passes
# ═══════════════════════════════════════════════════════════════════════


class TestStrictFullConfig:
    def test_minimal_strict_passes(self):
        ok, errors = validate_serialized_space(_minimal_strict_config(), strict=True)
        assert ok is True, f"Minimal strict failed: {errors}"

    def test_full_strict_passes(self):
        ok, errors = validate_serialized_space(_full_strict_config(), strict=True)
        assert ok is True, f"Full strict failed: {errors}"


# ═══════════════════════════════════════════════════════════════════════
# Strict mode — ID format
# ═══════════════════════════════════════════════════════════════════════


class TestStrictIdFormat:
    def test_short_id_rejected(self):
        config = _minimal_strict_config()
        config["config"] = {"sample_questions": [{"id": "q1", "question": ["Q?"]}]}
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("32-char" in e for e in errors)

    def test_uppercase_id_rejected(self):
        config = _minimal_strict_config()
        config["config"] = {
            "sample_questions": [{"id": "A" * 32, "question": ["Q?"]}]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("32-char" in e for e in errors)

    def test_hyphenated_uuid_rejected(self):
        config = _minimal_strict_config()
        config["config"] = {
            "sample_questions": [
                {"id": "a1b2c3d4-e5f6-0000-0000-00000000000a", "question": ["Q?"]}
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False

    def test_valid_hex_id_accepted(self):
        config = _minimal_strict_config()
        config["config"] = {
            "sample_questions": [{"id": _ID_A, "question": ["Q?"]}]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is True, f"Valid hex ID rejected: {errors}"

    def test_instruction_ids_validated(self):
        config = _minimal_strict_config()
        config["instructions"] = {
            "text_instructions": [{"id": "short", "content": ["text"]}]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("text_instructions" in e for e in errors)

    def test_join_spec_missing_id_strict(self):
        config = _minimal_strict_config()
        config["instructions"] = {
            "join_specs": [
                {
                    "left": {"identifier": "a.b.c", "alias": "c"},
                    "right": {"identifier": "a.b.d", "alias": "d"},
                    "sql": ["c.id = d.id"],
                }
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("join_specs" in e and "required" in e for e in errors)

    def test_snippet_ids_validated(self):
        config = _minimal_strict_config()
        config["instructions"] = {
            "sql_snippets": {
                "filters": [{"id": "bad", "sql": ["x > 1"]}]
            }
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("sql_snippets.filters" in e for e in errors)


# ═══════════════════════════════════════════════════════════════════════
# Strict mode — sorting
# ═══════════════════════════════════════════════════════════════════════


class TestStrictSorting:
    def test_unsorted_tables_rejected(self):
        config = _minimal_strict_config()
        config["data_sources"] = {
            "tables": [
                {"identifier": "cat.sch.z_table"},
                {"identifier": "cat.sch.a_table"},
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("not sorted" in e for e in errors)

    def test_sorted_tables_accepted(self):
        config = _minimal_strict_config()
        config["data_sources"] = {
            "tables": [
                {"identifier": "cat.sch.a_table"},
                {"identifier": "cat.sch.z_table"},
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is True, f"Sorted tables rejected: {errors}"

    def test_unsorted_column_configs_rejected(self):
        config = _minimal_strict_config()
        config["data_sources"] = {
            "tables": [
                {
                    "identifier": "cat.sch.t",
                    "column_configs": [
                        {"column_name": "z_col"},
                        {"column_name": "a_col"},
                    ],
                }
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("column_configs" in e and "not sorted" in e for e in errors)

    def test_unsorted_sample_questions_rejected(self):
        config = _minimal_strict_config()
        config["config"] = {
            "sample_questions": [
                {"id": _ID_B, "question": ["Q2"]},
                {"id": _ID_A, "question": ["Q1"]},
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("sample_questions" in e and "not sorted" in e for e in errors)

    def test_unsorted_benchmarks_rejected(self):
        config = _minimal_strict_config()
        config["benchmarks"] = {
            "questions": [
                {
                    "id": _ID_B,
                    "question": ["Q2"],
                    "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
                },
                {
                    "id": _ID_A,
                    "question": ["Q1"],
                    "answer": [{"format": "SQL", "content": ["SELECT 2"]}],
                },
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("benchmarks.questions" in e and "not sorted" in e for e in errors)


# ═══════════════════════════════════════════════════════════════════════
# Strict mode — uniqueness
# ═══════════════════════════════════════════════════════════════════════


class TestStrictUniqueness:
    def test_duplicate_question_ids_rejected(self):
        config = _minimal_strict_config()
        config["config"] = {
            "sample_questions": [
                {"id": _ID_A, "question": ["Q1"]},
            ]
        }
        config["benchmarks"] = {
            "questions": [
                {
                    "id": _ID_A,
                    "question": ["Q2"],
                    "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
                }
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("Duplicate question ID" in e for e in errors)

    def test_duplicate_instruction_ids_rejected(self):
        config = _minimal_strict_config()
        config["instructions"] = {
            "text_instructions": [{"id": _ID_A, "content": ["text"]}],
            "example_question_sqls": [
                {"id": _ID_A, "question": ["Q?"], "sql": ["SELECT 1"]}
            ],
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("Duplicate instruction ID" in e for e in errors)

    def test_duplicate_column_config_rejected(self):
        config = _minimal_strict_config()
        config["data_sources"] = {
            "tables": [
                {
                    "identifier": "cat.sch.t",
                    "column_configs": [
                        {"column_name": "col1"},
                        {"column_name": "col1"},
                    ],
                }
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("Duplicate column config" in e for e in errors)


# ═══════════════════════════════════════════════════════════════════════
# Strict mode — size and length limits
# ═══════════════════════════════════════════════════════════════════════


class TestStrictSizeLimits:
    def test_string_exceeding_limit_rejected(self):
        config = _minimal_strict_config()
        config["instructions"] = {
            "text_instructions": [
                {"id": _ID_A, "content": ["x" * 25_001]}
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("string length" in e for e in errors)

    def test_multiple_text_instructions_rejected(self):
        config = _minimal_strict_config()
        config["instructions"] = {
            "text_instructions": [
                {"id": _ID_A, "content": ["Instruction 1"]},
                {"id": _ID_B, "content": ["Instruction 2"]},
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("at most 1" in e for e in errors)

    def test_single_text_instruction_accepted(self):
        config = _minimal_strict_config()
        config["instructions"] = {
            "text_instructions": [
                {"id": _ID_A, "content": ["Just one"]}
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is True, f"Single text instruction rejected: {errors}"


# ═══════════════════════════════════════════════════════════════════════
# Strict mode — table identifier format
# ═══════════════════════════════════════════════════════════════════════


class TestStrictTableIdentifiers:
    def test_two_level_rejected(self):
        config = _minimal_strict_config()
        config["data_sources"] = {"tables": [{"identifier": "schema.table"}]}
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("three-level" in e for e in errors)

    def test_one_level_rejected(self):
        config = _minimal_strict_config()
        config["data_sources"] = {"tables": [{"identifier": "just_table"}]}
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False

    def test_three_level_accepted(self):
        config = _minimal_strict_config()
        config["data_sources"] = {"tables": [{"identifier": "cat.sch.tbl"}]}
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is True, f"Three-level identifier rejected: {errors}"

    def test_backtick_three_level_accepted(self):
        config = _minimal_strict_config()
        config["data_sources"] = {"tables": [{"identifier": "`cat`.`sch`.`tbl`"}]}
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is True, f"Backtick three-level rejected: {errors}"


# ═══════════════════════════════════════════════════════════════════════
# Strict mode — benchmark answers
# ═══════════════════════════════════════════════════════════════════════


class TestStrictBenchmarkAnswers:
    def test_missing_answer_rejected(self):
        config = _minimal_strict_config()
        config["benchmarks"] = {
            "questions": [
                {"id": _ID_A, "question": ["Q?"]}
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("exactly 1 answer" in e for e in errors)

    def test_wrong_format_rejected(self):
        config = _minimal_strict_config()
        config["benchmarks"] = {
            "questions": [
                {
                    "id": _ID_A,
                    "question": ["Q?"],
                    "answer": [{"format": "TEXT", "content": ["some text"]}],
                }
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("format must be 'SQL'" in e for e in errors)

    def test_valid_benchmark_accepted(self):
        config = _minimal_strict_config()
        config["benchmarks"] = {
            "questions": [
                {
                    "id": _ID_A,
                    "question": ["Q?"],
                    "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
                }
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is True, f"Valid benchmark rejected: {errors}"

    def test_benchmark_missing_id_rejected(self):
        config = _minimal_strict_config()
        config["benchmarks"] = {
            "questions": [
                {
                    "question": ["Q?"],
                    "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
                }
            ]
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("id is required" in e for e in errors)


# ═══════════════════════════════════════════════════════════════════════
# Strict mode — version
# ═══════════════════════════════════════════════════════════════════════


class TestStrictVersion:
    def test_missing_version_rejected(self):
        ok, errors = validate_serialized_space(
            {"data_sources": {"tables": []}}, strict=True
        )
        assert ok is False
        assert any("version" in e for e in errors)

    def test_invalid_version_rejected(self):
        ok, errors = validate_serialized_space(
            {"version": 99, "data_sources": {"tables": []}}, strict=True
        )
        assert ok is False
        assert any("version" in e and "1 or 2" in e for e in errors)

    def test_version_1_accepted(self):
        ok, errors = validate_serialized_space(
            {"version": 1, "data_sources": {"tables": []}}, strict=True
        )
        assert ok is True

    def test_version_2_accepted(self):
        ok, errors = validate_serialized_space(
            {"version": 2, "data_sources": {"tables": []}}, strict=True
        )
        assert ok is True


# ═══════════════════════════════════════════════════════════════════════
# Strict mode — sql_snippets sql must not be empty
# ═══════════════════════════════════════════════════════════════════════


class TestStrictSnippetSqlNotEmpty:
    def test_empty_filter_sql_rejected(self):
        config = _minimal_strict_config()
        config["instructions"] = {
            "sql_snippets": {
                "filters": [{"id": _ID_A, "sql": []}]
            }
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("sql must not be empty" in e for e in errors)

    def test_blank_filter_sql_rejected(self):
        config = _minimal_strict_config()
        config["instructions"] = {
            "sql_snippets": {
                "filters": [{"id": _ID_A, "sql": [""]}]
            }
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("sql must not be empty" in e for e in errors)

    def test_valid_filter_sql_accepted(self):
        config = _minimal_strict_config()
        config["instructions"] = {
            "sql_snippets": {
                "filters": [{"id": _ID_A, "sql": ["x > 1"]}]
            }
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is True, f"Valid snippet SQL rejected: {errors}"


# ═══════════════════════════════════════════════════════════════════════
# count_instruction_slots
# ═══════════════════════════════════════════════════════════════════════


class TestCountInstructionSlots:
    def test_empty_config(self):
        assert count_instruction_slots({}) == 0

    def test_instructions_only(self):
        config = {
            "instructions": {
                "text_instructions": [{"id": "ti1", "content": ["text"]}],
                "example_question_sqls": [
                    {"id": "eq1", "question": ["Q1?"], "sql": ["SELECT 1"]},
                    {"id": "eq2", "question": ["Q2?"], "sql": ["SELECT 2"]},
                ],
                "sql_functions": [
                    {"id": "sf1", "identifier": "cat.sch.fn1"},
                ],
            },
        }
        assert count_instruction_slots(config) == 4  # 1 text + 2 examples + 1 function

    def test_table_descriptions_counted(self):
        config = {
            "data_sources": {
                "tables": [
                    {"identifier": "cat.sch.t1", "description": ["Table one"]},
                    {"identifier": "cat.sch.t2", "description": ["Table two"]},
                    {"identifier": "cat.sch.t3", "description": ["Table three"]},
                ],
            },
        }
        assert count_instruction_slots(config) == 3

    def test_metric_view_descriptions_counted(self):
        config = {
            "data_sources": {
                "metric_views": [
                    {"identifier": "cat.sch.mv1", "description": ["MV desc"]},
                    {"identifier": "cat.sch.mv2", "description": ["MV desc 2"]},
                ],
            },
        }
        assert count_instruction_slots(config) == 2

    def test_empty_descriptions_not_counted(self):
        config = {
            "data_sources": {
                "tables": [
                    {"identifier": "cat.sch.t1", "description": []},
                    {"identifier": "cat.sch.t2", "description": None},
                    {"identifier": "cat.sch.t3"},
                ],
                "metric_views": [
                    {"identifier": "cat.sch.mv1", "description": []},
                ],
            },
        }
        assert count_instruction_slots(config) == 0

    def test_mixed_all_types(self):
        config = {
            "data_sources": {
                "tables": [
                    {"identifier": "cat.sch.t1", "description": ["Desc"]},
                    {"identifier": "cat.sch.t2"},
                ],
                "metric_views": [
                    {"identifier": "cat.sch.mv1", "description": ["MV desc"]},
                ],
            },
            "instructions": {
                "text_instructions": [{"id": "ti1", "content": ["text"]}],
                "example_question_sqls": [
                    {"id": "eq1", "question": ["Q?"], "sql": ["SELECT 1"]},
                ],
                "sql_functions": [
                    {"id": "sf1", "identifier": "cat.sch.fn"},
                    {"id": "sf2", "identifier": "cat.sch.fn2"},
                ],
            },
        }
        # 1 text + 1 example + 2 functions + 1 table desc + 1 mv desc = 6
        assert count_instruction_slots(config) == 6

    def test_no_data_sources_key(self):
        """Configs without data_sources (e.g. simplified metadata) count only instructions."""
        config = {
            "instructions": {
                "example_question_sqls": [
                    {"id": "eq1", "question": ["Q?"], "sql": ["SELECT 1"]},
                ],
            },
        }
        assert count_instruction_slots(config) == 1

    def test_strict_validation_rejects_with_descriptions(self):
        """Descriptions push slot count over 100 even with few instruction items."""
        config = _minimal_strict_config()
        config["data_sources"] = {
            "tables": [
                {"identifier": f"cat.sch.t{i:03d}", "description": [f"Table {i}"]}
                for i in range(6)
            ],
        }
        config["instructions"] = {
            "example_question_sqls": [
                {
                    "id": f"{i:032x}",
                    "question": [f"Question {i}?"],
                    "sql": [f"SELECT {i}"],
                }
                for i in range(95)
            ],
        }
        # 95 examples + 6 table descs = 101 > 100
        assert count_instruction_slots(config) == 101
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok is False
        assert any("Instruction slot budget exceeded" in e for e in errors)
        assert any("table_descs=6" in e for e in errors)
