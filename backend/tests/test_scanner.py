"""Tests for the IQ scoring engine (backend/services/scanner.py).

Tests calculate_score() and get_maturity_label() — pure functions that take
dicts and return dicts, no mocking required.

_enrich_with_uc_descriptions() tests use mocked WorkspaceClient.
"""

import copy
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from backend.services.scanner import (
    calculate_score,
    get_maturity_label,
    CONFIG_CHECK_COUNT,
    _enrich_with_uc_descriptions,
    _parse_identifier,
)


def _check_by_label(result, label):
    """Find a check by its label string."""
    for c in result["checks"]:
        if c["label"] == label:
            return c
    raise KeyError(f"No check with label '{label}' in {[c['label'] for c in result['checks']]}")


# ---------------------------------------------------------------------------
# get_maturity_label
# ---------------------------------------------------------------------------

class TestMaturityLabel:
    def test_all_pass_is_trusted(self):
        checks = [{"passed": True}] * 12
        assert get_maturity_label(checks) == "Trusted"

    def test_config_only_pass_is_ready_to_optimize(self):
        checks = [{"passed": True}] * CONFIG_CHECK_COUNT + [{"passed": False}] * 2
        assert get_maturity_label(checks) == "Ready to Optimize"

    def test_any_config_fail_is_not_ready(self):
        checks = [{"passed": True}] * 9 + [{"passed": False}] + [{"passed": True}] * 2
        assert get_maturity_label(checks) == "Not Ready"

    def test_all_fail_is_not_ready(self):
        checks = [{"passed": False}] * 12
        assert get_maturity_label(checks) == "Not Ready"


# ---------------------------------------------------------------------------
# calculate_score — full config / empty config
# ---------------------------------------------------------------------------

class TestScoreEndToEnd:
    def test_perfect_config_scores_12(self, full_space_data):
        result = calculate_score(full_space_data, optimization_run={"accuracy": 0.90})
        assert result["score"] == 12
        assert result["total"] == 12
        assert result["maturity"] == "Trusted"
        assert result["findings"] == []

    def test_empty_config_scores_0(self, empty_space_data):
        result = calculate_score(empty_space_data)
        assert result["score"] == 0
        assert result["maturity"] == "Not Ready"
        assert len(result["findings"]) > 0

    def test_findings_capped_at_8(self, empty_space_data):
        result = calculate_score(empty_space_data)
        assert len(result["findings"]) <= 8
        assert len(result["next_steps"]) <= 8
        assert len(result["warnings"]) <= 8

    def test_config_only_pass_gives_ready_to_optimize(self, full_space_data):
        result = calculate_score(full_space_data, optimization_run=None)
        assert result["score"] == 10
        assert result["maturity"] == "Ready to Optimize"


# ---------------------------------------------------------------------------
# Check 1: Space description
# ---------------------------------------------------------------------------

class TestSpaceDescription:
    def test_missing_description_fails(self, empty_space_data):
        result = calculate_score(empty_space_data)
        check = _check_by_label(result, "Space description")
        assert check["passed"] is False

    def test_meaningful_description_passes(self, full_space_data):
        check = _check_by_label(calculate_score(full_space_data), "Space description")
        assert check["passed"] is True

    def test_short_but_meaningful_description_warns(self, full_space_data):
        data = copy.deepcopy(full_space_data)
        data["description"] = "Sales analytics space for finance teams."
        check = _check_by_label(calculate_score(data), "Space description")
        assert check["passed"] is True
        assert check["severity"] == "warning"

    def test_placeholder_description_fails(self, full_space_data):
        data = copy.deepcopy(full_space_data)
        data["description"] = "TBD"
        check = _check_by_label(calculate_score(data), "Space description")
        assert check["passed"] is False


# ---------------------------------------------------------------------------
# Check 2: Table descriptions (≥80%)
# ---------------------------------------------------------------------------

class TestTableDescriptions:
    def test_all_described(self, full_space_data):
        check = _check_by_label(calculate_score(full_space_data), "Table descriptions")
        assert check["passed"] is True
        assert check["severity"] == "pass"

    def test_80_pct_boundary_pass(self):
        """4/5 = 80% should pass."""
        tables = [
            {"name": f"t{i}", "description": f"Useful table description {i}", "columns": []} for i in range(4)
        ] + [{"name": "t4", "columns": []}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Table descriptions")
        assert check["passed"] is True
        assert check["severity"] == "warning"  # <100% → warning

    def test_79_pct_boundary_fail(self):
        """3/4 = 75% should fail (below 80%)."""
        tables = [
            {"name": f"t{i}", "description": f"Useful table description {i}", "columns": []} for i in range(3)
        ] + [{"name": "t3", "columns": []}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Table descriptions")
        assert check["passed"] is False

    def test_comment_counts_as_description(self):
        """Tables with 'comment' instead of 'description' should count."""
        tables = [{"name": "t0", "comment": "Useful table comment", "columns": []}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Table descriptions")
        assert check["passed"] is True

    def test_metric_views_only_auto_passes(self, metric_view_only_space):
        """No tables but metric views → auto-pass (managed in UC)."""
        check = _check_by_label(calculate_score(metric_view_only_space), "Table descriptions")
        assert check["passed"] is True
        assert "Unity Catalog" in check["detail"]


# ---------------------------------------------------------------------------
# Check 3: Column descriptions (≥50%)
# ---------------------------------------------------------------------------

class TestColumnDescriptions:
    def test_all_described(self, full_space_data):
        check = _check_by_label(calculate_score(full_space_data), "Column descriptions")
        assert check["passed"] is True

    def test_50_pct_boundary_pass(self):
        """1/2 = 50% should pass."""
        tables = [{"name": "t", "columns": [
            {"name": "a", "description": "Useful column description"},
            {"name": "b"},
        ]}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Column descriptions")
        assert check["passed"] is True

    def test_below_50_pct_fail(self):
        """1/3 = 33% should fail."""
        tables = [{"name": "t", "columns": [
            {"name": "a", "description": "Useful column description"},
            {"name": "b"},
            {"name": "c"},
        ]}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Column descriptions")
        assert check["passed"] is False

    def test_no_synonyms_warning(self, full_space_data):
        """When cols are described but none have synonyms, we get a warning."""
        data = copy.deepcopy(full_space_data)
        # Remove synonyms from all columns
        for t in data["data_sources"]["tables"]:
            for c in t.get("columns", []):
                c.pop("synonyms", None)
        result = calculate_score(data)
        assert "No column synonyms defined" in result["warnings"]

    def test_column_configs_counted(self):
        """column_configs should be counted alongside columns."""
        tables = [{"name": "t", "columns": [], "column_configs": [
            {"name": "a", "description": "Useful column description"},
            {"name": "b"},
        ]}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Column descriptions")
        assert check["passed"] is True  # 1/2 = 50%

    def test_metric_views_only_auto_passes(self, metric_view_only_space):
        """No tables but metric views → auto-pass (managed in UC)."""
        check = _check_by_label(calculate_score(metric_view_only_space), "Column descriptions")
        assert check["passed"] is True
        assert "Unity Catalog" in check["detail"]


# ---------------------------------------------------------------------------
# Check 4: Text instructions (>50 chars)
# ---------------------------------------------------------------------------

class TestTextInstructions:
    def test_no_instructions(self, empty_space_data):
        check = _check_by_label(calculate_score(empty_space_data),
                                "Text instructions (>50 chars)")
        assert check["passed"] is False

    def test_exactly_50_chars_fails(self):
        tables = [{"name": "t", "columns": []}]
        data = {"data_sources": {"tables": tables},
                "instructions": {"text_instructions": [{"content": ["x" * 50]}]},
                "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Text instructions (>50 chars)")
        assert check["passed"] is False

    def test_51_chars_passes(self):
        tables = [{"name": "t", "columns": []}]
        data = {"data_sources": {"tables": tables},
                "instructions": {"text_instructions": [{"content": ["x" * 51]}]},
                "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Text instructions (>50 chars)")
        assert check["passed"] is True

    def test_2000_chars_no_length_warning(self):
        tables = [{"name": "t", "columns": []}]
        data = {"data_sources": {"tables": tables},
                "instructions": {"text_instructions": [{"content": ["x" * 2000]}]},
                "benchmarks": {}}
        result = calculate_score(data)
        check = _check_by_label(result, "Text instructions (>50 chars)")
        assert check["severity"] == "pass"
        assert not any("keep under 2,000" in w for w in result["warnings"])

    def test_over_2000_chars_warning(self):
        tables = [{"name": "t", "columns": []}]
        data = {"data_sources": {"tables": tables},
                "instructions": {"text_instructions": [{"content": ["x" * 2001]}]},
                "benchmarks": {}}
        result = calculate_score(data)
        check = _check_by_label(result, "Text instructions (>50 chars)")
        assert check["severity"] == "warning"
        assert any("keep under 2,000" in w for w in result["warnings"])

    def test_sql_in_text_warning(self):
        tables = [{"name": "t", "columns": []}]
        text = "Use SELECT * FROM orders WHERE region = 'AMER' for American orders."
        data = {"data_sources": {"tables": tables},
                "instructions": {"text_instructions": [{"content": [text]}]},
                "benchmarks": {}}
        result = calculate_score(data)
        assert any("SQL patterns found" in w for w in result["warnings"])

    def test_content_as_string(self):
        """content can be a plain string (not a list)."""
        tables = [{"name": "t", "columns": []}]
        data = {"data_sources": {"tables": tables},
                "instructions": {"text_instructions": [{"content": "x" * 60}]},
                "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Text instructions (>50 chars)")
        assert check["passed"] is True


# ---------------------------------------------------------------------------
# Check 5: Join specifications
# ---------------------------------------------------------------------------

class TestJoinSpecs:
    def test_present(self, full_space_data):
        check = _check_by_label(calculate_score(full_space_data), "Join specifications")
        assert check["passed"] is True

    def test_absent_multi_table_generates_finding(self):
        tables = [{"name": "t1", "columns": []}, {"name": "t2", "columns": []}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        result = calculate_score(data)
        assert "No join specifications for multi-table space" in result["findings"]

    def test_absent_single_table_no_finding(self):
        tables = [{"name": "t1", "columns": []}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        result = calculate_score(data)
        check = _check_by_label(result, "Join specifications")
        assert check["passed"] is True
        assert "No join specifications for multi-table space" not in result["findings"]

    def test_partial_multi_table_join_coverage_warns(self):
        tables = [{"name": f"t{i}", "columns": []} for i in range(3)]
        data = {
            "data_sources": {"tables": tables},
            "instructions": {"join_specs": [{"id": "j1"}]},
            "benchmarks": {},
        }
        result = calculate_score(data)
        check = _check_by_label(result, "Join specifications")
        assert check["passed"] is True
        assert check["severity"] == "warning"
        assert any("relationship coverage" in w for w in result["warnings"])

    def test_absent_with_table_and_metric_view_passes(self):
        """Metric views do not create a join-spec requirement."""
        data = {
            "data_sources": {
                "tables": [{"name": "t1", "columns": []}],
                "metric_views": [{"identifier": "cat.sch.mv1"}],
            },
            "instructions": {},
            "benchmarks": {},
        }
        result = calculate_score(data)
        check = _check_by_label(result, "Join specifications")
        assert check["passed"] is True
        assert "No join specifications for multi-table space" not in result["findings"]

    def test_absent_with_multiple_metric_views_passes(self):
        data = {
            "data_sources": {
                "tables": [],
                "metric_views": [
                    {"identifier": "cat.sch.mv1"},
                    {"identifier": "cat.sch.mv2"},
                    {"identifier": "cat.sch.mv3"},
                ],
            },
            "instructions": {},
            "benchmarks": {},
        }
        result = calculate_score(data)
        check = _check_by_label(result, "Join specifications")
        assert check["passed"] is True
        assert check["detail"] == "0 join spec(s) for 0 table(s)"

    def test_metric_views_do_not_hide_missing_table_joins(self):
        data = {
            "data_sources": {
                "tables": [
                    {"name": "t1", "columns": []},
                    {"name": "t2", "columns": []},
                ],
                "metric_views": [{"identifier": "cat.sch.mv1"}],
            },
            "instructions": {},
            "benchmarks": {},
        }
        result = calculate_score(data)
        check = _check_by_label(result, "Join specifications")
        assert check["passed"] is False
        assert "No join specifications for multi-table space" in result["findings"]

    def test_metric_views_do_not_inflate_join_coverage_requirement(self):
        data = {
            "data_sources": {
                "tables": [
                    {"name": "t1", "columns": []},
                    {"name": "t2", "columns": []},
                    {"name": "t3", "columns": []},
                ],
                "metric_views": [
                    {"identifier": "cat.sch.mv1"},
                    {"identifier": "cat.sch.mv2"},
                ],
            },
            "instructions": {"join_specs": [{"id": "j1"}, {"id": "j2"}]},
            "benchmarks": {},
        }
        result = calculate_score(data)
        check = _check_by_label(result, "Join specifications")
        assert check["passed"] is True
        assert check["severity"] == "pass"
        assert check["detail"] == "2 join spec(s) for 3 table(s)"


# ---------------------------------------------------------------------------
# Check 6: Data source count 1-12
# ---------------------------------------------------------------------------

class TestTableCount:
    def test_0_tables_fails(self, empty_space_data):
        check = _check_by_label(calculate_score(empty_space_data), "Data source count 1-12")
        assert check["passed"] is False

    def test_1_table_passes(self):
        tables = [{"name": "t", "columns": []}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Data source count 1-12")
        assert check["passed"] is True

    def test_12_tables_passes(self):
        tables = [{"name": f"t{i}", "columns": []} for i in range(12)]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Data source count 1-12")
        assert check["passed"] is True

    def test_9_tables_passes_with_warning(self):
        tables = [{"name": f"t{i}", "columns": []} for i in range(9)]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        result = calculate_score(data)
        check = _check_by_label(result, "Data source count 1-12")
        assert check["passed"] is True
        assert check["severity"] == "warning"
        assert any("focused" in w for w in result["warnings"])

    def test_13_tables_fails(self):
        tables = [{"name": f"t{i}", "columns": []} for i in range(13)]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Data source count 1-12")
        assert check["passed"] is False

    def test_metric_views_counted_toward_limit(self):
        """10 tables + 5 metric views = 15 data sources → fails."""
        tables = [{"name": f"t{i}", "columns": []} for i in range(10)]
        mvs = [{"identifier": f"cat.sch.mv{i}"} for i in range(5)]
        data = {"data_sources": {"tables": tables, "metric_views": mvs}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Data source count 1-12")
        assert check["passed"] is False


# ---------------------------------------------------------------------------
# Check 7: SQL guidance artifacts
# ---------------------------------------------------------------------------

class TestSqlGuidanceArtifacts:
    def test_none_fails(self, empty_space_data):
        check = _check_by_label(calculate_score(empty_space_data), "SQL guidance artifacts")
        assert check["passed"] is False

    def test_one_example_sql_passes(self):
        data = {"data_sources": {"tables": [{"name": "t", "columns": []}]},
                "instructions": {"example_question_sqls": [{"id": "1"}]},
                "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "SQL guidance artifacts")
        assert check["passed"] is True

    def test_sql_function_passes(self):
        data = {"data_sources": {"tables": [{"name": "t", "columns": []}]},
                "instructions": {"sql_functions": [{"id": "f1"}]},
                "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "SQL guidance artifacts")
        assert check["passed"] is True

    def test_sql_snippets_pass_with_missing_type_warning(self):
        data = {"data_sources": {"tables": [{"name": "t", "columns": []}]},
                "instructions": {"sql_snippets": {"measures": [{"id": "m1"}]}},
                "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "SQL guidance artifacts")
        assert check["passed"] is True
        assert check["severity"] == "warning"

    def test_all_sql_guidance_types_pass_cleanly(self, full_space_data):
        check = _check_by_label(calculate_score(full_space_data), "SQL guidance artifacts")
        assert check["passed"] is True
        assert check["severity"] == "pass"

    def test_examples_with_usage_guidance_pass_cleanly(self):
        data = {"data_sources": {"tables": [{"name": "t", "columns": []}]},
                "instructions": {"example_question_sqls": [
                    {"id": str(i), "usage_guidance": ["Use for regional aggregation questions"]} for i in range(3)
                ]},
                "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "SQL guidance artifacts")
        assert check["passed"] is True
        assert check["severity"] == "pass"

    def test_missing_usage_guidance_warning(self):
        """If >50% lack usage_guidance, generate a warning."""
        data = {"data_sources": {"tables": [{"name": "t", "columns": []}]},
                "instructions": {"example_question_sqls": [{"id": str(i)} for i in range(15)]},
                "benchmarks": {}}
        result = calculate_score(data)
        assert any("lack usage_guidance" in w for w in result["warnings"])


# ---------------------------------------------------------------------------
# Check 9: Entity/format matching
# ---------------------------------------------------------------------------

class TestEntityFormatMatching:
    def test_none_fails(self):
        tables = [{"name": "t", "columns": [{"name": "c"}]}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Entity/format matching")
        assert check["passed"] is False

    def test_entity_matching_passes(self, full_space_data):
        check = _check_by_label(calculate_score(full_space_data), "Entity/format matching")
        assert check["passed"] is True

    def test_over_100_entity_warning(self):
        cols = [{"name": f"c{i}", "enable_entity_matching": True} for i in range(105)]
        tables = [{"name": "t", "columns": cols}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Entity/format matching")
        assert check["passed"] is True
        assert check["severity"] == "warning"
        assert "approaching" in check["detail"]

    def test_over_120_entity_warning(self):
        cols = [{"name": f"c{i}", "enable_entity_matching": True} for i in range(125)]
        tables = [{"name": "t", "columns": cols}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Entity/format matching")
        assert check["severity"] == "warning"
        assert "exceeds" in check["detail"]

    def test_rls_advisory_warning(self):
        tables = [{"name": "t", "row_filter": "true",
                    "columns": [{"name": "c", "enable_entity_matching": True}]}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        result = calculate_score(data)
        assert any("row-level security" in w for w in result["warnings"])


# ---------------------------------------------------------------------------
# Check 9: 10+ benchmark questions
# ---------------------------------------------------------------------------

class TestBenchmarks:
    def test_0_fails(self, empty_space_data):
        check = _check_by_label(calculate_score(empty_space_data), "10+ benchmark questions")
        assert check["passed"] is False

    def test_9_fails(self):
        data = {"data_sources": {"tables": [{"name": "t", "columns": []}]},
                "instructions": {},
                "benchmarks": {"questions": [{"id": str(i)} for i in range(9)]}}
        check = _check_by_label(calculate_score(data), "10+ benchmark questions")
        assert check["passed"] is False

    def test_10_passes(self):
        data = {"data_sources": {"tables": [{"name": "t", "columns": []}]},
                "instructions": {},
                "benchmarks": {"questions": [{"id": str(i)} for i in range(10)]}}
        check = _check_by_label(calculate_score(data), "10+ benchmark questions")
        assert check["passed"] is True


# ---------------------------------------------------------------------------
# Check 10: Column visibility / noise control
# ---------------------------------------------------------------------------

class TestColumnVisibilityNoiseControl:
    def test_clean_small_schema_passes(self, full_space_data):
        check = _check_by_label(calculate_score(full_space_data), "Column visibility / noise control")
        assert check["passed"] is True
        assert check["severity"] == "pass"

    def test_noisy_large_schema_warns_at_15_pct(self):
        cols = [{"name": f"business_col_{i}", "description": "Useful business column"} for i in range(17)]
        cols += [{"name": "etl_batch_id"}, {"name": "raw_payload_json"}, {"name": "debug_flag"}]
        data = {"data_sources": {"tables": [{"name": "t", "columns": cols}]}, "instructions": {}, "benchmarks": {}}
        result = calculate_score(data)
        check = _check_by_label(result, "Column visibility / noise control")
        assert check["passed"] is True
        assert check["severity"] == "warning"

    def test_noisy_large_schema_fails_at_30_pct(self):
        cols = [{"name": f"business_col_{i}", "description": "Useful business column"} for i in range(14)]
        cols += [
            {"name": "etl_batch_id"},
            {"name": "raw_payload_json"},
            {"name": "debug_flag"},
            {"name": "audit_user"},
            {"name": "col_1"},
            {"name": "load_timestamp"},
        ]
        data = {"data_sources": {"tables": [{"name": "t", "columns": cols}]}, "instructions": {}, "benchmarks": {}}
        result = calculate_score(data)
        check = _check_by_label(result, "Column visibility / noise control")
        assert check["passed"] is False
        assert any("visible columns look internal/noisy" in f for f in result["findings"])

    def test_excluded_noise_columns_do_not_count(self):
        cols = [{"name": f"business_col_{i}", "description": "Useful business column"} for i in range(14)]
        cols += [
            {"name": "etl_batch_id", "exclude": True},
            {"name": "raw_payload_json", "exclude": True},
            {"name": "debug_flag", "exclude": True},
            {"name": "audit_user", "exclude": True},
            {"name": "col_1", "exclude": True},
            {"name": "load_timestamp", "exclude": True},
        ]
        data = {"data_sources": {"tables": [{"name": "t", "columns": cols}]}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Column visibility / noise control")
        assert check["passed"] is True


# ---------------------------------------------------------------------------
# Checks 11-12: Optimization
# ---------------------------------------------------------------------------

class TestOptimization:
    def test_no_run(self, full_space_data):
        result = calculate_score(full_space_data, optimization_run=None)
        check11 = _check_by_label(result, "Optimization workflow completed")
        check12 = _check_by_label(result, "Optimization accuracy ≥ 85%")
        assert check11["passed"] is False
        assert check12["passed"] is False
        assert result["optimization_accuracy"] is None

    def test_accuracy_84_fails(self, full_space_data):
        result = calculate_score(full_space_data, optimization_run={"accuracy": 0.84})
        check = _check_by_label(result, "Optimization accuracy ≥ 85%")
        assert check["passed"] is False
        assert result["optimization_accuracy"] == 0.84

    def test_accuracy_85_passes(self, full_space_data):
        result = calculate_score(full_space_data, optimization_run={"accuracy": 0.85})
        check = _check_by_label(result, "Optimization accuracy ≥ 85%")
        assert check["passed"] is True

    def test_accuracy_100_passes(self, full_space_data):
        result = calculate_score(full_space_data, optimization_run={"accuracy": 1.0})
        assert _check_by_label(result, "Optimization accuracy ≥ 85%")["passed"] is True


# ---------------------------------------------------------------------------
# Check 8 addendum: Metric view entity matching
# ---------------------------------------------------------------------------

class TestMetricViewEntityMatching:
    def test_metric_view_entity_matching_counted(self):
        """Metric view columns with entity matching should pass Check 8."""
        data = {
            "data_sources": {
                "tables": [],
                "metric_views": [{
                    "identifier": "cat.sch.mv1",
                    "column_configs": [
                        {"name": "region", "enable_entity_matching": True},
                    ],
                }],
            },
            "instructions": {},
            "benchmarks": {},
        }
        check = _check_by_label(calculate_score(data), "Entity/format matching")
        assert check["passed"] is True

    def test_metric_view_format_assistance_counted(self):
        """Metric view columns with format assistance should pass Check 8."""
        data = {
            "data_sources": {
                "tables": [],
                "metric_views": [{
                    "identifier": "cat.sch.mv1",
                    "column_configs": [
                        {"name": "order_date", "enable_format_assistance": True},
                    ],
                }],
            },
            "instructions": {},
            "benchmarks": {},
        }
        check = _check_by_label(calculate_score(data), "Entity/format matching")
        assert check["passed"] is True


# ---------------------------------------------------------------------------
# _enrich_with_uc_descriptions
# ---------------------------------------------------------------------------

def _mock_table_info(comment="", columns=None):
    """Build a mock TableInfo object matching the Databricks SDK shape."""
    cols = []
    for c in (columns or []):
        cols.append(SimpleNamespace(name=c["name"], comment=c.get("comment", ""), type_text=c.get("type_text", "")))
    return SimpleNamespace(comment=comment, columns=cols)


class TestUCEnrichment:
    def test_enriches_table_comment(self):
        space_data = {
            "data_sources": {
                "tables": [{"identifier": "cat.sch.orders", "columns": []}],
            },
        }
        ws = MagicMock()
        ws.tables.get.return_value = _mock_table_info(comment="All customer orders")
        count = _enrich_with_uc_descriptions(space_data, ws)
        assert count == 1
        assert space_data["data_sources"]["tables"][0]["comment"] == "All customer orders"

    def test_enriches_column_comment(self):
        space_data = {
            "data_sources": {
                "tables": [{
                    "identifier": "cat.sch.orders",
                    "description": "has table desc",
                    "column_configs": [
                        {"column_name": "order_id"},
                        {"column_name": "amount"},
                    ],
                }],
            },
        }
        ws = MagicMock()
        ws.tables.get.return_value = _mock_table_info(
            comment="All orders",
            columns=[
                {"name": "order_id", "comment": "Primary key"},
                {"name": "amount", "comment": "Order total in USD"},
            ],
        )
        count = _enrich_with_uc_descriptions(space_data, ws)
        assert count == 2  # 2 columns enriched (table already has description)
        cols = space_data["data_sources"]["tables"][0]["column_configs"]
        assert cols[0]["comment"] == "Primary key"
        assert cols[1]["comment"] == "Order total in USD"

    def test_no_overwrite_existing_description(self):
        space_data = {
            "data_sources": {
                "tables": [{
                    "identifier": "cat.sch.orders",
                    "description": "Existing table desc",
                    "column_configs": [
                        {"column_name": "order_id", "description": "Existing col desc"},
                    ],
                }],
            },
        }
        ws = MagicMock()
        ws.tables.get.return_value = _mock_table_info(
            comment="UC table comment",
            columns=[{"name": "order_id", "comment": "UC col comment"}],
        )
        count = _enrich_with_uc_descriptions(space_data, ws)
        assert count == 0
        tbl = space_data["data_sources"]["tables"][0]
        assert tbl["description"] == "Existing table desc"
        assert "comment" not in tbl  # not set since description exists
        assert tbl["column_configs"][0]["description"] == "Existing col desc"

    def test_no_overwrite_existing_comment(self):
        space_data = {
            "data_sources": {
                "tables": [{
                    "identifier": "cat.sch.orders",
                    "comment": "Existing comment",
                    "column_configs": [
                        {"column_name": "order_id", "comment": "Existing col comment"},
                    ],
                }],
            },
        }
        ws = MagicMock()
        ws.tables.get.return_value = _mock_table_info(
            comment="UC table comment",
            columns=[{"name": "order_id", "comment": "UC col comment"}],
        )
        count = _enrich_with_uc_descriptions(space_data, ws)
        assert count == 0

    def test_partial_failure_continues(self):
        space_data = {
            "data_sources": {
                "tables": [
                    {"identifier": "cat.sch.t1", "columns": []},
                    {"identifier": "cat.sch.t2", "columns": []},
                ],
            },
        }
        ws = MagicMock()
        def _get_table(full_name):
            if full_name == "cat.sch.t1":
                raise Exception("Permission denied")
            return _mock_table_info(comment="Table 2 desc")

        ws.tables.get.side_effect = _get_table
        count = _enrich_with_uc_descriptions(space_data, ws)
        assert count == 1
        assert space_data["data_sources"]["tables"][0].get("comment") is None
        assert space_data["data_sources"]["tables"][1]["comment"] == "Table 2 desc"

    def test_empty_space_noop(self):
        space_data = {"data_sources": {"tables": [], "metric_views": []}}
        ws = MagicMock()
        count = _enrich_with_uc_descriptions(space_data, ws)
        assert count == 0
        ws.tables.get.assert_not_called()

    def test_enrichment_makes_check2_pass(self):
        """End-to-end: table with no inline description but UC comment → Check 2 passes."""
        space_data = {
            "data_sources": {
                "tables": [{"identifier": "cat.sch.orders", "columns": []}],
            },
            "instructions": {},
            "benchmarks": {},
        }
        ws = MagicMock()
        ws.tables.get.return_value = _mock_table_info(comment="All customer orders")
        _enrich_with_uc_descriptions(space_data, ws)
        result = calculate_score(space_data)
        check = _check_by_label(result, "Table descriptions")
        assert check["passed"] is True

    def test_enrichment_makes_check3_pass(self):
        """End-to-end: columns with no inline description but UC comment → Check 3 passes."""
        space_data = {
            "data_sources": {
                "tables": [{
                    "identifier": "cat.sch.orders",
                    "description": "Orders",
                    "column_configs": [
                        {"column_name": "order_id"},
                        {"column_name": "amount"},
                    ],
                }],
            },
            "instructions": {},
            "benchmarks": {},
        }
        ws = MagicMock()
        ws.tables.get.return_value = _mock_table_info(
            comment="Orders",
            columns=[
                {"name": "order_id", "comment": "Primary key column"},
                {"name": "amount", "comment": "Order total amount"},
            ],
        )
        _enrich_with_uc_descriptions(space_data, ws)
        result = calculate_score(space_data)
        check = _check_by_label(result, "Column descriptions")
        assert check["passed"] is True

    def test_metric_view_enrichment(self):
        """Metric views should also be enriched from UC."""
        space_data = {
            "data_sources": {
                "tables": [],
                "metric_views": [{"identifier": "cat.sch.mv1"}],
            },
        }
        ws = MagicMock()
        ws.tables.get.return_value = _mock_table_info(comment="Revenue metrics")
        count = _enrich_with_uc_descriptions(space_data, ws)
        assert count == 1
        assert space_data["data_sources"]["metric_views"][0]["comment"] == "Revenue metrics"

    def test_skips_two_part_identifier(self):
        """Two-part identifiers (no catalog) are skipped — can't call UC API without catalog."""
        space_data = {
            "data_sources": {
                "tables": [{"identifier": "sch.orders", "columns": []}],
            },
        }
        ws = MagicMock()
        count = _enrich_with_uc_descriptions(space_data, ws)
        assert count == 0
        ws.tables.get.assert_not_called()


# ---------------------------------------------------------------------------
# _parse_identifier
# ---------------------------------------------------------------------------

class TestParseIdentifier:
    def test_three_parts(self):
        assert _parse_identifier("cat.sch.tbl") == ("cat", "sch", "tbl")

    def test_backticks_stripped(self):
        assert _parse_identifier("`cat`.`sch`.`tbl`") == ("cat", "sch", "tbl")

    def test_two_parts(self):
        assert _parse_identifier("sch.tbl") == ("", "sch", "tbl")

    def test_one_part(self):
        assert _parse_identifier("tbl") == ("", "", "tbl")

    def test_empty_string(self):
        assert _parse_identifier("") == ("", "", "")


# ---------------------------------------------------------------------------
# scan_space: GSO run selection for the header "% benchmark accuracy"
# ---------------------------------------------------------------------------


class TestScanSpaceGsoSelection:
    """The header pulls optimization_accuracy from the latest terminal GSO run's
    best_accuracy (scan_space), NOT from the Optimize tab's per-iteration path.
    These lock in which run statuses count — the APPLIED case is the regression
    that showed a stale pre-GSO score after a successful apply.
    """

    @staticmethod
    def _patch_common(monkeypatch, *, gso_runs, legacy_run=None):
        """Stub scan_space's external reads: config fetch, UC enrichment,
        legacy optimization_runs, GSO runs, and persistence."""
        import backend.services.scanner as scanner
        import backend.services.gso_lakebase as gso_lakebase

        # Minimal scorable config (1 table, no benchmarks needed for accuracy).
        space_data = {"data_sources": {"tables": [{"name": "t", "columns": []}]},
                      "instructions": {}, "benchmarks": {"questions": []}}
        monkeypatch.setattr(
            scanner,
            "get_serialized_space",
            lambda _sid, *, include_top_level_description=False: space_data,
        )
        # Skip UC enrichment (it's already wrapped in try/except, but make it a
        # clean no-op so the test doesn't depend on a WorkspaceClient).
        monkeypatch.setattr(scanner, "get_workspace_client", lambda: MagicMock(), raising=False)
        monkeypatch.setattr(scanner, "_enrich_with_uc_descriptions", lambda *a, **k: 0)

        async def _legacy(_sid):
            return legacy_run
        monkeypatch.setattr(scanner, "get_latest_optimization_run", _legacy)

        async def _gso(_sid):
            return list(gso_runs)
        monkeypatch.setattr(gso_lakebase, "load_gso_runs_for_space", _gso)

        async def _save(_sid, _result):
            return None
        monkeypatch.setattr(scanner, "save_scan_result", _save)

    @pytest.mark.asyncio
    async def test_applied_run_supplies_accuracy(self, monkeypatch):
        """An APPLIED run's best_accuracy IS the live config — it must drive the
        header. This is the reported bug: applying a 90% run left the header on
        the stale legacy score."""
        from backend.services.scanner import scan_space
        self._patch_common(
            monkeypatch,
            gso_runs=[{"status": "APPLIED", "best_accuracy": 90.0,
                       "completed_at": "2026-07-08T00:00:00Z"}],
            legacy_run={"accuracy": 0.53},  # stale pre-GSO baseline
        )
        result = await scan_space("space-1")
        assert result["optimization_accuracy"] == 0.90

    @pytest.mark.asyncio
    async def test_converged_run_supplies_accuracy(self, monkeypatch):
        from backend.services.scanner import scan_space
        self._patch_common(
            monkeypatch,
            gso_runs=[{"status": "CONVERGED", "best_accuracy": 88.0,
                       "completed_at": "2026-07-08T00:00:00Z"}],
        )
        result = await scan_space("space-1")
        assert result["optimization_accuracy"] == 0.88

    @pytest.mark.asyncio
    async def test_discarded_run_is_ignored(self, monkeypatch):
        """A DISCARDED run reverted the live config, so its accuracy must NOT
        drive the header — fall back to the legacy baseline instead."""
        from backend.services.scanner import scan_space
        self._patch_common(
            monkeypatch,
            gso_runs=[{"status": "DISCARDED", "best_accuracy": 90.0,
                       "completed_at": "2026-07-08T00:00:00Z"}],
            legacy_run={"accuracy": 0.53},
        )
        result = await scan_space("space-1")
        assert result["optimization_accuracy"] == 0.53

    @pytest.mark.asyncio
    async def test_scan_requests_top_level_space_description(self, monkeypatch):
        """scan_space must score the API-level Space description, not only the
        parsed serialized_space payload."""
        import backend.services.scanner as scanner
        import backend.services.gso_lakebase as gso_lakebase

        def _get_serialized_space(_sid, *, include_top_level_description=False):
            assert include_top_level_description is True
            return {
                "description": "Useful top-level description for this sales analytics space.",
                "data_sources": {"tables": [{"name": "t", "columns": []}]},
                "instructions": {},
                "benchmarks": {"questions": []},
            }

        monkeypatch.setattr(scanner, "get_serialized_space", _get_serialized_space)
        monkeypatch.setattr(scanner, "get_workspace_client", lambda: MagicMock(), raising=False)
        monkeypatch.setattr(scanner, "_enrich_with_uc_descriptions", lambda *a, **k: 0)

        async def _legacy(_sid):
            return None
        monkeypatch.setattr(scanner, "get_latest_optimization_run", _legacy)

        async def _gso(_sid):
            return []
        monkeypatch.setattr(gso_lakebase, "load_gso_runs_for_space", _gso)

        async def _save(_sid, _result):
            return None
        monkeypatch.setattr(scanner, "save_scan_result", _save)

        result = await scanner.scan_space("space-1")
        check = _check_by_label(result, "Space description")
        assert check["passed"] is True

    @pytest.mark.asyncio
    async def test_applied_beats_older_converged(self, monkeypatch):
        """Most-recent APPLIED run wins over an older CONVERGED run (runs arrive
        most-recent-first)."""
        from backend.services.scanner import scan_space
        self._patch_common(
            monkeypatch,
            gso_runs=[
                {"status": "APPLIED", "best_accuracy": 90.0, "completed_at": "2026-07-08T00:00:00Z"},
                {"status": "CONVERGED", "best_accuracy": 70.0, "completed_at": "2026-07-01T00:00:00Z"},
            ],
        )
        result = await scan_space("space-1")
        assert result["optimization_accuracy"] == 0.90

    @pytest.mark.asyncio
    async def test_fraction_scale_accuracy_not_double_normalized(self, monkeypatch):
        """best_accuracy already on the 0–1 scale (<=1.0) is passed through, not
        divided by 100 again."""
        from backend.services.scanner import scan_space
        self._patch_common(
            monkeypatch,
            gso_runs=[{"status": "APPLIED", "best_accuracy": 0.9,
                       "completed_at": "2026-07-08T00:00:00Z"}],
        )
        result = await scan_space("space-1")
        assert result["optimization_accuracy"] == 0.90

    @pytest.mark.asyncio
    async def test_no_gso_falls_back_to_legacy(self, monkeypatch):
        from backend.services.scanner import scan_space
        self._patch_common(
            monkeypatch, gso_runs=[], legacy_run={"accuracy": 0.53},
        )
        result = await scan_space("space-1")
        assert result["optimization_accuracy"] == 0.53
