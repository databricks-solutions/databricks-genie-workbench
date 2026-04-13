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
# Check 1: Data sources exist
# ---------------------------------------------------------------------------

class TestDataSourcesExist:
    def test_no_tables(self, empty_space_data):
        result = calculate_score(empty_space_data)
        check = _check_by_label(result, "Data sources exist")
        assert check["passed"] is False

    def test_has_tables(self, full_space_data):
        check = _check_by_label(calculate_score(full_space_data), "Data sources exist")
        assert check["passed"] is True

    def test_metric_views_only_passes(self, metric_view_only_space):
        check = _check_by_label(calculate_score(metric_view_only_space), "Data sources exist")
        assert check["passed"] is True
        assert "1 metric view(s)" in check["detail"]

    def test_both_tables_and_metric_views(self):
        data = {
            "data_sources": {
                "tables": [{"name": "t1", "columns": []}],
                "metric_views": [{"identifier": "cat.sch.mv1"}],
            },
            "instructions": {},
            "benchmarks": {},
        }
        check = _check_by_label(calculate_score(data), "Data sources exist")
        assert check["passed"] is True
        assert "1 table(s)" in check["detail"]
        assert "1 metric view(s)" in check["detail"]


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
            {"name": f"t{i}", "description": f"desc{i}", "columns": []} for i in range(4)
        ] + [{"name": "t4", "columns": []}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Table descriptions")
        assert check["passed"] is True
        assert check["severity"] == "warning"  # <100% → warning

    def test_79_pct_boundary_fail(self):
        """3/4 = 75% should fail (below 80%)."""
        tables = [
            {"name": f"t{i}", "description": f"desc{i}", "columns": []} for i in range(3)
        ] + [{"name": "t3", "columns": []}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Table descriptions")
        assert check["passed"] is False

    def test_comment_counts_as_description(self):
        """Tables with 'comment' instead of 'description' should count."""
        tables = [{"name": "t0", "comment": "has comment", "columns": []}]
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
            {"name": "a", "description": "desc"},
            {"name": "b"},
        ]}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Column descriptions")
        assert check["passed"] is True

    def test_below_50_pct_fail(self):
        """1/3 = 33% should fail."""
        tables = [{"name": "t", "columns": [
            {"name": "a", "description": "desc"},
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
            {"name": "a", "description": "desc"},
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

    def test_over_2000_chars_warning(self):
        tables = [{"name": "t", "columns": []}]
        data = {"data_sources": {"tables": tables},
                "instructions": {"text_instructions": [{"content": ["x" * 2500]}]},
                "benchmarks": {}}
        result = calculate_score(data)
        check = _check_by_label(result, "Text instructions (>50 chars)")
        assert check["severity"] == "warning"

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

    def test_absent_multi_source_generates_finding(self):
        tables = [{"name": "t1", "columns": []}, {"name": "t2", "columns": []}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        result = calculate_score(data)
        assert "No join specifications for multi-source space" in result["findings"]

    def test_absent_single_table_no_finding(self):
        tables = [{"name": "t1", "columns": []}]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        result = calculate_score(data)
        assert "No join specifications for multi-source space" not in result["findings"]

    def test_absent_with_table_and_metric_view(self):
        """1 table + 1 metric view = 2 sources → finding generated."""
        data = {
            "data_sources": {
                "tables": [{"name": "t1", "columns": []}],
                "metric_views": [{"identifier": "cat.sch.mv1"}],
            },
            "instructions": {},
            "benchmarks": {},
        }
        result = calculate_score(data)
        assert "No join specifications for multi-source space" in result["findings"]


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

    def test_9_tables_warning(self):
        tables = [{"name": f"t{i}", "columns": []} for i in range(9)]
        data = {"data_sources": {"tables": tables}, "instructions": {}, "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "Data source count 1-12")
        assert check["passed"] is True
        assert check["severity"] == "warning"

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
# Check 7: 8+ example SQLs
# ---------------------------------------------------------------------------

class TestExampleSqls:
    def test_0_examples_fails(self, empty_space_data):
        check = _check_by_label(calculate_score(empty_space_data), "8+ example SQLs")
        assert check["passed"] is False

    def test_7_examples_fails(self):
        data = {"data_sources": {"tables": [{"name": "t", "columns": []}]},
                "instructions": {"example_question_sqls": [{"id": str(i)} for i in range(7)]},
                "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "8+ example SQLs")
        assert check["passed"] is False

    def test_8_examples_passes(self):
        data = {"data_sources": {"tables": [{"name": "t", "columns": []}]},
                "instructions": {"example_question_sqls": [{"id": str(i)} for i in range(8)]},
                "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "8+ example SQLs")
        assert check["passed"] is True

    def test_10_examples_warning_for_sweet_spot(self):
        """8-14 examples pass but with a warning suggesting 10-15."""
        data = {"data_sources": {"tables": [{"name": "t", "columns": []}]},
                "instructions": {"example_question_sqls": [{"id": str(i)} for i in range(10)]},
                "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "8+ example SQLs")
        assert check["passed"] is True
        assert check["severity"] == "warning"

    def test_15_examples_no_warning(self):
        data = {"data_sources": {"tables": [{"name": "t", "columns": []}]},
                "instructions": {"example_question_sqls": [
                    {"id": str(i), "usage_guidance": ["g"]} for i in range(15)
                ]},
                "benchmarks": {}}
        check = _check_by_label(calculate_score(data), "8+ example SQLs")
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
# Check 8: SQL snippets
# ---------------------------------------------------------------------------

class TestSqlSnippets:
    def test_none_fails(self, empty_space_data):
        label = "SQL snippets (functions/expressions/measures/filters)"
        check = _check_by_label(calculate_score(empty_space_data), label)
        assert check["passed"] is False

    def test_functions_only_passes_with_warning(self):
        data = {"data_sources": {"tables": [{"name": "t", "columns": []}]},
                "instructions": {"sql_functions": [{"id": "f1"}]},
                "benchmarks": {}}
        label = "SQL snippets (functions/expressions/measures/filters)"
        check = _check_by_label(calculate_score(data), label)
        assert check["passed"] is True
        assert check["severity"] == "warning"  # missing filters and measures

    def test_all_types_pass(self, full_space_data):
        label = "SQL snippets (functions/expressions/measures/filters)"
        check = _check_by_label(calculate_score(full_space_data), label)
        assert check["passed"] is True
        assert check["severity"] == "pass"


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
# Check 10: 10+ benchmark questions
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
# Check 9 addendum: Metric view entity matching
# ---------------------------------------------------------------------------

class TestMetricViewEntityMatching:
    def test_metric_view_entity_matching_counted(self):
        """Metric view columns with entity matching should pass Check 9."""
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
        """Metric view columns with format assistance should pass Check 9."""
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
        ws.tables.get.side_effect = [
            Exception("Permission denied"),
            _mock_table_info(comment="Table 2 desc"),
        ]
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
                {"name": "order_id", "comment": "PK"},
                {"name": "amount", "comment": "Total"},
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
