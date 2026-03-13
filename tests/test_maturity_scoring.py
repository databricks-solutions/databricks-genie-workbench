"""Tests for the config-driven maturity scoring engine.

Covers: calculate_score, get_maturity_stage, _scale_count, and all
registered check functions against known space data fixtures.
"""

import pytest
from backend.services.scanner import (
    calculate_score,
    get_maturity_stage,
    _scale_count,
    _CHECKS,
)
from backend.services.maturity_config import get_default_config


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def default_config():
    return get_default_config()


@pytest.fixture
def empty_space():
    """A space with no tables, no instructions, nothing."""
    return {"data_sources": {"tables": []}, "instructions": {}}


@pytest.fixture
def nascent_space():
    """Minimal space — just tables attached."""
    return {
        "data_sources": {
            "tables": [
                {"identifier": "cat.schema.orders", "table_name": "cat.schema.orders", "columns": [{"name": "id"}]},
            ],
        },
        "instructions": {},
    }


@pytest.fixture
def developing_space():
    """A reasonably configured space hitting Developing stage."""
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.schema.orders",
                    "table_name": "cat.schema.orders",
                    "description": "Customer orders",
                    "catalog": "cat",
                    "columns": [
                        {"name": "id", "description": "Primary key"},
                        {"name": "amount", "description": "Order amount"},
                        {"name": "status", "comment": "Order status"},
                    ],
                },
                {
                    "identifier": "cat.schema.customers",
                    "table_name": "cat.schema.customers",
                    "description": "Customer records",
                    "catalog": "cat",
                    "columns": [
                        {"name": "id", "description": "Primary key"},
                        {"name": "name", "description": "Full name"},
                    ],
                },
            ],
        },
        "instructions": {
            "text_instructions": [
                {"content": "This space answers questions about customer orders and revenue metrics for the sales team."},
                {"content": "Use the orders table as the primary fact table. Join to customers on customer_id."},
            ],
            "example_question_sqls": [
                {"question": "Total revenue", "sql": "SELECT SUM(amount) FROM orders"},
                {"question": "Orders by status", "sql": "SELECT status, COUNT(*) FROM orders GROUP BY 1"},
                {"question": "Top customers", "sql": "SELECT name, SUM(amount) FROM orders JOIN customers USING(id) GROUP BY 1 ORDER BY 2 DESC"},
            ],
            "join_specs": [{"left": "orders", "right": "customers", "on": "customer_id"}],
            "sql_snippets": {
                "filters": [{"name": "last_30d", "sql": "date >= CURRENT_DATE - 30"}],
            },
        },
    }


@pytest.fixture
def optimized_space(developing_space):
    """A fully configured space that should hit Optimized."""
    space = developing_space.copy()
    space["instructions"] = {
        **developing_space["instructions"],
        "example_question_sqls": [
            {"question": f"Q{i}", "sql": f"SELECT {i}"} for i in range(15)
        ],
        "sql_snippets": {
            **developing_space["instructions"]["sql_snippets"],
            "expressions": [{"name": "aov", "sql": "SUM(amount)/COUNT(*)"}],
            "measures": [{"name": "total_rev", "sql": "SUM(amount)"}, {"name": "order_count", "sql": "COUNT(*)"}],
        },
        "sql_functions": [{"name": "fiscal_quarter", "sql": "..."}],
    }
    space["benchmarks"] = {
        "questions": [{"question": f"BQ{i}"} for i in range(10)],
    }
    return space


# ---------------------------------------------------------------------------
# Unit tests: helpers
# ---------------------------------------------------------------------------

class TestScaleCount:
    def test_zero_value(self):
        assert _scale_count(0.0, {"target": 5}) == 0.0

    def test_at_target(self):
        assert _scale_count(5.0, {"target": 5}) == 1.0

    def test_above_target_capped(self):
        assert _scale_count(10.0, {"target": 5}) == 1.0

    def test_half_target(self):
        assert _scale_count(2.5, {"target": 5}) == 0.5

    def test_zero_target(self):
        assert _scale_count(0.0, {"target": 0}) == 0.0
        assert _scale_count(1.0, {"target": 0}) == 1.0


class TestGetMaturityStage:
    @pytest.fixture
    def stages(self):
        return [
            {"name": "Nascent", "range": [0, 29]},
            {"name": "Basic", "range": [30, 49]},
            {"name": "Developing", "range": [50, 69]},
            {"name": "Proficient", "range": [70, 84]},
            {"name": "Optimized", "range": [85, 100]},
        ]

    def test_zero_is_nascent(self, stages):
        assert get_maturity_stage(0, stages) == "Nascent"

    def test_boundary_29_is_nascent(self, stages):
        assert get_maturity_stage(29, stages) == "Nascent"

    def test_boundary_30_is_basic(self, stages):
        assert get_maturity_stage(30, stages) == "Basic"

    def test_boundary_50_is_developing(self, stages):
        assert get_maturity_stage(50, stages) == "Developing"

    def test_boundary_70_is_proficient(self, stages):
        assert get_maturity_stage(70, stages) == "Proficient"

    def test_boundary_85_is_optimized(self, stages):
        assert get_maturity_stage(85, stages) == "Optimized"

    def test_100_is_optimized(self, stages):
        assert get_maturity_stage(100, stages) == "Optimized"

    def test_empty_stages(self):
        assert get_maturity_stage(50, []) == "Unknown"


# ---------------------------------------------------------------------------
# Unit tests: registered check functions
# ---------------------------------------------------------------------------

class TestCheckFunctions:
    def test_all_criteria_have_checks(self, default_config):
        """Every criterion in the default config must have a registered check."""
        for criterion in default_config["criteria"]:
            assert criterion["id"] in _CHECKS, f"No check registered for: {criterion['id']}"

    def test_tables_attached_empty(self, empty_space):
        assert _CHECKS["tables_attached"](empty_space) is False

    def test_tables_attached_with_table(self, nascent_space):
        assert _CHECKS["tables_attached"](nascent_space) is True

    def test_table_count(self, developing_space):
        assert _CHECKS["table_count"](developing_space) == 2.0

    def test_columns_exist_empty(self, empty_space):
        assert _CHECKS["columns_exist"](empty_space) is False

    def test_columns_exist(self, nascent_space):
        assert _CHECKS["columns_exist"](nascent_space) is True

    def test_instructions_defined_empty(self, empty_space):
        assert _CHECKS["instructions_defined"](empty_space) is False

    def test_instructions_defined(self, developing_space):
        assert _CHECKS["instructions_defined"](developing_space) is True

    def test_column_descriptions_full(self, developing_space):
        # All 5 columns have descriptions
        result = _CHECKS["column_descriptions"](developing_space)
        assert result == 1.0

    def test_column_descriptions_empty(self, nascent_space):
        # 1 column, no description
        result = _CHECKS["column_descriptions"](nascent_space)
        assert result == 0.0

    def test_joins_defined(self, developing_space):
        assert _CHECKS["joins_defined"](developing_space) is True

    def test_joins_not_defined(self, nascent_space):
        assert _CHECKS["joins_defined"](nascent_space) is False

    def test_sample_questions(self, developing_space):
        assert _CHECKS["sample_questions"](developing_space) == 3.0

    def test_filter_snippets(self, developing_space):
        assert _CHECKS["filter_snippets"](developing_space) is True

    def test_unity_catalog(self, developing_space):
        assert _CHECKS["unity_catalog"](developing_space) is True

    def test_unity_catalog_no_tables(self, empty_space):
        assert _CHECKS["unity_catalog"](empty_space) is False

    def test_benchmark_questions_none(self, developing_space):
        assert _CHECKS["benchmark_questions"](developing_space) == 0.0

    def test_benchmark_questions(self, optimized_space):
        assert _CHECKS["benchmark_questions"](optimized_space) == 10.0


# ---------------------------------------------------------------------------
# Integration: calculate_score with default config
# ---------------------------------------------------------------------------

class TestCalculateScore:
    def test_empty_space_scores_zero(self, empty_space, default_config):
        result = calculate_score(empty_space, default_config)
        assert result["score"] == 0
        assert result["maturity"] == "Nascent"
        assert len(result["findings"]) > 0

    def test_nascent_space(self, nascent_space, default_config):
        result = calculate_score(nascent_space, default_config)
        # Should get tables_attached (10) + table_count (2 for 1 table) + columns_exist (5) = 17
        assert result["score"] > 0
        assert result["score"] < 30
        assert result["maturity"] == "Nascent"

    def test_developing_space(self, developing_space, default_config):
        result = calculate_score(developing_space, default_config)
        assert result["score"] >= 50
        assert result["maturity"] in ("Developing", "Proficient")

    def test_optimized_space(self, optimized_space, default_config):
        result = calculate_score(optimized_space, default_config)
        assert result["score"] >= 85
        assert result["maturity"] == "Optimized"

    def test_score_capped_at_100(self, optimized_space, default_config):
        result = calculate_score(optimized_space, default_config)
        assert result["score"] <= 100

    def test_result_structure(self, nascent_space, default_config):
        result = calculate_score(nascent_space, default_config)
        assert "score" in result
        assert "maturity" in result
        assert "breakdown" in result
        assert "criteria_results" in result
        assert "findings" in result
        assert "next_steps" in result
        assert "scanned_at" in result

    def test_breakdown_has_all_stages(self, nascent_space, default_config):
        result = calculate_score(nascent_space, default_config)
        for key in ("nascent", "basic", "developing", "proficient", "optimized"):
            assert key in result["breakdown"]

    def test_criteria_results_match_enabled(self, nascent_space, default_config):
        """Each enabled criterion should produce a result."""
        result = calculate_score(nascent_space, default_config)
        enabled_ids = {c["id"] for c in default_config["criteria"] if c.get("enabled", True)}
        result_ids = {r["id"] for r in result["criteria_results"]}
        assert result_ids == enabled_ids

    def test_disabled_criterion_excluded(self, nascent_space, default_config):
        """Disabling a criterion should exclude it from results."""
        config = default_config.copy()
        config["criteria"] = [
            {**c, "enabled": False} if c["id"] == "tables_attached" else c
            for c in config["criteria"]
        ]
        result = calculate_score(nascent_space, config)
        result_ids = {r["id"] for r in result["criteria_results"]}
        assert "tables_attached" not in result_ids

    def test_adjusted_points(self, nascent_space, default_config):
        """Changing a criterion's points should affect the score."""
        normal = calculate_score(nascent_space, default_config)

        boosted_config = default_config.copy()
        boosted_config["criteria"] = [
            {**c, "points": 50} if c["id"] == "tables_attached" else c
            for c in boosted_config["criteria"]
        ]
        boosted = calculate_score(nascent_space, boosted_config)
        assert boosted["score"] > normal["score"]
