"""Tests for pattern detection and type mapping (backend/services/create_agent_tools.py).

Tests PII_PATTERNS, ETL_PATTERNS, _base_col_type(), and _TYPE_HINT_MAP —
pure regex and mapping lookups, no mocking required.
"""

import pytest

from backend.services.create_agent_tools import (
    PII_PATTERNS,
    ETL_PATTERNS,
    _base_col_type,
    _TYPE_HINT_MAP,
    _STRING_TYPES,
    _DATE_TYPES,
    _NUMERIC_TYPES,
    _BOOLEAN_TYPES,
    _update_config,
)


# ---------------------------------------------------------------------------
# PII pattern detection
# ---------------------------------------------------------------------------

class TestPiiPatterns:
    @pytest.mark.parametrize("col_name", [
        "email", "email_address", "user_email",
        "phone", "phone_number",
        "ssn", "social_security",
        "credit_card", "card_number",
        "password", "secret", "api_key",
        "salary", "income", "wage",
        "passport", "license_number",
        "dob", "date_of_birth", "birth_date",
        "bank_account", "routing_number",
        "address", "zip_code",
    ])
    def test_pii_detected(self, col_name):
        assert PII_PATTERNS.search(col_name) is not None, f"{col_name} should match PII"

    @pytest.mark.parametrize("col_name", [
        "customer_id", "order_date", "amount", "region",
        "product_name", "quantity", "status", "created_by",
    ])
    def test_non_pii_not_detected(self, col_name):
        assert PII_PATTERNS.search(col_name) is None, f"{col_name} should NOT match PII"


# ---------------------------------------------------------------------------
# ETL pattern detection
# ---------------------------------------------------------------------------

class TestEtlPatterns:
    @pytest.mark.parametrize("col_name", [
        "_etl_timestamp", "_etl_batch_id",
        "_load_date", "_load_id",
        "_dlt_id", "_dlt_sequence",
        "__metadata", "__internal",
        "_rescued_data",
        "_created_at", "_updated_at", "_modified_at", "_loaded_at",
        "_job_id", "_run_id", "_task_id",
        "dwh_created", "stg_order", "src_system", "etl_flag",
    ])
    def test_etl_detected(self, col_name):
        assert ETL_PATTERNS.search(col_name) is not None, f"{col_name} should match ETL"

    @pytest.mark.parametrize("col_name", [
        "created_at",       # no leading underscore
        "order_timestamp",  # not an ETL pattern
        "user_id",          # normal column
        "region",
        "amount",
        "job_title",        # not _job_id
    ])
    def test_non_etl_not_detected(self, col_name):
        assert ETL_PATTERNS.search(col_name) is None, f"{col_name} should NOT match ETL"


# ---------------------------------------------------------------------------
# _base_col_type
# ---------------------------------------------------------------------------

class TestBaseColType:
    @pytest.mark.parametrize("type_text,expected", [
        ("DECIMAL(18,2)", "decimal"),
        ("VARCHAR(255)", "varchar"),
        ("ARRAY<STRING>", "array"),
        ("timestamp", "timestamp"),
        ("BIGINT", "bigint"),
        ("STRING", "string"),
        ("MAP<STRING,INT>", "map"),
        ("int", "int"),
        ("  FLOAT  ", "float"),
    ])
    def test_normalization(self, type_text, expected):
        assert _base_col_type(type_text) == expected


# ---------------------------------------------------------------------------
# _TYPE_HINT_MAP
# ---------------------------------------------------------------------------

class TestTypeHintMap:
    @pytest.mark.parametrize("input_type,expected", [
        ("NUMBER", "INTEGER"),
        ("INT", "INTEGER"),
        ("BIGINT", "INTEGER"),
        ("SMALLINT", "INTEGER"),
        ("TINYINT", "INTEGER"),
        ("FLOAT", "DOUBLE"),
        ("TIMESTAMP", "DATE"),
    ])
    def test_mapping(self, input_type, expected):
        assert _TYPE_HINT_MAP[input_type] == expected

    def test_unmapped_types_not_in_map(self):
        for t in ["STRING", "DOUBLE", "DECIMAL", "DATE", "BOOLEAN"]:
            assert t not in _TYPE_HINT_MAP, f"{t} should NOT be in map (already valid)"


# ---------------------------------------------------------------------------
# Type sets
# ---------------------------------------------------------------------------

class TestTypeSets:
    def test_string_types(self):
        assert "string" in _STRING_TYPES
        assert "varchar" in _STRING_TYPES
        assert "char" in _STRING_TYPES

    def test_date_types(self):
        assert "date" in _DATE_TYPES
        assert "timestamp" in _DATE_TYPES
        assert "timestamp_ntz" in _DATE_TYPES

    def test_numeric_types(self):
        for t in ["int", "bigint", "float", "double", "decimal"]:
            assert t in _NUMERIC_TYPES

    def test_boolean_types(self):
        assert "boolean" in _BOOLEAN_TYPES


# ---------------------------------------------------------------------------
# _update_config — SQL snippet actions
# ---------------------------------------------------------------------------

def _empty_config():
    return {"data_sources": {"tables": []}, "instructions": {}, "benchmarks": {}}


class TestUpdateConfigAddMeasure:
    def test_adds_measure_with_alias(self):
        cfg = _empty_config()
        result = _update_config(
            [{"action": "add_measure", "alias": "total_rev", "display_name": "Total Revenue", "sql": "SUM(revenue)"}],
            config=cfg,
        )
        measures = result["config"]["instructions"]["sql_snippets"]["measures"]
        assert len(measures) == 1
        assert measures[0]["alias"] == "total_rev"
        assert measures[0]["display_name"] == "Total Revenue"
        assert measures[0]["sql"] == ["SUM(revenue)"]
        assert len(measures[0]["id"]) == 32  # hex ID

    def test_skips_measure_without_alias(self):
        cfg = _empty_config()
        result = _update_config(
            [{"action": "add_measure", "display_name": "Total Revenue", "sql": "SUM(revenue)"}],
            config=cfg,
        )
        measures = result["config"].get("instructions", {}).get("sql_snippets", {}).get("measures", [])
        assert len(measures) == 0
        assert any("Skipped" in a for a in result["applied"])

    def test_skips_measure_without_sql(self):
        cfg = _empty_config()
        result = _update_config(
            [{"action": "add_measure", "alias": "total_rev"}],
            config=cfg,
        )
        measures = result["config"].get("instructions", {}).get("sql_snippets", {}).get("measures", [])
        assert len(measures) == 0

    def test_display_name_optional(self):
        cfg = _empty_config()
        result = _update_config(
            [{"action": "add_measure", "alias": "total_rev", "sql": "SUM(revenue)"}],
            config=cfg,
        )
        measures = result["config"]["instructions"]["sql_snippets"]["measures"]
        assert len(measures) == 1
        assert "display_name" not in measures[0]


class TestUpdateConfigAddExpression:
    def test_adds_expression_with_alias(self):
        cfg = _empty_config()
        result = _update_config(
            [{"action": "add_expression", "alias": "profit_margin", "sql": "(revenue - cost) / revenue"}],
            config=cfg,
        )
        exprs = result["config"]["instructions"]["sql_snippets"]["expressions"]
        assert len(exprs) == 1
        assert exprs[0]["alias"] == "profit_margin"

    def test_skips_expression_without_alias(self):
        cfg = _empty_config()
        result = _update_config(
            [{"action": "add_expression", "sql": "SUM(x)"}],
            config=cfg,
        )
        exprs = result["config"].get("instructions", {}).get("sql_snippets", {}).get("expressions", [])
        assert len(exprs) == 0


class TestUpdateConfigAddFilter:
    def test_adds_filter(self):
        cfg = _empty_config()
        result = _update_config(
            [{"action": "add_filter", "display_name": "Current Year", "sql": "YEAR(date) = YEAR(CURRENT_DATE())"}],
            config=cfg,
        )
        filters = result["config"]["instructions"]["sql_snippets"]["filters"]
        assert len(filters) == 1
        assert filters[0]["display_name"] == "Current Year"
        assert "alias" not in filters[0]  # filters don't have alias

    def test_strips_where_prefix(self):
        cfg = _empty_config()
        result = _update_config(
            [{"action": "add_filter", "display_name": "Active", "sql": "WHERE status = 'active'"}],
            config=cfg,
        )
        filters = result["config"]["instructions"]["sql_snippets"]["filters"]
        assert filters[0]["sql"] == ["status = 'active'"]
