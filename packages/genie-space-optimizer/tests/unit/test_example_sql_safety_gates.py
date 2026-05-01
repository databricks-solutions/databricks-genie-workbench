"""Deterministic teaching-safety gates run before the LLM judge."""
import pytest

from genie_space_optimizer.optimization.example_safety import (
    TeachingSafetyResult,
    check_teaching_safety,
)


def _snapshot():
    return {
        "_asset_semantics": {
            "main.s.dim_location": {
                "asset_type": "table",
                "columns": [{"name": "store_id"}, {"name": "city"}],
            },
            "main.s.store_count_day": {
                "asset_type": "metric_view",
                "measures": [{"name": "store_count"}],
                "dimensions": [{"name": "day"}],
            },
            "main.s.orders": {
                "asset_type": "table",
                "columns": [{"name": "order_id"}, {"name": "customer_id"}],
            },
            "main.s.customers": {
                "asset_type": "table",
                "columns": [{"name": "id"}, {"name": "name"}],
            },
        },
        "_uc_foreign_keys": [],
        "instructions": {"join_specs": []},
    }


def test_safe_table_query_passes():
    result = check_teaching_safety(
        question="how many stores?",
        sql="SELECT COUNT(*) AS n FROM main.s.dim_location",
        metadata_snapshot=_snapshot(),
    )
    assert result.safe is True
    assert result.reasons == []


def test_metric_view_without_measure_blocks():
    result = check_teaching_safety(
        question="how many stores per day?",
        sql="SELECT day, COUNT(*) FROM main.s.store_count_day GROUP BY day",
        metadata_snapshot=_snapshot(),
    )
    assert result.safe is False
    assert any("metric_view_without_measure" in r for r in result.reasons)


def test_table_with_measure_blocks():
    result = check_teaching_safety(
        question="how many stores?",
        sql="SELECT MEASURE(store_count) FROM main.s.dim_location",
        metadata_snapshot=_snapshot(),
    )
    assert result.safe is False
    assert any("table_used_with_measure" in r for r in result.reasons)


def test_double_measure_wrap_blocks():
    result = check_teaching_safety(
        question="store count by day",
        sql="SELECT day, MEASURE(MEASURE(store_count)) FROM main.s.store_count_day GROUP BY day",
        metadata_snapshot=_snapshot(),
    )
    assert result.safe is False
    assert any("anti_pattern_double_measure" in r for r in result.reasons)


def test_unregistered_join_blocks():
    snap = _snapshot()
    result = check_teaching_safety(
        question="orders by customer name",
        sql=("SELECT c.name, COUNT(*) FROM main.s.orders o "
             "JOIN main.s.customers c ON o.customer_id = c.id "
             "GROUP BY c.name"),
        metadata_snapshot=snap,
    )
    assert result.safe is False
    assert any("unregistered_join" in r for r in result.reasons)


def test_registered_join_passes():
    snap = _snapshot()
    snap["instructions"]["join_specs"].append({
        "left": {"identifier": "main.s.orders"},
        "right": {"identifier": "main.s.customers"},
        "sql": ["o.customer_id = c.id"],
    })
    result = check_teaching_safety(
        question="orders by customer name",
        sql=("SELECT c.name, COUNT(*) FROM main.s.orders o "
             "JOIN main.s.customers c ON o.customer_id = c.id "
             "GROUP BY c.name"),
        metadata_snapshot=snap,
    )
    assert result.safe is True


def test_extra_filter_not_in_question_blocks():
    result = check_teaching_safety(
        question="what is the average order total?",
        sql=("SELECT AVG(total) FROM main.s.orders "
             "WHERE region = 'NORTHEAST' AND year = 2025"),
        metadata_snapshot=_snapshot(),
    )
    assert result.safe is False
    assert any("extra_filter_not_in_question" in r for r in result.reasons)


def test_unknown_asset_blocks():
    result = check_teaching_safety(
        question="q",
        sql="SELECT * FROM main.s.does_not_exist",
        metadata_snapshot=_snapshot(),
    )
    assert result.safe is False
    assert any("unknown_asset" in r for r in result.reasons)
