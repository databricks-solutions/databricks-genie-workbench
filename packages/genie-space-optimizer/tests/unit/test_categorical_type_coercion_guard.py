from __future__ import annotations

from genie_space_optimizer.optimization.evaluation import (
    check_categorical_type_coercion_violations,
)


def _profile() -> dict:
    return {
        "cat.sch.fact": {
            "columns": {
                "same_store_7now": {
                    "cardinality": 2,
                    "distinct_values": ["Y", "N"],
                },
                "amount_str": {
                    "cardinality": 3,
                    "distinct_values": ["1", "2", "3"],
                },
            }
        }
    }


def test_rejects_numeric_equality_against_categorical_string():
    violations = check_categorical_type_coercion_violations(
        "SELECT * FROM cat.sch.fact WHERE same_store_7now = 1",
        _profile(),
    )

    assert violations == [
        ("same_store_7now", "numeric_comparison", ["Y", "N"])
    ]


def test_rejects_numeric_in_list_against_categorical_string():
    violations = check_categorical_type_coercion_violations(
        "SELECT * FROM cat.sch.fact WHERE same_store_7now IN (0, 1)",
        _profile(),
    )

    assert violations == [
        ("same_store_7now", "numeric_comparison", ["Y", "N"])
    ]


def test_allows_string_comparison_against_categorical_string():
    violations = check_categorical_type_coercion_violations(
        "SELECT * FROM cat.sch.fact WHERE same_store_7now = 'Y'",
        _profile(),
    )

    assert violations == []


def test_allows_numeric_string_column_comparison():
    violations = check_categorical_type_coercion_violations(
        "SELECT * FROM cat.sch.fact WHERE amount_str = 1",
        _profile(),
    )

    assert violations == []
