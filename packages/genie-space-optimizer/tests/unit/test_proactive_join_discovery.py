from __future__ import annotations


def test_example_sql_rows_are_converted_to_positive_eval_rows():
    from genie_space_optimizer.optimization.harness import (
        _example_sqls_to_positive_eval_rows,
    )

    rows = _example_sqls_to_positive_eval_rows([
        {
            "question": "Sales by location",
            "expected_sql": (
                "SELECT l.location_id, SUM(f.sales) "
                "FROM cat.sch.fact_sales f "
                "JOIN cat.sch.dim_location l ON f.location_id = l.location_id "
                "GROUP BY l.location_id"
            ),
        }
    ])

    assert rows[0]["arbiter/value"] == "synthetic_example"
    assert rows[0]["request"]["expected_sql"].startswith("SELECT l.location_id")
    assert rows[0]["inputs/expected_sql"].startswith("SELECT l.location_id")


def test_join_discovery_result_has_explicit_observability_fields():
    from genie_space_optimizer.optimization.harness import (
        _empty_join_discovery_result,
    )

    result = _empty_join_discovery_result()

    for key in (
        "fk_rows_available",
        "fk_candidates_built",
        "execution_candidates",
        "example_sql_join_candidates",
        "joins_skipped_metric_view",
        "type_incompatible",
        "spec_validation_rejected",
    ):
        assert key in result
