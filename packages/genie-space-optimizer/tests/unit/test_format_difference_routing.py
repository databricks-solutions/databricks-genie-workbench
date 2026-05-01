"""Pin format_difference -> lever 5 routing."""

from __future__ import annotations

from genie_space_optimizer.optimization.optimizer import (
    _ROOT_CAUSE_LEVER_MAP,
    _format_difference_example_sql_template,
)


def test_format_difference_maps_to_lever_5() -> None:
    assert _ROOT_CAUSE_LEVER_MAP["format_difference"] == 5, (
        "format_difference must route to lever 5 so the strategist can emit example_sql"
    )


def test_format_difference_example_sql_template_returns_concrete_sql() -> None:
    body = _format_difference_example_sql_template(
        question="Which regions had the highest sales last quarter?",
        expected_sql="SELECT region_name, SUM(sales) FROM mv_sales GROUP BY 1 ORDER BY 2 DESC",
    )
    assert "GROUP BY" in body.upper()
    assert "ORDER BY" in body.upper()
    assert "SELECT" in body.upper()
