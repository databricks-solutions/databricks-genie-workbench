"""Pin that the RCA-forced instruction proposal dispatches on structured root_cause."""

from __future__ import annotations

from genie_space_optimizer.optimization.optimizer import (
    _build_rca_forced_instruction_body,
)


def test_column_disambiguation_emits_disambiguation_body_not_rank_body() -> None:
    body = _build_rca_forced_instruction_body(
        root_cause="column_disambiguation",
        grounding_terms=["region", "market"],
        question="Which regions had highest sales last month?",
        expected_sql="SELECT region_name, SUM(sales) FROM ... GROUP BY 1 ORDER BY 2 DESC",
    )
    assert body is not None
    assert "rank = 1" not in body.lower()
    assert "disambig" in body.lower() or "region_name" in body.lower()


def test_plural_top_n_collapse_emits_rank_body() -> None:
    body = _build_rca_forced_instruction_body(
        root_cause="plural_top_n_collapse",
        grounding_terms=["plural", "top_n"],
        question="Which 5 regions had highest sales?",
        expected_sql="SELECT region_name, SUM(sales) FROM ... ORDER BY 2 DESC LIMIT 5",
    )
    assert body is not None
    assert "ORDER BY" in body or "do not collapse" in body.lower()


def test_format_difference_emits_example_sql_template() -> None:
    body = _build_rca_forced_instruction_body(
        root_cause="format_difference",
        grounding_terms=[],
        question="What were total sales last quarter?",
        expected_sql="SELECT SUM(sales) FROM mv_sales WHERE quarter = 'Q1'",
    )
    assert body is not None
    assert "EXAMPLE SQL" in body or "SELECT" in body.upper()


def test_unknown_root_cause_returns_none() -> None:
    body = _build_rca_forced_instruction_body(
        root_cause="other",
        grounding_terms=["foo"],
        question="?",
        expected_sql="",
    )
    assert body is None
