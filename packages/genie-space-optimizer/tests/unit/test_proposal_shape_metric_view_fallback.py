"""Pin that metric-view columns survive proposal_shape normalization."""

from __future__ import annotations

from genie_space_optimizer.optimization.proposal_shape import (
    normalize_column_proposals,
)


def test_metric_view_column_resolves_via_metric_view_fallback() -> None:
    """A column that exists only in a metric view definition must resolve."""
    proposal = {
        "proposal_id": "P009",
        "type": "update_column_description",
        "column": "is_finance_monthly_same_store",
        "table": "",
        "metric_view_columns": [
            {
                "metric_view_full_name": "cat.sch.mv_esr_store_sales",
                "column_name": "is_finance_monthly_same_store",
            }
        ],
    }
    output, decisions = normalize_column_proposals(
        [proposal], uc_columns=[],
    )
    assert len(output) == 1, (
        f"metric-view column dropped; decisions={decisions}"
    )
    assert output[0]["table"] == "cat.sch.mv_esr_store_sales"
    assert output[0]["column"] == "is_finance_monthly_same_store"


def test_explicit_table_wins_over_metric_view_lookup() -> None:
    """If table is already set, normalization must not override it."""
    proposal = {
        "proposal_id": "P010",
        "type": "update_column_description",
        "column": "region_combination",
        "table": "cat.sch.mv_esr_dim_location",
    }
    output, _ = normalize_column_proposals([proposal], uc_columns=[])
    assert output[0]["table"] == "cat.sch.mv_esr_dim_location"


def test_completely_unknown_column_still_dropped_with_reason() -> None:
    """A column with no table, no UC match, no metric view stays dropped."""
    proposal = {
        "proposal_id": "P011",
        "type": "update_column_description",
        "column": "does_not_exist_anywhere",
        "table": "",
    }
    output, decisions = normalize_column_proposals([proposal], uc_columns=[])
    assert output == []
    assert any(d.get("reason") == "missing_table_for_column" for d in decisions)
