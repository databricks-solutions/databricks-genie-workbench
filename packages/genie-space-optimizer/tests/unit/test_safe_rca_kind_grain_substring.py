"""Phase 4a — unit tests for ``_mentions_grain_or_grouping_mismatch``."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.rca import (
    _mentions_grain_or_grouping_mismatch,
)


@pytest.mark.parametrize(
    "text",
    [
        # Anchor gs_013 — joined counterfactual_fixes
        (
            "Including time_window in GROUP BY produces 6 rows (2 per zone) "
            "instead of 3 rows"
        ),
        "Remove time_window from the SELECT and GROUP BY",
        (
            "should either split into two CTEs like the expected SQL (one for "
            "day, one for mtd) and join on zone_combination"
        ),
        (
            "results should be pivoted so each zone has one row with both "
            "day and MTD values side by side, rather than grouping by "
            "time_window to produce separate rows"
        ),
        "should not include time_window in the GROUP BY",
    ],
)
def test_grain_phrases_match(text: str) -> None:
    assert _mentions_grain_or_grouping_mismatch(text) is True


@pytest.mark.parametrize(
    "text",
    [
        "",
        "Result mismatch: expected 3 rows, got 1 rows.",
        "Use ROW_NUMBER() instead of RANK() for route_rank",
        "Remove the filter on PAYMENT_CURRENCY_CD = USD",
        "the metric view should expose a dimension for zone_vp_name",
    ],
)
def test_non_grain_phrases_do_not_match(text: str) -> None:
    assert _mentions_grain_or_grouping_mismatch(text) is False
