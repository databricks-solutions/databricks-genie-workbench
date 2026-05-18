"""Phase 4a — unit tests for ``_mentions_top_n_collapse`` substring matcher."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.rca import _mentions_top_n_collapse


@pytest.mark.parametrize(
    "text",
    [
        # Anchor gs_009 — joined counterfactual_fixes
        (
            "Use ROW_NUMBER() instead of RANK() for route_rank to ensure "
            "exactly 10 rows are returned, or use LIMIT 10 after filtering "
            "fare_rank = 1"
        ),
        # Anchor gs_026 — joined counterfactual_fixes
        (
            "the query should return all zone VPs ordered by total CY sales "
            "descending, not just the top 1"
        ),
        (
            "Generated SQL returns only the top-ranked zone (rank=1) instead "
            "of all zones ordered by total CY sales descending."
        ),
        # Variants the matcher must also fire on
        "RANK can return more than 10 rows when there are ties",
        "should clarify that top 10 means exactly 10 rows",
        "use LIMIT rather than RANK",
        "to avoid ties producing more than 10 rows",
    ],
)
def test_top_n_collapse_phrases_match(text: str) -> None:
    assert _mentions_top_n_collapse(text) is True


@pytest.mark.parametrize(
    "text",
    [
        "",
        "Result mismatch: expected 10 rows (hash=ab), got 16 rows (hash=cd).",
        "the metric view should expose a dimension for zone_vp_name",
        "remove the filter on PAYMENT_CURRENCY_CD = USD",
        "Use RANK with ties allowed — ranking is the desired output",
    ],
)
def test_non_top_n_phrases_do_not_match(text: str) -> None:
    assert _mentions_top_n_collapse(text) is False
