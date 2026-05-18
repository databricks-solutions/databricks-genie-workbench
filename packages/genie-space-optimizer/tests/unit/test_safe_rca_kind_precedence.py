"""Phase 4a — assert _safe_rca_kind's typed-rcakind precedence rules.

These tests pin the exact ordering the deterministic classifier
uses when multiple substring matchers could fire on the same fix
text. The two anchor scenarios that motivate this ordering are:

* gs_009 — text contains both top-N rewrite hints (ROW_NUMBER vs
  RANK) AND defensive-filter hints (IS NOT NULL on airport codes).
  Top-N is the dominant signal.
* gs_013 — text contains GROUP BY removal hints. The classifier
  must not bucket this as a filter or top-N mismatch.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.rca import RcaKind, _safe_rca_kind


_GS_009_FIX = (
    "Use ROW_NUMBER() instead of RANK() for route_rank to ensure exactly "
    "10 rows are returned, or use LIMIT 10 after filtering fare_rank = 1. "
    "Additionally, the IS NOT NULL filters on ORIG_AIRPORT_CD and "
    "DEST_AIRPORT_CD exclude NULL routes which changes the counts. To fix, "
    "the Genie Space should specify that top N selections should use LIMIT "
    "rather than RANK, or clarify tie-breaking behavior."
)

_GS_013_FIX = (
    "Including time_window in GROUP BY produces 6 rows (2 per zone) instead "
    "of 3 rows. Remove time_window from the SELECT and GROUP BY, and instead "
    "use separate CTEs or filter conditions to compute day and mtd measures "
    "independently, then join on zone_combination."
)

_GS_024_FIX = (
    "Remove the filter on PAYMENT_CURRENCY_CD = USD since the question says "
    "total payment amount in USD referring to the columns unit, not a "
    "filter. Also remove the IS NOT NULL filters on FORM_OF_PAYMENT_CD and "
    "CREDIT_CARD_PAYMENT_VENDOR_CD as they exclude valid NULL groupings."
)


def test_top_n_text_wins_over_defensive_filter_text() -> None:
    """gs_009 — top-N matcher must fire before defensive-filter matcher."""
    kind = _safe_rca_kind(
        value=None,
        failure_type="unknown",
        metadata={"counterfactual_fix": _GS_009_FIX},
    )
    assert kind == RcaKind.TOP_N_CARDINALITY_COLLAPSE


def test_grain_text_wins_over_filter_text() -> None:
    """gs_013 — grain matcher must fire before any filter matcher."""
    kind = _safe_rca_kind(
        value=None,
        failure_type="unknown",
        metadata={"counterfactual_fix": _GS_013_FIX},
    )
    assert kind == RcaKind.GRAIN_OR_GROUPING_MISMATCH


def test_pure_defensive_filter_still_classified() -> None:
    """gs_024 — no top-N or grain signal, so defensive-filter still wins."""
    kind = _safe_rca_kind(
        value=None,
        failure_type="unknown",
        metadata={"counterfactual_fix": _GS_024_FIX},
    )
    assert kind == RcaKind.EXTRA_DEFENSIVE_FILTER


def test_unknown_text_falls_through_to_unknown() -> None:
    """Pessimistic scenario — only the boilerplate Result-mismatch string."""
    kind = _safe_rca_kind(
        value=None,
        failure_type="unknown",
        metadata={
            "counterfactual_fix": (
                "Result mismatch: expected 10 rows (hash=ab), got 16 rows "
                "(hash=cd). Check joins, filters, or aggregation logic in "
                "the generated SQL."
            )
        },
    )
    assert kind == RcaKind.UNKNOWN


def test_wrong_clause_is_also_consulted_for_top_n() -> None:
    """When counterfactual_fix is empty, the matcher reads wrong_clause."""
    kind = _safe_rca_kind(
        value=None,
        failure_type="unknown",
        metadata={
            "counterfactual_fix": "",
            "wrong_clause": "use LIMIT 10 instead of RANK to avoid ties",
        },
    )
    assert kind == RcaKind.TOP_N_CARDINALITY_COLLAPSE
