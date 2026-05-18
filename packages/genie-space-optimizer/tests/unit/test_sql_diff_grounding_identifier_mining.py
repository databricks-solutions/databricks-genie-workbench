"""Phase 4a — unit tests for the text-to-identifier miner."""
from __future__ import annotations

from genie_space_optimizer.optimization.sql_diff_grounding import (
    extract_sql_identifiers_from_text,
)


def test_uppercase_snake_case_identifiers():
    text = (
        "Remove the filter on PAYMENT_CURRENCY_CD = USD since the question "
        "says total payment amount in USD. Also remove the IS NOT NULL "
        "filters on FORM_OF_PAYMENT_CD and CREDIT_CARD_PAYMENT_VENDOR_CD."
    )
    out = extract_sql_identifiers_from_text(text)
    assert "PAYMENT_CURRENCY_CD" in out
    assert "FORM_OF_PAYMENT_CD" in out
    assert "CREDIT_CARD_PAYMENT_VENDOR_CD" in out
    assert "USD" not in out  # too short / non-snake-case


def test_backtick_quoted_identifiers():
    text = (
        "The Genie Space should map zone VP to the column `zone_vp_name` "
        "from `mv_esr_dim_location` rather than `zone_combination` from "
        "`mv_7now_store_sales`."
    )
    out = extract_sql_identifiers_from_text(text)
    assert "zone_vp_name" in out
    assert "mv_esr_dim_location" in out
    assert "zone_combination" in out
    assert "mv_7now_store_sales" in out


def test_lowercase_snake_case_in_prose():
    text = (
        "Including time_window in GROUP BY produces 6 rows. Remove "
        "time_window from the SELECT and join on zone_combination."
    )
    out = extract_sql_identifiers_from_text(text)
    assert "time_window" in out
    assert "zone_combination" in out


def test_short_english_tokens_rejected():
    text = (
        "The query should return all rows ordered by total cy sales "
        "descending, and not just the top one."
    )
    out = extract_sql_identifiers_from_text(text)
    # No snake_case identifiers with ≥1 underscore — all noise rejected.
    assert out == frozenset()


def test_sql_keywords_rejected():
    text = "use GROUP BY zone_vp_name and ORDER BY total_sales"
    out = extract_sql_identifiers_from_text(text)
    assert "zone_vp_name" in out
    assert "total_sales" in out
    assert "GROUP" not in out
    assert "ORDER" not in out
    assert "GROUP BY" not in out


def test_empty_or_none_text_returns_empty_set():
    assert extract_sql_identifiers_from_text("") == frozenset()
    assert extract_sql_identifiers_from_text(None) == frozenset()
