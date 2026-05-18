"""Phase 4a — unit tests for grounding_terms_from_fix_text."""
from __future__ import annotations

from genie_space_optimizer.optimization.rca_card_builder import (
    grounding_terms_from_fix_text,
)


def test_terms_in_sql_corpus_survive():
    asi = {
        "airline_gs_024": {
            "counterfactual_fix": (
                "Remove the filter on PAYMENT_CURRENCY_CD = USD and the "
                "IS NOT NULL filters on FORM_OF_PAYMENT_CD."
            ),
        }
    }
    generated_sql = {
        "airline_gs_024": (
            "SELECT SUM(PAYMENT_AMT) FROM payments WHERE "
            "PAYMENT_CURRENCY_CD = 'USD' AND FORM_OF_PAYMENT_CD IS NOT NULL"
        )
    }
    reference_sql = {"airline_gs_024": "SELECT SUM(PAYMENT_AMT) FROM payments"}
    terms = grounding_terms_from_fix_text(
        asi_by_qid=asi,
        generated_sql_by_qid=generated_sql,
        reference_sql_by_qid=reference_sql,
    )
    assert "PAYMENT_CURRENCY_CD" in terms
    assert "FORM_OF_PAYMENT_CD" in terms


def test_terms_only_in_text_but_not_in_sql_are_dropped():
    """The miner must intersect with the SQL corpus so self_grounding
    can never reject a term emitted by this function."""
    asi = {
        "airline_gs_024": {
            "counterfactual_fix": (
                "Map MEASURE_FOO to MEASURE_BAR in the metric view."
            )
        }
    }
    generated_sql = {"airline_gs_024": "SELECT 1 FROM payments"}
    reference_sql = {"airline_gs_024": "SELECT 1 FROM payments"}
    terms = grounding_terms_from_fix_text(
        asi_by_qid=asi,
        generated_sql_by_qid=generated_sql,
        reference_sql_by_qid=reference_sql,
    )
    assert "MEASURE_FOO" not in terms
    assert "MEASURE_BAR" not in terms


def test_lowercase_backtick_quoted_identifiers_survive():
    asi = {
        "7now_gs_026": {
            "counterfactual_fix": (
                "Map zone VP to `zone_vp_name` from `mv_esr_dim_location` "
                "rather than `zone_combination` from `mv_7now_store_sales`."
            ),
        }
    }
    sql = {
        "7now_gs_026": (
            "SELECT zone_vp_name, SUM(amt) FROM mv_esr_dim_location "
            "JOIN mv_7now_store_sales USING (zone_combination)"
        )
    }
    terms = grounding_terms_from_fix_text(
        asi_by_qid=asi,
        generated_sql_by_qid={"7now_gs_026": ""},
        reference_sql_by_qid=sql,
    )
    assert "zone_vp_name" in terms
    assert "mv_esr_dim_location" in terms
    assert "mv_7now_store_sales" in terms
    assert "zone_combination" in terms


def test_empty_inputs_return_empty_frozenset():
    assert (
        grounding_terms_from_fix_text(
            asi_by_qid={},
            generated_sql_by_qid={},
            reference_sql_by_qid={},
        )
        == frozenset()
    )


def test_wrong_clause_is_also_mined():
    asi = {
        "qid_x": {
            "counterfactual_fix": "",
            "wrong_clause": "use LIMIT 10 instead of fare_rank=1",
        }
    }
    sql = {"qid_x": "SELECT * FROM trips WHERE fare_rank = 1 LIMIT 10"}
    terms = grounding_terms_from_fix_text(
        asi_by_qid=asi,
        generated_sql_by_qid=sql,
        reference_sql_by_qid={"qid_x": ""},
    )
    assert "fare_rank" in terms
