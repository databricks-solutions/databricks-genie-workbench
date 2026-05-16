"""``sql_diff_grounding`` extracts deterministic grounding-term sets
from an ASI ``SqlDiff`` payload so the RCA card builder always has
something to put in ``grounding_terms`` for SQL-shape failures (the
C3 root cause Trial-5 Run A surfaced as ``wrong_aggregation`` with
no grounded card)."""

from __future__ import annotations

from genie_space_optimizer.optimization.sql_diff_grounding import (
    extract_aggregation_terms,
    extract_filter_terms,
)


def test_extract_aggregation_terms_pulls_expected_vs_actual_atoms():
    sql_diff = {
        "aggregations": {
            "expected": ["SUM(amount)", "COUNT(DISTINCT user_id)"],
            "actual": ["AVG(amount)", "COUNT(user_id)"],
        }
    }
    terms = extract_aggregation_terms(sql_diff)
    assert "SUM(amount)" in terms
    assert "AVG(amount)" in terms
    assert "COUNT(DISTINCT user_id)" in terms
    assert "COUNT(user_id)" in terms


def test_extract_aggregation_terms_returns_empty_for_missing_section():
    assert extract_aggregation_terms({}) == ()
    assert extract_aggregation_terms({"aggregations": {}}) == ()
    assert extract_aggregation_terms({"aggregations": {"expected": []}}) == ()


def test_extract_aggregation_terms_is_a_tuple_of_strings():
    """``grounding_terms`` is a tuple — the extractor must return one."""
    terms = extract_aggregation_terms({"aggregations": {"expected": ["SUM(x)"]}})
    assert isinstance(terms, tuple)
    assert all(isinstance(t, str) for t in terms)


def test_extract_aggregation_terms_dedupes_and_preserves_order():
    sql_diff = {
        "aggregations": {
            "expected": ["SUM(x)", "SUM(x)", "COUNT(y)"],
            "actual": ["SUM(x)"],
        }
    }
    terms = extract_aggregation_terms(sql_diff)
    assert terms == ("SUM(x)", "COUNT(y)")


def test_extract_filter_terms_pulls_where_predicates():
    sql_diff = {
        "filters": {
            "expected": ["status = 'active'", "created_at >= '2024-01-01'"],
            "actual": ["status IN ('active','pending')"],
        }
    }
    terms = extract_filter_terms(sql_diff)
    assert "status = 'active'" in terms
    assert "status IN ('active','pending')" in terms


def test_extract_filter_terms_handles_missing_section():
    assert extract_filter_terms({}) == ()


# ── Integration: build_card pulls SqlDiff atoms into grounding ─────


def test_grounding_terms_from_asi_unions_in_sql_diff_atoms():
    """grounding_terms_from_asi is the single source of truth for
    what counts as a grounding term. After Phase 3 it must include
    SqlDiff aggregation/filter atoms in addition to blame_set."""
    from genie_space_optimizer.optimization.rca_card_builder import (
        grounding_terms_from_asi,
    )
    asi_by_qid = {
        "Q42": {
            "failure_type": "wrong_aggregation",
            "blame_set": ["fact_orders.amount"],
            "sql_diff": {
                "aggregations": {
                    "expected": ["SUM(amount)"],
                    "actual": ["AVG(amount)"],
                },
            },
        },
    }
    terms = grounding_terms_from_asi(asi_by_qid)
    assert "fact_orders.amount" in terms  # blame_set (existing)
    assert "SUM(amount)" in terms  # SqlDiff (new in Phase 3)
    assert "AVG(amount)" in terms


def test_build_card_grounds_wrong_aggregation_via_sql_diff_atoms():
    """End-to-end: when the cluster carries a SqlDiff with aggregation
    atoms that appear in the generated / reference SQL, build_card
    returns a non-None card — the Trial-5 Run A C3 break (empty
    grounding_terms → rca_card_grounded=False) cannot recur."""
    from genie_space_optimizer.optimization.rca_card_builder import build_card

    asi_by_qid = {
        "Q42": {
            "failure_type": "wrong_aggregation",
            "rca_kind": "measure_swap",
            "blame_set": [],
            "sql_diff": {
                "aggregations": {
                    "expected": ["SUM(amount)"],
                    "actual": ["AVG(amount)"],
                },
            },
        },
    }
    card, fail_reason, _ = build_card(
        cluster_id="C1",
        qids=("Q42",),
        asi_by_qid=asi_by_qid,
        generated_sql_by_qid={"Q42": "SELECT AVG(amount) FROM t"},
        reference_sql_by_qid={"Q42": "SELECT SUM(amount) FROM t"},
    )
    assert card is not None, (
        f"build_card returned None (self_check_failure_reason={fail_reason!r})"
    )
    terms = set(card.grounding_terms or ())
    assert "SUM(amount)" in terms
    assert "AVG(amount)" in terms
