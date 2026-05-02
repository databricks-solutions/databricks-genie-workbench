"""Track 5 — SQL-shape patch quality predicates."""
from __future__ import annotations


def test_is_unrequested_is_not_null_filter_detects_tautology() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_unrequested_is_not_null_filter,
    )

    weak_patch = {
        "type": "add_sql_snippet_filter",
        "snippet": "WHERE customer_id IS NOT NULL OR customer_id IS NULL",
        "root_cause": "missing_filter",
    }
    assert is_unrequested_is_not_null_filter(weak_patch) is True


def test_is_unrequested_is_not_null_filter_detects_bare_is_not_null() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_unrequested_is_not_null_filter,
    )

    weak_patch = {
        "type": "add_sql_snippet_filter",
        "snippet": "WHERE zone_combination IS NOT NULL",
        "root_cause": "extra_defensive_filter",
    }
    assert is_unrequested_is_not_null_filter(weak_patch) is True


def test_is_unrequested_is_not_null_filter_allows_justified_predicate() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_unrequested_is_not_null_filter,
    )

    justified_patch = {
        "type": "add_sql_snippet_filter",
        "snippet": "WHERE customer_id IS NOT NULL",
        "root_cause": "missing_filter",
        "justification": (
            "Question asks for the count of customers who placed an "
            "order; rows without a customer_id are not customers and "
            "must be excluded."
        ),
    }
    assert is_unrequested_is_not_null_filter(justified_patch) is False


def test_is_unrequested_currency_filter_detects_already_correct_currency() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_unrequested_currency_filter,
    )

    weak_patch = {
        "type": "add_sql_snippet_filter",
        "snippet": "WHERE currency = 'USD'",
        "root_cause": "missing_filter",
        "metric_native_currency": "USD",
        "question_requested_currency": "USD",
    }
    assert is_unrequested_currency_filter(weak_patch) is True


def test_is_unrequested_currency_filter_allows_currency_mismatch() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_unrequested_currency_filter,
    )

    legit_patch = {
        "type": "add_sql_snippet_filter",
        "snippet": "WHERE currency = 'USD'",
        "root_cause": "missing_filter",
        "metric_native_currency": "EUR",
        "question_requested_currency": "USD",
    }
    assert is_unrequested_currency_filter(legit_patch) is False


def test_is_rank_when_limit_n_required_detects_rank_misuse() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_rank_when_limit_n_required,
    )

    weak_patch = {
        "type": "add_sql_snippet_calculation",
        "snippet": "SELECT *, RANK() OVER (ORDER BY revenue DESC) AS rn FROM t",
        "root_cause": "plural_top_n_collapse",
        "question_requests_exact_top_n": True,
    }
    assert is_rank_when_limit_n_required(weak_patch) is True


def test_is_rank_when_limit_n_required_allows_row_number() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_rank_when_limit_n_required,
    )

    canonical_patch = {
        "type": "add_sql_snippet_calculation",
        "snippet": (
            "SELECT *, ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn "
            "FROM t QUALIFY rn <= 5"
        ),
        "root_cause": "plural_top_n_collapse",
        "question_requests_exact_top_n": True,
    }
    assert is_rank_when_limit_n_required(canonical_patch) is False


def test_is_rank_when_limit_n_required_allows_rank_when_ties_intended() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_rank_when_limit_n_required,
    )

    ties_intended_patch = {
        "type": "add_sql_snippet_calculation",
        "snippet": "SELECT *, RANK() OVER (ORDER BY revenue DESC) AS rn FROM t",
        "root_cause": "plural_top_n_collapse",
        "question_requests_exact_top_n": False,
    }
    assert is_rank_when_limit_n_required(ties_intended_patch) is False
