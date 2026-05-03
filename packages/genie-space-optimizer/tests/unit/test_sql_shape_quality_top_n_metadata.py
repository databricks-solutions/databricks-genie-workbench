"""TDD: top-N intent detection for RCA-time classifier routing."""
from __future__ import annotations

from genie_space_optimizer.optimization.sql_shape_quality import (
    metadata_indicates_top_n_intent,
)


def test_rank_without_limit_plus_top_keyword_returns_true():
    md = {
        "question_text": "What are the top 5 destination cities by passenger volume in 2023?",
        "genie_sql": (
            "SELECT city, RANK() OVER (ORDER BY passengers DESC) AS r "
            "FROM trips WHERE year = 2023 AND r >= 1"
        ),
    }
    assert metadata_indicates_top_n_intent(md) is True


def test_rank_with_limit_n_returns_false():
    """LIMIT N already present → not a collapse."""
    md = {
        "question_text": "What are the top 5 destination cities?",
        "genie_sql": (
            "SELECT city, RANK() OVER (ORDER BY passengers DESC) AS r "
            "FROM trips ORDER BY r LIMIT 5"
        ),
    }
    assert metadata_indicates_top_n_intent(md) is False


def test_rank_with_row_number_filter_returns_false():
    """`ROW_NUMBER() ... WHERE rn <= N` is the canonical shape — not a collapse."""
    md = {
        "question_text": "Top 5 destination cities",
        "genie_sql": (
            "SELECT city FROM (SELECT city, ROW_NUMBER() OVER (ORDER BY p DESC) "
            "AS rn FROM t) WHERE rn <= 5"
        ),
    }
    assert metadata_indicates_top_n_intent(md) is False


def test_no_rank_function_returns_false():
    """SQL has no RANK/DENSE_RANK; can't be a top-N collapse pattern."""
    md = {
        "question_text": "Top 5 cities",
        "genie_sql": "SELECT city, sum(passengers) FROM t GROUP BY city",
    }
    assert metadata_indicates_top_n_intent(md) is False


def test_rank_without_top_keyword_returns_false():
    """RANK without ranking intent → don't override; could be windowed analytics."""
    md = {
        "question_text": "Show passengers per city ranked over time",
        "genie_sql": "SELECT city, RANK() OVER (ORDER BY p DESC) AS r FROM t",
    }
    assert metadata_indicates_top_n_intent(md) is False


def test_explicit_question_requests_exact_top_n_overrides_keyword_check():
    """If the producer already stamped `question_requests_exact_top_n=True`,
    skip the keyword sniff and trust the producer."""
    md = {
        "question_text": "irrelevant",  # producer already asserted intent
        "question_requests_exact_top_n": True,
        "genie_sql": "SELECT city, RANK() OVER (ORDER BY p DESC) FROM t",
    }
    assert metadata_indicates_top_n_intent(md) is True


def test_handles_missing_keys_gracefully():
    assert metadata_indicates_top_n_intent({}) is False
    assert metadata_indicates_top_n_intent({"question_text": "top 5"}) is False
    assert metadata_indicates_top_n_intent({"genie_sql": "RANK()"}) is False


def test_recognizes_dense_rank_function():
    md = {
        "question_text": "What are the top 3 highest-rated products?",
        "genie_sql": "SELECT name, DENSE_RANK() OVER (ORDER BY rating DESC) FROM p",
    }
    assert metadata_indicates_top_n_intent(md) is True


def test_recognizes_lowest_keyword():
    md = {
        "question_text": "Which 5 carriers had the lowest on-time rate?",
        "genie_sql": "SELECT carrier, RANK() OVER (ORDER BY rate ASC) FROM c",
    }
    assert metadata_indicates_top_n_intent(md) is True
