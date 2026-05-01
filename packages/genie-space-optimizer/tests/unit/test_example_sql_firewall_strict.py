"""Strict-mode firewall: SQL fingerprint or n-gram overlap blocks
example-SQL candidates regardless of question similarity. Toggleable
via GSO_EXAMPLE_SQL_FIREWALL_STRICT (default true)."""
import os

import pytest

from genie_space_optimizer.optimization.leakage import (
    BenchmarkCorpus,
    LeakageOracle,
)


@pytest.fixture
def benchmark_corpus():
    return BenchmarkCorpus.from_benchmarks([
        {
            "id": "b1",
            "question": "What is total revenue by region?",
            "expected_sql": (
                "SELECT region, SUM(amount) AS total FROM sales GROUP BY region"
            ),
        },
    ])


def test_strict_mode_blocks_sql_fingerprint_match(monkeypatch, benchmark_corpus):
    monkeypatch.setenv("GSO_EXAMPLE_SQL_FIREWALL_STRICT", "true")
    oracle = LeakageOracle(benchmark_corpus)
    decision = oracle.evaluate_example_sql(
        question="Show me sales totals per geography",
        sql="SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
    )
    assert decision.block is True
    assert decision.reason == "sql_pattern_overlap_strict"


def test_relaxed_mode_warns_only_on_sql_overlap(monkeypatch, benchmark_corpus):
    monkeypatch.setenv("GSO_EXAMPLE_SQL_FIREWALL_STRICT", "false")
    oracle = LeakageOracle(benchmark_corpus)
    decision = oracle.evaluate_example_sql(
        question="Show me sales totals per geography",
        sql="SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
    )
    assert decision.block is False
    assert decision.warning is True
    assert decision.reason == "sql_pattern_overlap_warning"


def test_default_is_strict(monkeypatch, benchmark_corpus):
    monkeypatch.delenv("GSO_EXAMPLE_SQL_FIREWALL_STRICT", raising=False)
    oracle = LeakageOracle(benchmark_corpus)
    decision = oracle.evaluate_example_sql(
        question="completely unrelated question about widgets",
        sql="SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
    )
    assert decision.block is True


def test_no_overlap_passes_in_strict_mode(monkeypatch, benchmark_corpus):
    monkeypatch.setenv("GSO_EXAMPLE_SQL_FIREWALL_STRICT", "true")
    oracle = LeakageOracle(benchmark_corpus)
    decision = oracle.evaluate_example_sql(
        question="What products have the highest return rate?",
        sql="SELECT product_id, COUNT(*) FROM returns GROUP BY product_id",
    )
    assert decision.block is False
    assert decision.warning is False
