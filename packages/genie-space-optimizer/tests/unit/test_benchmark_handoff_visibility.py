from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.benchmarks import assert_benchmark_handoff_visible


def test_assert_benchmark_handoff_visible_allows_matching_counts() -> None:
    assert_benchmark_handoff_visible(
        expected_total=30,
        actual_total=30,
        domain="sales",
        table_name="cat.sch.genie_benchmarks_sales",
    )


def test_assert_benchmark_handoff_visible_fails_on_partial_dataset() -> None:
    with pytest.raises(RuntimeError, match="Benchmark handoff mismatch"):
        assert_benchmark_handoff_visible(
            expected_total=30,
            actual_total=16,
            domain="sales",
            table_name="cat.sch.genie_benchmarks_sales",
        )


def test_assert_benchmark_handoff_visible_ignores_zero_expected() -> None:
    assert_benchmark_handoff_visible(
        expected_total=0,
        actual_total=16,
        domain="sales",
        table_name="cat.sch.genie_benchmarks_sales",
    )
