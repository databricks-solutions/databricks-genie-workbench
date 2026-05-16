"""Plan C2 — unit tests for the SQL-shape overlap helper and feature
flag used by ``_t24_counterfactual_scan``.

Suite ordering:
  - Flag tests (this file's first block).
  - Shape-token extraction (``extract_snippet_shape_tokens``).
  - Benchmark overlap predicate (``benchmark_has_shape_overlap``).
"""
from __future__ import annotations

from typing import Any


def test_flag_defaults_off(monkeypatch: Any) -> None:
    monkeypatch.delenv("GSO_SQL_SHAPE_OVERLAP_GATE", raising=False)
    from genie_space_optimizer.common.config import (
        sql_shape_overlap_gate_enabled,
    )
    assert sql_shape_overlap_gate_enabled() is False


def test_flag_on_when_env_var_set_to_one(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_SQL_SHAPE_OVERLAP_GATE", "1")
    from genie_space_optimizer.common.config import (
        sql_shape_overlap_gate_enabled,
    )
    assert sql_shape_overlap_gate_enabled() is True


def test_flag_accepts_truthy_strings(monkeypatch: Any) -> None:
    from genie_space_optimizer.common.config import (
        sql_shape_overlap_gate_enabled,
    )
    for v in ("true", "TRUE", "yes", "Yes", "Y", "1"):
        monkeypatch.setenv("GSO_SQL_SHAPE_OVERLAP_GATE", v)
        assert sql_shape_overlap_gate_enabled() is True, v
    for v in ("0", "false", "no", "off", "", "garbage"):
        monkeypatch.setenv("GSO_SQL_SHAPE_OVERLAP_GATE", v)
        assert sql_shape_overlap_gate_enabled() is False, v


def test_extract_shape_tokens_returns_empty_for_non_dict() -> None:
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        extract_snippet_shape_tokens,
    )
    assert extract_snippet_shape_tokens(None) == frozenset()
    assert extract_snippet_shape_tokens("not a dict") == frozenset()
    assert extract_snippet_shape_tokens([]) == frozenset()


def test_extract_shape_tokens_returns_empty_when_no_sql_and_no_column() -> None:
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        extract_snippet_shape_tokens,
    )
    assert extract_snippet_shape_tokens({}) == frozenset()
    assert extract_snippet_shape_tokens({
        "patch_type": "add_sql_snippet_filter",
        "target": "catalog.schema.t",
    }) == frozenset()


def test_extract_shape_tokens_includes_column_field() -> None:
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        extract_snippet_shape_tokens,
    )
    patch = {
        "patch_type": "add_sql_snippet_filter",
        "column": "is_refunded",
        "sql": "",
    }
    assert "is_refunded" in extract_snippet_shape_tokens(patch)


def test_extract_shape_tokens_parses_identifiers_from_sql_body() -> None:
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        extract_snippet_shape_tokens,
    )
    patch = {
        "patch_type": "add_sql_snippet_measure",
        "column": "revenue",
        "sql": "SUM(price * quantity)",
    }
    tokens = extract_snippet_shape_tokens(patch)
    assert "revenue" in tokens
    assert "price" in tokens
    assert "quantity" in tokens
    # SUM is a SQL keyword stopword — filtered out.
    assert "sum" not in tokens


def test_extract_shape_tokens_filters_sql_keywords() -> None:
    """Common SQL keywords must NOT appear in the token set (they would
    flag every benchmark and defeat the purpose of the gate)."""
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        extract_snippet_shape_tokens,
    )
    patch = {
        "sql": (
            "SELECT id FROM orders WHERE status = 'shipped' "
            "AND total > 100 GROUP BY id ORDER BY total DESC LIMIT 10"
        ),
    }
    tokens = extract_snippet_shape_tokens(patch)
    for kw in (
        "select", "from", "where", "and", "or", "group", "by", "order",
        "limit", "desc", "having", "join", "left", "right", "inner",
        "outer", "with", "distinct", "true", "false", "null", "is",
        "not", "in", "between", "like", "as", "on", "case", "when",
        "then", "else", "end",
        # Aggregate function names also blocked:
        "sum", "avg", "count", "min", "max",
    ):
        assert kw not in tokens, f"keyword {kw} leaked into token set"
    # Real identifiers survive.
    assert "orders" in tokens
    assert "status" in tokens
    assert "total" in tokens


def test_extract_shape_tokens_filters_short_tokens() -> None:
    """Tokens shorter than 3 chars are too noisy (single letters,
    numbers like ``id``) and would flag too many benchmarks."""
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        extract_snippet_shape_tokens,
    )
    patch = {"sql": "x = y AND a > b"}
    tokens = extract_snippet_shape_tokens(patch)
    # x, y, a, b are all 1 char — filtered.
    assert tokens == frozenset()


def test_extract_shape_tokens_includes_two_char_identifiers_via_column() -> None:
    """The ``column`` field bypasses the short-token filter because
    operators sometimes use 2-char column names (``id``, ``ts``)."""
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        extract_snippet_shape_tokens,
    )
    patch = {"column": "id", "sql": ""}
    assert "id" in extract_snippet_shape_tokens(patch)


def test_extract_shape_tokens_lowercases() -> None:
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        extract_snippet_shape_tokens,
    )
    patch = {"column": "Revenue", "sql": "SUM(Price * Quantity)"}
    tokens = extract_snippet_shape_tokens(patch)
    assert "revenue" in tokens
    assert "price" in tokens
    assert "quantity" in tokens
    # No casing leakage.
    assert "Revenue" not in tokens
    assert "Price" not in tokens


def test_benchmark_overlap_returns_false_for_empty_shape_tokens() -> None:
    """When shape tokens are empty, the caller falls back to
    table-only matching. The predicate returns False to signal "shape
    check did not match" — the caller composes this with the existing
    table-name check via the convention "empty shape tokens means
    skip shape gate"."""
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        benchmark_has_shape_overlap,
    )
    benchmark = {
        "id": "gs_001",
        "required_columns": ["orders.total"],
        "expected_response": "SELECT total FROM orders",
    }
    assert benchmark_has_shape_overlap(benchmark, frozenset()) is False


def test_benchmark_overlap_matches_required_columns_tail() -> None:
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        benchmark_has_shape_overlap,
    )
    benchmark = {
        "id": "gs_001",
        "required_columns": ["catalog.schema.orders.total"],
        "expected_response": "",
    }
    assert benchmark_has_shape_overlap(
        benchmark, frozenset({"total"}),
    ) is True


def test_benchmark_overlap_matches_required_columns_exact() -> None:
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        benchmark_has_shape_overlap,
    )
    benchmark = {
        "id": "gs_001",
        "required_columns": ["total"],
        "expected_response": "",
    }
    assert benchmark_has_shape_overlap(
        benchmark, frozenset({"total"}),
    ) is True


def test_benchmark_overlap_matches_expected_sql() -> None:
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        benchmark_has_shape_overlap,
    )
    benchmark = {
        "id": "gs_001",
        "required_columns": [],
        "expected_response": (
            "SELECT SUM(price * quantity) FROM orders"
        ),
    }
    assert benchmark_has_shape_overlap(
        benchmark, frozenset({"quantity"}),
    ) is True
    assert benchmark_has_shape_overlap(
        benchmark, frozenset({"price"}),
    ) is True


def test_benchmark_overlap_matches_expected_sql_via_alias() -> None:
    """The benchmark may carry the expected SQL under
    ``expected_sql`` or ``ground_truth_sql`` instead of
    ``expected_response``. All three are checked."""
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        benchmark_has_shape_overlap,
    )
    benchmark = {
        "id": "gs_001",
        "required_columns": [],
        "ground_truth_sql": "SELECT revenue FROM orders",
    }
    assert benchmark_has_shape_overlap(
        benchmark, frozenset({"revenue"}),
    ) is True


def test_benchmark_overlap_returns_false_when_no_tokens_match() -> None:
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        benchmark_has_shape_overlap,
    )
    benchmark = {
        "id": "gs_001",
        "required_columns": ["orders.customer_id"],
        "expected_response": "SELECT customer_id FROM orders",
    }
    assert benchmark_has_shape_overlap(
        benchmark, frozenset({"revenue", "discount"}),
    ) is False


def test_benchmark_overlap_word_boundary_in_sql() -> None:
    """Substring matches in SQL must be word-boundary-aware so a token
    ``status`` doesn't match ``order_status_history``. Implementation
    matches via regex with ``\\b`` boundaries."""
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        benchmark_has_shape_overlap,
    )
    benchmark = {
        "id": "gs_001",
        "required_columns": [],
        "expected_response": (
            "SELECT * FROM order_status_history WHERE event_type='paid'"
        ),
    }
    # ``status`` is contained in ``order_status_history`` but only as a
    # SUBSTRING, not as a standalone token. Must NOT match.
    assert benchmark_has_shape_overlap(
        benchmark, frozenset({"status"}),
    ) is False
    # ``event_type`` IS a standalone token. Must match.
    assert benchmark_has_shape_overlap(
        benchmark, frozenset({"event_type"}),
    ) is True


def test_benchmark_overlap_handles_non_dict_benchmark() -> None:
    from genie_space_optimizer.optimization.sql_shape_overlap import (
        benchmark_has_shape_overlap,
    )
    assert benchmark_has_shape_overlap(None, frozenset({"x"})) is False
    assert benchmark_has_shape_overlap("not a dict", frozenset({"x"})) is False
