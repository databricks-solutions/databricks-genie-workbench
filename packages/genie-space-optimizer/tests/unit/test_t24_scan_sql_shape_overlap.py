"""Plan C2 — pin the flag-off / flag-on matrix for the shape-overlap
gate in ``_t24_counterfactual_scan``.

Test matrix:
  - Flag OFF + table-only overlap → stamped as high-risk (today's
    behaviour, preserved by default).
  - Flag ON + table overlap + shape overlap → stamped as high-risk.
  - Flag ON + table overlap + NO shape overlap → NOT stamped (the
    bug fix; today this would be a false positive).
  - Flag ON + non-snippet proposal (empty shape tokens) → fall back to
    table-only matching (current behaviour preserved for non-L6).
"""
from __future__ import annotations

from typing import Any


def _bench(qid: str, *, required_tables=None, required_columns=None,
           expected_sql: str = "") -> dict:
    return {
        "id": qid,
        "required_tables": list(required_tables or ()),
        "required_columns": list(required_columns or ()),
        "expected_response": expected_sql,
    }


def _ag(affected: list[str]) -> dict:
    return {"id": "AG_TEST_SHAPE", "affected_questions": list(affected)}


def test_flag_off_preserves_table_only_match(monkeypatch: Any) -> None:
    """Today's behaviour: a snippet patch on table ``orders`` flags
    every passing benchmark whose required_tables includes ``orders``,
    regardless of which column the snippet references."""
    monkeypatch.setenv("GSO_SQL_SHAPE_OVERLAP_GATE", "0")
    from genie_space_optimizer.optimization.harness import (
        _t24_counterfactual_scan,
    )
    snippet = {
        "type": "add_sql_snippet_filter",
        "proposal_id": "P1",
        "target": "catalog.schema.orders",
        "column": "is_refunded",
        "sql": "is_refunded = TRUE",
    }
    benchmarks = [
        # Three benchmarks ALL referencing ``orders`` but none using
        # the snippet's column ``is_refunded``.
        _bench("gs_010", required_tables=["catalog.schema.orders"]),
        _bench("gs_020", required_tables=["catalog.schema.orders"]),
        _bench("gs_030", required_tables=["catalog.schema.orders"]),
    ]
    _t24_counterfactual_scan(
        all_proposals=[snippet],
        benchmarks=benchmarks,
        ag=_ag(["gs_001"]),
        prev_failure_qids={"gs_001"},
    )
    # Flag off → table match dominates → high_collateral_risk fires.
    # (threshold = 2 * 1 affected = 2; we have 3 dependents.)
    assert snippet.get("high_collateral_risk") is True
    assert len(snippet.get("passing_dependents", [])) == 3


def test_flag_on_with_shape_overlap_stamps_high_risk(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_SQL_SHAPE_OVERLAP_GATE", "1")
    from genie_space_optimizer.optimization.harness import (
        _t24_counterfactual_scan,
    )
    snippet = {
        "type": "add_sql_snippet_filter",
        "proposal_id": "P1",
        "target": "catalog.schema.orders",
        "column": "is_refunded",
        "sql": "is_refunded = TRUE",
    }
    benchmarks = [
        # Three benchmarks on ``orders`` that ALSO reference
        # ``is_refunded`` in their SQL. Both table AND shape match.
        _bench("gs_010", required_tables=["catalog.schema.orders"],
               expected_sql="SELECT * FROM orders WHERE is_refunded"),
        _bench("gs_020", required_tables=["catalog.schema.orders"],
               required_columns=["orders.is_refunded"]),
        _bench("gs_030", required_tables=["catalog.schema.orders"],
               expected_sql="SELECT id FROM orders WHERE is_refunded = TRUE"),
    ]
    _t24_counterfactual_scan(
        all_proposals=[snippet],
        benchmarks=benchmarks,
        ag=_ag(["gs_001"]),
        prev_failure_qids={"gs_001"},
    )
    assert snippet.get("high_collateral_risk") is True
    assert len(snippet.get("passing_dependents", [])) == 3


def test_flag_on_without_shape_overlap_does_not_stamp(monkeypatch: Any) -> None:
    """THE BUG FIX. Pre-Plan-C, this case is a false positive: all
    three benchmarks reference ``orders`` (the target table) but none
    use ``is_refunded`` (the snippet's column), yet ``high_collateral_risk``
    fires anyway because the table-only check matches."""
    monkeypatch.setenv("GSO_SQL_SHAPE_OVERLAP_GATE", "1")
    from genie_space_optimizer.optimization.harness import (
        _t24_counterfactual_scan,
    )
    snippet = {
        "type": "add_sql_snippet_filter",
        "proposal_id": "P1",
        "target": "catalog.schema.orders",
        "column": "is_refunded",
        "sql": "is_refunded = TRUE",
    }
    benchmarks = [
        # Benchmarks reference ``orders`` but NEVER ``is_refunded``.
        _bench("gs_010", required_tables=["catalog.schema.orders"],
               expected_sql="SELECT customer_id FROM orders"),
        _bench("gs_020", required_tables=["catalog.schema.orders"],
               required_columns=["orders.total"]),
        _bench("gs_030", required_tables=["catalog.schema.orders"],
               expected_sql="SELECT COUNT(*) FROM orders"),
    ]
    _t24_counterfactual_scan(
        all_proposals=[snippet],
        benchmarks=benchmarks,
        ag=_ag(["gs_001"]),
        prev_failure_qids={"gs_001"},
    )
    # Flag on + table overlap + NO shape overlap → no dependents
    # → no high_collateral_risk stamp.
    assert snippet.get("high_collateral_risk", False) is False
    assert snippet.get("passing_dependents") == []


def test_flag_on_non_snippet_proposal_falls_back_to_table_only(monkeypatch: Any) -> None:
    """Non-snippet proposals have no shape tokens. The shape-overlap
    gate degenerates to the legacy table-only check so their behaviour
    is unchanged."""
    monkeypatch.setenv("GSO_SQL_SHAPE_OVERLAP_GATE", "1")
    from genie_space_optimizer.optimization.harness import (
        _t24_counterfactual_scan,
    )
    instr = {
        "type": "rewrite_instruction",
        "proposal_id": "P1",
        "target": "catalog.schema.orders",
        # No ``column`` and no ``sql`` → empty shape tokens.
    }
    benchmarks = [
        _bench("gs_010", required_tables=["catalog.schema.orders"]),
        _bench("gs_020", required_tables=["catalog.schema.orders"]),
        _bench("gs_030", required_tables=["catalog.schema.orders"]),
    ]
    _t24_counterfactual_scan(
        all_proposals=[instr],
        benchmarks=benchmarks,
        ag=_ag(["gs_001"]),
        prev_failure_qids={"gs_001"},
    )
    # Empty shape tokens → fall back to table-only → high_risk fires.
    assert instr.get("high_collateral_risk") is True
    assert len(instr.get("passing_dependents", [])) == 3


def test_flag_on_threshold_still_enforced(monkeypatch: Any) -> None:
    """A snippet with shape overlap on ONE dependent doesn't trip the
    threshold (>= 2 * affected). Stamping still depends on the count
    being high enough."""
    monkeypatch.setenv("GSO_SQL_SHAPE_OVERLAP_GATE", "1")
    from genie_space_optimizer.optimization.harness import (
        _t24_counterfactual_scan,
    )
    snippet = {
        "type": "add_sql_snippet_filter",
        "proposal_id": "P1",
        "target": "catalog.schema.orders",
        "column": "is_refunded",
        "sql": "is_refunded = TRUE",
    }
    benchmarks = [
        _bench("gs_010", required_tables=["catalog.schema.orders"],
               expected_sql="SELECT * FROM orders WHERE is_refunded"),
    ]
    _t24_counterfactual_scan(
        all_proposals=[snippet],
        benchmarks=benchmarks,
        ag=_ag(["gs_001"]),
        prev_failure_qids={"gs_001"},
    )
    # Threshold = 2 * 1 = 2; only 1 dependent → not high risk.
    assert snippet.get("high_collateral_risk", False) is False
    assert snippet.get("passing_dependents") == ["gs_010"]
