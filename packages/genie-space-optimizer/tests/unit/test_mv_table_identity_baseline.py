"""MV-D116 identity pin: what splitting a measure by its tables may move.

Captured at 9d972e46, before the corpus scan told the same aggregate over two
tables apart. A measure seen over one table name keeps every key it had; only
``sum(amount)``, which this corpus runs over both orders and refunds, splits.
Regenerate only at a new base, from the repository root, with
``uv run --frozen --extra dev python packages/genie-space-optimizer/tests/unit/test_mv_table_identity_baseline.py``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import sqlglot
from genie_space_optimizer.optimization.mv_fingerprint import corpus_scan
from genie_space_optimizer.optimization.mv_state import mv_candidate_fingerprint

from genie_space_optimizer.optimization import mv_advisor

BASE = "9d972e46"
BASELINE_PATH = Path(__file__).parent / "data" / f"mv_identity_baseline_{BASE}.json"
SPACE = "01f04ac8c1f11c9a9e5b3b2b0e5d5c11"

STATEMENTS = {
    "orders_amount": "SELECT SUM(amount) AS total, region FROM main.sales.orders GROUP BY region",
    "refunds_amount": "SELECT SUM(amount) AS total, region FROM main.sales.refunds GROUP BY region",
    "orders_amount_join": (
        "SELECT SUM(amount) AS total FROM main.sales.orders o "
        "JOIN main.sales.refunds r ON o.order_id = r.order_id"
    ),
    "orders_count": "SELECT COUNT(order_id) AS n, region FROM main.sales.orders GROUP BY region",
    "refunds_count": "SELECT COUNT(refund_id) AS n, region FROM main.sales.refunds GROUP BY region",
    "customer_balance": "SELECT SUM(c_acctbal) AS bal FROM samples.tpch.customer",
    "customer_balance_short": "SELECT SUM(c.c_acctbal) AS bal FROM tpch.customer c",
    "lineitem_quantity": "SELECT SUM(l_quantity) AS q, l_shipmode FROM samples.tpch.lineitem GROUP BY l_shipmode",
    "lineitem_count": "SELECT COUNT(l_orderkey) AS n, l_shipmode FROM samples.tpch.lineitem GROUP BY l_shipmode",
    "lineitem_quantity_join": (
        "SELECT SUM(l_quantity) AS q FROM samples.tpch.lineitem l "
        "JOIN samples.tpch.orders o ON l.l_orderkey = o.o_orderkey"
    ),
}
COLLIDING_EXPR = "sum(amount)"
COLLIDING_LEAVES = frozenset({"orders", "refunds"})


def corpus() -> list[tuple[str, str]]:
    return [(sql, f"{key}_{i}") for key, sql in STATEMENTS.items() for i in range(8)]


def scan_identity() -> list[dict]:
    return [
        {
            "fingerprint": m.fingerprint,
            "canonical_expr": m.canonical_expr,
            "source_tables": list(m.source_tables),
            "recurrence": m.recurrence,
            "provenance_count": m.provenance_count,
            "has_unresolved_columns": m.has_unresolved_columns,
            "candidate_fingerprint": mv_candidate_fingerprint(
                SPACE, m.canonical_expr, m.source_tables
            ),
        }
        for m in corpus_scan(corpus()).measures
    ]


def bundle_identity() -> list[dict]:
    outcome = mv_advisor.advise_from_corpus(
        space_id=SPACE,
        run_id="run_baseline",
        corpus_entries=corpus(),
        applied_config=None,
        benchmarks=(),
        wide_schema_inventory=None,
        metric_view_reader=lambda tables: [],
        embedding_client=None,
        signal_reader=None,
        intent_texts=(),
        domain="",
        max_candidates=None,
        persist_proposal=lambda proposal, rendered: True,
        write_ddl_artifact=lambda proposal, rendered: True,
        read_suppressed_fingerprints=None,
    )
    return sorted(
        (
            {
                "dedup_fingerprint": p.dedup_fingerprint,
                "suggestion_id": p.suggestion_id,
                "source_tables": sorted(p.evidence.get("source_tables", ())),
                "members": sorted(
                    m["dedup_fingerprint"] for m in p.evidence.get("measures", ())
                ),
            }
            for p in outcome.proposals
        ),
        key=lambda b: b["dedup_fingerprint"],
    )


def capture() -> dict:
    return {
        "captured_at_head": BASE,
        "sqlglot": sqlglot.__version__,
        "space_id": SPACE,
        "scan": scan_identity(),
        "bundles": bundle_identity(),
    }


def _leaves(tables) -> set[str]:
    return {t.rsplit(".", 1)[-1] for t in tables}


def _untouched_measures(rows: list[dict]) -> list[dict]:
    return [r for r in rows if r["canonical_expr"] != COLLIDING_EXPR]


def _untouched_bundles(rows: list[dict]) -> list[dict]:
    return [b for b in rows if not (_leaves(b["source_tables"]) & COLLIDING_LEAVES)]


@pytest.fixture(scope="module")
def baseline() -> dict:
    return json.loads(BASELINE_PATH.read_text())


def test_baseline_was_captured_under_the_pinned_sqlglot(baseline):
    assert sqlglot.__version__ == "30.0.3"
    assert baseline["sqlglot"] == "30.0.3"
    assert baseline["captured_at_head"] == BASE


def test_the_baseline_holds_one_merged_row_for_the_colliding_measure(baseline):
    merged = [r for r in baseline["scan"] if r["canonical_expr"] == COLLIDING_EXPR]
    assert len(merged) == 1
    assert _leaves(merged[0]["source_tables"]) == COLLIDING_LEAVES


def test_measures_over_one_table_name_keep_their_identity(baseline):
    assert _untouched_measures(scan_identity()) == _untouched_measures(baseline["scan"])


def test_bundles_on_untouched_grains_keep_their_keys(baseline):
    kept = _untouched_bundles(baseline["bundles"])
    assert kept, "the baseline must hold at least one untouched bundle to pin"
    assert _untouched_bundles(bundle_identity()) == kept


def test_the_colliding_measure_now_splits_by_table(baseline):
    merged = next(r for r in baseline["scan"] if r["canonical_expr"] == COLLIDING_EXPR)
    split = [r for r in scan_identity() if r["canonical_expr"] == COLLIDING_EXPR]
    named = [r for r in split if r["source_tables"]]
    assert sorted(_leaves(r["source_tables"]) for r in named) == [{"orders"}, {"refunds"}]
    assert all(r["fingerprint"] == merged["fingerprint"] for r in split)
    assert merged["candidate_fingerprint"] not in {r["candidate_fingerprint"] for r in split}


# MV-D123: test_the_merged_key_the_advisor_rebuilds_is_the_one_recorded_at_base is replaced by the v2 baseline's map (test_mv_identity_v2_baseline.py).


LINEITEM = "samples.tpch.lineitem"
LINEITEM_EXPRS = tuple(
    f"{agg}({col})"
    for agg in ("SUM", "AVG", "MAX", "MIN")
    for col in ("l_quantity", "l_extendedprice", "l_discount", "l_tax")
)


def _cap_corpus(*, collide: bool) -> list[tuple[str, str]]:
    """``cap`` lineitem measures, plus ``SUM(amount)`` over orders and refunds
    recurring more often than any of them when ``collide``."""
    cap = mv_advisor.MV_ADVISOR_MAX_CANDIDATES
    assert 2 <= cap <= len(LINEITEM_EXPRS), "the pin needs cap single-table measures"
    entries = [
        (f"SELECT {expr} AS m, l_shipmode FROM {LINEITEM} GROUP BY l_shipmode", f"li{k}_{i}")
        for k, expr in enumerate(LINEITEM_EXPRS[:cap])
        for i in range(8)
    ]
    if collide:
        entries += [
            (f"SELECT SUM(amount) AS total, region FROM {table} GROUP BY region", f"{table}_{i}")
            for table in ("main.sales.orders", "main.sales.refunds")
            for i in range(9)
        ]
    return entries


def _advise(entries):
    return mv_advisor.advise_from_corpus(
        space_id=SPACE,
        run_id="run_cap",
        corpus_entries=entries,
        applied_config=None,
        benchmarks=(),
        wide_schema_inventory=None,
        metric_view_reader=lambda tables: [],
        embedding_client=None,
        signal_reader=None,
        intent_texts=(),
        domain="",
        max_candidates=None,
        persist_proposal=lambda proposal, rendered: True,
        write_ddl_artifact=lambda proposal, rendered: True,
        read_suppressed_fingerprints=None,
    )


def _members(outcome) -> dict[tuple[str, tuple[str, ...]], str]:
    return {
        (m["expr"], tuple(p.evidence.get("source_tables", ()))): m["dedup_fingerprint"]
        for p in outcome.proposals
        for m in p.evidence.get("measures", ())
    }


def test_split_halves_at_the_cap_move_membership_never_keys():
    cap = mv_advisor.MV_ADVISOR_MAX_CANDIDATES
    alone, colliding = _cap_corpus(collide=False), _cap_corpus(collide=True)
    before, after = _members(_advise(alone)), _members(_advise(colliding))
    assert len(before) == len(after) == cap

    pass_one = {
        mv_candidate_fingerprint(SPACE, m.canonical_expr, m.source_tables)
        for m in corpus_scan(colliding).measures
    }
    assert set(after.values()) <= pass_one
    shared = before.keys() & after.keys()
    assert {k: after[k] for k in shared} == {k: before[k] for k in shared}

    halves = sorted(tables for expr, tables in after if "amount" in expr)
    assert halves == [("main.sales.orders",), ("main.sales.refunds",)]
    # The halves take two slots, so two lineitem measures fall below the cap: the
    # lineitem bundle's membership, and so its bundle key, moves with them.
    assert len(shared) == cap - 2

    def lineitem_bundle(entries):
        return next(
            p for p in _advise(entries).proposals if p.evidence.get("source_tables") == [LINEITEM]
        )

    assert lineitem_bundle(colliding).dedup_fingerprint != lineitem_bundle(alone).dedup_fingerprint


if __name__ == "__main__":
    BASELINE_PATH.write_text(json.dumps(capture(), indent=2, sort_keys=True) + "\n")
    sys.exit(0)
