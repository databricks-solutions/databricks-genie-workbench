"""MV-D117 identity pin: what row-count attribution and struct paths may move.

Captured at 43b01544, before a column-free aggregate took its statement's table
and before ``alias.col.field`` kept its struct path. Only ``count(?n)`` and the
struct measure move; every other measure keeps every key it had, and so do the
bundles on grains neither touches. Regenerate only at a new base, with
``python`` on this file.
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

BASE = "43b01544"
BASELINE_PATH = Path(__file__).parent / "data" / f"mv_identity_baseline_{BASE}.json"
SPACE = "01f04ac8c1f11c9a9e5b3b2b0e5d5c11"

STATEMENTS = {
    "orders_rows": "SELECT COUNT(*) AS n, region FROM main.sales.orders GROUP BY region",
    "refunds_rows": "SELECT COUNT(*) AS n, region FROM main.sales.refunds GROUP BY region",
    "joined_rows": (
        "SELECT COUNT(*) AS n FROM main.sales.orders o "
        "JOIN main.sales.refunds r ON o.order_id = r.order_id"
    ),
    "orders_amount": "SELECT SUM(amount) AS total, region FROM main.sales.orders GROUP BY region",
    "orders_amount_schema_qualified": "SELECT SUM(sales.orders.amount) AS total FROM main.sales.orders",
    "orders_fee_struct": "SELECT SUM(o.payload.fee) AS fees FROM main.sales.orders o",
    "customer_balance": "SELECT SUM(c_acctbal) AS bal FROM samples.tpch.customer",
    "lineitem_quantity": "SELECT SUM(l_quantity) AS q, l_shipmode FROM samples.tpch.lineitem GROUP BY l_shipmode",
    "lineitem_count": "SELECT COUNT(l_orderkey) AS n, l_shipmode FROM samples.tpch.lineitem GROUP BY l_shipmode",
}
ROW_COUNT_EXPR = "count(?n)"
STRUCT_EXPR_BEFORE = "sum(fee)"
STRUCT_EXPR_AFTER = "sum(payload.fee)"
MOVED_EXPRS = {ROW_COUNT_EXPR, STRUCT_EXPR_BEFORE, STRUCT_EXPR_AFTER}
MOVED_LEAVES = frozenset({"orders", "refunds"})


def corpus() -> list[tuple[str, str]]:
    return [(sql, f"{key}_{i}") for key, sql in STATEMENTS.items() for i in range(8)]


def scan_identity() -> list[dict]:
    return [
        {
            "fingerprint": m.fingerprint,
            "canonical_expr": m.canonical_expr,
            "source_tables": list(m.source_tables),
            "source_columns": list(m.source_columns),
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
    return [r for r in rows if r["canonical_expr"] not in MOVED_EXPRS]


def _untouched_bundles(rows: list[dict]) -> list[dict]:
    return [b for b in rows if not (_leaves(b["source_tables"]) & MOVED_LEAVES)]


@pytest.fixture(scope="module")
def baseline() -> dict:
    return json.loads(BASELINE_PATH.read_text())


def test_baseline_was_captured_under_the_pinned_sqlglot(baseline):
    assert sqlglot.__version__ == "30.0.3"
    assert baseline["sqlglot"] == "30.0.3"
    assert baseline["captured_at_head"] == BASE


def test_the_baseline_holds_one_table_less_row_count(baseline):
    rows = [r for r in baseline["scan"] if r["canonical_expr"] == ROW_COUNT_EXPR]
    assert len(rows) == 1
    assert rows[0]["source_tables"] == []


def test_the_baseline_holds_the_struct_measure_as_an_unresolved_leaf(baseline):
    rows = [r for r in baseline["scan"] if r["canonical_expr"] == STRUCT_EXPR_BEFORE]
    assert len(rows) == 1
    assert rows[0]["source_tables"] == [] and rows[0]["has_unresolved_columns"] is True


def test_measures_other_than_row_counts_and_struct_fields_keep_their_identity(baseline):
    assert _untouched_measures(scan_identity()) == _untouched_measures(baseline["scan"])


def test_bundles_on_untouched_grains_keep_their_keys(baseline):
    kept = _untouched_bundles(baseline["bundles"])
    assert kept, "the baseline must hold at least one untouched bundle to pin"
    assert _untouched_bundles(bundle_identity()) == kept


def test_row_counts_split_by_their_single_table():
    rows = [r for r in scan_identity() if r["canonical_expr"] == ROW_COUNT_EXPR]
    assert sorted(sorted(_leaves(r["source_tables"])) for r in rows) == [
        [], ["orders"], ["refunds"],
    ]


def test_the_struct_measure_resolves_to_its_table_with_its_path():
    rows = {r["canonical_expr"]: r for r in scan_identity()}
    assert STRUCT_EXPR_BEFORE not in rows
    after = rows[STRUCT_EXPR_AFTER]
    assert _leaves(after["source_tables"]) == {"orders"}
    assert after["source_columns"] == ["payload"]
    assert after["has_unresolved_columns"] is False


if __name__ == "__main__":
    BASELINE_PATH.write_text(json.dumps(capture(), indent=2, sort_keys=True) + "\n")
    sys.exit(0)
