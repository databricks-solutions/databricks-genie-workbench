"""MV-D7/MV-D113 identity pin: fingerprints captured at ab2e68cf must never move.

Render changes (MV-D113) are free to change shipped SQL; they are not free to
change any identity a persisted candidate is keyed by.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import sqlglot
from genie_space_optimizer.optimization.mv_fingerprint import (
    canonicalize_sql_ast,
    corpus_scan,
    extract_dimensions,
    extract_filters,
    extract_join_keys,
    extract_measures,
    shapes_in_statement,
)
from genie_space_optimizer.optimization.mv_state import mv_candidate_fingerprint

from genie_space_optimizer.optimization import mv_advisor

BASELINE_PATH = Path(__file__).parent / "data" / "mv_identity_baseline_ab2e68cf.json"

SPACE = "01f04ac8c1f11c9a9e5b3b2b0e5d5c11"
LI = "samples.tpch.lineitem"
STATEMENTS = {
    "revenue": f"SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue, l_returnflag FROM {LI} GROUP BY l_returnflag",
    "revenue_alias": f"SELECT l.l_returnflag, SUM(l.l_extendedprice * (1 - l.l_discount)) FROM {LI} l GROUP BY 1",
    "count": f"SELECT COUNT(l_orderkey) AS orders, l_returnflag FROM {LI} GROUP BY l_returnflag",
    "mixed_case_literal": "SELECT SUM(CASE WHEN o.status = 'Paid' THEN o.amount END) FROM main.sales.orders o",
    "conditional_count": "SELECT COUNT(CASE WHEN o.Region = 'EMEA' THEN 1 END) FROM main.sales.orders o",
    "date_trunc": "SELECT DATE_TRUNC('MONTH', o.ts) AS m, COUNT(DISTINCT DATE_TRUNC('DAY', o.ts)) FROM main.sales.orders o GROUP BY 1",
    "datediff": "SELECT AVG(DATEDIFF(DAY, o.created, o.shipped)) FROM main.sales.orders o",
    "space_column": "SELECT SUM(o.`Order Amount`) FROM main.sales.orders o",
    "two_table": "SELECT SUM(l.qty * p.price) FROM main.sales.lines l JOIN main.sales.products p ON l.pid = p.id",
    "ratio": "SELECT SUM(CASE WHEN o_orderstatus = 'F' THEN o_totalprice ELSE 0 END) / SUM(o_totalprice) AS finished_share, o_orderpriority FROM samples.tpch.orders GROUP BY o_orderpriority",
    "pct_of_total": "SELECT o_orderpriority, SUM(o_totalprice) / SUM(SUM(o_totalprice)) OVER () FROM samples.tpch.orders GROUP BY o_orderpriority",
    "filtered": f"SELECT SUM(l_quantity) FROM {LI} WHERE l_shipdate >= DATE '1998-01-01' AND l_returnflag = 'R'",
}
BUNDLE_CORPUS_KEYS = ("revenue", "count", "mixed_case_literal", "conditional_count", "datediff", "ratio")


def statement_identity(sql: str) -> dict:
    return {
        "sql": sql,
        "canonical_sql": canonicalize_sql_ast(sql),
        "measures": [
            {
                "canonical_expr": m.canonical_expr,
                "fingerprint": m.fingerprint,
                "source_tables": list(m.source_tables),
                "candidate_fingerprint": mv_candidate_fingerprint(SPACE, m.canonical_expr, m.source_tables),
            }
            for m in extract_measures(sql)
        ],
        "dimensions": [[d.canonical_expr, d.fingerprint] for d in extract_dimensions(sql)],
        "filters": [[f.canonical_expr, f.fingerprint] for f in extract_filters(sql)],
        "join_keys": [[j.canonical_expr, j.fingerprint] for j in extract_join_keys(sql)],
        "shapes": [
            {"kind": s.kind, "fingerprint": s.fingerprint, "components": dict(s.components)}
            for s in shapes_in_statement(sql)
        ],
    }


def bundle_corpus() -> list[tuple[str, str]]:
    return [(STATEMENTS[k], f"{k}_{i}") for k in BUNDLE_CORPUS_KEYS for i in range(8)]


def bundle_fingerprints(corpus: list[tuple[str, str]]) -> list[dict]:
    outcome = mv_advisor.advise_from_corpus(
        space_id=SPACE, run_id="run_baseline", corpus_entries=corpus,
        applied_config=None, benchmarks=(), wide_schema_inventory=None,
        metric_view_reader=lambda tables: [], embedding_client=None,
        signal_reader=None, intent_texts=(), domain="", max_candidates=None,
        persist_proposal=lambda p, r: True, write_ddl_artifact=lambda p, r: True,
        read_suppressed_fingerprints=None,
    )
    bundles = [
        {
            "dedup_fingerprint": p.dedup_fingerprint,
            "members": sorted(m["dedup_fingerprint"] for m in p.evidence.get("measures", [])),
        }
        for p in outcome.proposals
    ]
    return sorted(bundles, key=lambda b: b["dedup_fingerprint"])


def capture() -> dict:
    corpus = bundle_corpus()
    scan = corpus_scan(corpus)
    return {
        "captured_at_head": "ab2e68cf",
        "sqlglot": sqlglot.__version__,
        "space_id": SPACE,
        "statements": {k: statement_identity(v) for k, v in STATEMENTS.items()},
        "corpus_scan_measures": [[m.fingerprint, m.canonical_expr, m.recurrence] for m in scan.measures],
        "corpus_scan_shapes": [[s.fingerprint, s.kind, s.recurrence] for s in scan.shapes],
        "bundles": bundle_fingerprints(corpus),
    }


@pytest.fixture(scope="module")
def baseline() -> dict:
    return json.loads(BASELINE_PATH.read_text())


def test_baseline_was_captured_under_the_pinned_sqlglot(baseline: dict) -> None:
    assert baseline["sqlglot"] == sqlglot.__version__ == "30.0.3"


@pytest.mark.parametrize("key", sorted(STATEMENTS))
def test_statement_identity_matches_the_baseline(baseline: dict, key: str) -> None:
    assert statement_identity(STATEMENTS[key]) == baseline["statements"][key]


def test_corpus_scan_identity_matches_the_baseline(baseline: dict) -> None:
    scan = corpus_scan(bundle_corpus())
    assert [[m.fingerprint, m.canonical_expr, m.recurrence] for m in scan.measures] == baseline["corpus_scan_measures"]
    assert [[s.fingerprint, s.kind, s.recurrence] for s in scan.shapes] == baseline["corpus_scan_shapes"]


def test_bundle_fingerprints_match_the_baseline(baseline: dict) -> None:
    assert bundle_fingerprints(bundle_corpus()) == baseline["bundles"]


if __name__ == "__main__":
    # Regenerate only for a deliberate MV-D7 identity change, recorded as a decision.
    json.dump(capture(), sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
