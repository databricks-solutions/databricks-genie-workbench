"""MV-D123 identity map: the v1 measure identity of every identity-v2 residual.

Captured at 3e71d66f, before identity v2 changed any code. Each statement names
the residual it exercises and whether its measure key is a control (v2 keeps
the captured key) or a mover (v2 moves it, as ``expected_v2`` says). Beside the
keys, the map records each statement's extracted occurrences as v1 grouped
them, so the frozen v1 grouping can rebuild every captured key after the
extractor fixes for R4 and R6 change what extraction returns. Regenerate only
at a new base, from the repository root, with
``PYTHONPATH=packages/genie-space-optimizer/tests/unit uv run --frozen --extra dev python -m test_mv_identity_v2_baseline``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import sqlglot
from genie_space_optimizer.optimization import mv_advisor
from genie_space_optimizer.optimization.mv_fingerprint import (
    FingerprintRecurrence,
    corpus_scan,
    extract_measures,
)
from genie_space_optimizer.optimization.mv_scoring import suggestion_id_for
from genie_space_optimizer.optimization.mv_state import (
    mv_bundle_fingerprint,
    mv_candidate_fingerprint,
)
from genie_space_optimizer.optimization.mv_tables import TableResolver

BASE = "3e71d66f"
BASELINE_PATH = Path(__file__).parent / "data" / f"mv_identity_baseline_{BASE}.json"
SPACE = "01f04ac8c1f11c9a9e5b3b2b0e5d5c11"
CAP = 12
"""Pinned rather than read from ``GSO_MV_ADVISOR_MAX_CANDIDATES``, so the cap
sits inside the ``samples.tpch.part`` grain at base, which v2 then moves."""

SPACE_TABLES = (
    "east.ops.shipments",
    "main.sales.orders",
    "main.sales.payload",
    "main.sales.refunds",
    "samples.tpch.customer",
    "samples.tpch.lineitem",
    "samples.tpch.part",
    "west.ops.shipments",
)
APPLIED_CONFIG = {"data_sources": {"tables": [{"identifier": t} for t in SPACE_TABLES]}}

CONTROL = "control"
MOVER = "mover"

STATEMENTS: dict[str, dict] = {
    "control_customer_balance": {
        "sql": "SELECT SUM(c_acctbal) AS bal FROM samples.tpch.customer",
        "residual": "three-part control",
        "role": CONTROL,
        "repeats": 12,
        "expected_v2": "same key",
    },
    "control_lineitem_quantity": {
        "sql": "SELECT SUM(l_quantity) AS q, l_shipmode FROM samples.tpch.lineitem GROUP BY l_shipmode",
        "residual": "three-part control",
        "role": CONTROL,
        "repeats": 12,
        "expected_v2": "same key",
    },
    "control_lineitem_count": {
        "sql": "SELECT COUNT(l_orderkey) AS n, l_shipmode FROM samples.tpch.lineitem GROUP BY l_shipmode",
        "residual": "three-part control",
        "role": CONTROL,
        "repeats": 12,
        "expected_v2": "same key",
    },
    "two_catalogs_east": {
        "sql": "SELECT SUM(weight) AS w FROM east.ops.shipments",
        "residual": "R1 two catalogs on one leaf",
        "role": MOVER,
        "repeats": 8,
        "expected_v2": "own row over east.ops.shipments",
    },
    "two_catalogs_west": {
        "sql": "SELECT SUM(weight) AS w FROM west.ops.shipments",
        "residual": "R1 two catalogs on one leaf",
        "role": MOVER,
        "repeats": 8,
        "expected_v2": "own row over west.ops.shipments",
    },
    "orders_spelled_three_part": {
        "sql": "SELECT SUM(amount) AS total, region FROM main.sales.orders GROUP BY region",
        "residual": "one table spelled three-, two- and one-part",
        "role": MOVER,
        "repeats": 8,
        "expected_v2": "one row over main.sales.orders",
    },
    "orders_spelled_two_part": {
        "sql": "SELECT SUM(amount) AS total FROM sales.orders",
        "residual": "one table spelled three-, two- and one-part",
        "role": MOVER,
        "repeats": 8,
        "expected_v2": "one row over main.sales.orders",
    },
    "orders_spelled_one_part": {
        "sql": "SELECT SUM(amount) AS total FROM orders",
        "residual": "one table spelled three-, two- and one-part",
        "role": MOVER,
        "repeats": 8,
        "expected_v2": "one row over main.sales.orders",
    },
    "unqualified_unique_leaf": {
        "sql": "SELECT MAX(refund_amount) AS m FROM refunds",
        "residual": "unqualified unique leaf",
        "role": MOVER,
        "repeats": 8,
        "expected_v2": "row over main.sales.refunds",
    },
    "unqualified_ambiguous_leaf": {
        "sql": "SELECT MIN(weight) AS lightest FROM shipments",
        "residual": "unqualified ambiguous leaf",
        "role": CONTROL,
        "repeats": 8,
        "expected_v2": "same key over the spelled name, refused as unresolved_table",
    },
    "derived_table_row_count": {
        "sql": "SELECT COUNT(*) AS n FROM (SELECT DISTINCT customer_id FROM main.sales.orders) sub",
        "residual": "R4 derived-table COUNT(*)",
        "role": MOVER,
        "repeats": 8,
        "expected_v2": "unresolved, never on main.sales.orders",
    },
    "cte_row_count": {
        "sql": (
            "WITH buyers AS (SELECT DISTINCT customer_id FROM main.sales.orders) "
            "SELECT COUNT(*) AS n FROM buyers"
        ),
        "residual": "R4 CTE COUNT(*)",
        "role": MOVER,
        "repeats": 8,
        "expected_v2": "unresolved, never on main.sales.orders",
    },
    "struct_field_beside_payload_join": {
        "sql": (
            "SELECT SUM(o.payload.fee) AS fees FROM main.sales.orders o "
            "JOIN main.sales.payload p ON o.order_id = p.order_id"
        ),
        "residual": "R6 o.payload.fee beside JOIN payload p",
        "role": MOVER,
        "repeats": 8,
        "expected_v2": "sum(payload.fee) over main.sales.orders",
    },
    "split_tax_orders": {
        "sql": "SELECT SUM(tax) AS t FROM main.sales.orders",
        "residual": "R5 split measure, orders half",
        "role": CONTROL,
        "repeats": 8,
        "expected_v2": "same key",
    },
    "split_tax_refunds": {
        "sql": "SELECT SUM(tax) AS t FROM main.sales.refunds",
        "residual": "R5 split measure, refunds half",
        "role": CONTROL,
        "repeats": 8,
        "expected_v2": "same key",
    },
    "tableless_history_row": {
        "sql": "SELECT SUM(tax) AS t",
        "residual": "R5 table-less history row",
        "role": CONTROL,
        "repeats": 8,
        "expected_v2": "same table-less key; joins neither half",
    },
    "c8_join_measure": {
        "sql": (
            "SELECT SUM(o.unit_price * r.quantity) AS v FROM main.sales.orders o "
            "JOIN main.sales.refunds r ON o.order_id = r.order_id"
        ),
        "residual": "R9 C-8 join measure",
        "role": CONTROL,
        "repeats": 8,
        "expected_v2": "same key; no longer the orders half's merged key",
    },
    "c8_single_table_measure": {
        "sql": "SELECT SUM(unit_price * quantity) AS v FROM main.sales.orders",
        "residual": "R9 C-8 single-table half",
        "role": CONTROL,
        "repeats": 8,
        "expected_v2": "same key; merged key retired",
    },
    "cap_tail_part_price": {
        "sql": "SELECT SUM(p_retailprice) AS p FROM samples.tpch.part",
        "residual": "R2 cap-near split",
        "role": CONTROL,
        "repeats": 6,
        "expected_v2": "same key; the part bundle's membership moves at the cap",
    },
    "cap_tail_part_size": {
        "sql": "SELECT AVG(p_size) AS s FROM samples.tpch.part",
        "residual": "R2 cap-near split",
        "role": CONTROL,
        "repeats": 6,
        "expected_v2": "same key; the part bundle's membership moves at the cap",
    },
    "cap_tail_part_top_price": {
        "sql": "SELECT MAX(p_retailprice) AS p FROM samples.tpch.part",
        "residual": "R2 cap-near split",
        "role": CONTROL,
        "repeats": 6,
        "expected_v2": "same key; the part bundle's membership moves at the cap",
    },
}
CONTROL_GRAINS = frozenset({"samples.tpch.customer", "samples.tpch.lineitem"})


def corpus() -> list[tuple[str, str]]:
    return [
        (spec["sql"], f"{name}_{i}")
        for name, spec in STATEMENTS.items()
        for i in range(spec["repeats"])
    ]


def _statement_of(provenance_id: str) -> str:
    return provenance_id.rsplit("_", 1)[0]


def statement_identity() -> dict[str, dict]:
    return {
        name: {
            "sql": spec["sql"],
            "residual": spec["residual"],
            "role": spec["role"],
            "repeats": spec["repeats"],
            "expected_v2": spec["expected_v2"],
            "v1_occurrence_inputs": [
                {
                    "expr_fingerprint": m.fingerprint,
                    "canonical_expr": m.canonical_expr,
                    "spelled_source_tables": list(m.source_tables),
                    "source_columns": list(m.source_columns),
                    "has_unresolved_columns": m.has_unresolved_columns,
                }
                for m in extract_measures(spec["sql"])
            ],
        }
        for name, spec in STATEMENTS.items()
    }


def scan_identity() -> list[dict]:
    measures = corpus_scan(corpus()).measures
    union: dict[str, set[str]] = {}
    rows: dict[str, int] = {}
    for m in measures:
        union.setdefault(m.fingerprint, set()).update(m.source_tables)
        rows[m.fingerprint] = rows.get(m.fingerprint, 0) + 1
    identity = []
    for m in measures:
        key = mv_candidate_fingerprint(SPACE, m.canonical_expr, m.source_tables)
        merged = (
            mv_candidate_fingerprint(SPACE, m.canonical_expr, union[m.fingerprint])
            if rows[m.fingerprint] > 1
            else key
        )
        identity.append(
            {
                "fingerprint": m.fingerprint,
                "canonical_expr": m.canonical_expr,
                "source_tables": list(m.source_tables),
                "from_statements": sorted({_statement_of(p) for p in m.provenance_ids}),
                "recurrence": m.recurrence,
                "provenance_count": m.provenance_count,
                "has_unresolved_columns": m.has_unresolved_columns,
                "candidate_fingerprint": key,
                "merged_fingerprint": merged,
                "suggestion_id": suggestion_id_for(key),
            }
        )
    return identity


def bundle_identity() -> list[dict]:
    outcome = mv_advisor.advise_from_corpus(
        space_id=SPACE,
        run_id="run_baseline",
        corpus_entries=corpus(),
        applied_config=APPLIED_CONFIG,
        benchmarks=(),
        wide_schema_inventory=None,
        metric_view_reader=lambda tables: [],
        embedding_client=None,
        signal_reader=None,
        intent_texts=(),
        domain="",
        max_candidates=CAP,
        persist_proposal=lambda proposal, rendered: True,
        write_ddl_artifact=lambda proposal, rendered: True,
        read_suppressed_fingerprints=None,
        read_kept_names=dict,
    )
    bundles = []
    for p in outcome.proposals:
        source_tables = sorted(p.evidence.get("source_tables", ()))
        members = p.evidence.get("measures", ())
        bundles.append(
            {
                "dedup_fingerprint": p.dedup_fingerprint,
                "suggestion_id": p.suggestion_id,
                "source_tables": source_tables,
                "members": sorted(m["dedup_fingerprint"] for m in members),
                "member_exprs": {m["dedup_fingerprint"]: m["expr"] for m in members},
                "role": CONTROL if set(source_tables) <= CONTROL_GRAINS else MOVER,
            }
        )
    return sorted(bundles, key=lambda b: b["dedup_fingerprint"])


def capture() -> dict:
    return {
        "captured_at_head": BASE,
        "sqlglot": sqlglot.__version__,
        "space_id": SPACE,
        "cap": CAP,
        "applied_config": APPLIED_CONFIG,
        "corpus_order": list(STATEMENTS),
        "statements": statement_identity(),
        "scan": scan_identity(),
        "bundles": bundle_identity(),
    }


@pytest.fixture(scope="module")
def baseline() -> dict:
    return json.loads(BASELINE_PATH.read_text())


def test_the_baseline_was_taken_at_base(baseline):
    assert sqlglot.__version__ == "30.0.3"
    assert baseline["sqlglot"] == "30.0.3"
    assert baseline["captured_at_head"] == BASE


# MV-D123: test_the_capture_is_reproducible is replaced by test_v1_reproduces_every_captured_key (test_mv_identity_v1.py) and Tasks 5/6's map assertions.


# ── Added by Task 5 (MV-D123): the scan rows' v2 map ─────────────────────

EXPECTED_V2: dict[str, tuple[str, tuple[str, ...]]] = {
    "two_catalogs_east": ("sum(weight)", ("east.ops.shipments",)),
    "two_catalogs_west": ("sum(weight)", ("west.ops.shipments",)),
    "orders_spelled_three_part": ("sum(amount)", ("main.sales.orders",)),
    "orders_spelled_two_part": ("sum(amount)", ("main.sales.orders",)),
    "orders_spelled_one_part": ("sum(amount)", ("main.sales.orders",)),
    "unqualified_unique_leaf": ("max(refund_amount)", ("main.sales.refunds",)),
    "derived_table_row_count": ("count(?n)", ()),
    "cte_row_count": ("count(?n)", ()),
    "struct_field_beside_payload_join": ("sum(payload.fee)", ("main.sales.orders",)),
}
"""Each mover's v2 calculation and full-name set, as its ``expected_v2`` says."""

UNRESOLVED_V2 = frozenset({"unqualified_ambiguous_leaf", "derived_table_row_count", "cte_row_count"})


def _v2_rows_by_statement(baseline: dict) -> dict[str, FingerprintRecurrence]:
    statements = baseline["statements"]
    entries = [
        (statements[name]["sql"], f"{name}_{i}")
        for name in baseline["corpus_order"]
        for i in range(statements[name]["repeats"])
    ]
    scan = corpus_scan(entries, resolver=TableResolver.from_config(baseline["applied_config"]))
    rows: dict[str, FingerprintRecurrence] = {}
    for row in scan.measures:
        for name in {_statement_of(p) for p in row.provenance_ids}:
            assert name not in rows, name
            rows[name] = row
    assert set(rows) == set(statements)
    return rows


def _captured_rows_by_statement(baseline: dict) -> dict[str, dict]:
    return {name: row for row in baseline["scan"] for name in row["from_statements"]}


def _v2_key(row: FingerprintRecurrence) -> str:
    return mv_candidate_fingerprint(SPACE, row.canonical_expr, row.source_tables)


def test_the_json_pins_the_module_constants(baseline):
    assert baseline["space_id"] == SPACE
    assert baseline["cap"] == CAP
    assert baseline["applied_config"] == APPLIED_CONFIG
    assert baseline["corpus_order"] == list(STATEMENTS)


def test_every_control_keeps_its_captured_key(baseline):
    v2_rows = _v2_rows_by_statement(baseline)
    captured = _captured_rows_by_statement(baseline)
    controls = [n for n, s in baseline["statements"].items() if s["role"] == CONTROL]
    assert controls
    for name in controls:
        row = v2_rows[name]
        assert _v2_key(row) == captured[name]["candidate_fingerprint"], name
        assert sorted({_statement_of(p) for p in row.provenance_ids}) == captured[name]["from_statements"], name
        assert row.has_unresolved_tables is (name in UNRESOLVED_V2), name


def test_every_mover_moves_onto_its_full_name_set(baseline):
    v2_rows = _v2_rows_by_statement(baseline)
    captured = _captured_rows_by_statement(baseline)
    movers = {n for n, s in baseline["statements"].items() if s["role"] == MOVER}
    assert movers == set(EXPECTED_V2)
    for name in sorted(movers):
        row = v2_rows[name]
        assert _v2_key(row) != captured[name]["candidate_fingerprint"], name
        assert (row.canonical_expr, row.source_tables) == EXPECTED_V2[name], name
        assert row.has_unresolved_tables is (name in UNRESOLVED_V2), name


# ── Added by Task 6 (MV-D123): the bundle-key and suggestion_id map ──────

EXPECTED_V2_BUNDLES: dict[tuple[str, ...], tuple[str, ...]] = {
    ("east.ops.shipments",): ("two_catalogs_east",),
    ("west.ops.shipments",): ("two_catalogs_west",),
    ("main.sales.orders",): (
        "orders_spelled_three_part",
        "orders_spelled_two_part",
        "orders_spelled_one_part",
        "struct_field_beside_payload_join",
        "split_tax_orders",
        "c8_single_table_measure",
    ),
    ("main.sales.refunds",): ("split_tax_refunds", "unqualified_unique_leaf"),
    ("samples.tpch.part",): ("cap_tail_part_size",),
}
"""Each mover bundle's grain and the statements its members come from.

R1 splits the shipments grain by catalog; the three spellings of ``SUM(amount)``
are one member; R4's row counts are unresolved and leave the orders view; R6's
``payload.fee`` joins orders and the payload view is gone; the unique leaf joins
refunds; the ambiguous leaf and the table-less row join nothing; and the part
grain keeps one measure under the cap."""


def _v2_proposals(baseline: dict) -> tuple:
    statements = baseline["statements"]
    entries = [
        (statements[name]["sql"], f"{name}_{i}")
        for name in baseline["corpus_order"]
        for i in range(statements[name]["repeats"])
    ]
    return mv_advisor.advise_from_corpus(
        space_id=SPACE,
        run_id="run_baseline",
        corpus_entries=entries,
        applied_config=baseline["applied_config"],
        resolver=TableResolver.from_config(baseline["applied_config"]),
        benchmarks=(),
        wide_schema_inventory=None,
        metric_view_reader=lambda tables: [],
        embedding_client=None,
        signal_reader=None,
        intent_texts=(),
        domain="",
        max_candidates=baseline["cap"],
        persist_proposal=lambda proposal, rendered: True,
        write_ddl_artifact=lambda proposal, rendered: True,
        read_suppressed_fingerprints=None,
        read_kept_names=dict,
    ).proposals


def _members(proposal) -> list[str]:
    return sorted(m["dedup_fingerprint"] for m in proposal.evidence["measures"])


def test_every_control_bundle_keeps_its_captured_key_and_id(baseline):
    proposals = {p.dedup_fingerprint: p for p in _v2_proposals(baseline)}
    controls = [b for b in baseline["bundles"] if b["role"] == CONTROL]
    assert len(controls) == 2
    for captured in controls:
        proposal = proposals[captured["dedup_fingerprint"]]
        assert proposal.suggestion_id == captured["suggestion_id"]
        assert sorted(proposal.evidence["source_tables"]) == captured["source_tables"]
        assert _members(proposal) == captured["members"]


def test_every_mover_bundle_moves_onto_its_full_name_members(baseline):
    key_of = {name: _v2_key(row) for name, row in _v2_rows_by_statement(baseline).items()}
    movers = {
        tuple(sorted(p.evidence["source_tables"])): p
        for p in _v2_proposals(baseline)
        if not set(p.evidence["source_tables"]) <= CONTROL_GRAINS
    }
    assert set(movers) == set(EXPECTED_V2_BUNDLES)
    captured_movers = {
        b["dedup_fingerprint"] for b in baseline["bundles"] if b["role"] == MOVER
    }
    for grain, statements in EXPECTED_V2_BUNDLES.items():
        proposal = movers[grain]
        member_keys = sorted({key_of[name] for name in statements})
        assert _members(proposal) == member_keys, grain
        assert proposal.dedup_fingerprint == mv_bundle_fingerprint(SPACE, member_keys, grain)
        assert proposal.suggestion_id == suggestion_id_for(proposal.dedup_fingerprint)
        assert proposal.dedup_fingerprint not in captured_movers, grain


if __name__ == "__main__":
    BASELINE_PATH.write_text(json.dumps(capture(), indent=2, sort_keys=True) + "\n")
    sys.exit(0)
