"""Tests for the space-scoped semantic model graph route (Prompt 12 + 12b, MV-D23).

Seams tested without Databricks:

- **Assembly** (``_build_semantic_graph``): tables split into source/fact (col 0)
  vs joined dimension (col 1); join edges carry the decoded ON predicate,
  relationship, and an SCD2 flag from the ``is_current`` guard; measure concepts
  land on the governance ladder — governed (DESCRIBE-enumerated MV measures,
  Prompt 12b Debt 2), curated (``sql_snippets.measures`` + measures harvested
  from ``example_question_sqls``, Debt 1), ungoverned (recurs only in proposal
  evidence) — with CANONICALIZED-EXPR identity so two spellings of one measure
  are one chip and a higher rung absorbs its twin (Debt 3).
- **Coverage lens** (``_apply_coverage``): curated-SQL touch counts per node,
  cold spots at 0, the MV-D15 status vocabulary (EMPTY / UNAVAILABLE / COMPUTED).
- **The route**: the base graph is read the OBO-tolerant way ``/space/fetch``
  reads (``get_serialized_space``); the governed chips ride a best-effort
  DESCRIBE read; proposals ride the SP-side Delta read; a never-optimized space
  still renders; a config-read failure surfaces as 502.
- **Compatibility**: a Prompt 12 client that never learned the lens keeps working
  — every 12b field is additive.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.models import MvProposal
from backend.routers import auto_optimize
from genie_space_optimizer.common import warehouse


_SPACE = {
    "data_sources": {
        "tables": [
            {"identifier": "finance.sales.orders"},
            {"identifier": "finance.sales.order_items"},
            {"identifier": "finance.ref.customer"},
        ],
        "metric_views": [
            {"identifier": "finance.sales.orders_metrics"},
        ],
    },
    "instructions": {
        "join_specs": [
            {
                "id": "a" * 32,
                "left": {"identifier": "finance.sales.order_items"},
                "right": {"identifier": "finance.sales.orders"},
                "sql": [
                    "`order_items`.`order_id` = `orders`.`order_id`",
                    "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
                ],
            },
            {
                "id": "b" * 32,
                "left": {"identifier": "finance.sales.orders"},
                "right": {"identifier": "finance.ref.customer"},
                "sql": [
                    "`orders`.`customer_id` = `customer`.`id` AND `customer`.`is_current` = true",
                    "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
                ],
            },
        ],
        "sql_snippets": {
            "measures": [
                {"id": "c" * 32, "display_name": "gross_margin", "sql": ["SUM(o.rev - o.cost)"]}
            ]
        },
    },
}


# A DESCRIBE-derived governed measure (Prompt 12b Debt 2). Field-shaped mapping;
# _field_attr reads either a mapping or a MetricViewField object.
def _governed(field_name: str, canonical_expr: str = "", mv_fqn: str = "finance.sales.orders_metrics"):
    return {
        "mv_fqn": mv_fqn,
        "field_name": field_name,
        "kind": "measure",
        "canonical_expr": canonical_expr,
    }


def _proposal(proposed_object: str, recurrence: int = 14, **over) -> MvProposal:
    base = dict(
        suggestion_id="sug1",
        dedup_fingerprint="fp1",
        target_space_id="space-1",
        candidate_type="PROPOSE",
        proposed_object=proposed_object,
        evidence={"recurrence_count": recurrence, "source_tables": ["finance.sales.orders"]},
    )
    base.update(over)
    return MvProposal(**base)


def _node(nodes: list[dict], node_id: str) -> dict | None:
    return next((n for n in nodes if n["id"] == node_id), None)


def _build(space, proposals, **kw):
    """Unpack the 4-tuple (nodes, edges, coverage_status, coverage_reason)."""
    return auto_optimize._build_semantic_graph(space, proposals, **kw)


# ── Pure assembly ───────────────────────────────────────────────────────────


def test_build_graph_splits_tables_joins_and_ladders_measures():
    nodes, edges, _status, _reason = _build(
        _SPACE, [], governed_fields=[_governed("order_count", "count(1)")]
    )
    node_by_id = {n.id: n for n in nodes}

    # order_items is only ever a left/fact table → col 0; customer is a join's
    # right (dimension) side → col 1.
    assert node_by_id["finance.sales.order_items"].col == 0
    assert node_by_id["finance.ref.customer"].col == 1

    # Metric view node in col 2; its DESCRIBE-enumerated measure is a governed
    # chip tied to it by membership.
    assert node_by_id["finance.sales.orders_metrics"].kind == "metric_view"
    assert node_by_id["measure:order_count"].governance == "governed"

    # Curated concept from sql_snippets.measures.
    assert node_by_id["measure:gross_margin"].governance == "curated"

    # The SCD2 join carries the decoded relationship, cleaned ON, and scd2 flag.
    scd2_edge = next(
        e for e in edges
        if e.kind == "join" and e.to == "finance.ref.customer"
    )
    assert scd2_edge.relationship == "many-to-one"
    assert scd2_edge.scd2 is True
    assert "`" not in (scd2_edge.on or "")
    assert "customer.is_current = true" in (scd2_edge.on or "")

    # A membership edge ties the governed measure to its metric view.
    assert any(
        e.kind == "membership" and e.from_ == "measure:order_count"
        and e.to == "finance.sales.orders_metrics"
        for e in edges
    )


def test_build_graph_governed_chips_come_from_describe_not_config_markers():
    """Debt 2: with no DESCRIBE read (empty governed_fields), the MV still renders
    as a governed node but WITHOUT fabricated measure chips — the honest fallback
    the deleted _is_measure_column probe used to produce."""
    nodes, _edges, _s, _r = _build(_SPACE, [])
    node_by_id = {n.id: n for n in nodes}
    assert node_by_id["finance.sales.orders_metrics"].kind == "metric_view"
    assert not any(n.governance == "governed" for n in nodes)
    # The deleted speculative probe is gone.
    assert not hasattr(auto_optimize, "_is_measure_column")


def test_build_graph_curated_concept_harvested_from_example_sql():
    """Debt 1: a measure in example_question_sqls joins the ladder as curated."""
    space = {
        "data_sources": {"tables": [{"identifier": "finance.sales.orders"}]},
        "instructions": {
            "example_question_sqls": [
                {"id": "q1", "sql": ["SELECT SUM(o.qty) FROM finance.sales.orders o"]}
            ]
        },
    }
    nodes, _edges, _s, _r = _build(space, [])
    curated = [n for n in nodes if n.kind == "measure" and n.governance == "curated"]
    assert len(curated) == 1
    assert "sum(qty)" in curated[0].label.lower()


def test_build_graph_expr_identity_merges_two_spellings():
    """Debt 3: a snippet measure and an example-SQL measure that canonicalize to
    the same expression are ONE chip (qualifiers/aliases differ, identity does
    not)."""
    space = {
        "data_sources": {"tables": [{"identifier": "finance.sales.orders"}]},
        "instructions": {
            "sql_snippets": {
                "measures": [{"id": "m1", "display_name": "margin", "sql": ["SUM(o.rev - o.cost)"]}]
            },
            "example_question_sqls": [
                {"id": "q1", "sql": ["SELECT SUM(ord.rev - ord.cost) FROM finance.sales.orders ord"]}
            ],
        },
    }
    nodes, _edges, _s, _r = _build(space, [])
    curated = [n for n in nodes if n.kind == "measure" and n.governance == "curated"]
    assert len(curated) == 1
    # The human label from the snippet wins over the raw-expr label.
    assert curated[0].label == "margin"


def test_build_graph_governed_absorbs_its_curated_twin():
    """Debt 3: a governed measure and a curated snippet that canonicalize the same
    are one chip at the governed rung (highest rung wins by expr identity)."""
    space = {
        "data_sources": {
            "tables": [{"identifier": "finance.sales.orders"}],
            "metric_views": [{"identifier": "finance.sales.m"}],
        },
        "instructions": {
            "sql_snippets": {
                "measures": [{"id": "m1", "display_name": "revenue", "sql": ["SUM(o.rev)"]}]
            }
        },
    }
    nodes, _e, _s, _r = _build(
        space, [], governed_fields=[_governed("revenue", "sum(rev)", mv_fqn="finance.sales.m")]
    )
    measures = [n for n in nodes if n.kind == "measure"]
    assert len(measures) == 1
    assert measures[0].governance == "governed"


def test_build_graph_adds_ungoverned_from_proposal_evidence():
    nodes, _edges, _s, _r = _build(
        _SPACE, [_proposal("finance.sales.order_revenue", recurrence=14)]
    )
    node_by_id = {n.id: n for n in nodes}
    ungoverned = node_by_id["measure:order_revenue"]
    assert ungoverned.governance == "ungoverned"
    assert "14" in (ungoverned.origin or "")


def test_build_graph_ungoverned_carries_benchmark_question_ids():
    """The evidence lens: benchmark question ids ride the ungoverned node."""
    nodes, _e, _s, _r = _build(
        _SPACE,
        [_proposal("finance.sales.order_revenue", evidence={"benchmark_question_ids": ["bq_1", "bq_2"]})],
    )
    node = next(n for n in nodes if n.id == "measure:order_revenue")
    assert node.benchmark_question_ids == ["bq_1", "bq_2"]


def test_build_graph_name_match_prefers_higher_rung():
    """A proposal whose name matches a curated concept is not re-added ungoverned."""
    nodes, _edges, _s, _r = _build(_SPACE, [_proposal("finance.sales.gross_margin")])
    margins = [n for n in nodes if n.label == "gross_margin"]
    assert len(margins) == 1
    assert margins[0].governance == "curated"


def test_build_graph_empty_space_has_no_measures_or_edges():
    nodes, edges, status, _reason = _build(
        {"data_sources": {"tables": [{"identifier": "finance.sales.orders"}]}}, []
    )
    assert [n.kind for n in nodes] == ["table"]
    assert edges == []
    # No curated SQL → the coverage lens is honestly EMPTY, not a zero it invented.
    assert status == "EMPTY"


# ── Coverage lens ────────────────────────────────────────────────────────────


def test_coverage_lens_counts_touches_and_marks_cold_spots():
    space = {
        "data_sources": {
            "tables": [
                {"identifier": "finance.sales.orders"},
                {"identifier": "finance.ref.customer"},
                {"identifier": "finance.ref.unused"},
            ],
        },
        "instructions": {
            "example_question_sqls": [
                {"id": "q1", "sql": ["SELECT SUM(o.qty) FROM finance.sales.orders o"]},
                {
                    "id": "q2",
                    "sql": [
                        "SELECT COUNT(1) FROM finance.sales.orders o "
                        "JOIN finance.ref.customer c ON o.cid = c.id"
                    ],
                },
            ]
        },
    }
    nodes, _edges, status, reason = _build(space, [])
    assert status == "COMPUTED"
    assert reason is None
    node_by_id = {n.id: n for n in nodes}
    # orders is touched by both statements; customer by one; unused is a cold spot.
    assert node_by_id["finance.sales.orders"].coverage == 2
    assert node_by_id["finance.ref.customer"].coverage == 1
    assert node_by_id["finance.ref.unused"].coverage == 0


def test_coverage_lens_unavailable_when_all_statements_fail_to_parse():
    space = {
        "data_sources": {"tables": [{"identifier": "finance.sales.orders"}]},
        "instructions": {
            "example_question_sqls": [{"id": "q1", "sql": ["not valid ;; sql @@@"]}]
        },
    }
    _nodes, _edges, status, reason = _build(space, [])
    assert status == "UNAVAILABLE"
    assert reason and "parse" in reason


# ── Route ───────────────────────────────────────────────────────────────────


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: MagicMock())
    # The governed DESCRIBE read is best-effort and Databricks-bound; default it
    # to the honest empty in the offline route tests, overridden where a chip is
    # asserted.
    monkeypatch.setattr(auto_optimize, "_read_governed_measure_fields", lambda space_data: [])
    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app)


def test_semantic_graph_reads_base_from_the_obo_tolerant_config_path(client, monkeypatch):
    """The base graph comes from get_serialized_space (the /space/fetch OBO path),
    not from a run artifact or an SP-only read; governed chips ride the DESCRIBE
    read."""
    seen: dict = {}

    def fake_serialized(space_id):
        seen["space_id"] = space_id
        return dict(_SPACE)

    monkeypatch.setattr(auto_optimize, "get_serialized_space", fake_serialized)
    monkeypatch.setattr(
        auto_optimize, "_read_governed_measure_fields",
        lambda space_data: [_governed("order_count", "count(1)")],
    )
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])

    resp = client.get("/api/auto-optimize/spaces/space-1/semantic-graph")
    assert resp.status_code == 200
    data = resp.json()
    assert seen["space_id"] == "space-1"
    assert data["space_id"] == "space-1"
    assert _node(data["nodes"], "measure:order_count")["governance"] == "governed"
    assert _node(data["nodes"], "measure:gross_margin")["governance"] == "curated"
    # Edges serialize with the "from" alias, not "from_".
    assert all("from" in e for e in data["edges"])
    assert data["proposals"] == []


def test_semantic_graph_lens_free_response_is_backward_compatible(client, monkeypatch):
    """Compatibility: the additive lens fields are present but a Prompt 12 client
    that ignores them sees an unchanged base graph."""
    monkeypatch.setattr(auto_optimize, "get_serialized_space", lambda space_id: dict(_SPACE))
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    resp = client.get("/api/auto-optimize/spaces/space-1/semantic-graph")
    assert resp.status_code == 200
    data = resp.json()
    # New top-level lens keys exist (additive), and the base shape is unchanged.
    assert "coverage_status" in data
    assert {"space_id", "nodes", "edges", "proposals"} <= set(data)


def test_semantic_graph_carries_proposals_and_ungoverned_overlay(client, monkeypatch):
    captured: dict = {}

    def fake_load(*args, **kwargs):
        captured.update(kwargs)
        return [{
            "suggestion_id": "sug_x", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1", "candidate_type": "PROPOSE",
            "proposed_object": "finance.sales.order_revenue",
            "evidence": {"recurrence_count": 9},
        }]

    monkeypatch.setattr(auto_optimize, "get_serialized_space", lambda space_id: dict(_SPACE))
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", fake_load)

    resp = client.get("/api/auto-optimize/spaces/space-1/semantic-graph")
    assert resp.status_code == 200
    data = resp.json()
    # Proposals ride the SP-side Delta read, space-scoped (never run-keyed).
    assert captured.get("target_space_id") == "space-1"
    assert data["proposals"][0]["suggestion_id"] == "sug_x"
    ungoverned = _node(data["nodes"], "measure:order_revenue")
    assert ungoverned["governance"] == "ungoverned"


def test_semantic_graph_renders_for_a_never_optimized_space(client, monkeypatch):
    monkeypatch.setattr(
        auto_optimize, "get_serialized_space",
        lambda space_id: {"data_sources": {"tables": [{"identifier": "finance.sales.orders"}]}},
    )
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    resp = client.get("/api/auto-optimize/spaces/space-1/semantic-graph")
    assert resp.status_code == 200
    data = resp.json()
    assert [n["kind"] for n in data["nodes"]] == ["table"]
    assert data["edges"] == []
    assert data["proposals"] == []


def test_semantic_graph_502_when_config_read_fails(client, monkeypatch):
    def boom(space_id):
        raise RuntimeError("no access")

    monkeypatch.setattr(auto_optimize, "get_serialized_space", boom)
    resp = client.get("/api/auto-optimize/spaces/space-1/semantic-graph")
    assert resp.status_code == 502
