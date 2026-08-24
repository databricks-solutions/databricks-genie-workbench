"""Tests for the space-scoped semantic model graph route (Prompt 12, MV-D23).

Two seams matter and are tested without Databricks:

- **Parse-free assembly** (``_build_semantic_graph``): tables split into
  source/fact (col 0) vs joined dimension (col 1); join edges carry the decoded
  ON predicate, relationship, and an SCD2 flag from the ``is_current`` guard;
  measure concepts land on the governance ladder — governed (a config-marked MV
  measure), curated (``sql_snippets.measures``), ungoverned (recurs only in
  proposal evidence) — with EXACT-NAME identity so a name at a higher rung wins.
- **The route**: the base graph is read the OBO-tolerant way ``/space/fetch``
  reads (``get_serialized_space``), so it reflects what the signed-in user may
  see; proposals ride the SP-side Delta read; a never-optimized space still
  renders; a config-read failure surfaces as 502.
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
            {
                "identifier": "finance.sales.orders_metrics",
                "column_configs": [
                    {"column_name": "order_count", "kind": "measure"},
                    {"column_name": "order_date", "kind": "dimension"},
                ],
            }
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


# ── Pure assembly ───────────────────────────────────────────────────────────


def test_build_graph_splits_tables_joins_and_ladders_measures():
    nodes, edges = auto_optimize._build_semantic_graph(_SPACE, [])
    node_by_id = {n.id: n for n in nodes}

    # order_items is only ever a left/fact table → col 0; customer is a join's
    # right (dimension) side → col 1.
    assert node_by_id["finance.sales.order_items"].col == 0
    assert node_by_id["finance.ref.customer"].col == 1

    # Metric view node in col 2; its config-marked measure is a governed chip,
    # the dimension column is not.
    assert node_by_id["finance.sales.orders_metrics"].kind == "metric_view"
    assert node_by_id["measure:order_count"].governance == "governed"
    assert "measure:order_date" not in node_by_id

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


def test_build_graph_adds_ungoverned_from_proposal_evidence():
    nodes, _ = auto_optimize._build_semantic_graph(
        _SPACE, [_proposal("finance.sales.order_revenue", recurrence=14)]
    )
    node_by_id = {n.id: n for n in nodes}
    ungoverned = node_by_id["measure:order_revenue"]
    assert ungoverned.governance == "ungoverned"
    assert "14" in (ungoverned.origin or "")


def test_build_graph_exact_name_match_prefers_higher_rung():
    """A proposal whose name matches a curated concept is not re-added ungoverned."""
    nodes, _ = auto_optimize._build_semantic_graph(
        _SPACE, [_proposal("finance.sales.gross_margin")]
    )
    margins = [n for n in nodes if n.label == "gross_margin"]
    assert len(margins) == 1
    assert margins[0].governance == "curated"


def test_build_graph_empty_space_has_no_measures_or_edges():
    nodes, edges = auto_optimize._build_semantic_graph(
        {"data_sources": {"tables": [{"identifier": "finance.sales.orders"}]}}, []
    )
    assert [n.kind for n in nodes] == ["table"]
    assert edges == []


# ── Route ───────────────────────────────────────────────────────────────────


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: MagicMock())
    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app)


def test_semantic_graph_reads_base_from_the_obo_tolerant_config_path(client, monkeypatch):
    """The base graph comes from get_serialized_space (the /space/fetch OBO path),
    not from a run artifact or an SP-only read."""
    seen: dict = {}

    def fake_serialized(space_id):
        seen["space_id"] = space_id
        return dict(_SPACE)

    monkeypatch.setattr(auto_optimize, "get_serialized_space", fake_serialized)
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])

    resp = client.get("/api/auto-optimize/spaces/space-1/semantic-graph")
    assert resp.status_code == 200
    data = resp.json()
    # The user-entitled config read is what produced the graph.
    assert seen["space_id"] == "space-1"
    assert data["space_id"] == "space-1"
    assert _node(data["nodes"], "measure:order_count")["governance"] == "governed"
    assert _node(data["nodes"], "measure:gross_margin")["governance"] == "curated"
    # Edges serialize with the "from" alias, not "from_".
    assert all("from" in e for e in data["edges"])
    assert data["proposals"] == []


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
