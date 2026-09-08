"""Ontology Map v2 backend route tests (MV-D73/D74, spec §2.3/§2.4/§3).

Lane 2 exposes the layered blob to the UI:

- ``GET /graph?origin=applied|proposed`` (default ``applied``): the graph is filtered
  to rollups of the requested provenance (MV-D74). ``ungrouped`` is always kept
  (neutral); assets are pruned to the kept rollups; edges with a filtered endpoint are
  dropped (no dangling edges). A legacy blob with no ``origin`` is served UNFILTERED
  (degrade — never blank the map, MV-D43).
- ``GET /graph/expand?node=<id>`` (MV-D73 §2.3): ONE node's children sliced from the
  blob's ``snippets`` index — measures ⇒ ``kind="measure"`` nodes + ``mv_measure`` edges;
  Pages ⇒ ``kind="page"`` nodes + ``page_source`` edges. Bounded; any miss ⇒ empty
  children, HTTP 200 (never a 500).

All seams are exercised offline: the mirror read + metastore resolver are monkeypatched,
so no Databricks / warehouse / Lakebase is touched.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.ontology.routers import graph as graph_router


# A layered snapshot blob shaped like the MV-D73 wheel producer: one applied rollup
# (governed tag), one proposed rollup (engine cluster), and the neutral ungrouped
# bucket; assets hang off each; a cross-rollup domain edge + a cross-rollup asset edge
# both go dangling under either filter. ``snippets`` keys the expand layer by the
# ``mv:<fqn>`` node id (measures) and the sub-domain rollup id (pages).
_BLOB = {
    "domains": {
        "nodes": [
            {"id": "dom:fin", "label": "Finance", "kind": "domain",
             "domain_id": "dom:fin", "member_count": 2, "origin": "applied"},
            {"id": "dom:ifec", "label": "Ifec", "kind": "domain",
             "domain_id": "dom:ifec", "member_count": 1, "origin": "proposed"},
            {"id": "ungrouped", "label": "Ungrouped", "kind": "ungrouped",
             "domain_id": "ungrouped"},
        ],
        "edges": [
            # Crosses applied↔proposed → dropped under BOTH filters (emitted-only guard).
            {"src": "dom:fin", "dst": "dom:ifec", "kind": "cross_domain"},
        ],
        "truncated": False,
    },
    "assets": {
        "nodes": [
            {"id": "mv:finance.sales.orders_metrics", "label": "orders_metrics",
             "kind": "metric_view", "domain_id": "dom:fin"},
            {"id": "tbl:finance.sales.orders", "label": "orders",
             "kind": "table", "domain_id": "dom:fin",
             # MV-D86 (Lane D2): additive per-node description + compact meta bag.
             "description": "Order line items", "meta": {"rows": "1200000", "format": "DELTA"}},
            {"id": "mv:ifec.media.plays", "label": "plays",
             "kind": "metric_view", "domain_id": "dom:ifec"},
            {"id": "mv:un.loose", "label": "loose",
             "kind": "metric_view", "domain_id": "ungrouped"},
        ],
        "edges": [
            # Both endpoints in the applied Finance domain → kept under applied.
            {"src": "mv:finance.sales.orders_metrics", "dst": "tbl:finance.sales.orders",
             "kind": "mv_membership"},
            # Crosses Finance(applied)↔Ifec(proposed) → dangling under either filter.
            {"src": "tbl:finance.sales.orders", "dst": "mv:ifec.media.plays",
             "kind": "co_query"},
        ],
        "truncated": False,
    },
    "snippets": {
        "mv:finance.sales.orders_metrics": {
            "measures": [
                {"ref": "finance.sales.orders_metrics.revenue", "name": "Revenue",
                 "expression": "SUM(amount)", "fmt": "$#,##0"},
                {"ref": "finance.sales.orders_metrics.gross_margin", "name": "Gross Margin",
                 "expression": "SUM(margin)", "fmt": "$#,##0"},
            ],
            "pages": [],
        },
        "dom:fin/tax": {
            "measures": [],
            "pages": [
                {"page_id": "p1", "title": "[Routing] Revenue", "archetype": "Routing",
                 "domain_id": "dom:fin/tax"},
            ],
        },
    },
    "subdomains": {"edges": []},
    "layout": "fcose",
    "node_count": 7,
    "edge_count": 3,
}

_AS_OF = "2026-09-01T09:00:00+00:00"


def _client(monkeypatch, snap) -> TestClient:
    """A graph-route client whose mirror read + metastore resolver are stubbed."""
    monkeypatch.setattr(graph_router.ont_settings, "_metastore_id", lambda: "ms1")

    async def _read(metastore_id):
        assert metastore_id == "ms1"
        return snap

    monkeypatch.setattr(graph_router.mirror, "read_graph_snapshot", _read)
    app = FastAPI()
    app.include_router(graph_router.router)
    return TestClient(app)


def _snap(blob=_BLOB, as_of=_AS_OF):
    return {"graph": blob, "as_of": as_of}


# ── §3 Applied vs Proposed filter on GET /graph ─────────────────────────────


def test_graph_default_is_applied_plus_ungrouped(monkeypatch):
    data = _client(monkeypatch, _snap()).get("/api/ontology/graph").json()
    assert data["state"] == "fresh"
    assert data["as_of"] == _AS_OF
    dom_ids = {n["id"] for n in data["domains"]["nodes"]}
    assert dom_ids == {"dom:fin", "ungrouped"}  # applied + ungrouped, NOT the proposed rollup
    # Assets pruned to the kept rollups (Finance + ungrouped), not the proposed Ifec asset.
    asset_ids = {n["id"] for n in data["assets"]["nodes"]}
    assert asset_ids == {"mv:finance.sales.orders_metrics", "tbl:finance.sales.orders", "mv:un.loose"}
    # No dangling edges: the applied↔proposed domain edge and the cross-rollup co-query
    # edge are both dropped; only the intra-Finance mv_membership edge survives.
    assert data["domains"]["edges"] == []
    assert [e["kind"] for e in data["assets"]["edges"]] == ["mv_membership"]
    # The additive origin field rides through untouched.
    assert {n["id"]: n["origin"] for n in data["domains"]["nodes"]} == {
        "dom:fin": "applied", "ungrouped": None,
    }


def test_graph_passes_through_description_and_meta(monkeypatch):
    """MV-D86 (Lane D2) contract parity: a node's additive ``description`` + ``meta`` ride
    through _level untouched; a node without them degrades to None (pre-MV-D86 blob)."""
    data = _client(monkeypatch, _snap()).get("/api/ontology/graph").json()
    by_id = {n["id"]: n for n in data["assets"]["nodes"]}
    orders = by_id["tbl:finance.sales.orders"]
    assert orders["description"] == "Order line items"
    assert orders["meta"] == {"rows": "1200000", "format": "DELTA"}
    # A node without the fields degrades cleanly to null (additive contract, MV-D43).
    metrics = by_id["mv:finance.sales.orders_metrics"]
    assert metrics["description"] is None
    assert metrics["meta"] is None


def test_graph_origin_proposed_returns_proposed_plus_ungrouped(monkeypatch):
    data = _client(monkeypatch, _snap()).get("/api/ontology/graph?origin=proposed").json()
    dom_ids = {n["id"] for n in data["domains"]["nodes"]}
    assert dom_ids == {"dom:ifec", "ungrouped"}
    asset_ids = {n["id"] for n in data["assets"]["nodes"]}
    assert asset_ids == {"mv:ifec.media.plays", "mv:un.loose"}
    # Both blob edges cross into the (now-filtered) Finance rollup → all dropped.
    assert data["domains"]["edges"] == []
    assert data["assets"]["edges"] == []


def test_graph_invalid_origin_falls_back_to_applied(monkeypatch):
    data = _client(monkeypatch, _snap()).get("/api/ontology/graph?origin=bogus").json()
    assert {n["id"] for n in data["domains"]["nodes"]} == {"dom:fin", "ungrouped"}


def test_graph_legacy_blob_without_origin_served_unfiltered(monkeypatch):
    # A pre-MV-D73 blob (no origin on any rollup) must render fully under the applied
    # default — never blank the map (MV-D43 degrade).
    legacy = {
        "domains": {"nodes": [
            {"id": "dom:a", "label": "A", "kind": "domain", "domain_id": "dom:a"},
            {"id": "dom:b", "label": "B", "kind": "domain", "domain_id": "dom:b"},
        ], "edges": [], "truncated": False},
        "assets": {"nodes": [], "edges": [], "truncated": False},
        "layout": "fcose", "node_count": 2, "edge_count": 0,
    }
    data = _client(monkeypatch, _snap(legacy)).get("/api/ontology/graph").json()
    assert {n["id"] for n in data["domains"]["nodes"]} == {"dom:a", "dom:b"}


# ── §2.3 expand-on-demand children ──────────────────────────────────────────


def test_expand_metric_view_returns_measure_children(monkeypatch):
    resp = _client(monkeypatch, _snap()).get(
        "/api/ontology/graph/expand?node=mv:finance.sales.orders_metrics"
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["parent_id"] == "mv:finance.sales.orders_metrics"
    assert data["as_of"] == _AS_OF
    assert [n["kind"] for n in data["nodes"]] == ["measure", "measure"]
    ids = {n["id"] for n in data["nodes"]}
    assert ids == {
        "measure:finance.sales.orders_metrics.revenue",
        "measure:finance.sales.orders_metrics.gross_margin",
    }
    # Measures hang under the parent metric-view's domain.
    assert {n["domain_id"] for n in data["nodes"]} == {"dom:fin"}
    labels = {n["label"] for n in data["nodes"]}
    assert labels == {"Revenue", "Gross Margin"}
    # One mv_measure edge per measure, sourced at the parent node.
    assert all(e["kind"] == "mv_measure" and e["src"] == "mv:finance.sales.orders_metrics"
               for e in data["edges"])
    assert {e["dst"] for e in data["edges"]} == ids


def test_expand_subdomain_returns_page_children(monkeypatch):
    resp = _client(monkeypatch, _snap()).get("/api/ontology/graph/expand?node=dom:fin/tax")
    assert resp.status_code == 200
    data = resp.json()
    assert [n["kind"] for n in data["nodes"]] == ["page"]
    page = data["nodes"][0]
    assert page["id"] == "page:p1"
    assert page["label"] == "[Routing] Revenue"
    assert page["domain_id"] == "dom:fin/tax"
    # verb/rel_class are additive optionals (MV-D82); expand edges don't set them → None.
    assert data["edges"] == [{"src": "dom:fin/tax", "dst": "page:p1",
                              "kind": "page_source", "weight": None,
                              "verb": None, "rel_class": None}]


def test_expand_unknown_node_is_empty_200(monkeypatch):
    resp = _client(monkeypatch, _snap()).get("/api/ontology/graph/expand?node=nope:xyz")
    assert resp.status_code == 200
    data = resp.json()
    assert data == {"nodes": [], "edges": [], "parent_id": "nope:xyz", "as_of": _AS_OF}


# ── Degrade-not-hang (MV-D43): cold / empty / failed reads ──────────────────


def test_graph_cold_when_snapshot_missing(monkeypatch):
    data = _client(monkeypatch, None).get("/api/ontology/graph").json()
    assert data["state"] == "cold"
    assert data["domains"]["nodes"] == [] and data["assets"]["nodes"] == []


def test_expand_empty_when_snapshot_missing(monkeypatch):
    resp = _client(monkeypatch, None).get("/api/ontology/graph/expand?node=mv:x")
    assert resp.status_code == 200
    assert resp.json() == {"nodes": [], "edges": [], "parent_id": "mv:x", "as_of": None}


def test_graph_degrades_to_cold_on_read_error(monkeypatch):
    monkeypatch.setattr(graph_router.ont_settings, "_metastore_id", lambda: "ms1")

    async def _boom(metastore_id):
        raise RuntimeError("mirror unreachable")

    monkeypatch.setattr(graph_router.mirror, "read_graph_snapshot", _boom)
    app = FastAPI()
    app.include_router(graph_router.router)
    client = TestClient(app)
    assert client.get("/api/ontology/graph").json()["state"] == "cold"
    # And expand degrades to empty children, not a 500.
    resp = client.get("/api/ontology/graph/expand?node=mv:x")
    assert resp.status_code == 200
    assert resp.json()["nodes"] == []


def test_expand_snippets_absent_is_empty_200(monkeypatch):
    # A fresh blob that predates the snippet layer (no ``snippets`` key) still expands
    # to empty children rather than raising.
    no_snip = {k: v for k, v in _BLOB.items() if k != "snippets"}
    resp = _client(monkeypatch, _snap(no_snip)).get(
        "/api/ontology/graph/expand?node=mv:finance.sales.orders_metrics"
    )
    assert resp.status_code == 200
    assert resp.json()["nodes"] == []
