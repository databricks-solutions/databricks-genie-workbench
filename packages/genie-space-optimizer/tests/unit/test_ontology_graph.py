"""L2 fused signal-graph — offline unit tests (Stage 1, MV-D52).

Covers the opt-in structural edge kinds added in Stage 1 (``join_key`` populated,
``mv_membership``, ``schema_affinity``) and the invariant that the byte-identical
``(graph, lineage_edges)``-only call is unchanged.
"""

from __future__ import annotations

from genie_space_optimizer.ontology import graph


def test_lineage_only_call_is_byte_identical_scaffold():
    """The (graph, lineage_edges)-only call must stay the Phase-2 scaffold: only
    tag_assignment + lineage_adjacency edges, no new kinds."""
    g = {"tags": [{"tag_key": "Finance", "members": [{"fqn": "c.fin.ledger"}]}]}
    sig = graph.build_signal_graph(g, [("c.fin.ledger", "c.fin.gl")])
    assert {e["kind"] for e in sig["edges"]} == {"tag_assignment", "lineage_adjacency"}


def test_join_key_layer_populated_fk_and_proxy_with_sources():
    sig = graph.build_signal_graph(
        {"tags": []},
        join_key_edges=[
            ("c.rev.fact_revenue", "c.rev.dim_route"),                       # FK (default source)
            ("c.rev.fact_revenue", "c.rev.dim_fare", 1.0, "foreign_key"),    # FK explicit
            ("c.rev.bookings", "c.res.pnr", 0.5, "shared_join_column"),      # proxy
        ],
    )
    jk = [e for e in sig["edges"] if e["kind"] == "join_key"]
    assert len(jk) == 3
    sources = {e["source"] for e in jk}
    assert sources == {"foreign_key", "shared_join_column"}
    # Asset↔asset, prefixed; the proxy carries its lower weight.
    proxy = next(e for e in jk if e["source"] == "shared_join_column")
    assert proxy["src"] == "asset:c.rev.bookings" and proxy["dst"] == "asset:c.res.pnr"
    assert proxy["weight"] == 0.5


def test_mv_membership_hub_edges():
    sig = graph.build_signal_graph(
        {"tags": []},
        mv_membership={"c.metrics.revenue_mv": ["c.rev.fact_revenue", "c.rev.dim_route"]},
    )
    mm = [e for e in sig["edges"] if e["kind"] == "mv_membership"]
    assert len(mm) == 2
    assert all(e["src"] == "mv:c.metrics.revenue_mv" for e in mm)
    assert {e["dst"] for e in mm} == {"asset:c.rev.fact_revenue", "asset:c.rev.dim_route"}
    assert any(n["id"] == "mv:c.metrics.revenue_mv" and n["kind"] == "metric_view" for n in sig["nodes"])


def test_schema_affinity_hub_edges():
    sig = graph.build_signal_graph(
        {"tags": []},
        schema_affinity={"c.revenue": ["c.revenue.fact", "c.revenue.dim"]},
    )
    sa = [e for e in sig["edges"] if e["kind"] == "schema_affinity"]
    assert len(sa) == 2
    assert all(e["src"] == "schema:c.revenue" and e["source"] == "information_schema" for e in sa)
    assert {e["dst"] for e in sa} == {"asset:c.revenue.fact", "asset:c.revenue.dim"}


def test_new_signals_stamp_as_of_like_existing_edges():
    sig = graph.build_signal_graph(
        {"tags": []},
        join_key_edges=[("a.b.c", "a.b.d")],
        as_of="2026-08-31T00:00:00+00:00",
    )
    assert all(e["as_of"] == "2026-08-31T00:00:00+00:00" for e in sig["edges"])
