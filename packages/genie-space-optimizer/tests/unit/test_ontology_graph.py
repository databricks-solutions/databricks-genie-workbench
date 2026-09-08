"""L2 fused signal-graph — offline unit tests (Stage 1, MV-D52).

Covers the opt-in structural edge kinds added in Stage 1 (``join_key`` populated,
``mv_membership``, ``schema_affinity``) and the invariant that the byte-identical
``(graph, lineage_edges)``-only call is unchanged.
"""

from __future__ import annotations

import json

import pytest

from genie_space_optimizer.ontology import graph, layout


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


def test_tag_value_threads_onto_assignment_edge_additively():
    """Stage 2 (MV-D54): a value-carrying member threads ``tag_value`` onto its
    assignment edge; a value-free member's edge stays byte-identical (no key)."""
    sig = graph.build_signal_graph({"tags": [{"tag_key": "mvm_subdomain", "members": [
        {"fqn": "c.s.a", "tag_value": "fare_pricing"},
        {"fqn": "c.s.b"},  # value-free -> byte-identical scaffold edge
    ]}]}, as_of="2026-08-31T00:00:00+00:00")
    by_dst = {e["dst"]: e for e in sig["edges"] if e["kind"] == "tag_assignment"}
    assert by_dst["asset:c.s.a"]["tag_value"] == "fare_pricing"
    # The value-free edge carries EXACTLY the scaffold keys — no tag_value.
    assert by_dst["asset:c.s.b"] == {
        "src": "tag:mvm_subdomain", "dst": "asset:c.s.b",
        "kind": "tag_assignment", "source": "tag_assignment",
        "as_of": "2026-08-31T00:00:00+00:00",
    }


# --- L7 estate-graph rollup: prefix key fix + hierarchy enrichment (MV-D71) ---

def _snap(signal_graph, node_domain_id, domain_meta=None, snippets_in=None):
    pytest.importorskip("igraph")
    row = layout.build_graph_snapshot(
        signal_graph, node_domain_id, domain_meta=domain_meta, snippets_in=snippets_in,
        metastore_id="m", workspace_id="w", run_id="r", as_of="2026-01-01T00:00:00+00:00",
    )
    return json.loads(row["graph"])


def test_prefixed_asset_nodes_colored_by_bare_fqn_map():
    """MV-D71 regression: signal-graph asset nodes are ``asset:<fqn>`` but the
    node→domain map is keyed by the BARE fqn. The rollup must resolve the prefix
    so assets get a real domain (not the single ``Ungrouped`` blob)."""
    sig = {
        "nodes": [{"id": "asset:c.fin.ledger", "kind": "table"},
                  {"id": "asset:c.fin.gl", "kind": "table"}],
        "edges": [{"src": "asset:c.fin.ledger", "dst": "asset:c.fin.gl",
                   "kind": "lineage_adjacency"}],
    }
    blob = _snap(sig, {"c.fin.ledger": "d1", "c.fin.gl": "d1"},
                 domain_meta={"d1": {"name": "Finance", "parent_id": None}})

    # Every asset resolved to the real domain — nothing fell through to Ungrouped.
    assert {n["domain_id"] for n in blob["assets"]["nodes"]} == {"d1"}
    # Exactly one rollup node, labelled with the human name, kind "domain".
    dom = blob["domains"]["nodes"]
    assert len(dom) == 1
    assert dom[0]["id"] == "d1"
    assert dom[0]["label"] == "Finance"
    assert dom[0]["kind"] == "domain"
    assert dom[0]["member_count"] == 2
    assert dom[0]["parent_id"] is None


def test_subdomain_rollup_carries_parent_linkage():
    """A sub-domain (parent_id set) rolls up as kind ``subdomain`` and threads the
    resolved parent name so the map can nest Sub-Domain under Domain."""
    sig = {
        "nodes": [{"id": "asset:c.rev.bookings", "kind": "table"}],
        "edges": [],
    }
    blob = _snap(sig, {"c.rev.bookings": "d2"}, domain_meta={
        "d1": {"name": "Revenue", "parent_id": None},
        "d2": {"name": "Bookings", "parent_id": "d1"},
    })
    dom = {n["id"]: n for n in blob["domains"]["nodes"]}
    assert dom["d2"]["kind"] == "subdomain"
    assert dom["d2"]["label"] == "Bookings"
    assert dom["d2"]["parent_id"] == "d1"
    assert dom["d2"]["parent_name"] == "Revenue"


def test_parent_domain_closure_backfills_missing_top_level():
    """Regression (dangling parent): a top-level domain whose assets all live in its
    SUB-domains has no directly-tagged asset, so the asset-keyed rollup never emits it —
    yet its sub-domains carry a ``parent_id`` pointing at it. The closure must backfill the
    missing parent as a top-level domain node so the renderer nests the sub-domains under
    it (instead of re-rooting them onto the Estate org root) and the show/hide panel can
    list + hide it."""
    sig = {
        "nodes": [
            {"id": "asset:c.ops.regs", "kind": "table"},
            {"id": "asset:c.ops.deals", "kind": "table"},
        ],
        "edges": [],
    }
    # Both assets tagged to SUB-domains (s1, s2); the top-level parent d_ops has no asset.
    blob = _snap(sig, {"c.ops.regs": "s1", "c.ops.deals": "s2"}, domain_meta={
        "d_ops": {"name": "Alaska Airlines Operations", "parent_id": None, "origin": "applied"},
        "s1": {"name": "Aircraft Registry", "parent_id": "d_ops", "origin": "applied"},
        "s2": {"name": "Carrier Agreements", "parent_id": "d_ops", "origin": "applied"},
    })
    dom = {n["id"]: n for n in blob["domains"]["nodes"]}
    # The parent is now emitted (no longer dangling).
    assert "d_ops" in dom
    assert dom["d_ops"]["kind"] == "domain"
    assert dom["d_ops"]["parent_id"] is None
    assert dom["d_ops"]["label"] == "Alaska Airlines Operations"
    assert dom["d_ops"]["origin"] == "applied"
    # Backfilled parent has no direct members (its assets live in the sub-domains).
    assert dom["d_ops"]["member_count"] == 0
    # Sub-domains nest under it; NO node has a dangling parent_id.
    for sub in ("s1", "s2"):
        assert dom[sub]["kind"] == "subdomain"
        assert dom[sub]["parent_id"] == "d_ops"
    ids = set(dom)
    assert all(n["parent_id"] in ids for n in blob["domains"]["nodes"] if n.get("parent_id"))


def test_parent_domain_closure_walks_multiple_levels():
    """The closure walks the whole chain: an asset tagged to a leaf sub-domain whose parent
    AND grandparent are both absent backfills both ancestors (grandparent as a top-level
    domain, parent as a sub-domain), leaving no dangling reference."""
    sig = {"nodes": [{"id": "asset:c.ops.x", "kind": "table"}], "edges": []}
    blob = _snap(sig, {"c.ops.x": "leaf"}, domain_meta={
        "top": {"name": "Top", "parent_id": None},
        "mid": {"name": "Mid", "parent_id": "top"},
        "leaf": {"name": "Leaf", "parent_id": "mid"},
    })
    dom = {n["id"]: n for n in blob["domains"]["nodes"]}
    assert {"leaf", "mid", "top"} <= set(dom)
    assert dom["top"]["kind"] == "domain" and dom["top"]["parent_id"] is None
    assert dom["mid"]["kind"] == "subdomain" and dom["mid"]["parent_id"] == "top"
    assert dom["leaf"]["parent_id"] == "mid"
    ids = set(dom)
    assert all(n["parent_id"] in ids for n in blob["domains"]["nodes"] if n.get("parent_id"))


def test_unmapped_assets_still_fall_back_to_ungrouped():
    """Assets with no domain in the map (and no meta) keep the Ungrouped rollup —
    the fix must not fabricate domains for genuinely unassigned assets."""
    sig = {"nodes": [{"id": "asset:c.x.orphan", "kind": "table"}], "edges": []}
    blob = _snap(sig, {}, domain_meta={})
    dom = blob["domains"]["nodes"]
    assert len(dom) == 1
    assert dom[0]["id"] == "ungrouped"
    assert dom[0]["kind"] == "ungrouped"
    assert dom[0]["label"] == "Ungrouped"


# --- Ontology Map v2 (MV-D73): origin, sub-domain edges, snippets index ---

def test_rollup_origin_applied_proposed_and_ungrouped():
    """MV-D73 §2.1: a governed-tag-backed domain (materialize threads origin=applied
    from tag_decision reuse/reassign) rolls up ``applied``; a pure engine cluster
    (origin=proposed, tag_decision=create) rolls up ``proposed``; the Ungrouped blob
    (no meta) is always ``proposed``."""
    sig = {
        "nodes": [
            {"id": "asset:c.fin.ledger", "kind": "table"},   # applied domain d1
            {"id": "asset:c.mkt.leads", "kind": "table"},    # proposed domain d2
            {"id": "asset:c.x.orphan", "kind": "table"},     # ungrouped
        ],
        "edges": [],
    }
    blob = _snap(
        sig,
        {"c.fin.ledger": "d1", "c.mkt.leads": "d2"},
        domain_meta={
            "d1": {"name": "Finance", "parent_id": None, "origin": "applied"},
            "d2": {"name": "Marketing", "parent_id": None, "origin": "proposed"},
        },
    )
    dom = {n["id"]: n for n in blob["domains"]["nodes"]}
    assert dom["d1"]["origin"] == "applied"
    assert dom["d2"]["origin"] == "proposed"
    assert dom["ungrouped"]["origin"] == "proposed"


def test_meta_without_origin_defaults_to_proposed():
    """A meta dict predating MV-D73 (no ``origin`` key) degrades to ``proposed`` —
    never silently ``applied`` (honest current-state, MV-D74)."""
    sig = {"nodes": [{"id": "asset:c.fin.ledger", "kind": "table"}], "edges": []}
    blob = _snap(sig, {"c.fin.ledger": "d1"},
                 domain_meta={"d1": {"name": "Finance", "parent_id": None}})
    assert blob["domains"]["nodes"][0]["origin"] == "proposed"


def _subdomain_meta():
    # Two sub-domains under one Domain, plus a bare top-level Domain (no parent).
    return {
        "dom": {"name": "Revenue", "parent_id": None, "origin": "applied"},
        "s1": {"name": "Bookings", "parent_id": "dom", "origin": "applied"},
        "s2": {"name": "Fares", "parent_id": "dom", "origin": "proposed"},
        "d0": {"name": "Ops", "parent_id": None, "origin": "proposed"},
    }


def test_subdomain_cross_edge_emitted_once_and_deduped():
    """MV-D73 §2.2: two assets in different sub-domains with cross edges aggregate to
    exactly one deduped subdomains.edges entry per (src_sub, dst_sub, kind)."""
    sig = {
        "nodes": [
            {"id": "asset:c.rev.bookings", "kind": "table"},   # s1
            {"id": "asset:c.rev.pnr", "kind": "table"},        # s1
            {"id": "asset:c.rev.fares", "kind": "table"},      # s2
        ],
        "edges": [
            {"src": "asset:c.rev.bookings", "dst": "asset:c.rev.fares", "kind": "join_key"},
            {"src": "asset:c.rev.pnr", "dst": "asset:c.rev.fares", "kind": "join_key"},  # same (s1,s2,join_key) -> deduped
        ],
    }
    blob = _snap(sig, {"c.rev.bookings": "s1", "c.rev.pnr": "s1", "c.rev.fares": "s2"},
                 domain_meta=_subdomain_meta())
    subs = blob["subdomains"]["edges"]
    assert len(subs) == 1
    assert (subs[0]["src"], subs[0]["dst"], subs[0]["kind"]) == ("s1", "s2", "join_key")


def test_subdomain_intra_edge_and_absent_endpoint_are_dropped():
    """An intra-sub edge yields no sub-domain edge; an edge to an asset that maps to a
    top-level Domain (not a sub-domain) has an absent endpoint and is dropped (no
    dangling endpoint)."""
    sig = {
        "nodes": [
            {"id": "asset:c.rev.bookings", "kind": "table"},   # s1
            {"id": "asset:c.rev.pnr", "kind": "table"},        # s1 (intra)
            {"id": "asset:c.ops.log", "kind": "table"},        # d0 top-level domain, no sub
        ],
        "edges": [
            {"src": "asset:c.rev.bookings", "dst": "asset:c.rev.pnr", "kind": "join_key"},  # intra-sub
            {"src": "asset:c.rev.bookings", "dst": "asset:c.ops.log", "kind": "lineage_adjacency"},  # endpoint has no sub
        ],
    }
    blob = _snap(sig, {"c.rev.bookings": "s1", "c.rev.pnr": "s1", "c.ops.log": "d0"},
                 domain_meta=_subdomain_meta())
    assert blob["subdomains"]["edges"] == []


def test_snippets_index_present_and_capped():
    """MV-D73 §2.3: snippets_in re-keys measures to the mv:<fqn> hub and pages to the
    sub-domain domain_id, capping each list at the module bound."""
    measures = [{"ref": f"c.m.rev_mv.m{i}", "name": f"m{i}", "expression": "SUM(x)", "fmt": "$#,##0"}
                for i in range(layout.MAX_SNIPPET_MEASURES + 5)]
    pages = [{"page_id": f"p{i}", "title": f"Page {i}", "archetype": "metric", "domain_id": "s1"}
             for i in range(layout.MAX_SNIPPET_PAGES + 3)]
    sig = {"nodes": [{"id": "asset:c.rev.bookings", "kind": "table"}], "edges": []}
    blob = _snap(
        sig, {"c.rev.bookings": "s1"}, domain_meta=_subdomain_meta(),
        snippets_in={"measures": {"c.m.rev_mv": measures}, "pages": {"s1": pages}},
    )
    snip = blob["snippets"]
    assert len(snip["mv:c.m.rev_mv"]["measures"]) == layout.MAX_SNIPPET_MEASURES
    assert snip["mv:c.m.rev_mv"]["measures"][0] == {
        "ref": "c.m.rev_mv.m0", "name": "m0", "expression": "SUM(x)", "fmt": "$#,##0",
    }
    assert len(snip["s1"]["pages"]) == layout.MAX_SNIPPET_PAGES
    assert snip["s1"]["pages"][0]["page_id"] == "p0"


def test_snippets_key_absent_when_snippets_in_absent():
    """Absent snippets_in ⇒ the blob omits the snippets key (byte-stable with today)."""
    sig = {"nodes": [{"id": "asset:c.rev.bookings", "kind": "table"}], "edges": []}
    blob = _snap(sig, {"c.rev.bookings": "s1"}, domain_meta=_subdomain_meta())
    assert "snippets" not in blob
    # subdomains is always a valid (possibly empty) key.
    assert blob["subdomains"]["edges"] == []


def test_empty_signal_graph_yields_valid_blob_with_empty_new_keys():
    """MV-D43: an empty graph still yields a valid blob; the new keys are empty/absent
    and the run succeeds."""
    pytest.importorskip("igraph")
    row = layout.build_graph_snapshot(
        {"nodes": [], "edges": []}, {}, snippets_in={"measures": {}, "pages": {}},
        metastore_id="m", workspace_id="w", run_id="r", as_of="2026-01-01T00:00:00+00:00",
    )
    blob = json.loads(row["graph"])
    assert blob["subdomains"]["edges"] == []
    assert "snippets" not in blob
    assert blob["domains"]["nodes"] == [] and blob["assets"]["nodes"] == []


def test_snapshot_blob_is_byte_identical_across_runs():
    """Determinism (fixed seed, no new RNG): the same input built twice is byte-for-byte
    identical, snippets included."""
    pytest.importorskip("igraph")
    sig = {
        "nodes": [
            {"id": "asset:c.rev.bookings", "kind": "table"},
            {"id": "asset:c.rev.fares", "kind": "metric_view"},
        ],
        "edges": [{"src": "asset:c.rev.bookings", "dst": "asset:c.rev.fares", "kind": "join_key"}],
    }
    args = dict(
        node_domain_id={"c.rev.bookings": "s1", "c.rev.fares": "s2"},
        domain_meta=_subdomain_meta(),
        snippets_in={"measures": {"c.m.rev_mv": [{"ref": "c.m.rev_mv.m0", "name": "m0",
                     "expression": "SUM(x)", "fmt": ""}]}, "pages": {"s1": [
                     {"page_id": "p0", "title": "T", "archetype": "metric", "domain_id": "s1"}]}},
        metastore_id="m", workspace_id="w", run_id="r", as_of="2026-01-01T00:00:00+00:00",
    )
    a = layout.build_graph_snapshot({"nodes": sig["nodes"], "edges": sig["edges"]},
                                    args.pop("node_domain_id"), **args)
    b = layout.build_graph_snapshot({"nodes": sig["nodes"], "edges": sig["edges"]},
                                    {"c.rev.bookings": "s1", "c.rev.fares": "s2"},
                                    domain_meta=_subdomain_meta(),
                                    snippets_in={"measures": {"c.m.rev_mv": [{"ref": "c.m.rev_mv.m0",
                                                 "name": "m0", "expression": "SUM(x)", "fmt": ""}]},
                                                 "pages": {"s1": [{"page_id": "p0", "title": "T",
                                                 "archetype": "metric", "domain_id": "s1"}]}},
                                    metastore_id="m", workspace_id="w", run_id="r",
                                    as_of="2026-01-01T00:00:00+00:00")
    assert a["graph"] == b["graph"]


# --- Ontology Map north-star: Data Lane (MV-D82) — org root, canonical parent,
#     attach_level, verb + rel_class ---

def _assets_by_id(blob):
    return {n["id"]: n for n in blob["assets"]["nodes"]}


def test_mv_membership_gives_asset_a_containment_parent():
    """Build B: a table that is the TARGET of an mv_membership edge attaches to that
    metric view — parent_id = the mv: source, attach_level = "asset"."""
    sig = {
        "nodes": [
            {"id": "mv:c.m.rev_mv", "kind": "metric_view"},
            {"id": "asset:c.rev.fact", "kind": "table"},
        ],
        "edges": [{"src": "mv:c.m.rev_mv", "dst": "asset:c.rev.fact", "kind": "mv_membership"}],
    }
    blob = _snap(sig, {"c.rev.fact": "d1"}, domain_meta={"d1": {"name": "Revenue", "parent_id": None}})
    fact = _assets_by_id(blob)["asset:c.rev.fact"]
    assert fact["parent_id"] == "mv:c.m.rev_mv"
    assert fact["attach_level"] == "asset"


def test_table_read_by_two_mvs_keeps_only_strongest_parent_surplus_stays_edge():
    """Build B (canonical-parent) + Build C: a table read by two MVs keeps ONE tree
    parent (higher weight wins); both memberships remain edges with verb "reads"."""
    sig = {
        "nodes": [
            {"id": "mv:c.m.a_mv", "kind": "metric_view"},
            {"id": "mv:c.m.b_mv", "kind": "metric_view"},
            {"id": "asset:c.rev.fact", "kind": "table"},
        ],
        "edges": [
            {"src": "mv:c.m.a_mv", "dst": "asset:c.rev.fact", "kind": "mv_membership", "weight": 0.9},
            {"src": "mv:c.m.b_mv", "dst": "asset:c.rev.fact", "kind": "mv_membership", "weight": 0.5},
        ],
    }
    blob = _snap(sig, {"c.rev.fact": "d1"}, domain_meta={"d1": {"name": "Revenue", "parent_id": None}})
    fact = _assets_by_id(blob)["asset:c.rev.fact"]
    assert fact["parent_id"] == "mv:c.m.a_mv"  # higher weight wins the single parent
    assert fact["attach_level"] == "asset"
    # Neither membership was deleted; the surplus (b_mv) survives as a verb edge.
    memberships = [e for e in blob["assets"]["edges"] if e["kind"] == "mv_membership"]
    assert len(memberships) == 2
    surplus = [e for e in memberships if e["src"] == "mv:c.m.b_mv"]
    assert len(surplus) == 1 and surplus[0]["verb"] == "reads"


def test_no_containment_asset_attaches_to_domain_subdomain_or_null():
    """Build B fallback: no containment → attach to the domain. A sub-domain asset →
    "subdomain", a top-level-domain asset → "domain", an ungrouped asset → parent null."""
    sig = {
        "nodes": [
            {"id": "asset:c.rev.book", "kind": "table"},    # s1 (sub-domain under dom)
            {"id": "asset:c.ops.log", "kind": "table"},     # d0 (top-level domain)
            {"id": "asset:c.x.orphan", "kind": "table"},    # ungrouped (unmapped)
        ],
        "edges": [],
    }
    blob = _snap(sig, {"c.rev.book": "s1", "c.ops.log": "d0"}, domain_meta=_subdomain_meta())
    a = _assets_by_id(blob)
    assert (a["asset:c.rev.book"]["attach_level"], a["asset:c.rev.book"]["parent_id"]) == ("subdomain", "s1")
    assert (a["asset:c.ops.log"]["attach_level"], a["asset:c.ops.log"]["parent_id"]) == ("domain", "d0")
    assert (a["asset:c.x.orphan"]["attach_level"], a["asset:c.x.orphan"]["parent_id"]) == ("domain", None)


def test_join_key_edge_verb_and_rel_class_cross_and_within_top_domain():
    """Build C: a join_key edge carries verb "shares"; rel_class is "xdom" across two
    top domains and "shared" within one (sub-domains sharing a parent Domain)."""
    # Cross: c.rev.fact's top domain is "dom" (via s1); c.ops.log's is "d0" → xdom.
    cross = _snap(
        {"nodes": [{"id": "asset:c.rev.fact", "kind": "table"},
                   {"id": "asset:c.ops.log", "kind": "table"}],
         "edges": [{"src": "asset:c.rev.fact", "dst": "asset:c.ops.log", "kind": "join_key"}]},
        {"c.rev.fact": "s1", "c.ops.log": "d0"}, domain_meta=_subdomain_meta(),
    )
    e = cross["assets"]["edges"][0]
    assert e["verb"] == "shares" and e["rel_class"] == "xdom"

    # Within: two sub-domains (s1, s2) both roll up to the same top domain "dom" → shared.
    within = _snap(
        {"nodes": [{"id": "asset:c.rev.book", "kind": "table"},
                   {"id": "asset:c.rev.fare", "kind": "table"}],
         "edges": [{"src": "asset:c.rev.book", "dst": "asset:c.rev.fare", "kind": "join_key"}]},
        {"c.rev.book": "s1", "c.rev.fare": "s2"}, domain_meta=_subdomain_meta(),
    )
    e2 = within["assets"]["edges"][0]
    assert e2["verb"] == "shares" and e2["rel_class"] == "shared"


def test_org_root_present_when_non_empty_and_absent_when_empty():
    """Build A: a non-empty graph emits a single ``org`` root keyed by metastore_id; an
    empty graph omits the root key entirely and still yields a valid blob (MV-D43)."""
    blob = _snap({"nodes": [{"id": "asset:c.rev.book", "kind": "table"}], "edges": []},
                 {"c.rev.book": "s1"}, domain_meta=_subdomain_meta())
    assert blob["root"] == {"id": "m", "label": "Estate", "kind": "org"}

    pytest.importorskip("igraph")
    row = layout.build_graph_snapshot(
        {"nodes": [], "edges": []}, {},
        metastore_id="m", workspace_id="w", run_id="r", as_of="2026-01-01T00:00:00+00:00",
    )
    empty = json.loads(row["graph"])
    assert "root" not in empty
    assert empty["domains"]["nodes"] == [] and empty["assets"]["nodes"] == []


def test_data_lane_keys_are_byte_identical_across_runs():
    """Determinism: a graph exercising containment + cross-domain edges (so root,
    parent_id, attach_level, verb, rel_class are all populated) is byte-identical twice."""
    pytest.importorskip("igraph")
    sig = {
        "nodes": [
            {"id": "mv:c.m.a_mv", "kind": "metric_view"},
            {"id": "asset:c.rev.fact", "kind": "table"},
            {"id": "asset:c.ops.log", "kind": "table"},
        ],
        "edges": [
            {"src": "mv:c.m.a_mv", "dst": "asset:c.rev.fact", "kind": "mv_membership", "weight": 0.9},
            {"src": "asset:c.rev.fact", "dst": "asset:c.ops.log", "kind": "join_key"},
        ],
    }
    kw = dict(metastore_id="m", workspace_id="w", run_id="r", as_of="2026-01-01T00:00:00+00:00")
    a = layout.build_graph_snapshot(sig, {"c.rev.fact": "s1", "c.ops.log": "d0"},
                                    domain_meta=_subdomain_meta(), **kw)
    b = layout.build_graph_snapshot(sig, {"c.rev.fact": "s1", "c.ops.log": "d0"},
                                    domain_meta=_subdomain_meta(), **kw)
    assert a["graph"] == b["graph"]


# --- Ontology Map north-star gap-closure: data enrichment (MV-D86, Lane D2) —
#     per-node description + compact meta bag + deeper agent⊃mv⊃table containment ---


def test_asset_node_carries_description_when_source_has_it():
    """MV-D86 accept (a): an asset node threads the UC comment carried on the 17d node
    onto its ``description``; a node without a comment degrades to ``None`` (never blank)."""
    sig = {"nodes": [
        {"id": "asset:c.rev.fact", "kind": "table", "description": "Revenue fact table"},
        {"id": "asset:c.rev.dim", "kind": "table"},  # no comment → null
    ], "edges": []}
    blob = _snap(sig, {"c.rev.fact": "d1", "c.rev.dim": "d1"},
                 domain_meta={"d1": {"name": "Revenue", "parent_id": None}})
    a = _assets_by_id(blob)
    assert a["asset:c.rev.fact"]["description"] == "Revenue fact table"
    assert a["asset:c.rev.dim"]["description"] is None


def test_rollup_node_carries_domain_description_when_meta_has_it():
    """MV-D86 accept (a): a domain rollup threads ``genie_ont_domains.description`` (via
    domain_meta) onto its node; a meta-less cluster / the Ungrouped blob stays ``None``."""
    sig = {"nodes": [{"id": "asset:c.fin.ledger", "kind": "table"},
                     {"id": "asset:c.x.orphan", "kind": "table"}], "edges": []}
    blob = _snap(sig, {"c.fin.ledger": "d1"},
                 domain_meta={"d1": {"name": "Finance", "parent_id": None,
                                     "description": "All finance-governed assets"}})
    dom = {n["id"]: n for n in blob["domains"]["nodes"]}
    assert dom["d1"]["description"] == "All finance-governed assets"
    assert dom["ungrouped"]["description"] is None


def test_meta_bag_carries_type_appropriate_keys_and_omits_absent():
    """MV-D86 accept (b): each kind's meta bag carries only the fields the inventory had,
    omits a field with no signal, and a node with zero signals yields ``meta = None``."""
    sig = {"nodes": [
        {"id": "asset:c.rev.fact", "kind": "table", "row_count": 1000000,
         "data_format": "DELTA", "freshness": "2026-09-01", "storage_path": "s3://x/fact"},
        {"id": "mv:c.m.rev_mv", "kind": "metric_view", "measure_count": 4,
         "dimension_count": 3},  # no freshness → omitted
        {"id": "agent:sales", "kind": "agent", "queries_28d": 128},  # no sample_questions
        {"id": "asset:c.plain.tbl", "kind": "table"},  # no signals → meta None
    ], "edges": []}
    blob = _snap(sig, {"c.rev.fact": "d1", "c.plain.tbl": "d1"},
                 domain_meta={"d1": {"name": "Revenue", "parent_id": None}})
    a = _assets_by_id(blob)
    assert a["asset:c.rev.fact"]["meta"] == {
        "rows": "1000000", "format": "DELTA", "freshness": "2026-09-01", "path": "s3://x/fact"}
    mv = a["mv:c.m.rev_mv"]
    assert mv["meta"] == {"measures": "4", "dimensions": "3"}  # freshness omitted, no signal
    assert "freshness" not in mv["meta"]
    assert a["agent:sales"]["meta"] == {"queries_28d": "128"}  # sample_questions omitted
    assert a["asset:c.plain.tbl"]["meta"] is None  # no signals at all


def test_containment_depth_reaches_agent_metric_view_table():
    """MV-D86 accept (c): with an ``agent_scope`` (agent→table) and an ``mv_membership``
    (mv→table) over the SAME table, the tree nests agent ⊃ metric_view ⊃ table — the MV
    is derived under the agent that scopes its source table (not a flat sub-area)."""
    sig = {"nodes": [
        {"id": "agent:sales_agent", "kind": "agent"},
        {"id": "mv:c.m.rev_mv", "kind": "metric_view"},
        {"id": "asset:c.rev.fact", "kind": "table"},
    ], "edges": [
        {"src": "agent:sales_agent", "dst": "asset:c.rev.fact", "kind": "agent_scope"},
        {"src": "mv:c.m.rev_mv", "dst": "asset:c.rev.fact", "kind": "mv_membership"},
    ]}
    blob = _snap(sig, {"c.rev.fact": "d1"},
                 domain_meta={"d1": {"name": "Revenue", "parent_id": None}})
    a = _assets_by_id(blob)
    # The table's single parent is the MV (mv_membership outranks agent_scope, Build B).
    assert a["asset:c.rev.fact"]["parent_id"] == "mv:c.m.rev_mv"
    assert a["asset:c.rev.fact"]["attach_level"] == "asset"
    # The MV nests under the agent that scopes its source table (MV-D86 deeper containment).
    assert a["mv:c.m.rev_mv"]["parent_id"] == "agent:sales_agent"
    assert a["mv:c.m.rev_mv"]["attach_level"] == "asset"
    # The agent tops the chain, attached to its domain (not another asset).
    assert a["agent:sales_agent"]["parent_id"] is None
    assert a["agent:sales_agent"]["attach_level"] == "domain"
    # Walk parent_id: table → mv → agent — three distinct depths, NOT a flat sub-area.
    chain, cur, seen = [], "asset:c.rev.fact", set()
    while cur and cur not in seen:
        seen.add(cur)
        chain.append(cur)
        cur = a.get(cur, {}).get("parent_id")
    assert chain == ["asset:c.rev.fact", "mv:c.m.rev_mv", "agent:sales_agent"]


def test_mv_without_scoping_agent_keeps_domain_fallback():
    """MV-D86: the derived agent⊃mv link fires ONLY from a real overlap — an MV whose
    tables no agent scopes keeps its domain fallback (no fabricated parent)."""
    sig = {"nodes": [
        {"id": "mv:c.m.rev_mv", "kind": "metric_view"},
        {"id": "asset:c.rev.fact", "kind": "table"},
    ], "edges": [
        {"src": "mv:c.m.rev_mv", "dst": "asset:c.rev.fact", "kind": "mv_membership"},
    ]}
    blob = _snap(sig, {"c.rev.fact": "d1", "c.m.rev_mv": "d1"},
                 domain_meta={"d1": {"name": "Revenue", "parent_id": None}})
    mv = _assets_by_id(blob)["mv:c.m.rev_mv"]
    assert mv["parent_id"] == "d1" and mv["attach_level"] == "domain"


def test_enriched_keys_are_byte_identical_across_runs():
    """MV-D86 accept (d): a graph exercising description + meta + agent⊃mv⊃table containment
    is byte-for-byte identical built twice (fixed seed, deterministic derivation)."""
    pytest.importorskip("igraph")
    sig = {"nodes": [
        {"id": "agent:a", "kind": "agent", "queries_28d": 42, "sample_question_count": 6},
        {"id": "mv:c.m.mv", "kind": "metric_view", "measure_count": 2},
        {"id": "asset:c.rev.fact", "kind": "table", "description": "rev", "row_count": 10,
         "data_format": "DELTA"},
    ], "edges": [
        {"src": "agent:a", "dst": "asset:c.rev.fact", "kind": "agent_scope"},
        {"src": "mv:c.m.mv", "dst": "asset:c.rev.fact", "kind": "mv_membership"},
    ]}
    kw = dict(
        domain_meta={"d1": {"name": "Rev", "parent_id": None, "description": "the revenue domain"}},
        metastore_id="m", workspace_id="w", run_id="r", as_of="2026-01-01T00:00:00+00:00",
    )
    x = layout.build_graph_snapshot(sig, {"c.rev.fact": "d1"}, **kw)
    y = layout.build_graph_snapshot(sig, {"c.rev.fact": "d1"}, **kw)
    assert x["graph"] == y["graph"]


# --- Ontology Map interaction pass: per-edge evidence bag (MV-D88, Lane E) —
#     reveal-don't-invent detail sourced from the fused signal-graph edge ---


def _asset_edges_by_pair(blob):
    return {(e["src"], e["dst"]): e for e in blob["assets"]["edges"]}


def test_join_key_edge_detail_columns_present_and_absent():
    """MV-D88 accept (a): a join_key edge carries ``detail.columns`` when the fused edge
    names them (and ``kind`` "foreign key" from source ``foreign_key``); a proxy edge with
    no named columns omits ``columns`` and reports ``kind`` "shared column"."""
    sig = {
        "nodes": [
            {"id": "asset:c.rev.fact", "kind": "table"},
            {"id": "asset:c.rev.dim", "kind": "table"},
            {"id": "asset:c.rev.bridge", "kind": "table"},
        ],
        "edges": [
            # Declared FK, with named join columns → detail.columns present.
            {"src": "asset:c.rev.fact", "dst": "asset:c.rev.dim", "kind": "join_key",
             "source": "foreign_key", "columns": ["route_id"]},
            # Shared-join-column proxy, no columns named → detail.kind only, no columns key.
            {"src": "asset:c.rev.fact", "dst": "asset:c.rev.bridge", "kind": "join_key",
             "source": "shared_join_column"},
        ],
    }
    blob = _snap(sig, {"c.rev.fact": "d1", "c.rev.dim": "d1", "c.rev.bridge": "d1"},
                 domain_meta={"d1": {"name": "Revenue", "parent_id": None}})
    edges = _asset_edges_by_pair(blob)
    fk = edges[("asset:c.rev.fact", "asset:c.rev.dim")]
    assert fk["detail"] == {"columns": "route_id", "kind": "foreign key"}
    proxy = edges[("asset:c.rev.fact", "asset:c.rev.bridge")]
    assert proxy["detail"] == {"kind": "shared column"}
    assert "columns" not in proxy["detail"]


def test_join_key_multi_column_label():
    """Multiple shared columns render as a plain comma-joined label."""
    sig = {"nodes": [{"id": "asset:c.a.x", "kind": "table"},
                     {"id": "asset:c.a.y", "kind": "table"}],
           "edges": [{"src": "asset:c.a.x", "dst": "asset:c.a.y", "kind": "join_key",
                      "source": "foreign_key", "columns": ["route_id", "day"]}]}
    blob = _snap(sig, {"c.a.x": "d1", "c.a.y": "d1"},
                 domain_meta={"d1": {"name": "A", "parent_id": None}})
    e = blob["assets"]["edges"][0]
    assert e["detail"]["columns"] == "route_id, day"


def test_co_query_and_semantic_sim_detail_are_plain_labels_not_floats():
    """MV-D88 accept (b): co_query renders a plain session count and semantic_sim a plain
    similarity band — both strings, never a bare float (MV-D35)."""
    sig = {
        "nodes": [
            {"id": "asset:c.a.t1", "kind": "table"},
            {"id": "asset:c.a.t2", "kind": "table"},
            {"id": "asset:c.a.t3", "kind": "table"},
        ],
        "edges": [
            {"src": "asset:c.a.t1", "dst": "asset:c.a.t2", "kind": "co_query", "weight": 42.0},
            {"src": "asset:c.a.t1", "dst": "asset:c.a.t3", "kind": "semantic_sim", "weight": 0.95},
        ],
    }
    blob = _snap(sig, {"c.a.t1": "d1", "c.a.t2": "d1", "c.a.t3": "d1"},
                 domain_meta={"d1": {"name": "A", "parent_id": None}})
    edges = {e["kind"]: e for e in blob["assets"]["edges"]}
    cq = edges["co_query"]["detail"]
    assert cq == {"co_queried": "42 sessions"}
    assert isinstance(cq["co_queried"], str)
    ss = edges["semantic_sim"]["detail"]
    assert ss == {"similarity": "very high"}
    assert isinstance(ss["similarity"], str)


def test_semantic_sim_bands_track_l3_thresholds():
    """The band follows the L3 dedup_gate boundaries: ≥0.90 very high, ≥0.72 high, else
    moderate — so the label is meaningful, not an arbitrary cut."""
    sig = {"nodes": [
        {"id": "asset:c.a.hub", "kind": "table"},
        {"id": "asset:c.a.vh", "kind": "table"},
        {"id": "asset:c.a.h", "kind": "table"},
        {"id": "asset:c.a.m", "kind": "table"},
    ], "edges": [
        {"src": "asset:c.a.hub", "dst": "asset:c.a.vh", "kind": "semantic_sim", "weight": 0.90},
        {"src": "asset:c.a.hub", "dst": "asset:c.a.h", "kind": "semantic_sim", "weight": 0.72},
        {"src": "asset:c.a.hub", "dst": "asset:c.a.m", "kind": "semantic_sim", "weight": 0.71},
    ]}
    blob = _snap(sig, {"c.a.hub": "d1", "c.a.vh": "d1", "c.a.h": "d1", "c.a.m": "d1"},
                 domain_meta={"d1": {"name": "A", "parent_id": None}})
    band = {e["dst"]: e["detail"]["similarity"] for e in blob["assets"]["edges"]}
    assert band == {"asset:c.a.vh": "very high", "asset:c.a.h": "high", "asset:c.a.m": "moderate"}


def test_lineage_agent_and_mv_membership_detail():
    """Directional + role verbs: lineage_adjacency → ``flow`` "feeds", agent_scope →
    ``role`` "queries", mv_membership → ``role`` "aggregates" + measure count from snippets."""
    sig = {"nodes": [
        {"id": "agent:sales", "kind": "agent"},
        {"id": "mv:c.m.rev_mv", "kind": "metric_view"},
        {"id": "asset:c.rev.fact", "kind": "table"},
        {"id": "asset:c.rev.dim", "kind": "table"},
    ], "edges": [
        {"src": "asset:c.rev.fact", "dst": "asset:c.rev.dim", "kind": "lineage_adjacency"},
        {"src": "agent:sales", "dst": "asset:c.rev.fact", "kind": "agent_scope"},
        {"src": "mv:c.m.rev_mv", "dst": "asset:c.rev.fact", "kind": "mv_membership"},
    ]}
    blob = _snap(sig, {"c.rev.fact": "d1", "c.rev.dim": "d1"},
                 domain_meta={"d1": {"name": "Revenue", "parent_id": None}},
                 snippets_in={"measures": {"c.m.rev_mv": [
                     {"ref": "c.m.rev_mv.m0", "name": "m0", "expression": "SUM(x)", "fmt": ""},
                     {"ref": "c.m.rev_mv.m1", "name": "m1", "expression": "SUM(y)", "fmt": ""},
                 ]}, "pages": {}})
    edges = {e["kind"]: e for e in blob["assets"]["edges"]}
    assert edges["lineage_adjacency"]["detail"] == {"flow": "feeds"}
    assert edges["agent_scope"]["detail"] == {"role": "queries"}
    assert edges["mv_membership"]["detail"] == {"role": "aggregates", "measures": "2"}


def test_mv_membership_detail_without_snippets_omits_measures():
    """Reveal-don't-invent: no snippet index ⇒ the mv_membership ``measures`` key is
    omitted; only the deterministic ``role`` survives."""
    sig = {"nodes": [
        {"id": "mv:c.m.rev_mv", "kind": "metric_view"},
        {"id": "asset:c.rev.fact", "kind": "table"},
    ], "edges": [
        {"src": "mv:c.m.rev_mv", "dst": "asset:c.rev.fact", "kind": "mv_membership"},
    ]}
    blob = _snap(sig, {"c.rev.fact": "d1"}, domain_meta={"d1": {"name": "Revenue", "parent_id": None}})
    e = next(e for e in blob["assets"]["edges"] if e["kind"] == "mv_membership")
    assert e["detail"] == {"role": "aggregates"}


def test_no_signal_edges_omit_detail():
    """MV-D88 accept (c): an unlisted edge kind, and a co_query edge with no weight, carry
    NO ``detail`` key in the blob (so the route degrades to ``detail=None``)."""
    sig = {"nodes": [
        {"id": "asset:c.a.t1", "kind": "table"},
        {"id": "asset:c.a.t2", "kind": "table"},
        {"id": "asset:c.a.t3", "kind": "table"},
    ], "edges": [
        {"src": "asset:c.a.t1", "dst": "asset:c.a.t2", "kind": "mystery_kind"},   # unlisted
        {"src": "asset:c.a.t1", "dst": "asset:c.a.t3", "kind": "co_query"},       # weight absent
    ]}
    blob = _snap(sig, {"c.a.t1": "d1", "c.a.t2": "d1", "c.a.t3": "d1"},
                 domain_meta={"d1": {"name": "A", "parent_id": None}})
    for e in blob["assets"]["edges"]:
        assert "detail" not in e


def test_edge_detail_propagates_to_domain_and_subdomain_rollups():
    """A cross-sub join_key surfaces the SAME evidence bag on the domain rollup edge and
    the sub-domain rollup edge (carried from the representative asset edge, like weight)."""
    sig = {"nodes": [
        {"id": "asset:c.rev.book", "kind": "table"},   # s1
        {"id": "asset:c.rev.fare", "kind": "table"},   # s2
    ], "edges": [
        {"src": "asset:c.rev.book", "dst": "asset:c.rev.fare", "kind": "join_key",
         "source": "foreign_key", "columns": ["fare_id"]},
    ]}
    blob = _snap(sig, {"c.rev.book": "s1", "c.rev.fare": "s2"}, domain_meta=_subdomain_meta())
    dedges = blob["domains"]["edges"]
    assert len(dedges) == 1
    assert dedges[0]["detail"] == {"columns": "fare_id", "kind": "foreign key"}
    subs = blob["subdomains"]["edges"]
    assert len(subs) == 1
    assert subs[0]["detail"] == {"columns": "fare_id", "kind": "foreign key"}


def test_edge_detail_keys_are_byte_identical_across_runs():
    """MV-D88 accept (d): a graph exercising every detail-bearing edge kind (with the new
    ``detail`` key populated) is byte-for-byte identical when built twice."""
    pytest.importorskip("igraph")
    sig = {"nodes": [
        {"id": "agent:sales", "kind": "agent"},
        {"id": "mv:c.m.rev_mv", "kind": "metric_view"},
        {"id": "asset:c.rev.fact", "kind": "table"},
        {"id": "asset:c.rev.dim", "kind": "table"},
        {"id": "asset:c.rev.other", "kind": "table"},
    ], "edges": [
        {"src": "asset:c.rev.fact", "dst": "asset:c.rev.dim", "kind": "join_key",
         "source": "foreign_key", "columns": ["route_id", "day"]},
        {"src": "asset:c.rev.fact", "dst": "asset:c.rev.other", "kind": "co_query", "weight": 7.0},
        {"src": "asset:c.rev.dim", "dst": "asset:c.rev.other", "kind": "semantic_sim", "weight": 0.8},
        {"src": "asset:c.rev.fact", "dst": "asset:c.rev.dim", "kind": "lineage_adjacency"},
        {"src": "agent:sales", "dst": "asset:c.rev.fact", "kind": "agent_scope"},
        {"src": "mv:c.m.rev_mv", "dst": "asset:c.rev.fact", "kind": "mv_membership"},
    ]}
    kw = dict(
        domain_meta={"d1": {"name": "Revenue", "parent_id": None}},
        snippets_in={"measures": {"c.m.rev_mv": [{"ref": "c.m.rev_mv.m0", "name": "m0",
                     "expression": "SUM(x)", "fmt": ""}]}, "pages": {}},
        metastore_id="m", workspace_id="w", run_id="r", as_of="2026-01-01T00:00:00+00:00",
    )
    ids = {"c.rev.fact": "d1", "c.rev.dim": "d1", "c.rev.other": "d1"}
    a = layout.build_graph_snapshot(sig, dict(ids), **kw)
    b = layout.build_graph_snapshot(sig, dict(ids), **kw)
    assert a["graph"] == b["graph"]
    assert '"detail"' in a["graph"]  # the new key really rides in the serialized blob
