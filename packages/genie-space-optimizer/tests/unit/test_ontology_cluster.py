"""L4 clustering engine — offline unit tests (Phase 3b §11).

Deterministic + in-process; the naming LLM is mocked (never called). Covers the
two-level tree, connectedness (the Leiden guarantee), multiplex + soft-seed,
reuse/create/reassign, determinism/idempotency, and naming-degrade + LeakageOracle.
"""

from __future__ import annotations

from genie_space_optimizer.ontology import cluster, er, graph


# ── Fixtures ────────────────────────────────────────────────────────────────

_COMMERCIAL_MEMBERS = [
    "finance.sales.orders", "finance.sales.order_items", "finance.sales.order_revenue",
    "marketing.campaigns.leads", "marketing.campaigns.spend",
]
# `orders` is a lineage STAR hub (highest betweenness -> the anchor chip); marketing
# has its own spine; one co_query edge glues sales<->marketing under `Commercial`.
_COMMERCIAL_LINEAGE = [
    ("finance.sales.orders", "finance.sales.order_items"),
    ("finance.sales.orders", "finance.sales.order_revenue"),
    ("marketing.campaigns.leads", "marketing.campaigns.spend"),
]
_COMMERCIAL_COQUERY = [("finance.sales.orders", "marketing.campaigns.leads", 1.0)]


def _commercial_graph(extra_tags: list[dict] | None = None) -> dict:
    tags = [{"tag_key": "Commercial", "members": [{"fqn": f} for f in _COMMERCIAL_MEMBERS]}]
    tags += extra_tags or []
    return graph.build_signal_graph(
        {"tags": tags}, _COMMERCIAL_LINEAGE, co_query_edges=_COMMERCIAL_COQUERY,
    )


def _schema_namer(identifiers, anchor, company):
    """Deterministic test namer: schema -> business name (no LLM)."""
    schemas = {i.split(".")[1] for i in identifiers if "." in i}
    if "sales" in schemas:
        return "Sales"
    if "campaigns" in schemas:
        return "Marketing"
    return None


def _asset_edges(sig: dict) -> set[frozenset[str]]:
    """All asset<->asset adjacencies (structural + projected tag/agent cliques) for a
    connectivity check."""
    edges: set[frozenset[str]] = set()
    tag_groups: dict[str, set[str]] = {}
    agent_groups: dict[str, set[str]] = {}
    for e in sig["edges"]:
        a, b, kind = e["src"], e["dst"], e["kind"]
        if kind in cluster.STRUCTURAL_KINDS and a.startswith("asset:") and b.startswith("asset:"):
            edges.add(frozenset((a[6:], b[6:])))
        elif kind == "tag_assignment" and a.startswith("tag:") and b.startswith("asset:"):
            tag_groups.setdefault(a, set()).add(b[6:])
        elif kind == "agent_scope" and b.startswith("asset:"):
            agent_groups.setdefault(a, set()).add(b[6:])
    for grp in list(tag_groups.values()) + list(agent_groups.values()):
        members = sorted(grp)
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                edges.add(frozenset((members[i], members[j])))
    return edges


def _is_connected(members: tuple[str, ...], edges: set[frozenset[str]]) -> bool:
    members = tuple(members)
    if len(members) <= 1:
        return True
    adj: dict[str, set[str]] = {m: set() for m in members}
    mset = set(members)
    for e in edges:
        a, b = tuple(e)
        if a in mset and b in mset:
            adj[a].add(b)
            adj[b].add(a)
    seen = {members[0]}
    stack = [members[0]]
    while stack:
        cur = stack.pop()
        for nb in adj[cur]:
            if nb not in seen:
                seen.add(nb)
                stack.append(nb)
    return seen == mset


# ── Two-level tree (the worked example) ─────────────────────────────────────


def test_two_level_tree_commercial_sales_marketing():
    sig = _commercial_graph()
    props = cluster.qualify_subdomain_keys(cluster.cluster(sig, namer=_schema_namer))
    domains = [p for p in props if p.parent_id is None]
    subs = [p for p in props if p.parent_id is not None]

    assert len(domains) == 1
    dom = domains[0]
    assert dom.name == "Commercial" and dom.tag_decision == "reuse" and dom.tag_key == "Commercial"
    # orders is the lineage-centrality anchor (the MV-D35 headline chip).
    assert dom.evidence["anchor"] == "finance.sales.orders"

    assert {s.name for s in subs} == {"Sales", "Marketing"}
    assert all(s.parent_id == dom.domain_id for s in subs)
    sales = next(s for s in subs if s.name == "Sales")
    marketing = next(s for s in subs if s.name == "Marketing")
    assert set(sales.members) == {
        "finance.sales.orders", "finance.sales.order_items", "finance.sales.order_revenue",
    }
    assert set(marketing.members) == {"marketing.campaigns.leads", "marketing.campaigns.spend"}


# ── Connectedness (the Leiden guarantee Louvain does not give) ──────────────


def test_every_community_is_internally_connected():
    # A fixture with two dense blobs bridged only through the shared tag: a
    # modularity optimiser can leave a community internally disconnected; Leiden's
    # refinement guarantees each emitted community is a connected subgraph.
    tags = [{"tag_key": "Ops", "members": [{"fqn": f} for f in (
        "c.a.t1", "c.a.t2", "c.b.t3", "c.b.t4",
    )]}]
    lineage = [("c.a.t1", "c.a.t2"), ("c.b.t3", "c.b.t4")]
    sig = graph.build_signal_graph({"tags": tags}, lineage)
    edges = _asset_edges(sig)
    props = cluster.cluster(sig, namer=lambda i, a, c: None)
    assert props
    for p in props:
        assert _is_connected(p.members, edges), f"{p.name} is not internally connected: {p.members}"


# ── Multiplex + soft seed (initial_membership, NOT is_membership_fixed) ─────


def test_multiplex_layers_and_soft_seed_not_fixed(monkeypatch):
    import leidenalg as la

    captured: dict = {}
    orig_opt = la.Optimiser.optimise_partition_multiplex
    orig_init = la.CPMVertexPartition.__init__

    def spy_opt(self, partitions, layer_weights=None, n_iterations=2, is_membership_fixed=None):
        captured["n_layers"] = len(partitions)
        captured["layer_weights"] = layer_weights
        captured["is_membership_fixed"] = is_membership_fixed
        return orig_opt(self, partitions, layer_weights=layer_weights,
                        n_iterations=n_iterations, is_membership_fixed=is_membership_fixed)

    def spy_init(self, graph, initial_membership=None, **kw):
        if initial_membership is not None:
            captured["initial_membership_seen"] = True
        return orig_init(self, graph, initial_membership=initial_membership, **kw)

    monkeypatch.setattr(la.Optimiser, "optimise_partition_multiplex", spy_opt)
    monkeypatch.setattr(la.CPMVertexPartition, "__init__", spy_init)

    cluster.cluster(_commercial_graph(), namer=_schema_namer)

    # Separate layers (lineage + co_query + projected tag clique) — NOT a pre-fused scalar.
    assert captured["n_layers"] >= 2
    assert captured["layer_weights"] is not None and len(captured["layer_weights"]) == captured["n_layers"]
    # SOFT seed: initial_membership supplied, but is_membership_fixed NEVER set.
    assert captured.get("initial_membership_seen") is True
    assert captured["is_membership_fixed"] is None


# ── Reuse / create ──────────────────────────────────────────────────────────


def test_reuse_domain_create_subs_no_duplicate_tag():
    props = cluster.qualify_subdomain_keys(cluster.cluster(_commercial_graph(), namer=_schema_namer))
    dom = next(p for p in props if p.parent_id is None)
    assert dom.tag_decision == "reuse" and dom.tag_key == "Commercial"
    subs = [p for p in props if p.parent_id is not None]
    # No matching sub-domain tag exists -> create with a Domain/Sub-convention key.
    for s in subs:
        assert s.tag_decision == "create"
        assert s.tag_key.startswith("Commercial/") and "/" in s.tag_key
        assert s.tag_value == s.name
    # Never a duplicate of the reused Commercial tag.
    assert [p.tag_key for p in props].count("Commercial") == 1


def test_reuse_subdomain_when_governed_subtag_exists():
    # A Commercial/Sales governed tag already anchors the sales assets -> the Sales
    # sub-domain reuses it (never mints a duplicate).
    extra = [{"tag_key": "Commercial/Sales", "members": [{"fqn": f} for f in (
        "finance.sales.orders", "finance.sales.order_items", "finance.sales.order_revenue",
    )]}]
    props = cluster.qualify_subdomain_keys(cluster.cluster(_commercial_graph(extra), namer=_schema_namer))
    sales = next(p for p in props if p.parent_id is not None and set(p.members) >= {"finance.sales.orders"})
    assert sales.tag_decision == "reuse" and sales.tag_key == "Commercial/Sales"
    assert sales.tag_value == "Sales"


# ── Reassignment proposal (soft-seed conflict) vs below-margin reuse ────────


def _sales_ops_graph(moved_n: int) -> dict:
    """A 'Sales' domain tag on a1..a4; `moved_n` of them are strongly lineage-bound
    to a dense untagged Ops cluster, so the soft seed lets the graph pull them away."""
    tagged = ["x.sales.a1", "x.sales.a2", "x.sales.a3", "x.sales.a4"]
    lineage = [("x.sales.a1", "x.sales.a2"),
               ("y.ops.b1", "y.ops.b2"), ("y.ops.b2", "y.ops.b3"), ("y.ops.b1", "y.ops.b3")]
    for i in range(moved_n):
        a = tagged[3 - i]
        lineage += [(a, "y.ops.b1"), (a, "y.ops.b2"), (a, "y.ops.b3")]
    return graph.build_signal_graph({"tags": [{"tag_key": "Sales", "members": [{"fqn": f} for f in tagged]}]}, lineage)


def test_reassign_conflict_block_and_no_tag_write():
    props = cluster.cluster(_sales_ops_graph(2), namer=lambda i, a, c: None)
    reassigns = [p for p in props if p.tag_decision == "reassign"]
    assert len(reassigns) == 1
    r = reassigns[0]
    assert r.tag_key == "Sales"
    conflict = r.evidence["conflict"]
    assert conflict["existing_tag"] == "Sales"
    assert conflict["moved_members"]  # the members the graph pulled away
    assert conflict["margin"] > cluster.REASSIGN_MARGIN
    # A reassign is a PROPOSAL — never a governed-tag write. The DomainProposal carries
    # only proposal fields; there is no SET/UNSET TAG anywhere (see the firewall test).
    assert r.tag_decision in ("reuse", "create", "reassign")


def test_below_margin_stays_reuse():
    props = cluster.cluster(_sales_ops_graph(1), namer=lambda i, a, c: None)
    assert not any(p.tag_decision == "reassign" for p in props)
    sales = [p for p in props if p.tag_key == "Sales" and p.tag_decision == "reuse"]
    assert sales, "below-margin disagreement must stay reuse"


# ── Determinism / idempotency ───────────────────────────────────────────────


def test_deterministic_stable_domain_ids_and_no_dups():
    sig = _commercial_graph()
    run1 = cluster.cluster(sig, namer=_schema_namer)
    run2 = cluster.cluster(sig, namer=_schema_namer)
    assert [p.domain_id for p in run1] == [p.domain_id for p in run2]
    assert [p.members for p in run1] == [p.members for p in run2]
    ids = [p.domain_id for p in run1]
    assert len(ids) == len(set(ids))  # no duplicate domain ids


def test_domain_id_is_fingerprint_of_sorted_members():
    a = cluster.domain_id_of(["b.x.t2", "a.y.t1"])
    b = cluster.domain_id_of(["a.y.t1", "b.x.t2"])  # order-independent
    assert a == b and a.startswith("sug_")
    assert cluster.domain_id_of(["a.y.t1"]) != a


# ── Naming degrade + LeakageOracle ──────────────────────────────────────────


def test_naming_degrades_to_anchor_when_llm_fails():
    def boom(identifiers, anchor, company):
        raise RuntimeError("serving endpoint unavailable")

    props = cluster.qualify_subdomain_keys(cluster.cluster(_commercial_graph(), namer=boom))
    subs = [p for p in props if p.parent_id is not None]
    # Run still succeeds; create sub-domains fall back to a deterministic anchor-derived
    # name (schema segment) instead of an LLM name.
    assert subs and {s.name for s in subs} == {"Sales", "Campaigns"}


def test_leakage_oracle_rejects_invented_identifier():
    # An echoed raw identifier (underscore/digit token absent from the members) is a
    # leak -> rejected -> anchor-derived fallback.
    assert cluster.name_leaks("secret_table_9", ["finance.sales.orders"])
    assert not cluster.name_leaks("Commercial", ["finance.sales.orders"])

    def leaky(identifiers, anchor, company):
        return "leaked_table_42"

    props = cluster.cluster(_commercial_graph(), namer=leaky)
    subs = [p for p in props if p.parent_id is not None]
    assert subs and all(s.name != "leaked_table_42" for s in subs)


def test_pii_name_rejected():
    assert cluster.name_leaks("customer_ssn", ["finance.sales.orders"])


# ── Empty / trivial graph -> zero proposals ─────────────────────────────────


def test_empty_graph_yields_no_proposals():
    assert cluster.cluster({"nodes": [], "edges": []}) == []


# ── Canonical collapse: duplicate tags do not double-seed ───────────────────


# ── Rules-first grouping (Stage 1, MV-D51/52/53) ────────────────────────────


def test_fk_connected_component_domain_with_no_tag():
    # No governed tag at all — a foreign-key-connected component still forms a Domain.
    sig = graph.build_signal_graph(
        {"tags": []},
        join_key_edges=[
            ("c.rev.fact_revenue", "c.rev.dim_route"),
            ("c.rev.fact_revenue", "c.rev.dim_fare"),
        ],
    )
    domains = [p for p in cluster.cluster(sig, namer=lambda i, a, c: "Revenue") if p.parent_id is None]
    assert len(domains) == 1
    assert set(domains[0].members) == {"c.rev.fact_revenue", "c.rev.dim_route", "c.rev.dim_fare"}
    assert domains[0].tag_decision == "create"  # no tag to reuse
    assert "foreign key" in domains[0].evidence["reason"]


def test_shared_schema_domain_with_no_tag():
    # Assets sharing a schema (no tag, no lineage) group by the shared-schema rule.
    sig = graph.build_signal_graph(
        {"tags": []},
        schema_affinity={"c.loyalty": ["c.loyalty.members", "c.loyalty.tiers"]},
    )
    domains = [p for p in cluster.cluster(sig, namer=lambda i, a, c: None) if p.parent_id is None]
    assert len(domains) == 1
    assert set(domains[0].members) == {"c.loyalty.members", "c.loyalty.tiers"}
    assert "shared schema" in domains[0].evidence["reason"]


def test_metric_view_membership_domain_with_no_tag():
    sig = graph.build_signal_graph(
        {"tags": []},
        mv_membership={"c.metrics.rev_mv": ["c.rev.fact", "c.rev.dim"]},
    )
    domains = [p for p in cluster.cluster(sig, namer=lambda i, a, c: None) if p.parent_id is None]
    assert len(domains) == 1
    assert set(domains[0].members) == {"c.rev.fact", "c.rev.dim"}
    assert "metric view" in domains[0].evidence["reason"]


def test_tag_only_single_asset_makes_no_domain():
    # A governed tag on one asset, with no structural signal, no longer creates a
    # single-asset Domain (MV-D52 — a tag never solo-creates a Domain).
    sig = graph.build_signal_graph({"tags": [{"tag_key": "Widget", "members": [{"fqn": "c.s.only"}]}]})
    assert cluster.cluster(sig, namer=lambda i, a, c: None) == []


def test_facet_tag_never_becomes_a_domain():
    # A FACET tag (Data Tier) is routed out of candidacy; its assets carry no other
    # signal, so no Domain is created and no proposal reuses the facet tag.
    sig = graph.build_signal_graph(
        {"tags": [{"tag_key": "Data Tier", "members": [{"fqn": "c.s.a"}, {"fqn": "c.s.b"}]}]}
    )
    props = cluster.cluster(sig, namer=lambda i, a, c: None)
    assert all("Data Tier" != (p.tag_key or "") for p in props)


def test_leiden_runs_only_on_the_remainder():
    # An FK component is resolved by a rule; a separate lineage-only pair falls through
    # to Leiden. Each carries its own reason — rules first, clustering for the rest.
    sig = graph.build_signal_graph(
        {"tags": []},
        [("c.a.x", "c.a.y")],                     # lineage-only pair → Leiden remainder
        join_key_edges=[("c.b.p", "c.b.q")],      # FK pair → rule-resolved
    )
    reasons = {
        frozenset(p.members): p.evidence["reason"]
        for p in cluster.cluster(sig, namer=lambda i, a, c: None) if p.parent_id is None
    }
    assert "foreign key" in reasons[frozenset({"c.b.p", "c.b.q"})]
    assert "community detection" in reasons[frozenset({"c.a.x", "c.a.y"})]


def test_every_proposal_carries_a_plain_reason():
    for p in cluster.cluster(_commercial_graph(), namer=_schema_namer):
        assert isinstance(p.evidence.get("reason"), str) and p.evidence["reason"]


def test_duplicate_tags_collapse_to_canonical_seed():
    # 17d merged Finance/finance -> they must not seed two rival domains.
    sig = graph.build_signal_graph(
        {"tags": [
            {"tag_key": "Finance", "members": [{"fqn": "c.fin.ledger"}]},
            {"tag_key": "finance", "members": [{"fqn": "c.fin.gl"}]},
        ]},
        [("c.fin.ledger", "c.fin.gl")],
    )
    verdict = er.DedupeVerdict(
        canonical_id="dedupe_x", members=("Finance", "finance"),
        verdict="merge", method="exact", score=1.0, reason=None,
    )
    props = cluster.cluster(sig, identity=[verdict], namer=lambda i, a, c: None)
    domains = [p for p in props if p.parent_id is None]
    assert len(domains) == 1  # one canonical Finance domain, not two
    assert domains[0].tag_decision == "reuse"
    assert domains[0].tag_key == "Finance"  # sorted-first canonical representative


# ── Stage 2: sub-domains from an EXPLICIT boundary (MV-D54) ─────────────────
#
# Precedence: (1) governed slash sub-tags → (2) a value-carrying tag's distinct
# values → (3) schema-within-domain → (4) MV/FK component; the finer Leiden split
# is the FALLBACK ONLY when NONE apply. Each sub-domain carries a boundary reason.


def _fine_calls(monkeypatch):
    """Spy on the sub-split Leiden invocations: record ``(gamma, frozenset(verts))``
    for every ``_run_multiplex`` call so a test can assert the finer split (gamma_fine)
    fires ONLY over a Domain with no explicit boundary."""
    calls: list[tuple[float, frozenset[str]]] = []
    orig = cluster._run_multiplex

    def spy(vertices, edges_by_kind, *, gamma, seed_membership):
        calls.append((gamma, frozenset(vertices)))
        return orig(vertices, edges_by_kind, gamma=gamma, seed_membership=seed_membership)

    monkeypatch.setattr(cluster, "_run_multiplex", spy)
    return calls


def test_slash_subtag_becomes_subdomain_under_parent():
    # A governed 'Reservation/PNR' sub-tag whose parent is the Reservation domain tag
    # -> a PNR sub-domain (reuse), Y under X, carrying the sub-tag boundary reason.
    tags = [
        {"tag_key": "Reservation", "members": [{"fqn": f} for f in (
            "air.res.pnr", "air.res.segment", "air.res.ticket", "air.res.coupon")]},
        {"tag_key": "Reservation/PNR", "members": [{"fqn": f} for f in ("air.res.pnr", "air.res.segment")]},
    ]
    sig = graph.build_signal_graph(
        {"tags": tags}, [("air.res.pnr", "air.res.segment"), ("air.res.ticket", "air.res.coupon")])
    props = cluster.cluster(sig, namer=lambda i, a, c: None)
    dom = next(p for p in props if p.parent_id is None)
    assert dom.tag_key == "Reservation"
    subs = [p for p in props if p.parent_id is not None]
    pnr = next(s for s in subs if s.tag_key == "Reservation/PNR")
    assert pnr.parent_id == dom.domain_id
    # tag_value keeps the governed value; the rendered name is humanized ("PNR"->"Pnr").
    assert pnr.tag_decision == "reuse" and pnr.tag_value == "PNR" and pnr.name == "Pnr"
    assert set(pnr.members) == {"air.res.pnr", "air.res.segment"}
    assert pnr.evidence["reason"] == "sub-tag: Reservation/PNR"


def test_value_carrying_tag_names_subdomains_by_value(monkeypatch):
    # A value-carrying tag (mvm_subdomain=fare_pricing / route_ops) over the Domain's
    # assets -> one sub-domain per distinct value (reuse of the tag+value); Leiden is
    # NOT consulted (an explicit boundary exists).
    fine = _fine_calls(monkeypatch)
    _V = ["rev.core.fare_a", "rev.core.fare_b", "rev.core.route_x", "rev.core.route_y"]
    tags = [
        # A domain membership tag binds the Domain; mvm_subdomain carries the per-asset
        # values that split it into sub-domains (the value tag is not the domain tag).
        {"tag_key": "Revenue", "members": [{"fqn": f} for f in _V]},
        {"tag_key": "mvm_subdomain", "members": [
            {"fqn": "rev.core.fare_a", "tag_value": "fare_pricing"},
            {"fqn": "rev.core.fare_b", "tag_value": "fare_pricing"},
            {"fqn": "rev.core.route_x", "tag_value": "route_ops"},
            {"fqn": "rev.core.route_y", "tag_value": "route_ops"},
        ]},
    ]
    # An FK component forms the Domain deterministically (no coarse Leiden).
    sig = graph.build_signal_graph(
        {"tags": tags},
        join_key_edges=[
            ("rev.core.fare_a", "rev.core.fare_b"),
            ("rev.core.fare_a", "rev.core.route_x"),
            ("rev.core.route_x", "rev.core.route_y"),
        ],
    )
    props = cluster.cluster(sig, namer=lambda i, a, c: "Revenue")
    subs = [p for p in props if p.parent_id is not None]
    assert {s.name for s in subs} == {"Fare Pricing", "Route Ops"}
    for s in subs:
        assert s.tag_decision == "reuse" and s.tag_key == "mvm_subdomain"
        assert s.tag_value in {"fare_pricing", "route_ops"}
        assert s.evidence["reason"] == f"mvm_subdomain={s.tag_value}"
    fare = next(s for s in subs if s.tag_value == "fare_pricing")
    assert set(fare.members) == {"rev.core.fare_a", "rev.core.fare_b"}
    # No explicit-boundary Domain ever reaches the finer Leiden split.
    assert not any(g == cluster.GAMMA_FINE for g, _ in fine)


def test_schema_within_domain_becomes_subdomains(monkeypatch):
    # A Domain spanning two schemas (no slash / no value tag) splits one sub-domain per
    # schema; the finer Leiden split is NOT consulted.
    fine = _fine_calls(monkeypatch)
    sig = graph.build_signal_graph(
        {"tags": []},
        join_key_edges=[
            ("air.rev.fact", "air.rev.dim"),      # x.rev schema
            ("air.rev.fact", "air.ops.log"),      # bridges to air.ops schema
        ],
    )
    props = cluster.cluster(sig, namer=lambda i, a, c: None)
    subs = [p for p in props if p.parent_id is not None]
    reasons = {s.evidence["reason"] for s in subs}
    assert reasons == {"schema: air.rev", "schema: air.ops"}
    rev = next(s for s in subs if s.evidence["reason"] == "schema: air.rev")
    assert set(rev.members) == {"air.rev.fact", "air.rev.dim"}
    assert not any(g == cluster.GAMMA_FINE for g, _ in fine)


def test_leiden_is_the_fallback_only_where_no_explicit_boundary(monkeypatch):
    # Domain A (FK across two schemas) has an explicit schema boundary -> schema subs,
    # no Leiden. Domain B (single schema, two lineage blobs, no explicit boundary)
    # falls back to the finer Leiden split. Assert the finer split fires ONLY over B.
    fine = _fine_calls(monkeypatch)
    a_assets = {"a.rev.fact", "a.rev.dim", "a.ops.log"}
    b_assets = {"c.mkt.x1", "c.mkt.x2", "c.mkt.y1", "c.mkt.y2"}
    sig = graph.build_signal_graph(
        {"tags": []},
        [("c.mkt.x1", "c.mkt.x2"), ("c.mkt.y1", "c.mkt.y2")],   # B: two disconnected blobs
        join_key_edges=[("a.rev.fact", "a.rev.dim"), ("a.rev.fact", "a.ops.log")],  # A: FK component
        mv_membership={"c.mkt.mv": sorted(b_assets)},           # B: one MV -> one Domain
    )
    props = cluster.cluster(sig, namer=lambda i, a, c: None)
    a_subs = [p for p in props if p.parent_id is not None and set(p.members) <= a_assets]
    b_subs = [p for p in props if p.parent_id is not None and set(p.members) <= b_assets]
    assert {s.evidence["reason"] for s in a_subs} == {"schema: a.rev", "schema: a.ops"}
    assert b_subs and all(s.evidence["reason"] == "split by structure" for s in b_subs)
    # Every finer-γ (sub-split) Leiden call is over B's assets — never A's.
    fine_verts = [v for g, v in fine if g == cluster.GAMMA_FINE]
    assert fine_verts and all(v <= b_assets for v in fine_verts)
    assert not any(v & a_assets for v in fine_verts)


# ── Stage 3: curated-tag absorbs FK component + name dedup/qualification ────


def _prop(domain_id_members, *, parent_id=None, name="D", tag_decision="create",
          tag_key="D", tag_value="D", reason="grouped by foreign key / shared join column",
          extra_ev=None):
    ev = {"reason": reason, "anchor": None, "shared_spine": [], "co_query_count": 0,
          "tag_prior": [], "seed": []}
    if extra_ev:
        ev.update(extra_ev)
    members = tuple(sorted(domain_id_members))
    return cluster.DomainProposal(
        domain_id=cluster.domain_id_of(members), parent_id=parent_id, name=name,
        description="d", tag_decision=tag_decision, tag_key=tag_key, tag_value=tag_value,
        evidence=ev, members=members,
    )


def test_curated_tag_absorbs_overlapping_fk_component():
    # A curated (reuse) Domain and an FK-component (create) Domain covering the same
    # business area (curated ⊂ FK) collapse to ONE — the curated wins name+reuse, the
    # FK signal becomes corroborating evidence, and the FK row disappears.
    curated = _prop(
        {"air.maint.work_order", "air.maint.part", "air.maint.aircraft"},
        name="Maintenance and Engineering", tag_decision="reuse",
        tag_key="Maintenance", tag_value="Maintenance", reason="grouped by curated domain tag: Maintenance",
    )
    fk = _prop(
        {"air.maint.work_order", "air.maint.part", "air.maint.aircraft",
         "air.maint.inspection", "air.maint.log"},
        name="Airline Demo Mvm Maintenance", tag_decision="create",
        tag_key="Airline Demo Mvm Maintenance", tag_value="Airline Demo Mvm Maintenance",
    )
    out = cluster.absorb_curated_into_structural([curated, fk])
    tops = [p for p in out if p.parent_id is None]
    assert len(tops) == 1
    merged = tops[0]
    assert merged.tag_decision == "reuse" and merged.name == "Maintenance and Engineering"
    assert merged.tag_key == "Maintenance"
    # The FK component's members are absorbed; the FK reason is corroboration.
    assert "air.maint.inspection" in merged.members and "air.maint.log" in merged.members
    assert merged.evidence["corroborating"]
    # The FK component no longer stands as its own Domain.
    assert not any(p.name == "Airline Demo Mvm Maintenance" for p in out)


def test_low_overlap_curated_and_fk_stay_distinct():
    curated = _prop({"a.x.t1", "a.x.t2"}, name="Alpha", tag_decision="reuse", tag_key="Alpha")
    fk = _prop({"b.y.p", "b.y.q", "b.y.r"}, name="Beta", tag_decision="create", tag_key="Beta")
    out = cluster.absorb_curated_into_structural([curated, fk])
    assert len([p for p in out if p.parent_id is None]) == 2  # no overlap → both kept


def test_absorb_reparents_fk_subdomains():
    curated = _prop({"m.a.t1", "m.a.t2", "m.a.t3"}, name="Maint", tag_decision="reuse", tag_key="Maint")
    fk = _prop({"m.a.t1", "m.a.t2", "m.a.t3", "m.a.t4"}, name="MvmMaint", tag_decision="create", tag_key="MvmMaint")
    sub = _prop({"m.a.t1", "m.a.t2"}, parent_id=fk.domain_id, name="Sub", reason="schema: m.a")
    out = cluster.absorb_curated_into_structural([curated, fk, sub])
    merged = next(p for p in out if p.parent_id is None)
    resub = next(p for p in out if p.parent_id is not None)
    assert resub.parent_id == merged.domain_id  # re-parented onto the merged Domain


def test_two_curated_each_absorb_own_fk_twin_no_cross_reparent():
    # Two independent curated Domains, each with its own FK twin + a sub-domain. A
    # sub-domain must re-parent onto ITS curated's merge, never the other's.
    cA = _prop({"a.x.t1", "a.x.t2", "a.x.t3"}, name="Alpha", tag_decision="reuse", tag_key="Alpha")
    fkA = _prop({"a.x.t1", "a.x.t2", "a.x.t3", "a.x.t4"}, name="AlphaFk", tag_decision="create", tag_key="AlphaFk")
    subA = _prop({"a.x.t1", "a.x.t2"}, parent_id=fkA.domain_id, name="SubA", reason="schema: a.x")
    cB = _prop({"b.y.t1", "b.y.t2", "b.y.t3"}, name="Beta", tag_decision="reuse", tag_key="Beta")
    fkB = _prop({"b.y.t1", "b.y.t2", "b.y.t3", "b.y.t4"}, name="BetaFk", tag_decision="create", tag_key="BetaFk")
    subB = _prop({"b.y.t1", "b.y.t2"}, parent_id=fkB.domain_id, name="SubB", reason="schema: b.y")

    out = cluster.absorb_curated_into_structural([cA, fkA, subA, cB, fkB, subB])
    merged_alpha = next(p for p in out if p.name == "Alpha")
    merged_beta = next(p for p in out if p.name == "Beta")
    resub_a = next(p for p in out if p.name == "SubA")
    resub_b = next(p for p in out if p.name == "SubB")
    assert resub_a.parent_id == merged_alpha.domain_id
    assert resub_b.parent_id == merged_beta.domain_id


def test_dedupe_qualifies_two_same_named_top_level_domains():
    a = _prop({"finance.cost.a", "finance.cost.b", "finance.cost.c"}, name="Cost Attribution")
    b = _prop({"ops.cost.x", "ops.cost.y", "ops.cost.z"}, name="Cost Attribution")
    out = cluster.dedupe_domain_names([a, b])
    names = sorted(p.name for p in out)
    assert len(names) == len(set(names))  # no two share a rendered name
    assert names == ["Cost Attribution (Cost)", "Cost Attribution (Cost)"] or all(
        n.startswith("Cost Attribution (") for n in names
    )
    # domain_id (member fingerprint) is untouched by the label fix.
    assert {p.domain_id for p in out} == {a.domain_id, b.domain_id}


def test_dedupe_collapses_identical_member_sets():
    members = {"c.s.a", "c.s.b"}
    a = _prop(members, name="Dup")
    b = _prop(members, name="Dup")  # identical members → same domain_id
    assert a.domain_id == b.domain_id
    out = cluster.dedupe_domain_names([a, b])
    assert len(out) == 1  # collapsed to one


def test_dedupe_leaves_unique_names_untouched():
    a = _prop({"c.s.a", "c.s.b"}, name="Alpha", tag_key="Alpha")
    b = _prop({"c.t.x", "c.t.y"}, name="Beta", tag_key="Beta")
    out = cluster.dedupe_domain_names([a, b])
    assert sorted(p.name for p in out) == ["Alpha", "Beta"]


def test_every_subdomain_carries_a_boundary_reason():
    # Whatever the boundary source, every sub-domain stamps a plain boundary reason.
    _BOUNDARY = ("sub-tag: ", "mvm_subdomain=", "schema: ", "foreign-key component",
                 "metric view: ", "split by structure")
    props = cluster.cluster(_commercial_graph(), namer=_schema_namer)
    subs = [p for p in props if p.parent_id is not None]
    assert subs
    for s in subs:
        reason = s.evidence.get("reason")
        assert isinstance(reason, str) and reason.startswith(_BOUNDARY)
