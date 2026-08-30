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
