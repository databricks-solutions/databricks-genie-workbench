"""L2 fused signal-graph (Phase 3a — completed weighted heterograph).

Phase 2 shipped the scaffold (nodes + tag_assignment/lineage edges). Phase 3a
completes it into the full weighted heterograph the design names: it adds the
``co_query`` and ``agent_scope`` edge kinds, a ``cost`` node attribute, and the
``semantic_sim`` edges that L3 (``er.py``) contributes — every edge carrying its
per-edge ``source`` + ``as_of`` (the Provenanced discipline).

Stage 1 of the curation redesign (MV-D52) fills in the strong-but-unused
structural signals as opt-in edge kinds: ``join_key`` (FK/PK + a shared-join-column
proxy, populating the layer 17d only declared), ``mv_membership`` (a metric view →
its source tables, hub-projected), and ``schema_affinity`` (assets sharing a schema,
hub-projected). All are readable from ``information_schema`` / MV YAML with no new
dependency; each edge stamps a ``source`` so the clusterer can name the reason.

There is still **NO clustering / no Louvain / no community detection** here
(MV-D39 stays a scaffolded dependency; communities are 17e). Pure and offline: no
I/O, no graph library (``igraph`` is 17e). Every extra edge kind is opt-in — a
caller that passes only ``graph`` (+ ``lineage_edges``) gets the exact Phase-2
scaffold back, so existing callers/tests are byte-identical.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_signal_graph(
    graph: dict[str, Any],
    lineage_edges: Iterable[tuple[str, str]] | None = None,
    *,
    co_query_edges: Iterable[tuple] | None = None,
    agent_scopes: dict[str, list[str]] | None = None,
    agent_names: dict[str, str] | None = None,
    dashboard_scopes: dict[str, list[str]] | None = None,
    dashboard_names: dict[str, str] | None = None,
    costs: dict[str, float] | None = None,
    semantic_sim_edges: Iterable[tuple] | None = None,
    join_key_edges: Iterable[tuple] | None = None,
    mv_membership: dict[str, list[str]] | None = None,
    schema_affinity: dict[str, list[str]] | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    """Return ``{"nodes": [...], "edges": [...]}`` — the fused heterograph.

    Nodes: one per governed tag (``kind="tag"``), one per member asset (``kind`` =
    its asset type, with an optional ``cost`` attribute), one per Agent scope
    (``kind="agent"``). Edges carry ``kind`` + ``source`` + ``as_of``:
    ``tag_assignment``, ``lineage_adjacency``, ``co_query`` (asset↔asset), and
    ``agent_scope`` (agent→asset); ``semantic_sim`` edges (from L3) are appended when
    supplied. No weights beyond an optional per-edge ``weight``; no clusters.

    Stage-1 structural signals (MV-D52), all opt-in:
    - ``join_key_edges``: asset↔asset FK/PK + shared-join-column proxy edges, kind
      ``join_key``. Each item is ``(a, b)`` | ``(a, b, weight)`` | ``(a, b, weight,
      source)`` | ``(a, b, weight, source, columns)``; ``source`` defaults to
      ``"foreign_key"`` (use ``"shared_join_column"`` for the lower-weight proxy) so the
      clusterer can name the grouping reason, and the optional ``columns`` names the join
      key(s) so the map's edge detail reads "shares key ``route_id``" (MV-D88).
    - ``mv_membership``: ``{mv_fqn: [source_table_fqn, ...]}``. Emits a metric-view hub
      node (``mv:<fqn>``) → each source asset, kind ``mv_membership``.
    - ``schema_affinity``: ``{schema_key: [asset_fqn, ...]}``. Emits a schema hub node
      (``schema:<key>``) → each asset, kind ``schema_affinity``.
    """
    stamp = as_of or graph.get("as_of") or _now_iso()
    costs = costs or {}
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    by_id: dict[str, dict[str, Any]] = {}
    seen = by_id.keys()

    def add_node(node_id: str, kind: str) -> dict[str, Any]:
        node = by_id.get(node_id)
        if node is None:
            node = {"id": node_id, "kind": kind}
            by_id[node_id] = node
            nodes.append(node)
        return node

    def add_asset(fqn: str, kind: str = "table") -> dict[str, Any]:
        node = add_node(f"asset:{fqn}", kind)
        if fqn in costs and "cost" not in node:
            node["cost"] = float(costs[fqn])
        return node

    def add_edge(
        src: str, dst: str, kind: str, source: str,
        weight: float | None = None, tag_value: str | None = None,
        columns: Iterable[str] | None = None,
    ) -> None:
        edge: dict[str, Any] = {"src": src, "dst": dst, "kind": kind, "source": source, "as_of": stamp}
        if weight is not None:
            edge["weight"] = float(weight)
        # Join column name(s) for a join_key edge (MV-D88): reveal-don't-invent, so an
        # empty/absent list is omitted (the layout's _edge_detail drops ``columns`` then).
        if columns:
            named = [str(c) for c in columns if c is not None and str(c) != ""]
            if named:
                edge["columns"] = named
        # Stage-2 (MV-D54): a value-carrying assignment's ``tag_value`` rides the
        # edge ADDITIVELY — present only when the member carries one, so a value-free
        # assignment edge stays byte-identical to the Phase-2 scaffold shape.
        if tag_value is not None and str(tag_value) != "":
            edge["tag_value"] = str(tag_value)
        edges.append(edge)

    # Tag → asset assignment edges (Phase-2 scaffold). A value-carrying member's
    # ``tag_value`` (Stage 2) threads onto the edge so the clusterer can split a
    # Domain into sub-domains by distinct value; a value-free member is unchanged.
    for t in graph.get("tags", []):
        tag_id = f"tag:{t['tag_key']}"
        add_node(tag_id, "tag")
        for m in t.get("members", []):
            add_asset(m["fqn"], m.get("asset_type", "table"))
            add_edge(tag_id, f"asset:{m['fqn']}", "tag_assignment", "tag_assignment",
                     tag_value=m.get("tag_value"))

    # Lineage adjacency (asset → asset).
    for pair in lineage_edges or []:
        a, b = pair[0], pair[1]
        add_asset(a)
        add_asset(b)
        add_edge(f"asset:{a}", f"asset:{b}", "lineage_adjacency", "lineage")

    # Co-query co-occurrence (asset ↔ asset), from query.history. Accepts
    # ``(a, b)`` or ``(a, b, weight)``.
    for pair in co_query_edges or []:
        a, b = pair[0], pair[1]
        weight = pair[2] if len(pair) > 2 else None
        add_asset(a)
        add_asset(b)
        add_edge(f"asset:{a}", f"asset:{b}", "co_query", "query_history", weight)

    # Agent scope (agent → asset the Agent reads). A space with an EMPTY scope still
    # gets its ``agent:<id>`` node (no edges) so a tagged-but-scopeless Agent still lands
    # on the map (Stage 2, MV-D91). ``agent_names`` (optional) threads the space display
    # name onto the node ADDITIVELY — present only when supplied, so a value-free call is
    # byte-identical to the pre-Stage-2 scaffold.
    names = agent_names or {}
    for agent_id, fqns in (agent_scopes or {}).items():
        agent_node = f"agent:{agent_id}"
        node = add_node(agent_node, "agent")
        label = names.get(agent_id)
        if label and "label" not in node:
            node["label"] = str(label)
        for fqn in fqns:
            add_asset(fqn)
            add_edge(agent_node, f"asset:{fqn}", "agent_scope", "agent_scope")

    # Dashboard scope (dashboard → asset the dashboard's datasets read). Mirrors the
    # agent overlay exactly (Stage 3, MV-D92): a dashboard with an EMPTY scope still gets
    # its ``dashboard:<id>`` node (no edges) so a tagged-but-scopeless dashboard still
    # lands on the map. ``dashboard_names`` (optional) threads the display name onto the
    # node ADDITIVELY. UNSET ⇒ byte-identical graph for every pre-Stage-3 caller (MV-D43).
    dnames = dashboard_names or {}
    for dashboard_id, fqns in (dashboard_scopes or {}).items():
        dashboard_node = f"dashboard:{dashboard_id}"
        node = add_node(dashboard_node, "dashboard")
        label = dnames.get(dashboard_id)
        if label and "label" not in node:
            node["label"] = str(label)
        for fqn in fqns:
            add_asset(fqn)
            add_edge(dashboard_node, f"asset:{fqn}", "dashboard_scope", "dashboard_scope")

    # Semantic-similarity edges contributed by L3 (``er.py``). Accepts ``(a, b)``
    # or ``(a, b, score)``; nodes are assumed already present but added defensively.
    for pair in semantic_sim_edges or []:
        a, b = pair[0], pair[1]
        weight = pair[2] if len(pair) > 2 else None
        if a not in seen:
            add_node(a, "node")
        if b not in seen:
            add_node(b, "node")
        add_edge(a, b, "semantic_sim", "embedding", weight)

    # Join-key structural edges (Stage 1, MV-D52): FK/PK relationships +
    # shared-join-column proxies, asset↔asset. Accepts ``(a, b)`` | ``(a, b, weight)``
    # | ``(a, b, weight, source)`` | ``(a, b, weight, source, columns)``; ``source`` names
    # FK vs proxy for the grouping reason, ``columns`` names the join key(s) (MV-D88).
    for item in join_key_edges or []:
        a, b = item[0], item[1]
        weight = item[2] if len(item) > 2 else None
        source = item[3] if len(item) > 3 else "foreign_key"
        columns = item[4] if len(item) > 4 else None
        add_asset(a)
        add_asset(b)
        add_edge(f"asset:{a}", f"asset:{b}", "join_key", source, weight, columns=columns)

    # Metric-view membership (Stage 1, MV-D52): an MV → its source tables, projected
    # from a metric-view hub node so the clusterer can group an MV's sources.
    for mv_fqn, sources in (mv_membership or {}).items():
        mv_node = f"mv:{mv_fqn}"
        add_node(mv_node, "metric_view")
        for src in sources:
            add_asset(src)
            add_edge(mv_node, f"asset:{src}", "mv_membership", "metric_view")

    # Schema affinity (Stage 1, MV-D52): assets sharing a schema (esp. named business
    # areas), projected from a schema hub node.
    for schema_key, fqns in (schema_affinity or {}).items():
        schema_node = f"schema:{schema_key}"
        add_node(schema_node, "schema")
        for fqn in fqns:
            add_asset(fqn)
            add_edge(schema_node, f"asset:{fqn}", "schema_affinity", "information_schema")

    return {"nodes": nodes, "edges": edges}


# ── Lineage centrality (L6 rank input, precomputed here — MV-D35) ───────────


def lineage_centrality(signal_graph: dict[str, Any]) -> dict[str, float]:
    """Per-asset lineage centrality on the fused subgraph, normalized to [0, 1] — the
    ``lineage-centrality`` factor the L6 ranker (``rank.py``) reads (the load-bearing
    spine everything joins to outranks a leaf).

    **Degree centrality** over the structural asset↔asset edge kinds
    (``lineage_adjacency`` + ``co_query``) — pure and ``igraph``-free (betweenness is
    ``cluster.py``'s lazy-``igraph`` anchor pick; degree is the cheap, deterministic
    ranking proxy computed here so the score stays offline). Keyed by bare asset FQN
    (the ``asset:`` prefix stripped) to match how ``rank`` addresses assets. Normalized
    by the max degree so the busiest spine asset is 1.0; an empty/edgeless graph yields
    ``{}`` (the factor is then simply absent, lowering coverage — never a false 0)."""
    degree: dict[str, int] = {}
    for e in signal_graph.get("edges", []):
        if e.get("kind") not in ("lineage_adjacency", "co_query"):
            continue
        for node_id in (e.get("src"), e.get("dst")):
            if isinstance(node_id, str) and node_id.startswith("asset:"):
                fqn = node_id.split(":", 1)[1]
                degree[fqn] = degree.get(fqn, 0) + 1
    if not degree:
        return {}
    peak = max(degree.values())
    if peak <= 0:
        return {}
    return {fqn: round(count / peak, 6) for fqn, count in degree.items()}


# ── PageRank centrality (Stage 3 §5.1, MV-D96) ──────────────────────────────

# Structural + hub edge kinds authority propagates over. The two asset↔asset kinds
# (``lineage_adjacency`` + ``co_query``) are exactly what ``lineage_centrality``'s degree
# proxy counts; the four extra kinds (``join_key`` FK spines, and the ``mv_membership`` /
# ``agent_scope`` / ``dashboard_scope`` hubs) let authority flow from heavily-connected
# MVs / Agents / dashboards INTO the tables they touch — connectivity a bare degree count
# over the two asset↔asset kinds cannot see.
_PAGERANK_EDGE_KINDS: frozenset[str] = frozenset(
    {"lineage_adjacency", "co_query", "join_key", "mv_membership", "agent_scope", "dashboard_scope"}
)
_PAGERANK_DAMPING = 0.85


def pagerank_centrality(signal_graph: dict[str, Any]) -> dict[str, float]:
    """Per-asset PageRank centrality on the fused subgraph, normalized to [0, 1] — a
    richer drop-in for ``lineage_centrality`` as the ``centrality`` factor the L6 ranker
    (``rank.py``) reads. Authority flows over the structural + hub edge kinds
    (``_PAGERANK_EDGE_KINDS``), so a table many dashboards / Agents / MVs and FK spines
    point at outranks a leaf — connectivity the degree proxy over the two asset↔asset
    kinds alone would miss. Same contract as ``lineage_centrality``: keyed by bare asset
    FQN (the ``asset:`` prefix stripped), values in [0, 1] with the busiest asset at 1.0.

    ``igraph`` is lazy-imported INSIDE the function (already a lazy dependency for
    ``cluster.py``). If ``igraph`` is unavailable OR the graph is edgeless over the
    propagation kinds, DEGRADE to ``lineage_centrality`` (MV-D43/D45) — never raise, never
    a false 0. Deterministic: vertices + edges are sorted before building, and igraph's
    PRPACK solver is a direct (non-iterative) solve, so two runs are identical."""
    # Collect the directed edges authority flows along (hub → asset, spine → table).
    edge_pairs: list[tuple[str, str]] = []
    for e in signal_graph.get("edges", []):
        if e.get("kind") not in _PAGERANK_EDGE_KINDS:
            continue
        src, dst = e.get("src"), e.get("dst")
        if isinstance(src, str) and isinstance(dst, str) and src != dst:
            edge_pairs.append((src, dst))
    if not edge_pairs:
        # Edgeless over the propagation kinds — nothing for PageRank to flow through;
        # fall back to the degree proxy (itself ``{}`` for a truly edgeless graph).
        return lineage_centrality(signal_graph)

    try:
        import igraph as ig  # lazy — keeps the module importable without the graph lib
    except Exception:
        return lineage_centrality(signal_graph)

    # Deterministic vertex + edge order (identical output across runs — MV-D82).
    vertices = sorted({v for pair in edge_pairs for v in pair})
    idx = {v: i for i, v in enumerate(vertices)}
    e_idx = sorted((idx[a], idx[b]) for a, b in edge_pairs)

    g = ig.Graph(n=len(vertices), directed=True)
    g.add_edges(e_idx)
    scores = g.pagerank(directed=True, damping=_PAGERANK_DAMPING)

    # Keep the asset vertices only, key by bare FQN, normalize by the max asset score.
    asset_scores = {
        v.split(":", 1)[1]: scores[idx[v]] for v in vertices if v.startswith("asset:")
    }
    peak = max(asset_scores.values(), default=0.0)
    if peak <= 0:
        return lineage_centrality(signal_graph)
    return {fqn: round(s / peak, 6) for fqn, s in asset_scores.items()}


# ── Certified-seeded home assignment (Stage 3 §5.2, MV-D96) ─────────────────


def certified_home(
    signal_graph: dict[str, Any],
    seeds_by_domain: Mapping[str, Sequence[str]],
) -> dict[str, str]:
    """Assign each asset to the domain whose CERTIFIED anchors it flows closest to —
    personalized PageRank seeded on each domain's certified assets (§5.2, MV-D96). The
    legitimacy gate (``rank.py``) reads the result to steer a below-bar fragment toward
    the domain its members actually belong to (by trusted-anchor gravity), a sharper
    "add to existing domain" hint than the bare shared-schema guess.

    ``seeds_by_domain`` maps a ``domain_id`` → the CERTIFIED asset FQNs anchoring it.
    Builds the SAME directed fused graph as :func:`pagerank_centrality` (the six
    ``_PAGERANK_EDGE_KINDS``). For each domain with ≥1 seed present in the graph, runs
    ``personalized_pagerank(reset_vertices=<that domain's seed vertices>, damping=0.85)``;
    every asset vertex is then assigned to the domain whose seeded run gives it the
    highest mass. Returns ``{bare_fqn: domain_id}`` (the ``asset:`` prefix stripped, to
    match how ``rank`` addresses assets).

    Deterministic (MV-D82): domains, seeds, and vertices are all sorted, and a vertex
    ties to the lexicographically-smallest ``domain_id`` (domains are visited in sorted
    order and a home is replaced only on strictly-greater mass, so the first — smallest —
    domain to reach a given peak keeps it). ``igraph`` is lazy-imported INSIDE; if it is
    unavailable, the graph is edgeless over the propagation kinds, or no seed lands on a
    vertex, return ``{}`` (MV-D43/D45) — never raise. An empty result leaves the gate on
    its schema-hint fallback, byte-identical to the pre-Stage-3.2 path."""
    if not seeds_by_domain:
        return {}
    # The same directed edges authority flows along as pagerank_centrality.
    edge_pairs: list[tuple[str, str]] = []
    for e in signal_graph.get("edges", []):
        if e.get("kind") not in _PAGERANK_EDGE_KINDS:
            continue
        src, dst = e.get("src"), e.get("dst")
        if isinstance(src, str) and isinstance(dst, str) and src != dst:
            edge_pairs.append((src, dst))
    if not edge_pairs:
        return {}

    try:
        import igraph as ig  # lazy — keeps the module importable without the graph lib
    except Exception:
        return {}

    # Deterministic vertex + edge order (identical output across runs — MV-D82).
    vertices = sorted({v for pair in edge_pairs for v in pair})
    idx = {v: i for i, v in enumerate(vertices)}
    e_idx = sorted((idx[a], idx[b]) for a, b in edge_pairs)

    g = ig.Graph(n=len(vertices), directed=True)
    g.add_edges(e_idx)

    # Per-vertex winner: the domain whose seeded PPR run gives it the most mass. Visiting
    # domains in sorted order + replacing only on STRICTLY-greater mass makes ties resolve
    # to the lexicographically-smallest domain_id.
    best_domain: dict[int, str] = {}
    best_mass: dict[int, float] = {}
    for did in sorted(seeds_by_domain):
        seed_vs = sorted({idx[f"asset:{s}"] for s in seeds_by_domain[did] if f"asset:{s}" in idx})
        if not seed_vs:
            continue
        scores = g.personalized_pagerank(reset_vertices=seed_vs, damping=_PAGERANK_DAMPING, directed=True)
        for vi, mass in enumerate(scores):
            if vi not in best_mass or mass > best_mass[vi]:
                best_mass[vi] = mass
                best_domain[vi] = did

    if not best_domain:
        return {}
    return {
        v.split(":", 1)[1]: best_domain[idx[v]]
        for v in vertices
        if v.startswith("asset:") and idx[v] in best_domain
    }


# ── Related-asset traversal (Stage 4.1j §Related assets, MV-D102) ────────────

# The edge kinds a Page's Related section is drawn from — the RELATEDNESS edges (the
# governance ``tag_assignment`` / ``schema_affinity`` hub kinds are excluded: a shared tag
# or schema is a grouping signal, not a per-asset "related" relationship). Each carries a
# ``KIND_PRIOR`` weight so a hard structural tie (an FK join key) outranks a fuzzy one (an
# embedding similarity) when the endpoints' centralities are otherwise comparable.
_RELATED_KINDS: frozenset[str] = frozenset(
    {"join_key", "lineage_adjacency", "co_query", "mv_membership",
     "semantic_sim", "dashboard_scope", "agent_scope"}
)
_KIND_PRIOR: dict[str, float] = {
    "join_key": 1.0,          # FK / proven shared join column — the strongest structural tie
    "mv_membership": 0.9,     # feeds / is fed by the same governed metric view
    "lineage_adjacency": 0.8, # a proven upstream/downstream lineage neighbor
    "co_query": 0.7,          # frequently queried together (query.history co-occurrence)
    "dashboard_scope": 0.6,   # read by the same governed dashboard
    "agent_scope": 0.5,       # scoped by the same serving Genie Agent
    "semantic_sim": 0.4,      # embedding similarity — the softest signal
}
# A centrality FLOOR so a neighbor missing from the (asset-only) centrality map — a hub
# node (mv / dashboard / agent), or any asset on an edgeless-for-PageRank graph — still
# scores by weight × prior rather than collapsing to 0. Small enough that a high-centrality
# spine asset still outranks a leaf, large enough that hubs stay eligible for the top slots.
_CENTRALITY_FLOOR = 0.1
# Mirrors ``pages._RELATED_WHY`` (kept inline to preserve the graph→pages layering — this
# low-level module never imports the page miner). The agent's "why" is reconciled back to
# the page constant in ``pages._asset_why`` (setdefault), so a drift here is harmless.
_RELATED_KIND_WHY: dict[str, str] = {
    "lineage_adjacency": "Connected in table lineage",
    "co_query": "Frequently queried together",
    "mv_membership": "Measure on the same metric view",
    "semantic_sim": "Semantically similar",
    "dashboard_scope": "Used by a governed dashboard",
    "agent_scope": "Serving Genie Agent that answers questions about this concept.",
}


def _bare(node_id: str) -> str:
    """The bare identifier of a graph node id (the ``asset:`` / ``mv:`` / ``agent:`` /
    ``dashboard:`` prefix stripped). A prefixless id is returned unchanged."""
    return node_id.split(":", 1)[1] if ":" in node_id else node_id


def _join_key_why(columns: Any) -> str:
    """"Shares join key ``<col>``" naming the FK column(s) (MV-D88); a column-less join
    edge (the shared-join-column proxy may omit them) degrades to a generic phrase."""
    named = [str(c) for c in (columns or []) if c is not None and str(c) != ""]
    if not named:
        return "Shares a join key"
    return "Shares join key " + ", ".join(f"`{c}`" for c in named)


def related_assets(
    signal_graph: dict[str, Any],
    anchor_fqns: Iterable[str],
    centrality: Mapping[str, float] | None = None,
    *,
    max_out: int = 6,
    exclude: frozenset[str] | set[str] = frozenset(),
) -> list[dict[str, Any]]:
    """Deterministic graph-derived Related assets for a Page (Stage 4.1j, MV-D102).

    From each ``asset:<anchor>`` node, take the 1-hop neighbors over the relatedness edge
    kinds (:data:`_RELATED_KINDS`) — the OTHER endpoint of every such edge, in either
    direction. Each neighbor is scored::

        score = (edge weight or 1.0) × _KIND_PRIOR[kind] × (centrality["asset:"+t] + floor)

    where ``t`` is the neighbor's bare id and ``centrality`` is the per-asset PageRank map
    (:func:`pagerank_centrality`, keyed by bare FQN). A neighbor absent from ``centrality``
    — a hub node (mv / dashboard / agent) or an asset on an edgeless-for-PageRank graph —
    still scores via ``_CENTRALITY_FLOOR`` rather than collapsing to 0. A neighbor reached
    over several edges/kinds is DEDUPED keep-max (the highest-scoring edge wins, ties broken
    deterministically by the sorted edge walk). Anchors, the ``exclude`` set, and self-loops
    are dropped, so a Page never lists its own Sources.

    Returns ``[{"fqn", "kind", "source", "columns", "why", "score"}]`` sorted by ``score``
    desc then ``fqn`` asc, capped at ``max_out``. ``columns`` is present only for a
    ``join_key`` neighbor that named its FK column(s) (MV-D88). Only REAL graph nodes are
    ever returned — no FQN is invented. Pure + offline + ``igraph``-free; an edgeless graph
    (or an anchor set with no relatedness edges) yields ``[]``."""
    cent = centrality or {}
    anchors = {str(a) for a in anchor_fqns}
    anchor_nodes = {f"asset:{a}" for a in anchors}
    drop = anchors | {str(x) for x in exclude}

    # keep-max per neighbor fqn; ``_walk`` is deterministic (edges are visited in list
    # order and a neighbor is replaced only on a STRICTLY-greater score).
    best: dict[str, dict[str, Any]] = {}
    for e in signal_graph.get("edges", []):
        kind = e.get("kind")
        if kind not in _RELATED_KINDS:
            continue
        src, dst = e.get("src"), e.get("dst")
        if not (isinstance(src, str) and isinstance(dst, str)):
            continue
        # The edge must touch exactly one anchor asset node; the OTHER endpoint is the
        # neighbor. (An edge between two anchors yields a neighbor that is itself an anchor
        # and is dropped below; an edge touching no anchor is irrelevant.)
        if src in anchor_nodes:
            neighbor = dst
        elif dst in anchor_nodes:
            neighbor = src
        else:
            continue
        t = _bare(neighbor)
        if not t or t in drop or neighbor in anchor_nodes:
            continue
        weight = e.get("weight")
        w = float(weight) if isinstance(weight, (int, float)) else 1.0
        prior = _KIND_PRIOR.get(kind, 0.5)
        score = round(w * prior * (float(cent.get(f"asset:{t}", 0.0)) + _CENTRALITY_FLOOR), 6)
        columns = e.get("columns") if kind == "join_key" else None
        why = _join_key_why(columns) if kind == "join_key" else _RELATED_KIND_WHY.get(kind, "Related asset")
        entry = {
            "fqn": t, "kind": kind, "source": str(e.get("source") or kind),
            "columns": [str(c) for c in columns] if columns else [], "why": why, "score": score,
        }
        prev = best.get(t)
        if prev is None or score > prev["score"]:
            best[t] = entry

    ranked = sorted(best.values(), key=lambda r: (-r["score"], r["fqn"]))
    return ranked[: max(0, int(max_out))]
