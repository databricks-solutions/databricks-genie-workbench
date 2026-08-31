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
from typing import Any, Iterable


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_signal_graph(
    graph: dict[str, Any],
    lineage_edges: Iterable[tuple[str, str]] | None = None,
    *,
    co_query_edges: Iterable[tuple] | None = None,
    agent_scopes: dict[str, list[str]] | None = None,
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
      source)``; ``source`` defaults to ``"foreign_key"`` (use ``"shared_join_column"``
      for the lower-weight proxy) so the clusterer can name the grouping reason.
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

    def add_edge(src: str, dst: str, kind: str, source: str, weight: float | None = None) -> None:
        edge: dict[str, Any] = {"src": src, "dst": dst, "kind": kind, "source": source, "as_of": stamp}
        if weight is not None:
            edge["weight"] = float(weight)
        edges.append(edge)

    # Tag → asset assignment edges (Phase-2 scaffold).
    for t in graph.get("tags", []):
        tag_id = f"tag:{t['tag_key']}"
        add_node(tag_id, "tag")
        for m in t.get("members", []):
            add_asset(m["fqn"], m.get("asset_type", "table"))
            add_edge(tag_id, f"asset:{m['fqn']}", "tag_assignment", "tag_assignment")

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

    # Agent scope (agent → asset the Agent reads).
    for agent_id, fqns in (agent_scopes or {}).items():
        agent_node = f"agent:{agent_id}"
        add_node(agent_node, "agent")
        for fqn in fqns:
            add_asset(fqn)
            add_edge(agent_node, f"asset:{fqn}", "agent_scope", "agent_scope")

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
    # | ``(a, b, weight, source)``; ``source`` names FK vs proxy for the grouping reason.
    for item in join_key_edges or []:
        a, b = item[0], item[1]
        weight = item[2] if len(item) > 2 else None
        source = item[3] if len(item) > 3 else "foreign_key"
        add_asset(a)
        add_asset(b)
        add_edge(f"asset:{a}", f"asset:{b}", "join_key", source, weight)

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
