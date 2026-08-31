"""L2 fused signal-graph (Phase 3a — completed weighted heterograph).

Phase 2 shipped the scaffold (nodes + tag_assignment/lineage edges). Phase 3a
completes it into the full weighted heterograph the design names: it adds the
``co_query`` and ``agent_scope`` edge kinds, a ``cost`` node attribute, and the
``semantic_sim`` edges that L3 (``er.py``) contributes — every edge carrying its
per-edge ``source`` + ``as_of`` (the Provenanced discipline).

There is still **NO clustering / no Louvain / no community detection** here
(MV-D39 stays a scaffolded dependency; communities are 17e). Pure and offline: no
I/O, no graph library (``igraph`` is 17e). The extra edge kinds are opt-in — a
caller that passes only ``graph`` (+ ``lineage_edges``) gets the exact Phase-2
scaffold back, so existing callers/tests are unchanged.
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
    as_of: str | None = None,
) -> dict[str, Any]:
    """Return ``{"nodes": [...], "edges": [...]}`` — the fused heterograph.

    Nodes: one per governed tag (``kind="tag"``), one per member asset (``kind`` =
    its asset type, with an optional ``cost`` attribute), one per Agent scope
    (``kind="agent"``). Edges carry ``kind`` + ``source`` + ``as_of``:
    ``tag_assignment``, ``lineage_adjacency``, ``co_query`` (asset↔asset), and
    ``agent_scope`` (agent→asset); ``semantic_sim`` edges (from L3) are appended when
    supplied. No weights beyond an optional per-edge ``weight``; no clusters.
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
