"""L2 fused signal-graph scaffold (Phase 2 — structure only).

Builds the nodes + edges of the fused signal graph from the tag graph and
(optionally) lineage adjacency, as a **structural dependency** for the Phase-3
proposal engine. There is deliberately **NO clustering / no Louvain / no community
detection here** (MV-D39 is a dependency, not an algorithm, in Phase 2) — those
arrive in Phase 3 attached to this same builder, so no new builder is needed then.

Pure and offline: no I/O, no graph library. The materializer builds this as a
dependency and does not persist it (Phase 2 writes no proposals).
"""

from __future__ import annotations

from typing import Any


def build_signal_graph(
    graph: dict[str, Any],
    lineage_edges: list[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Return ``{"nodes": [...], "edges": [...]}`` — the fused-graph structure.

    Nodes: one per governed tag (``kind="tag"``) and one per member asset
    (``kind`` = its asset type). Edges: ``tag_assignment`` (tag → asset) and, when
    supplied, ``lineage_adjacency`` (asset → asset). No weights, no clusters.
    """
    nodes: list[dict[str, str]] = []
    edges: list[dict[str, str]] = []
    seen: set[str] = set()

    def add_node(node_id: str, kind: str) -> None:
        if node_id not in seen:
            seen.add(node_id)
            nodes.append({"id": node_id, "kind": kind})

    for t in graph.get("tags", []):
        tag_id = f"tag:{t['tag_key']}"
        add_node(tag_id, "tag")
        for m in t.get("members", []):
            asset_id = f"asset:{m['fqn']}"
            add_node(asset_id, m.get("asset_type", "table"))
            edges.append({"src": tag_id, "dst": asset_id, "kind": "tag_assignment"})

    for a, b in lineage_edges or []:
        src, dst = f"asset:{a}", f"asset:{b}"
        add_node(src, "table")
        add_node(dst, "table")
        edges.append({"src": src, "dst": dst, "kind": "lineage_adjacency"})

    return {"nodes": nodes, "edges": edges}
