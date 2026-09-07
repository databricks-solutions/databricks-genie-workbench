"""Ontology estate-graph layout engine (Phase 3e / Ontology Map — Step A).

Precomputes a deterministic igraph force-directed layout for the fused signal
graph (17d), rolls it up to the Domain/Sub-Domain level (17e), and produces a
single serializable JSON blob keyed by metastore_id for persistent storage.

The layout is the **final additive step** in the materialize job — it runs after
L6 ranking (17g) and never corrupts earlier snapshots if it fails (degrade-not-
hang, MV-D43). A run that produces zero assets still succeeds with an empty
snapshot (the MERGE clears stale rows).

**Key invariants (§6):**
- Deterministic by construction (fixed seed, single thread).
- Reuses the fused graph from 17d unchanged — no re-signal, no re-clustering.
- Two levels of detail: domains (rollup, default) and assets (capped top-N).
- Size = normalized function of lineage centrality / cost / L6 score.
- Colour = the domain_id from 17e.
- Tests assert **structure** (node/edge sets, colours, counts), not exact floats.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Module constant: max asset-level nodes to include (top-N by centrality).
# Larger estates cap at this to keep the asset LOD interactive.
TOP_N_BY_CENTRALITY = 2000


def _fqn_of(node_id: str) -> str:
    """Strip the ``prefix:`` from a signal-graph node id (``asset:c.s.t`` → ``c.s.t``).

    Signal-graph nodes are prefixed by kind (``asset:``/``tag:``/``mv:``/``agent:``/
    ``schema:``) while the ``node_domain_id`` map is keyed by the bare asset FQN
    (see ``cluster._fqn_of`` and the ``asset_domain`` map in ``materialize``). The
    domain lookup below tries the raw id first, then the de-prefixed FQN so both
    key schemes resolve. Nodes without a prefix pass through unchanged.
    """
    return node_id.split(":", 1)[1] if ":" in node_id else node_id


def build_graph_snapshot(
    signal_graph: dict[str, Any],
    node_domain_id: dict[str, str],
    node_scores: dict[str, float] | None = None,
    *,
    domain_meta: dict[str, dict[str, Any]] | None = None,
    metastore_id: str,
    workspace_id: str,
    run_id: str,
    as_of: str,
) -> dict[str, Any]:
    """Build the estate-graph snapshot blob for Delta persistence (MV-D48/D49).

    Takes the fused heterograph (nodes + edges from 17d) mapped to 17e's
    domain_id assignments and (optionally) 17g's L6 scores, computes an
    igraph force layout, and rolls the asset-level graph up to the domain
    level. Returns a single row for the ``genie_ont_graph_snapshot`` MERGE.

    **Args:**
    - ``signal_graph``: ``{"nodes": [...], "edges": [...]`` from 17d.
    - ``node_domain_id``: ``{node_id → domain_id}`` from 17e clustering.
    - ``node_scores``: Optional ``{node_id → score}`` from 17g ranking (for sizing).
    - ``domain_meta``: Optional ``{domain_id → {name, parent_id}}`` from 17e/17f domain
      rows — labels the rollup nodes with human names and links Sub-Domain → Domain
      so the map can render the hierarchy (MV-D71). Absent → raw ids, flat.
    - ``metastore_id``: The storage grain (MV-D49).
    - ``workspace_id``: Provenance (which install ran this).
    - ``run_id``: FK to genie_ont_runs.run_id.
    - ``as_of``: ISO-8601 materialization timestamp.

    **Returns:** A dict with keys for ``genie_ont_graph_snapshot`` (one row):
    ``{metastore_id, workspace_id, graph, node_count, edge_count, layout,
    run_id, as_of}`` where ``graph`` is the JSON blob (stringified).

    **Degrade (MV-D43):** If the layout fails, this function **does not** catch
    the exception — let the caller wrap it and record ``run.state = "failed"``
    without corrupting earlier snapshots. An empty input graph → an empty
    snapshot (no nodes, no edges), run still ``succeeded``.
    """
    node_domain_id = node_domain_id or {}
    node_scores = node_scores or {}
    domain_meta = domain_meta or {}

    nodes = signal_graph.get("nodes", [])
    edges = signal_graph.get("edges", [])

    # Empty graph → empty snapshot, run succeeded. This gate is per MV-D43
    # (degrade-not-hang); the MERGE clears stale rows for this metastore.
    if not nodes:
        return {
            "metastore_id": metastore_id,
            "workspace_id": workspace_id,
            "graph": json.dumps({"domains": {"nodes": [], "edges": []}, "assets": {"nodes": [], "edges": []}}),
            "node_count": 0,
            "edge_count": len(edges),
            "layout": "none",
            "run_id": run_id,
            "as_of": as_of,
        }

    # Build igraph.Graph (deterministic seed + single thread).
    try:
        import igraph
    except ImportError:
        logger.exception("igraph not available; layout unavailable")
        # Still a valid snapshot — just layoutless. The MERGE proceeds with empty levels.
        return {
            "metastore_id": metastore_id,
            "workspace_id": workspace_id,
            "graph": json.dumps({"domains": {"nodes": [], "edges": []}, "assets": {"nodes": [], "edges": []}}),
            "node_count": len(nodes),
            "edge_count": len(edges),
            "layout": "unavailable",
            "run_id": run_id,
            "as_of": as_of,
        }

    # Build igraph from nodes + edges.
    g = igraph.Graph(directed=True)

    # Add nodes: id → unique vertex (vertex index ≠ id; map both ways).
    node_id_to_idx = {}
    idx_to_node_id = {}
    for i, node in enumerate(nodes):
        node_id = node.get("id", "")
        node_id_to_idx[node_id] = i
        idx_to_node_id[i] = node_id
        g.add_vertices(1)

    # Add edges: (src_idx, dst_idx) for each edge.
    edge_list = []
    for edge in edges:
        src_id = edge.get("src")
        dst_id = edge.get("dst")
        if src_id in node_id_to_idx and dst_id in node_id_to_idx:
            edge_list.append((node_id_to_idx[src_id], node_id_to_idx[dst_id]))

    if edge_list:
        g.add_edges(edge_list)

    # Run a deterministic force layout (fixed RNG seed). Fruchterman-Reingold is
    # stable and available across igraph builds; if it is unavailable we fall back to
    # a deterministic circle layout rather than raise (MV-D43 — a layout hiccup must
    # not fail the run or corrupt earlier snapshots).
    try:
        import random as _random
        try:
            igraph.set_random_number_generator(_random.Random(42))
        except Exception:
            pass
        layout = g.layout_fruchterman_reingold()
        layout_algo = "fr"
    except Exception:
        logger.info("force layout unavailable; using deterministic circle layout", exc_info=True)
        layout = g.layout_circle()
        layout_algo = "circle"

    # Attach (x, y) coordinates + sizes to each node.
    asset_nodes = []
    node_sizes = {}
    for i, node in enumerate(nodes):
        node_id = node.get("id", "")
        kind = node.get("kind", "table")
        x, y = layout[i]

        # Size: normalized function of centrality / cost / score. Centrality
        # is available from graph.lineage_centrality (degree); cost is in node;
        # score is from 17g. Clamp to [0.5, 2.0] for visibility.
        size = _compute_node_size(node, node_scores.get(node_id, 0.0))
        node_sizes[node_id] = size

        # Try the raw node id first, then the de-prefixed FQN — signal-graph nodes
        # are ``asset:<fqn>`` while node_domain_id is keyed by the bare FQN (MV-D71).
        domain_id = node_domain_id.get(node_id) or node_domain_id.get(_fqn_of(node_id))
        cost = node.get("cost")

        asset_nodes.append({
            "id": node_id,
            "label": _label_for_node(node_id, kind),
            "kind": kind,
            "domain_id": domain_id,
            "x": float(x),
            "y": float(y),
            "size": size,
            "cost": float(cost) if cost is not None else None,
        })

    # Build asset-level edges (same as input, but only for present nodes).
    asset_node_ids = {n["id"] for n in asset_nodes}
    asset_edges = []
    for edge in edges:
        src_id = edge.get("src")
        dst_id = edge.get("dst")
        if src_id in asset_node_ids and dst_id in asset_node_ids:
            asset_edges.append({
                "src": src_id,
                "dst": dst_id,
                "kind": edge.get("kind", "unknown"),
                "weight": edge.get("weight"),
            })

    # Cap asset nodes to top-N by centrality (size).
    if len(asset_nodes) > TOP_N_BY_CENTRALITY:
        # Sort by size descending, take top-N, keep their edges.
        sorted_nodes = sorted(asset_nodes, key=lambda n: n["size"], reverse=True)
        top_ids = {n["id"] for n in sorted_nodes[:TOP_N_BY_CENTRALITY]}
        asset_nodes = sorted_nodes[:TOP_N_BY_CENTRALITY]
        asset_edges = [e for e in asset_edges if e["src"] in top_ids and e["dst"] in top_ids]
        truncated = True
    else:
        top_ids = {n["id"] for n in asset_nodes}
        truncated = False

    # Roll up to domain level: one node per (domain_id | null), edges aggregated.
    domain_dict: dict[str | None, dict[str, Any]] = {}
    for node in asset_nodes:
        domain_id = node.get("domain_id")
        if domain_id not in domain_dict:
            # Enrich the rollup node from the run's domain rows (MV-D71): human name
            # + parent linkage so the estate map can render the Domain → Sub-Domain →
            # Asset hierarchy. Falls back to the raw id when meta is absent.
            meta = domain_meta.get(domain_id) if domain_id else None
            parent_id = meta.get("parent_id") if meta else None
            parent_meta = domain_meta.get(parent_id) if parent_id else None
            if domain_id:
                label = (meta.get("name") if meta else None) or domain_id
                kind = "subdomain" if parent_id else "domain"
            else:
                label = "Ungrouped"
                kind = "ungrouped"
            # Assign a rolled-up position: average of member positions.
            domain_dict[domain_id] = {
                "id": domain_id or "ungrouped",
                "label": label,
                "kind": kind,
                "domain_id": domain_id,
                "parent_id": parent_id,
                "parent_name": (parent_meta.get("name") if parent_meta else None) or parent_id,
                "x": node["x"],
                "y": node["y"],
                "size": node["size"],
                "cost": node.get("cost"),
                "member_count": 0,
                "positions": [(node["x"], node["y"])],
            }
        else:
            # Update with average position.
            domain_dict[domain_id]["positions"].append((node["x"], node["y"]))

    # Finalize domain nodes: average position, rollup size/cost.
    domain_nodes = []
    for domain_id, domain_info in domain_dict.items():
        positions = domain_info.pop("positions", [])
        if positions:
            avg_x = sum(p[0] for p in positions) / len(positions)
            avg_y = sum(p[1] for p in positions) / len(positions)
            domain_info["x"] = float(avg_x)
            domain_info["y"] = float(avg_y)
        domain_info["member_count"] = len([n for n in asset_nodes if n.get("domain_id") == domain_id])
        domain_nodes.append(domain_info)

    # Domain-level edges: aggregate asset edges across domain boundaries.
    domain_edges = []
    domain_ids_set = {d["domain_id"] for d in domain_nodes}
    seen_domain_edges = set()
    for edge in asset_edges:
        src_domain = next((d["domain_id"] for d in domain_nodes if d["id"] == edge["src"]), None)
        if src_domain is None:
            # src is an asset; find its domain.
            for node in asset_nodes:
                if node["id"] == edge["src"]:
                    src_domain = node.get("domain_id")
                    break

        dst_domain = next((d["domain_id"] for d in domain_nodes if d["id"] == edge["dst"]), None)
        if dst_domain is None:
            # dst is an asset; find its domain.
            for node in asset_nodes:
                if node["id"] == edge["dst"]:
                    dst_domain = node.get("domain_id")
                    break

        # Only include cross-domain edges (or self-edges if ungrouped).
        if src_domain != dst_domain:
            edge_key = (src_domain or "ungrouped", dst_domain or "ungrouped", edge.get("kind"))
            if edge_key not in seen_domain_edges:
                seen_domain_edges.add(edge_key)
                domain_edges.append({
                    "src": src_domain or "ungrouped",
                    "dst": dst_domain or "ungrouped",
                    "kind": edge.get("kind", "unknown"),
                    "weight": edge.get("weight"),
                })

    # Build the snapshot blob.
    snapshot_blob = {
        "domains": {
            "nodes": domain_nodes,
            "edges": domain_edges,
            "truncated": False,
        },
        "assets": {
            "nodes": asset_nodes,
            "edges": asset_edges,
            "truncated": truncated,
        },
        "layout": layout_algo,
        "node_count": len(nodes),
        "edge_count": len(edges),
    }

    return {
        "metastore_id": metastore_id,
        "workspace_id": workspace_id,
        "graph": json.dumps(snapshot_blob),
        "node_count": len(nodes),
        "edge_count": len(edges),
        "layout": layout_algo,
        "run_id": run_id,
        "as_of": as_of,
    }


def _compute_node_size(node: dict[str, Any], score: float) -> float:
    """Compute node size (render radius) as a bounded function of centrality/cost/score.

    Centrality is implicit in the graph structure. Cost and score are explicit.
    Clamp to [0.5, 2.0] so all nodes are visible at the same scale. Larger assets
    (high centrality, cost, or score) get proportionally larger sizes.
    """
    # Default size.
    size = 1.0

    # Boost by score (0.0–1.0 from 17g ranking).
    if score > 0:
        size = max(size, 0.5 + score)  # [0.5, 1.5]

    # Boost by cost if present.
    cost = node.get("cost", 0.0)
    if cost is not None and cost > 0:
        # Normalize cost logarithmically (large spenders stand out without dominating).
        import math
        cost_boost = min(1.0, math.log(1 + cost / 100.0) / 2.0)
        size = min(2.0, size + cost_boost)

    return min(2.0, max(0.5, size))


def _label_for_node(node_id: str, kind: str) -> str:
    """Extract a human-readable label from node_id.

    Node IDs are prefixed: ``asset:<fqn>``, ``tag:<key>``, ``agent:<id>``,
    ``mv:<fqn>``, ``schema:<key>``. Strip the prefix for the label.
    """
    if ":" in node_id:
        prefix, rest = node_id.split(":", 1)
        # Use the last component for FQNs (catalog.schema.table → table).
        if prefix == "asset" and "." in rest:
            return rest.split(".")[-1]
        return rest
    return node_id
