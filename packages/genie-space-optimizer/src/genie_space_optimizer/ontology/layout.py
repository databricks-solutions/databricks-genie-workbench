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

# Ontology Map — typed display-assets (MV-D89). The map's DISPLAY projection is exactly
# these kinds; every other signal-graph node (a ``tag:``/``schema:`` hub, or a bare
# ``node`` from a defensively-added semantic-sim endpoint) is STRUCTURAL — it drives
# domain resolution + membership edges but is never emitted into ``assets.nodes``. The
# ``mv:<fqn>`` measure-hub carries kind ``metric_view`` too, so it is excluded by id
# prefix (``_STRUCTURAL_ID_PREFIXES``) and folded into the typed ``asset:<fqn>`` node
# (Build C) rather than by kind. The cap then ranks REAL assets, not tag plumbing.
_DISPLAY_KINDS = frozenset({"table", "metric_view", "agent", "dashboard", "measure"})
_STRUCTURAL_ID_PREFIXES = ("tag:", "schema:", "mv:")

# Bounded per-parent "business-snippet" index (MV-D73 §2.3): the expand-on-demand
# source is baked in the deterministic batch (no request-path warehouse), so cap the
# MV measures and sub-domain Pages per parent to keep the snapshot blob small (MV-D49).
MAX_SNIPPET_MEASURES = 50
MAX_SNIPPET_PAGES = 50

# Ontology Map north-star — Data Lane (MV-D82). Containment precedence for the
# canonical single-parent rule (Build B): an asset that is the TARGET of several
# containment edges keeps exactly ONE tree parent — the strongest kind wins
# (mv_membership ⊃ agent_scope), ties break by higher weight then source id asc.
# Every surplus containment stays a typed verb cross-link (Build C).
_CONTAINMENT_RANK = {"mv_membership": 2, "agent_scope": 1}

# Plain-language verb per signal-edge kind (Build C, §3.4 / MV-D23). An unlisted
# kind degrades to the raw kind string (never blank) so the map stays honest.
_VERB_BY_KIND = {
    "mv_membership": "reads",
    "lineage_adjacency": "reads",
    "agent_scope": "uses",
    "dashboard_scope": "reads",
    "join_key": "shares",
    "co_query": "also queried with",
    "semantic_sim": "similar to",
}


# Ontology Map north-star gap-closure (MV-D86, Lane D2): the additive per-node ``meta``
# bag. Reveal-don't-invent (MV-D82): each entry maps an OUTPUT meta key ← the fused-graph
# node field it reads; a field the 17d inventory did not carry on the node is omitted
# (never fabricated), a node with zero present fields yields ``meta=None``. Values are
# coerced to str so the bag stays a compact ``Dict[str,str]`` (MV-D49, blob-only). Keys are
# emitted in this fixed order so the snapshot blob is byte-stable across runs. Measures ride
# the expand-on-demand snippet layer (MV-D73), not the base snapshot — but a ``measure`` node
# that ever appears in the fused graph degrades cleanly here too.
_META_FIELDS_BY_KIND: dict[str, tuple[tuple[str, str], ...]] = {
    "table": (("rows", "row_count"), ("format", "data_format"),
              ("freshness", "freshness"), ("path", "storage_path")),
    "metric_view": (("measures", "measure_count"), ("dimensions", "dimension_count"),
                    ("freshness", "freshness")),
    "measure": (("expression", "expression"), ("format", "format")),
    "agent": (("sample_questions", "sample_question_count"), ("queries_28d", "queries_28d")),
    "dashboard": (("viewers_28d", "viewers_28d"), ("refresh", "refresh")),
}


def _clean_description(value: Any) -> str | None:
    """A plain-language description coerced to a non-empty ``str`` or ``None`` (MV-D86).

    Reveal-don't-invent: a missing/blank source (no UC comment on the 17d node, no
    ``description`` on the domain row) degrades to ``None`` so Lane P falls back to
    generic copy rather than rendering an empty string."""
    if value is None or str(value) == "":
        return None
    return str(value)


def _node_meta(node: dict[str, Any]) -> dict[str, str] | None:
    """Compact ``Dict[str,str]`` meta bag for one fused-graph node (MV-D86, Lane D2).

    Reads only the type-appropriate fields the 17d inventory already carried on the node
    (``_META_FIELDS_BY_KIND``), coercing each present value to a string; a field with no
    signal is omitted and a node with no present fields yields ``None`` — reveal-don't-
    invent (MV-D82), additive + degrade-clean (MV-D43). An unlisted kind (``tag``,
    ``schema``, ``org``, …) has no meta."""
    spec = _META_FIELDS_BY_KIND.get(node.get("kind", ""))
    if not spec:
        return None
    bag: dict[str, str] = {}
    for out_key, field in spec:
        val = node.get(field)
        if val is not None and str(val) != "":
            bag[out_key] = str(val)
    return bag or None


def _verb_of(kind: str) -> str:
    """The plain-language verb for a signal-edge ``kind`` (MV-D82 §3.4).

    Falls back to the raw kind for an unlisted edge kind (e.g. ``schema_affinity``,
    ``tag_assignment``) — reveal-don't-invent, never a blank verb."""
    return _VERB_BY_KIND.get(kind, kind)


def _walk_to_top_domain(domain_id: str | None, domain_meta: dict[str, dict[str, Any]]) -> str | None:
    """Follow ``parent_id`` up the domain_meta chain to the top-level domain id.

    A sub-domain (``parent_id`` set) resolves to its ancestor Domain; a top-level
    domain (or an id absent from meta, e.g. ``ungrouped``) resolves to itself; a
    ``None`` domain resolves to ``None``. Cycle-guarded + bounded so a malformed
    meta chain degrades to its last seen id instead of hanging (MV-D43)."""
    seen: set[str] = set()
    cur = domain_id
    while cur is not None and cur not in seen:
        seen.add(cur)
        meta = domain_meta.get(cur)
        parent = meta.get("parent_id") if meta else None
        if not parent:
            return cur
        cur = parent
    return cur


def _rel_class_of(top_a: str | None, top_b: str | None) -> str:
    """``"shared"`` when both endpoints resolve to the SAME (non-null) top domain,
    else ``"xdom"`` (MV-D82 §3.4). Two unresolved endpoints are ``xdom`` — they do
    not share a real domain."""
    return "shared" if (top_a is not None and top_a == top_b) else "xdom"


# Ontology Map interaction pass — per-edge evidence bag (MV-D88, Lane E). A compact,
# reveal-don't-invent ``Dict[str,str]`` for the hover edge-tooltip (MV-D87, Lane P2),
# sourced ONLY from what the fused signal-graph edge already carries. A key with no
# signal is omitted, values are plain labels (never a raw float, MV-D35), and an unlisted
# edge kind yields ``None`` so the blob omits ``detail`` and Lane P2 degrades to verb +
# endpoints. Insertion order is fixed so the snapshot blob stays byte-stable (MV-D49).
# Similarity bands track L3's dedup_gate thresholds (er.MERGE_THRESHOLD 0.90 /
# er.ESCALATE_LOW 0.72) so the label follows the same boundaries the engine used.
_SEM_SIM_VERY_HIGH = 0.90
_SEM_SIM_HIGH = 0.72


def _sim_band(weight: float) -> str:
    """A plain similarity band for a ``semantic_sim`` cosine ``weight`` (MV-D88).

    ``very high`` ≥ 0.90, ``high`` ≥ 0.72, else ``moderate`` — mirroring the L3
    thresholds so the tooltip never renders a bare float (MV-D35, honest-not-opaque)."""
    if weight >= _SEM_SIM_VERY_HIGH:
        return "very high"
    if weight >= _SEM_SIM_HIGH:
        return "high"
    return "moderate"


def _columns_label(cols: Any) -> str | None:
    """A plain join-column label from whatever the edge carried (MV-D88).

    Accepts a list/tuple of column names or a single string; drops blanks; a missing or
    empty value yields ``None`` — reveal-don't-invent, omit ``columns`` with no signal."""
    if cols is None:
        return None
    if isinstance(cols, (list, tuple)):
        parts = [str(c) for c in cols if c is not None and str(c) != ""]
        return ", ".join(parts) or None
    return str(cols) or None


def _edge_detail(edge: dict[str, Any], mv_measure_count: dict[str, int]) -> dict[str, str] | None:
    """The compact per-edge evidence bag for one fused-graph edge (MV-D88, Lane E).

    Reveal-don't-invent: every value is read from what the edge already carries — the
    ``source`` (FK vs shared-join-column proxy), the ``weight`` (a co-query count / a
    similarity cosine), the join ``columns`` if the edge names them, and the MV measure
    count from the baked snippet index. A key with no signal is omitted; an unlisted edge
    kind returns ``None`` so the blob omits ``detail`` (Lane P2 falls back to verb +
    endpoints). Values are plain labels, never a raw float (MV-D35)."""
    kind = edge.get("kind", "")
    weight = edge.get("weight")
    if kind == "join_key":
        bag: dict[str, str] = {}
        col_label = _columns_label(edge.get("columns"))
        if col_label:
            bag["columns"] = col_label
        # FK vs shared-join-column proxy, from the edge ``source`` (graph.add_edge).
        bag["kind"] = "foreign key" if edge.get("source") == "foreign_key" else "shared column"
        return bag
    if kind == "co_query":
        if weight is None:
            return None
        return {"co_queried": f"{int(round(float(weight)))} sessions"}
    if kind == "lineage_adjacency":
        return {"flow": "feeds"}
    if kind == "mv_membership":
        bag = {"role": "aggregates"}
        count = mv_measure_count.get(str(edge.get("src") or ""))
        if count:
            bag["measures"] = str(count)  # bare count, like the MV-D86 node meta bag
        return bag
    if kind == "semantic_sim":
        if weight is None:
            return None
        return {"similarity": _sim_band(float(weight))}
    if kind == "agent_scope":
        return {"role": "queries"}
    return None  # unlisted kind → no detail (Lane P2 degrades to verb + endpoints)


def _fqn_of(node_id: str) -> str:
    """Strip the ``prefix:`` from a signal-graph node id (``asset:c.s.t`` → ``c.s.t``).

    Signal-graph nodes are prefixed by kind (``asset:``/``tag:``/``mv:``/``agent:``/
    ``schema:``) while the ``node_domain_id`` map is keyed by the bare asset FQN
    (see ``cluster._fqn_of`` and the ``asset_domain`` map in ``materialize``). The
    domain lookup below tries the raw id first, then the de-prefixed FQN so both
    key schemes resolve. Nodes without a prefix pass through unchanged.
    """
    return node_id.split(":", 1)[1] if ":" in node_id else node_id


def _is_display_node(node_id: str, kind: str) -> bool:
    """True when a signal-graph node belongs in the DISPLAY projection (MV-D89).

    A display asset has a display ``kind`` (``table``/``metric_view``/``agent``/
    ``dashboard``/``measure``) AND is not a structural hub id (``tag:``/``schema:``/
    ``mv:``). The ``mv:<fqn>`` hub shares kind ``metric_view`` with a real typed MV, so
    the id-prefix guard is what excludes it (Build C folds it into ``asset:<fqn>``)."""
    if node_id.startswith(_STRUCTURAL_ID_PREFIXES):
        return False
    return kind in _DISPLAY_KINDS


def build_graph_snapshot(
    signal_graph: dict[str, Any],
    node_domain_id: dict[str, str],
    node_scores: dict[str, float] | None = None,
    *,
    domain_meta: dict[str, dict[str, Any]] | None = None,
    snippets_in: dict[str, Any] | None = None,
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
    - ``signal_graph``: ``{"nodes": [...], "edges": [...]`` from 17d. An asset node may
      carry an optional ``description`` (its UC comment) + type-appropriate stat fields
      (``row_count``, ``data_format``, ``measure_count``, ``queries_28d``, …) that MV-D86
      surfaces as the node's ``description`` + compact ``meta`` bag; a node without them
      degrades to ``None`` (reveal-don't-invent). An edge may carry ``source`` (FK vs
      shared-join-column proxy), ``weight`` (co-query count / similarity cosine), and
      optional join ``columns`` that MV-D88 surfaces as the edge's compact ``detail``
      evidence bag (all three edge sets); an unlisted edge kind omits ``detail``.
    - ``node_domain_id``: ``{node_id → domain_id}`` from 17e clustering.
    - ``node_scores``: Optional ``{node_id → score}`` from 17g ranking (for sizing).
    - ``domain_meta``: Optional ``{domain_id → {name, parent_id, origin[, description]}}`` from
      17e/17f domain rows — labels the rollup nodes with human names, links Sub-Domain →
      Domain so the map can render the hierarchy (MV-D71), and stamps ``origin`` on each rollup
      (``applied`` when the domain is backed by a governed-tag decision, else
      ``proposed``; MV-D73 §2.1). An optional ``description`` (MV-D86, Lane D2) is threaded
      onto the rollup node when present. Absent → raw ids, flat, ``proposed``, no description.
    - ``snippets_in``: Optional expand-on-demand "business-snippet" index (MV-D73 §2.3),
      baked in the deterministic batch (no request-path warehouse). Shape
      ``{"measures": {mv_fqn → [{ref,name,expression,fmt}]},
      "pages": {domain_id → [{page_id,title,archetype,domain_id}]}}``. Measures re-key to
      the ``mv:<fqn>`` hub node id, pages key to the sub-domain rollup ``domain_id``; both
      are capped per parent. Absent → the ``snippets`` blob key is omitted (byte-stable).
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
            "graph": json.dumps({"domains": {"nodes": [], "edges": []}, "assets": {"nodes": [], "edges": []}, "subdomains": {"edges": []}}),
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
            "graph": json.dumps({"domains": {"nodes": [], "edges": []}, "assets": {"nodes": [], "edges": []}, "subdomains": {"edges": []}}),
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

    # Attach (x, y) coordinates + sizes to each DISPLAY node. Build A (MV-D89): the
    # display projection narrows to real assets — ``tag:``/``schema:``/``mv:`` hubs stay in
    # the igraph layout + the structural lookups below (domain resolution, membership
    # edges, ``_top_of_asset``) but never become display nodes, so the cap ranks real
    # assets instead of tag plumbing. Skipped nodes keep their layout index ``i`` (the
    # igraph vertex set is unchanged), so coordinates for surviving nodes are identical.
    asset_nodes = []
    node_sizes = {}
    for i, node in enumerate(nodes):
        node_id = node.get("id", "")
        kind = node.get("kind", "table")
        if not _is_display_node(node_id, kind):
            continue
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
            "label": _label_for_node(node_id, kind, node),
            "kind": kind,
            "domain_id": domain_id,
            "x": float(x),
            "y": float(y),
            "size": size,
            "cost": float(cost) if cost is not None else None,
            # MV-D86 (Lane D2): the UC table/MV comment threaded onto the 17d node (else
            # None) + the compact type-appropriate meta bag (rows/format/…; None when the
            # inventory carried no signal). Additive + degrade-clean — Lane P surfaces
            # these in the hover-snippet + inspector when present (MV-D85).
            "description": _clean_description(node.get("description")),
            "meta": _node_meta(node),
        })

    # Top-domain resolver for an asset-level node id (Build C rel_class). Resolves the
    # node's domain via node_domain_id (raw id, then bare fqn — the MV-D71 prefix fix)
    # and walks domain_meta parents to the top Domain.
    def _top_of_asset(node_id: str) -> str | None:
        did = node_domain_id.get(node_id) or node_domain_id.get(_fqn_of(node_id))
        return _walk_to_top_domain(did, domain_meta)

    # Per-MV measure count for the mv_membership edge-detail (MV-D88, Lane E). Read from
    # the baked snippet index (no request-path warehouse) and keyed by the ``mv:<fqn>`` hub
    # node id, so an mv_membership edge (whose src IS that hub) can label how many measures
    # the metric view aggregates. Absent snippets ⇒ empty map ⇒ the ``measures`` key is omitted.
    mv_measure_count: dict[str, int] = {}
    if snippets_in:
        for mv_fqn, measures in (snippets_in.get("measures") or {}).items():
            mv_measure_count[f"mv:{mv_fqn}"] = len(measures)

    # Build asset-level edges (same as input, but only for present nodes). Each edge
    # now carries a plain-language ``verb`` and a ``shared``/``xdom`` ``rel_class`` from
    # its endpoints' top domains (Build C, MV-D82) plus an optional reveal-don't-invent
    # ``detail`` evidence bag (MV-D88, Lane E) — all additive; the src/dst/kind/weight
    # shape is unchanged for a reader that ignores the new keys. ``detail`` is omitted
    # entirely when the edge kind carries no signal (Lane P2 degrades to verb + endpoints).
    asset_node_ids = {n["id"] for n in asset_nodes}

    # Build C — dedupe the MV double-emit (MV-D90). A tagged Metric View can appear twice:
    # as the typed display node ``asset:<fqn>`` (from its tag membership, now kind
    # ``metric_view`` per Build B) AND as the structural ``mv:<fqn>`` measure-hub. Build A
    # already dropped the hub from the display projection; here we FOLD it in by mapping
    # ``mv:<fqn> -> asset:<fqn>`` whenever the typed asset survived, so the hub's
    # ``mv_membership`` edges reattach to the surviving node (Measures still expand) and
    # exactly one display node remains. A hub with no surviving ``asset:<fqn>`` (an untagged
    # MV) has no fold target — its edges drop with the hub, consistent with Build A. Sorted
    # for determinism (MV-D82).
    mv_hub_survivor: dict[str, str] = {}
    for node in nodes:
        nid = node.get("id", "")
        if nid.startswith("mv:"):
            survivor = f"asset:{nid[len('mv:'):]}"
            if survivor in asset_node_ids:
                mv_hub_survivor[nid] = survivor
    mv_hub_survivor = dict(sorted(mv_hub_survivor.items()))

    asset_edges = []
    for edge in edges:
        raw_src = edge.get("src")
        raw_dst = edge.get("dst")
        # Reattach a folded hub's edges to the surviving typed node (Build C); a non-hub
        # endpoint passes through unchanged (``.get(x, x)``).
        src_id = mv_hub_survivor.get(raw_src, raw_src)
        dst_id = mv_hub_survivor.get(raw_dst, raw_dst)
        if src_id in asset_node_ids and dst_id in asset_node_ids:
            kind = edge.get("kind", "unknown")
            asset_edge: dict[str, Any] = {
                "src": src_id,
                "dst": dst_id,
                "kind": kind,
                "weight": edge.get("weight"),
                "verb": _verb_of(kind),
                "rel_class": _rel_class_of(_top_of_asset(src_id), _top_of_asset(dst_id)),
            }
            # Detail reads the ORIGINAL edge (its ``mv:<fqn>`` src still keys the measure
            # count) so the folded edge keeps its evidence bag intact (MV-D88).
            detail = _edge_detail(edge, mv_measure_count)
            if detail is not None:
                asset_edge["detail"] = detail
            asset_edges.append(asset_edge)

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

    # Build B — canonical containment parent + attach_level per asset (MV-D82). From the
    # (post-cap) edges where the asset is the TARGET, pick the strongest containment by
    # precedence (mv_membership ⊃ agent_scope; tie → higher weight → source id asc):
    # parent_id = that mv:/agent: source, attach_level="asset". If none, attach to the
    # domain: a sub-domain (its domain_id has a parent_id) → "subdomain", a top-level
    # domain → "domain"; ungrouped/absent domain → parent_id null, "domain". Exactly ONE
    # parent — a table read by >1 MV keeps only the strongest; surplus memberships stay
    # verb edges (Build C). Additive keys on the existing asset-node dicts.
    incoming_containment: dict[str, list[dict[str, Any]]] = {}
    for edge in asset_edges:
        if edge["kind"] in _CONTAINMENT_RANK:
            incoming_containment.setdefault(edge["dst"], []).append(edge)

    # MV-D86 (Lane D2) — deeper containment: give a metric view a tree parent so the map
    # renders ``agent ⊃ metric_view ⊃ table`` instead of a flat sub-area. An MV is never the
    # TARGET of a containment edge (mv_membership/agent_scope both point at tables), so it
    # would otherwise fall straight to the domain. Derived ONLY from existing edges: an MV
    # attaches to the agent whose ``agent_scope`` covers the most of the MV's ``mv_membership``
    # source tables (tie → agent id asc — deterministic). No overlap ⇒ no derived parent (the
    # MV keeps its domain fallback). Reveal-don't-invent: no synthetic edge is emitted.
    agent_tables: dict[str, set[str]] = {}
    mv_tables: dict[str, set[str]] = {}
    for edge in asset_edges:
        if edge["kind"] == "agent_scope":
            agent_tables.setdefault(edge["src"], set()).add(edge["dst"])
        elif edge["kind"] == "mv_membership":
            mv_tables.setdefault(edge["src"], set()).add(edge["dst"])
    mv_agent_parent: dict[str, str] = {}
    for mv_id, tables in mv_tables.items():
        best_agent, best_overlap = None, 0
        for agent_id in sorted(agent_tables):
            overlap = len(tables & agent_tables[agent_id])
            if overlap > best_overlap:
                best_agent, best_overlap = agent_id, overlap
        if best_agent is not None:
            mv_agent_parent[mv_id] = best_agent

    for node in asset_nodes:
        cands = incoming_containment.get(node["id"])
        if cands:
            best = min(cands, key=lambda e: (
                -_CONTAINMENT_RANK[e["kind"]],
                -(e.get("weight") or 0.0),
                e["src"],
            ))
            node["parent_id"] = best["src"]
            node["attach_level"] = "asset"
        elif node["id"] in mv_agent_parent:
            # A metric view scoped by an agent → nest it under that agent (MV-D86).
            node["parent_id"] = mv_agent_parent[node["id"]]
            node["attach_level"] = "asset"
        else:
            did = node.get("domain_id")
            if did is not None:
                meta = domain_meta.get(did)
                node["parent_id"] = did
                node["attach_level"] = "subdomain" if (meta and meta.get("parent_id")) else "domain"
            else:
                node["parent_id"] = None
                node["attach_level"] = "domain"

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
            # Provenance (MV-D73 §2.1): a rollup is ``applied`` only when its domain is
            # backed by a governed-tag decision (threaded onto meta in materialize from
            # ``tag_decision``); every meta-less cluster and the Ungrouped blob is
            # ``proposed`` (a pure engine suggestion, not current governed state).
            origin = (meta.get("origin") if meta else None) or "proposed"
            # Assign a rolled-up position: average of member positions.
            domain_dict[domain_id] = {
                "id": domain_id or "ungrouped",
                "label": label,
                "kind": kind,
                "domain_id": domain_id,
                "parent_id": parent_id,
                "parent_name": (parent_meta.get("name") if parent_meta else None) or parent_id,
                "origin": origin,
                # MV-D86 (Lane D2): the domain/sub-domain description from the genie_ont_domains
                # row (threaded onto meta upstream); None for the Ungrouped blob or a meta-less
                # cluster. Additive + degrade-clean — Lane P shows it in the inspector.
                "description": _clean_description(meta.get("description")) if meta else None,
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

    # Parent-domain closure. ``domain_dict`` is keyed off each asset's ``domain_id``, so a
    # rollup node exists only for a domain/sub-domain that has DIRECTLY-tagged assets. A
    # top-level domain whose assets all live in its sub-domains (e.g. a governed "…
    # Operations" tag) therefore gets NO node, yet its sub-domains ARE emitted carrying a
    # ``parent_id`` that points at the absent parent — a dangling reference. The renderer's
    # degrade guard then re-roots those orphans onto the Estate root, so the real domain
    # never appears in the tree or the show/hide panel (and cannot be hidden). Fix: backfill
    # every ancestor referenced by an emitted node's ``parent_id`` but not itself emitted,
    # walking ``domain_meta`` to the top. Synthesized nodes take the meta name/description/
    # origin; position/size are averaged from the children that referenced them so the
    # rollup sits amid its sub-domains. Additive, deterministic (sorted), blob-only (MV-D49).
    emitted_ids = {d["id"] for d in domain_nodes}
    pending = [d for d in domain_nodes if d.get("parent_id") and d["parent_id"] not in emitted_ids]
    while pending:
        missing: dict[str, list[dict[str, Any]]] = {}
        for child in pending:
            missing.setdefault(child["parent_id"], []).append(child)
        new_parent_nodes: list[dict[str, Any]] = []
        for pid, kids in sorted(missing.items()):
            meta = domain_meta.get(pid)
            gp = meta.get("parent_id") if meta else None
            gp_meta = domain_meta.get(gp) if gp else None
            xs = [k["x"] for k in kids if k.get("x") is not None]
            ys = [k["y"] for k in kids if k.get("y") is not None]
            sizes = [k.get("size") for k in kids if k.get("size") is not None]
            new_parent_nodes.append({
                "id": pid,
                "label": (meta.get("name") if meta else None) or pid,
                "kind": "subdomain" if gp else "domain",
                "domain_id": pid,
                "parent_id": gp,
                "parent_name": (gp_meta.get("name") if gp_meta else None) or gp,
                "origin": (meta.get("origin") if meta else None) or "proposed",
                "description": _clean_description(meta.get("description")) if meta else None,
                "x": float(sum(xs) / len(xs)) if xs else 0.0,
                "y": float(sum(ys) / len(ys)) if ys else 0.0,
                "size": float(sum(sizes) / len(sizes)) if sizes else 1.0,
                "cost": None,
                "member_count": 0,
            })
        domain_nodes.extend(new_parent_nodes)
        emitted_ids.update(n["id"] for n in new_parent_nodes)
        # Close the next level up (a grandparent that is still absent).
        pending = [n for n in new_parent_nodes if n.get("parent_id") and n["parent_id"] not in emitted_ids]

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
                d_src = src_domain or "ungrouped"
                d_dst = dst_domain or "ungrouped"
                kind = edge.get("kind", "unknown")
                d_edge: dict[str, Any] = {
                    "src": d_src,
                    "dst": d_dst,
                    "kind": kind,
                    "weight": edge.get("weight"),
                    "verb": _verb_of(kind),
                    "rel_class": _rel_class_of(
                        _walk_to_top_domain(d_src, domain_meta),
                        _walk_to_top_domain(d_dst, domain_meta),
                    ),
                }
                # MV-D88 (Lane E): carry the representative asset edge's evidence bag up to
                # the rollup edge (consistent with its ``weight``, also from this edge).
                detail = edge.get("detail")
                if detail is not None:
                    d_edge["detail"] = detail
                domain_edges.append(d_edge)

    # Sub-domain rollup edges (MV-D73 §2.2): aggregate asset edges to the sub-domain
    # grain so the Sub-domains LOD is not edge-empty. A sub-domain is a domain_id whose
    # meta carries a parent_id (kind == "subdomain"); ``present_subs`` is the set that
    # actually survived to a rollup node (emitted-only guard — never emit an edge whose
    # endpoint sub-domain is absent). Cross-sub only, deduped by (src_sub, dst_sub, kind).
    present_subs = {d["id"] for d in domain_nodes if d.get("parent_id")}
    asset_sub: dict[str, str] = {}
    for node in asset_nodes:
        did = node.get("domain_id")
        if did and did in present_subs:
            asset_sub[node["id"]] = did
    subdomain_edges = []
    seen_sub_edges: set[tuple[str, str, Any]] = set()
    for edge in asset_edges:
        src_sub = asset_sub.get(edge["src"])
        dst_sub = asset_sub.get(edge["dst"])
        if not src_sub or not dst_sub or src_sub == dst_sub:
            continue
        edge_key = (src_sub, dst_sub, edge.get("kind"))
        if edge_key in seen_sub_edges:
            continue
        seen_sub_edges.add(edge_key)
        kind = edge.get("kind", "unknown")
        sub_edge: dict[str, Any] = {
            "src": src_sub,
            "dst": dst_sub,
            "kind": kind,
            "weight": edge.get("weight"),
            "verb": _verb_of(kind),
            "rel_class": _rel_class_of(
                _walk_to_top_domain(src_sub, domain_meta),
                _walk_to_top_domain(dst_sub, domain_meta),
            ),
        }
        # MV-D88 (Lane E): carry the representative asset edge's evidence bag through.
        detail = edge.get("detail")
        if detail is not None:
            sub_edge["detail"] = detail
        subdomain_edges.append(sub_edge)

    # Bounded expand-on-demand "business-snippet" index (MV-D73 §2.3): re-key measures to
    # the mv:<fqn> hub node id and pages to their sub-domain rollup domain_id, capping each
    # list per parent. Baked deterministically here (no request-path warehouse); absent
    # snippets_in ⇒ the key is omitted so the blob stays byte-stable with today.
    snippets_out: dict[str, dict[str, Any]] | None = None
    if snippets_in is not None:
        snippets_out = {}
        for mv_fqn, measures in (snippets_in.get("measures") or {}).items():
            # Build C (MV-D90): key measures to the SURVIVING display node id. When the MV
            # folded into a typed ``asset:<fqn>`` node, the expand route (``snippets[node]``)
            # is asked for ``asset:<fqn>`` — key there so Measures still expand; otherwise the
            # ``mv:<fqn>`` hub key is unchanged (byte-identical to today for that case).
            hub_id = f"mv:{mv_fqn}"
            snippets_out[mv_hub_survivor.get(hub_id, hub_id)] = {
                "measures": list(measures)[:MAX_SNIPPET_MEASURES]
            }
        for sub_id, pgs in (snippets_in.get("pages") or {}).items():
            snippets_out.setdefault(sub_id, {})["pages"] = list(pgs)[:MAX_SNIPPET_PAGES]

    # Build the snapshot blob. Build A (MV-D82): a single ``org`` root node keyed by the
    # metastore so a tree renderer has one estate root above the Domains. Emitted only on
    # the non-empty path (the empty/degrade returns above omit it — MV-D43); top-level
    # domain rollups keep parent_id=None (NOT rewired to root — degrade path stays intact).
    snapshot_blob = {
        "root": {"id": metastore_id, "label": "Estate", "kind": "org"},
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
        "subdomains": {
            "edges": subdomain_edges,
        },
        "layout": layout_algo,
        "node_count": len(nodes),
        "edge_count": len(edges),
    }
    if snippets_out is not None:
        snapshot_blob["snippets"] = snippets_out

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


def _label_for_node(node_id: str, kind: str, node: dict[str, Any] | None = None) -> str:
    """Extract a human-readable label from node_id.

    Node IDs are prefixed: ``asset:<fqn>``, ``tag:<key>``, ``agent:<id>``,
    ``mv:<fqn>``, ``schema:<key>``. Strip the prefix for the label.

    Stage 2 (MV-D91): when the fused-graph node carries an explicit ``label`` (an
    ``agent`` node threaded with its Genie space display name — a raw space id is not
    human-readable), prefer it. Additive + reveal-don't-invent: absent ⇒ the prefix-strip
    fallback below, so every other node kind is byte-identical."""
    if node is not None:
        label = node.get("label")
        if label:
            return str(label)
    if ":" in node_id:
        prefix, rest = node_id.split(":", 1)
        # Use the last component for FQNs (catalog.schema.table → table).
        if prefix == "asset" and "." in rest:
            return rest.split(".")[-1]
        return rest
    return node_id
