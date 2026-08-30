"""L4 domain / sub-domain clustering engine (Phase 3b).

Turns 17d's fused signal graph (``graph.build_signal_graph``) + the 17d identity
map into a two-level Domain -> Sub-Domain tree of :class:`DomainProposal`s, each
bound to a REUSE / CREATE / REASSIGN governed-tag decision (a *proposal field* —
never a UC write). Written by ``materialize.py`` into ``genie_ont_domains`` +
``genie_ont_members``.

The four building blocks (architecture §5, MV-D39):

  1. **Assemble weighted layers (multiplex).** The fused heterograph's edge kinds
     each become their OWN ``leidenalg`` layer with a per-layer weight — never
     hand-collapsed to one scalar. Structural signals (``lineage_adjacency`` backbone,
     ``join_key``, ``co_query``, ``semantic_sim`` weak glue) are asset<->asset directly;
     the strong PRIORS (``tag_assignment`` strongest, ``agent_scope`` seed) are
     projected to asset<->asset cliques. ``cost`` is a ranking weight (L6), not a layer.
  2. **Soft-seeded community detection (Domains).** Leiden via ``leidenalg`` over
     ``python-igraph`` with the **CPM** objective; existing governed tags + Agent
     scopes seed the partition via ``initial_membership`` — a STRONG but SOFT prior
     (``is_membership_fixed`` is never set, so strong graph evidence can still move a
     mis-seeded node). Catalog/schema is the thin-signal fallback prior. Leiden (not
     Louvain) guarantees every community is internally connected.
  3. **Recursive split (Sub-Domains).** Re-run detection at a finer CPM ``γ`` on each
     Domain's induced subgraph, over the STRUCTURAL layers only (the domain tag has
     already bound the domain — sub-domains are structural); the sub-communities become
     Sub-Domains.
  4. **Centrality + naming + binding.** Betweenness/degree on the lineage subgraph
     picks the anchor chip; the LLM names the cluster (company prior + member
     identifiers only — degrades to an anchor-derived name, MV-D43); each cluster is
     bound to 17d's identity verdict via a global per-level tag-ownership pass ->
     reuse / create / reassign.

Deterministic by construction: fixed ``leidenalg`` seed + fixed ``n_iterations``,
single-threaded, stable tie-break (sorted canonical refs), and ``domain_id`` is a
fingerprint of the cluster's SORTED canonical members — so a re-run over the same
graph yields the SAME domains/members (the MV-D26 suppression ledger in 17g needs
that id stable across runs).

Pure/offline: ``leidenalg``/``igraph`` are lazy-imported inside :func:`cluster`, and
the naming LLM is the ONLY external call (lazy-imported, degrades). No similarity
code and no governed-tag write of any kind (no tag apply, no UC-tag management tool)
— ``tag_decision`` is a proposal string only.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Any, Callable, Literal, Sequence

from genie_space_optimizer.ontology import er

logger = logging.getLogger(__name__)

# ── Tunable constants (architecture §5 "honest gap") ────────────────────────
# CPM resolution: LOWER γ -> fewer/bigger communities (Domains); HIGHER γ ->
# more/smaller communities (Sub-Domains). Exposed so a real estate graph can tune
# them; the soft tag/Agent seeding + the reassignment ledger are the stabiliser.
GAMMA_COARSE = 0.20   # Domains  (coarse pass)
GAMMA_FINE = 0.30     # Sub-Domains (per-Domain finer pass, structural layers only)

# Per-layer weights — NOT equal (the whole point of multiplex): lineage is the
# backbone, tag_assignment the strongest prior, semantic_sim weak glue. `cost` is
# absent by design (L6 ranking weight, not a clustering layer).
LAYER_WEIGHTS: dict[str, float] = {
    "lineage_adjacency": 5.0,   # backbone
    "join_key": 3.0,            # reinforce (17d emits none yet; forward-compatible)
    "tag_assignment": 5.0,      # strongest prior (reuse an existing tag over inventing)
    "agent_scope": 3.0,         # a curated "these go together" seed
    "co_query": 2.0,            # reinforce
    "semantic_sim": 1.0,        # weak glue
}

# Structural layers drive the recursive Sub-Domain split (the domain tag has already
# bound the Domain); the PRIOR layers seed + bind but do not fragment sub-domains.
STRUCTURAL_KINDS = ("lineage_adjacency", "join_key", "co_query", "semantic_sim")
PRIOR_KINDS = ("tag_assignment", "agent_scope")

# Determinism: a fixed RNG seed + fixed iteration count + single thread makes the
# (otherwise stochastic) Leiden reproducible. leidenalg exposes no `beta` knob; the
# seed is what pins the refinement randomness.
LEIDEN_SEED = 1_729
N_ITERATIONS = 5

# Reassignment margin (τ_reassign): the fraction of a governed tag's seeded members
# the graph must pull into a DIFFERENT community before we surface a `reassign`
# conflict for human adjudication (17g). Conservative so only strong contradictions
# reach the reviewer; below it the binder stays `reuse`.
REASSIGN_MARGIN = 0.30

# A namer turns (member identifiers, anchor fqn, company prior) into a cluster name,
# or None to fall back to a deterministic anchor-derived name (MV-D43 degrade).
Namer = Callable[[list[str], "str | None", "str | None"], "str | None"]


@dataclass(frozen=True)
class DomainProposal:
    """One Domain (``parent_id is None``) or Sub-Domain proposal. Maps 1:1 onto the
    ``genie_ont_domains`` columns and expands ``members`` into ``genie_ont_members``
    rows. ``score`` is NULL/0.0 here — L6 ranking is 17g."""

    domain_id: str            # derived: sug_<fingerprint of sorted canonical members>
    parent_id: str | None     # None = domain; set = sub-domain (self-ref to a domain)
    name: str                 # LLM-named (or anchor-derived fallback)
    description: str
    tag_decision: Literal["reuse", "create", "reassign"]
    tag_key: str | None       # existing tag (reuse), moved-from tag (reassign), or proposed key (create)
    tag_value: str | None     # sub-domain value in the Domain/Sub `/` convention
    evidence: dict            # {anchor, shared_spine, co_query_count, tag_prior, seed, conflict?}
    members: tuple[str, ...]  # canonical asset refs (-> genie_ont_members rows)


# ── Derived, stable ids ─────────────────────────────────────────────────────


def domain_id_of(members: Sequence[str]) -> str:
    """Fingerprint of the sorted canonical member set — stable across runs so the
    17g suppression ledger can suppress a dismissed proposal by id (idempotent)."""
    digest = hashlib.sha256("".join(sorted(members)).encode("utf-8")).hexdigest()
    return f"sug_{digest[:16]}"


# ── Node-id helpers (the fused graph uses ``tag:``/``agent:``/``asset:`` ids) ──


def _is_asset(node_id: str) -> bool:
    return node_id.startswith("asset:")


def _is_tag(node_id: str) -> bool:
    return node_id.startswith("tag:")


def _fqn_of(node_id: str) -> str:
    return node_id.split(":", 1)[1] if ":" in node_id else node_id


def _schema_prefix(fqn: str) -> str:
    """catalog.schema of an asset fqn (the thin-signal fallback seed prior)."""
    parts = fqn.split(".")
    return ".".join(parts[:2]) if len(parts) >= 2 else fqn


def _tag_canonical_map(identity: Sequence[Any] | None) -> dict[str, str]:
    """tag_key -> canonical representative tag_key, from 17d's ER merge verdicts, so
    clustering runs over CANONICAL tags (a ``Finance``/``finance`` duplicate collapses
    to one seed and never mints a duplicate). Non-merged tags map to self."""
    out: dict[str, str] = {}
    for v in identity or []:
        if getattr(v, "verdict", None) != "merge":
            continue
        members = sorted(str(m) for m in (getattr(v, "members", ()) or ()))
        if len(members) < 2:
            continue
        rep = members[0]  # deterministic representative (sorted-first)
        for m in members:
            out[m] = rep
    return out


class _UF:
    """Tiny union-find for building the initial seed partition."""

    def __init__(self, items: Sequence[str]):
        self.parent = {i: i for i in items}

    def find(self, x: str) -> str:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            lo, hi = (ra, rb) if ra < rb else (rb, ra)  # stable, order-independent
            self.parent[hi] = lo


# ── Multiplex Leiden (soft-seeded, CPM) ─────────────────────────────────────


def _run_multiplex(
    vertices: list[str],
    edges_by_kind: dict[str, list[tuple[str, str, float]]],
    *,
    gamma: float,
    seed_membership: list[int] | None,
) -> list[int]:
    """Run soft-seeded multiplex Leiden-CPM over ``vertices``; return the shared
    community membership aligned to ``vertices``.

    One ``leidenalg`` layer per edge kind (per-layer weight from ``LAYER_WEIGHTS``);
    ``initial_membership`` seeds the partition as a SOFT prior. ``is_membership_fixed``
    is deliberately NOT passed, so strong graph evidence can move a seeded node. Fixed
    seed + iterations + single thread = deterministic.
    """
    import igraph as ig  # lazy — keeps the module importable without the graph lib
    import leidenalg as la

    idx = {v: i for i, v in enumerate(vertices)}
    n = len(vertices)
    if n == 0:
        return []

    partitions = []
    layer_weights: list[float] = []
    for kind in sorted(edges_by_kind):  # stable layer order
        pairs = edges_by_kind[kind]
        g = ig.Graph(n=n, directed=False)
        e_idx: list[tuple[int, int]] = []
        e_w: list[float] = []
        for a, b, w in pairs:
            ia, ib = idx.get(a), idx.get(b)
            if ia is None or ib is None or ia == ib:
                continue
            e_idx.append((ia, ib))
            e_w.append(float(w))
        if e_idx:
            g.add_edges(e_idx)
            g.es["weight"] = e_w
        part = la.CPMVertexPartition(
            g,
            initial_membership=list(seed_membership) if seed_membership is not None else None,
            weights="weight" if e_idx else None,
            resolution_parameter=gamma,
        )
        partitions.append(part)
        layer_weights.append(LAYER_WEIGHTS.get(kind, 1.0))

    if not partitions:
        return list(range(n))

    optimiser = la.Optimiser()
    optimiser.set_rng_seed(LEIDEN_SEED)
    # SOFT seed: no is_membership_fixed argument -> a node may move on disagreement.
    optimiser.optimise_partition_multiplex(
        partitions, layer_weights=layer_weights, n_iterations=N_ITERATIONS,
    )
    return list(partitions[0].membership)


def _communities(vertices: list[str], membership: list[int]) -> list[list[str]]:
    """Group vertices by community id; return communities sorted by (−size, first
    sorted member) so the output order is deterministic."""
    groups: dict[int, list[str]] = {}
    for v, c in zip(vertices, membership):
        groups.setdefault(c, []).append(v)
    out = [sorted(vs) for vs in groups.values()]
    out.sort(key=lambda vs: (-len(vs), vs[0] if vs else ""))
    return out


def _clique_edges(groups: Sequence[Sequence[str]]) -> list[tuple[str, str, float]]:
    """Project a set of hub->members groups into asset<->asset clique edges (the
    principled asset-only version of a tag/agent star)."""
    out: list[tuple[str, str, float]] = []
    for members in groups:
        ms = sorted(set(members))
        for i in range(len(ms)):
            for j in range(i + 1, len(ms)):
                out.append((ms[i], ms[j], 1.0))
    return out


# ── Centrality (lineage anchor) ─────────────────────────────────────────────


def _anchor(assets: list[str], lineage_pairs: list[tuple[str, str, float]]) -> str | None:
    """Highest-betweenness asset on the community's lineage subgraph (the spine
    everything joins to — the MV-D35 headline chip). Tie-break: degree, then sorted
    fqn. Falls back to the sorted-first asset when there is no lineage."""
    if not assets:
        return None
    import igraph as ig

    ordered = sorted(assets)
    idx = {a: i for i, a in enumerate(ordered)}
    g = ig.Graph(n=len(ordered), directed=False)
    e: list[tuple[int, int]] = []
    for a, b, _w in lineage_pairs:
        ia, ib = idx.get(a), idx.get(b)
        if ia is not None and ib is not None and ia != ib:
            e.append((ia, ib))
    if not e:
        return ordered[0]
    g.add_edges(e)
    bet = g.betweenness()
    deg = g.degree()
    best = max(range(len(ordered)), key=lambda i: (bet[i], deg[i], -idx[ordered[i]]))
    return ordered[best]


# ── Naming (LLM, degrades to anchor-derived) + leakage guard ────────────────


def _humanize(token: str) -> str:
    return " ".join(w.capitalize() for w in token.replace("_", " ").split()) or token


def _anchor_name(anchor_fqn: str | None, assets: list[str]) -> str:
    """Deterministic fallback name (MV-D43): the anchor's schema segment, else its
    table segment, else the first sorted asset's schema."""
    fqn = anchor_fqn or (sorted(assets)[0] if assets else "")
    parts = fqn.split(".")
    if len(parts) >= 2:
        return _humanize(parts[1])   # schema (e.g. finance.sales.orders -> Sales)
    return _humanize(parts[-1]) if parts and parts[0] else "Domain"


def name_leaks(name: str, member_identifiers: Sequence[str]) -> bool:
    """LeakageOracle for cluster names (the 17d tag-name discipline, extended):
    reject a name that echoes PII, or that leaks an INVENTED identifier — a token
    carrying an underscore/digit or an fqn dot that does not appear in the members we
    supplied (a hallucinated raw identifier). Real domain names (`Commercial`,
    `Sales`) carry no such tokens and pass."""
    if not name or er.pii_reject(name):
        return True
    haystack = " ".join(str(m) for m in member_identifiers).casefold()
    for raw in name.split():
        tok = raw.strip().casefold()
        if not tok:
            continue
        if "." in tok:  # fqn-shaped -> a raw identifier has no place in a name
            return True
        if ("_" in tok or any(ch.isdigit() for ch in tok)) and tok not in haystack:
            return True
    return False


def _cluster_name(
    *,
    bound_tag_value: str | None,
    namer: Namer | None,
    anchor_fqn: str | None,
    assets: list[str],
    company: str | None,
) -> str:
    """Name a cluster. A tag-bound cluster (reuse/reassign) takes the governed
    vocabulary directly. A create cluster asks the namer (company prior + member
    identifiers only), validated by the LeakageOracle; on any failure it degrades to
    a deterministic anchor-derived name and the run still succeeds."""
    if bound_tag_value:
        return _humanize(bound_tag_value)
    identifiers = sorted(assets)
    if namer is not None:
        try:
            proposed = namer(identifiers, anchor_fqn, company)
        except Exception as exc:  # noqa: BLE001 — degrade, never block the run
            logger.info("ontology cluster naming failed (%s); anchor-derived name", exc)
            proposed = None
        if proposed and not name_leaks(proposed, identifiers):
            return proposed.strip()
    return _anchor_name(anchor_fqn, assets)


def default_namer(model: str | None = None, company: str | None = None) -> Namer:
    """Namer backed by ``call_serving_endpoint`` (lazy backend import; degrades to
    None if the backend/LLM is unreachable on the job cluster). Company prior + member
    identifiers ONLY — the Context Pack vocabulary prior is Phase 4."""

    def _name(identifiers: list[str], anchor: str | None, comp: str | None) -> str | None:
        try:
            from backend.services.llm_utils import call_serving_endpoint
            chosen = model
            if chosen:
                from backend.services.model_catalog import validate_chat_model
                chosen = validate_chat_model(chosen)
        except Exception:  # noqa: BLE001 — backend/LLM not reachable here -> degrade
            return None
        prior = comp if comp is not None else company
        prompt = (
            "You name a data DOMAIN from the assets it groups. Reply with ONLY a short "
            "1-3 word business-domain name (e.g. 'Commercial', 'Sales', 'Risk & "
            "Compliance'). Do NOT echo table or column identifiers.\n\n"
            f"Company context: {prior or 'n/a'}\n"
            f"Anchor asset: {anchor or 'n/a'}\n"
            f"Member assets: {', '.join(identifiers[:40])}\n"
        )
        try:
            resp = call_serving_endpoint([{"role": "user", "content": prompt}], model=chosen, max_tokens=24)
        except Exception as exc:  # noqa: BLE001 — degrade, never block the run
            logger.info("ontology cluster naming call failed: %s", exc)
            return None
        return (resp or "").strip().splitlines()[0].strip() or None

    return _name


# ── The engine ──────────────────────────────────────────────────────────────


def _canon_node(node_id: str, canon_tag: dict[str, str]) -> str:
    """Map a tag node to its canonical tag (collapse 17d duplicates); pass others
    through unchanged."""
    if _is_tag(node_id):
        key = _fqn_of(node_id)
        return "tag:" + canon_tag.get(key, key)
    return node_id


@dataclass(frozen=True)
class _Bind:
    tag_key: str
    moved: tuple[str, ...]
    margin: float


def _bind_level(
    communities: list[set[str]],
    level_tags: list[str],
    tag_members: dict[str, set[str]],
) -> list[_Bind | None]:
    """Assign each governed tag at this level to exactly ONE owning community (the one
    holding the most of its seeded members, deterministic tie-break), so two
    communities never both claim to reuse the same tag. Returns, per community, the
    owned tag + how many of its seeded members were pulled elsewhere (for the reassign
    check), or None (-> create)."""
    owner: dict[str, int] = {}
    for t in sorted(level_tags):
        members = tag_members.get(t, set())
        best: tuple[int, int] | None = None  # (overlap, -community_index)
        for i, s in enumerate(communities):
            ov = len(members & s)
            if ov > 0 and (best is None or (ov, -i) > best):
                best = (ov, -i)
        if best is not None:
            owner[t] = -best[1]

    owned_by: dict[int, list[str]] = {}
    for t, i in owner.items():
        owned_by.setdefault(i, []).append(t)

    result: list[_Bind | None] = [None] * len(communities)
    for i, s in enumerate(communities):
        ts = owned_by.get(i)
        if not ts:
            continue
        t = sorted(ts, key=lambda k: (-len(tag_members[k] & s), k))[0]
        members = tag_members[t]
        moved = tuple(sorted(members - s))
        margin = (len(moved) / len(members)) if members else 0.0
        result[i] = _Bind(tag_key=t, moved=moved, margin=margin)
    return result


def cluster(
    signal_graph: dict[str, Any],
    *,
    identity: Sequence[Any] | None = None,
    company: str | None = None,
    namer: Namer | None = None,
    gamma_coarse: float = GAMMA_COARSE,
    gamma_fine: float = GAMMA_FINE,
) -> list[DomainProposal]:
    """Cluster the fused signal graph into a Domain -> Sub-Domain proposal tree.

    ``identity`` is 17d's ER verdicts (duck-typed ``.verdict``/``.members``) used to
    collapse duplicate tags to canonical seeds; ``namer`` is injected for tests (the
    real job passes :func:`default_namer`). Deterministic and offline.
    """
    canon_tag = _tag_canonical_map(identity)

    # ── Collect asset<->asset structural signals + tag/agent hub memberships. ───
    asset_type: dict[str, str] = {}
    for nd in signal_graph.get("nodes", []):
        if _is_asset(nd["id"]):
            asset_type[_fqn_of(nd["id"])] = nd.get("kind", "table")

    struct: dict[str, list[tuple[str, str, float]]] = {k: [] for k in STRUCTURAL_KINDS}
    tag_members: dict[str, set[str]] = {}     # canonical tag_key -> seeded asset fqns
    agent_members: dict[str, set[str]] = {}
    for e in signal_graph.get("edges", []):
        kind = e.get("kind", "")
        a = _canon_node(e["src"], canon_tag)
        b = _canon_node(e["dst"], canon_tag)
        w = float(e.get("weight", 1.0) or 1.0)
        if kind in STRUCTURAL_KINDS and _is_asset(a) and _is_asset(b):
            struct[kind].append((_fqn_of(a), _fqn_of(b), w))
        elif kind == "tag_assignment" and _is_tag(a) and _is_asset(b):
            tag_members.setdefault(_fqn_of(a), set()).add(_fqn_of(b))
        elif kind == "agent_scope" and _is_asset(b):
            agent_members.setdefault(_fqn_of(a), set()).add(_fqn_of(b))

    # Asset vertex universe (everything the layers or hubs touch).
    assets: set[str] = set(asset_type)
    for pairs in struct.values():
        for a, b, _w in pairs:
            assets.add(a)
            assets.add(b)
    for m in tag_members.values():
        assets |= m
    for m in agent_members.values():
        assets |= m
    vertices = sorted(assets)
    if not vertices:
        return []

    lineage_pairs = struct["lineage_adjacency"]
    coquery_pairs = struct["co_query"]

    # ── Layers: structural direct + priors as projected cliques (asset-only). ───
    coarse_layers: dict[str, list[tuple[str, str, float]]] = {
        k: v for k, v in struct.items() if v
    }
    tag_clique = _clique_edges(list(tag_members.values()))
    agent_clique = _clique_edges(list(agent_members.values()))
    if tag_clique:
        coarse_layers["tag_assignment"] = tag_clique
    if agent_clique:
        coarse_layers["agent_scope"] = agent_clique

    # ── Soft seed: union tag/agent cliques; assets with no hub fall back to their
    # catalog.schema prefix. Contiguous re-index -> a valid partition. ──────────
    uf = _UF(vertices)
    for grp in list(tag_members.values()) + list(agent_members.values()):
        ms = sorted(grp)
        for other in ms[1:]:
            uf.union(ms[0], other)
    seeded = {v for v in vertices if uf.find(v) != v}
    by_prefix: dict[str, list[str]] = {}
    for v in vertices:
        if v not in seeded:
            by_prefix.setdefault(_schema_prefix(v), []).append(v)
    for grp in by_prefix.values():
        for other in grp[1:]:
            uf.union(grp[0], other)
    roots = sorted({uf.find(v) for v in vertices})
    root_idx = {r: i for i, r in enumerate(roots)}
    seed_membership = [root_idx[uf.find(v)] for v in vertices]

    # ── Coarse pass -> Domains. ─────────────────────────────────────────────────
    coarse_mem = _run_multiplex(vertices, coarse_layers, gamma=gamma_coarse, seed_membership=seed_membership)
    domains = _communities(vertices, coarse_mem)
    domain_sets = [set(c) for c in domains]

    domain_level_tags = [t for t in tag_members if "/" not in t]
    domain_binds = _bind_level(domain_sets, domain_level_tags, tag_members)

    proposals: list[DomainProposal] = []
    for community, bind in zip(domains, domain_binds):
        dom = _make_proposal(
            community, parent_id=None, bind=bind, tag_members=tag_members,
            agent_members=agent_members, lineage_pairs=lineage_pairs,
            coquery_pairs=coquery_pairs, namer=namer, company=company,
        )
        proposals.append(dom)

        # ── Recursive split -> Sub-Domains (STRUCTURAL layers only, finer γ). ────
        sset = set(community)
        sub_layers = {
            k: [(a, b, w) for (a, b, w) in struct[k] if a in sset and b in sset]
            for k in STRUCTURAL_KINDS
        }
        sub_layers = {k: v for k, v in sub_layers.items() if v}
        if len(community) > 1 and sub_layers:
            sub_mem = _run_multiplex(community, sub_layers, gamma=gamma_fine, seed_membership=None)
            subs = _communities(community, sub_mem)
        else:
            subs = [community]
        if len(subs) <= 1:
            continue  # no finer structure -> the Domain holds its members directly

        sub_sets = [set(c) for c in subs]
        sub_level_tags = [t for t in tag_members if "/" in t and (tag_members[t] & sset)]
        sub_binds = _bind_level(sub_sets, sub_level_tags, tag_members)
        for sub, sbind in zip(subs, sub_binds):
            proposals.append(_make_proposal(
                sub, parent_id=dom.domain_id, bind=sbind, tag_members=tag_members,
                agent_members=agent_members, lineage_pairs=lineage_pairs,
                coquery_pairs=coquery_pairs, namer=namer, company=company,
            ))
    return proposals


def _make_proposal(
    assets: list[str],
    *,
    parent_id: str | None,
    bind: _Bind | None,
    tag_members: dict[str, set[str]],
    agent_members: dict[str, set[str]],
    lineage_pairs: list[tuple[str, str, float]],
    coquery_pairs: list[tuple[str, str, float]],
    namer: Namer | None,
    company: str | None,
) -> DomainProposal:
    """Build one Domain/Sub-Domain proposal from its asset set + the level's tag
    binding, attaching evidence (MV-D35)."""
    members = tuple(sorted(assets))
    s = set(members)
    is_sub = parent_id is not None

    tag_decision: Literal["reuse", "create", "reassign"] = "create"
    tag_key: str | None = None
    tag_value: str | None = None
    conflict: dict | None = None

    if bind is not None:
        tag_key = bind.tag_key
        tag_value = bind.tag_key.split("/", 1)[1] if "/" in bind.tag_key else bind.tag_key
        if bind.moved and bind.margin > REASSIGN_MARGIN:
            tag_decision = "reassign"
            conflict = {
                "existing_tag": bind.tag_key,
                "moved_members": list(bind.moved),
                "margin": round(bind.margin, 4),
            }
        else:
            tag_decision = "reuse"

    community_lineage = [(a, b, w) for (a, b, w) in lineage_pairs if a in s and b in s]
    anchor_fqn = _anchor(list(members), community_lineage)
    name = _cluster_name(
        bound_tag_value=tag_value if tag_decision in ("reuse", "reassign") else None,
        namer=namer, anchor_fqn=anchor_fqn, assets=list(members), company=company,
    )
    if tag_decision == "create":
        tag_value = name
        tag_key = name  # a domain proposes a top-level key; a sub is qualified later

    spine = sorted({a for a, b, _w in community_lineage} | {b for a, b, _w in community_lineage})
    internal_coquery = sum(1 for (a, b, _w) in coquery_pairs if a in s and b in s)
    seed_agents = sorted(ag for ag, mem in agent_members.items() if mem & s)
    level_tag_prior = sorted(
        t for t in tag_members if ("/" in t) == is_sub and (tag_members[t] & s)
    )
    evidence: dict[str, Any] = {
        "anchor": anchor_fqn,
        "shared_spine": spine,
        "co_query_count": internal_coquery,
        "tag_prior": level_tag_prior,
        "seed": seed_agents,
    }
    if conflict is not None:
        evidence["conflict"] = conflict

    return DomainProposal(
        domain_id=domain_id_of(members),
        parent_id=parent_id,
        name=name,
        description=_describe(name, tag_decision, anchor_fqn, len(members)),
        tag_decision=tag_decision,
        tag_key=tag_key,
        tag_value=tag_value,
        evidence=evidence,
        members=members,
    )


def _describe(name: str, decision: str, anchor: str | None, n: int) -> str:
    verb = {"reuse": "reuses an existing governed tag", "create": "proposes a new governed tag",
            "reassign": "flags a governed-tag disagreement for review"}[decision]
    tail = f" anchored on `{anchor}`" if anchor else ""
    return f"'{name}' — {n} asset(s){tail}; {verb}."


def qualify_subdomain_keys(proposals: list[DomainProposal]) -> list[DomainProposal]:
    """For CREATE sub-domains, qualify the proposed key into the parent's
    ``Domain/Sub`` convention (the parent's key + `/` + the sub value). Reuse/reassign
    subs already carry the governed `Domain/Sub` key. Pure; returns new proposals."""
    by_id = {p.domain_id: p for p in proposals}
    out: list[DomainProposal] = []
    for p in proposals:
        if p.parent_id and p.tag_decision == "create" and p.tag_key and "/" not in p.tag_key:
            parent = by_id.get(p.parent_id)
            parent_key = (parent.tag_key or parent.name) if parent else ""
            qualified = f"{parent_key}/{p.tag_value or p.name}"
            out.append(DomainProposal(
                p.domain_id, p.parent_id, p.name, p.description, p.tag_decision,
                qualified, p.tag_value or p.name, p.evidence, p.members,
            ))
        else:
            out.append(p)
    return out
