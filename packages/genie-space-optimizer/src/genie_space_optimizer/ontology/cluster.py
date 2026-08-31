"""L4 domain / sub-domain clustering engine (Phase 3b).

Turns 17d's fused signal graph (``graph.build_signal_graph``) + the 17d identity
map into a two-level Domain -> Sub-Domain tree of :class:`DomainProposal`s, each
bound to a REUSE / CREATE / REASSIGN governed-tag decision (a *proposal field* —
never a UC write). Written by ``materialize.py`` into ``genie_ont_domains`` +
``genie_ont_members``.

The building blocks (architecture §5, MV-D39 + Stage-1 MV-D51/52/53):

  0. **Facet routing (MV-D51).** Before a governed tag can seed or bind a Domain it
     is classified (``transforms.classify_tag``): FACET tags (sensitivity / tier /
     quality / lifecycle / synthetic / certification / demo / team …) are dropped from
     the tag priors entirely — a Domain names WHAT DATA IS ABOUT, not an attribute OF it.
  1. **Rules-first Domain grouping (MV-D53).** Assets group DETERMINISTICALLY where a
     strong signal is decisive, in precedence: curated domain tag (an aboutness tag that
     acts as a governance domain) → FK-connected component (``join_key``) → metric-view
     membership → shared schema (edgeless assets only, so structurally-connected assets
     stay for the clusterer). Each rule stamps a plain ``reason`` into ``evidence``.
  2. **Leiden on the REMAINDER only (MV-D53, MV-D39 retained).** Assets no rule
     resolves fall through to soft-seeded multiplex Leiden-CPM over ``python-igraph``:
     each edge kind is its own layer (per-layer weight; ``tag_assignment`` is demoted to
     CORROBORATION weight, MV-D52), governed tags + Agent scopes seed via
     ``initial_membership`` (SOFT — ``is_membership_fixed`` is never set, so strong graph
     evidence can still move a mis-seeded node → a ``reassign`` proposal), catalog/schema
     is the thin-signal fallback prior. A leftover community held together ONLY by a tag
     prior (no structural edge) is dropped — a tag NEVER solo-creates a Domain (MV-D52).
  3. **Sub-Domains from an EXPLICIT boundary (Stage 2, MV-D54).** Per Domain, derive
     sub-domains from an explicit boundary FIRST, in precedence: governed slash
     sub-tags (``Domain/Sub``) → a value-carrying tag's distinct values
     (``mvm_subdomain=fare_pricing``) → schema-within-domain → MV/FK component. The
     finer CPM-``γ`` Leiden split over the STRUCTURAL layers is the FALLBACK ONLY when
     no explicit boundary exists (17e ran it unconditionally). Each sub-domain stamps
     its boundary reason into ``evidence``.
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

from genie_space_optimizer.ontology import er, transforms

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
    "join_key": 5.0,            # decisive structural signal (FK) — Stage 1 populates it
    "tag_assignment": 2.5,      # CORROBORATION, not seed (MV-D52): demoted 5.0 → lineage/2
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


# ── Rules-first Domain grouping (MV-D53) ────────────────────────────────────


def _components(vertices: Sequence[str], edges: Sequence[tuple]) -> list[set[str]]:
    """Connected components over undirected asset↔asset ``edges`` (union-find)."""
    verts = list(vertices)
    idx = set(verts)
    uf = _UF(verts)
    for e in edges:
        a, b = e[0], e[1]
        if a in idx and b in idx:
            uf.union(a, b)
    comps: dict[str, set[str]] = {}
    for v in verts:
        comps.setdefault(uf.find(v), set()).add(v)
    return list(comps.values())


def _structural_asset_set(struct: dict[str, list[tuple[str, str, float]]]) -> set[str]:
    """Assets that touch at least one STRUCTURAL asset↔asset edge (so the shared-schema
    rule can leave them for the clusterer instead of pre-binding them by schema)."""
    out: set[str] = set()
    for kind in STRUCTURAL_KINDS:
        for a, b, _w in struct.get(kind, []):
            out.add(a)
            out.add(b)
    return out


def _rules_first_partition(
    vertices: Sequence[str],
    *,
    struct: dict[str, list[tuple[str, str, float]]],
    tag_members: dict[str, set[str]],
    agent_members: dict[str, set[str]],
    mv_members: dict[str, set[str]],
    schema_members: dict[str, set[str]],
    curated_domain_keys: set[str],
) -> tuple[list[tuple[frozenset[str], str]], set[str]]:
    """Group assets DETERMINISTICALLY where a signal is decisive (MV-D53), in
    precedence: curated domain tag → FK-connected component → metric-view membership →
    shared schema (edgeless assets only). Each group carries a plain reason. Returns
    ``(rule_groups, remainder)`` — the remainder falls through to Leiden."""
    resolved: set[str] = set()
    groups: list[tuple[set[str], str]] = []

    def emit(members: set[str], reason: str, *, min_size: int) -> None:
        ms = {m for m in members if m not in resolved}
        if len(ms) < min_size:
            return
        groups.append((ms, reason))
        resolved.update(ms)

    # R1 — curated domain tag (authoritative): the tag's own members + its sub-tags'.
    for dkey in sorted(curated_domain_keys):
        members = set(tag_members.get(dkey, set()))
        for sk, mem in tag_members.items():
            if sk.startswith(dkey + "/"):
                members |= mem
        emit(members, f"grouped by curated domain tag: {dkey}", min_size=1)

    # R2 — FK-connected component (+ shared-join proxy): components over join_key edges.
    jk = struct.get("join_key", [])
    if jk:
        remaining = [v for v in vertices if v not in resolved]
        for comp in sorted(_components(remaining, jk), key=lambda c: (-len(c), sorted(c)[0] if c else "")):
            emit(comp, "grouped by foreign key / shared join column", min_size=2)

    # R3 — metric-view membership: an MV's source tables form a curated group.
    for mv, mem in sorted(mv_members.items()):
        emit(set(mem), f"grouped by metric view: {mv}", min_size=2)

    # R4 — shared schema: EDGELESS assets only (structurally-connected assets stay for
    # the clusterer so the soft-seed reassignment path is preserved).
    structural_assets = _structural_asset_set(struct)
    for sk, mem in sorted(schema_members.items()):
        emit({m for m in mem if m not in structural_assets}, f"grouped by shared schema: {sk}", min_size=2)

    remainder = {v for v in vertices if v not in resolved}
    return [(frozenset(m), r) for (m, r) in groups], remainder


def _leiden_communities(
    subset: set[str],
    *,
    struct: dict[str, list[tuple[str, str, float]]],
    tag_members: dict[str, set[str]],
    agent_members: dict[str, set[str]],
    gamma: float,
) -> list[set[str]]:
    """Soft-seeded multiplex Leiden over the REMAINDER only (MV-D53). Structural layers
    direct + tag/agent priors projected to cliques (restricted to ``subset``); the
    thin-signal fallback prior is the catalog.schema prefix. Mirrors the shipped 17e
    coarse pass, now scoped to the assets no rule resolved."""
    verts = sorted(subset)
    if not verts:
        return []

    coarse_layers: dict[str, list[tuple[str, str, float]]] = {}
    for kind in STRUCTURAL_KINDS:
        pairs = [(a, b, w) for (a, b, w) in struct.get(kind, []) if a in subset and b in subset]
        if pairs:
            coarse_layers[kind] = pairs
    tag_clique = _clique_edges([{a for a in mem if a in subset} for mem in tag_members.values()])
    agent_clique = _clique_edges([{a for a in mem if a in subset} for mem in agent_members.values()])
    if tag_clique:
        coarse_layers["tag_assignment"] = tag_clique
    if agent_clique:
        coarse_layers["agent_scope"] = agent_clique

    uf = _UF(verts)
    for grp in list(tag_members.values()) + list(agent_members.values()):
        ms = sorted(a for a in grp if a in subset)
        for other in ms[1:]:
            uf.union(ms[0], other)
    seeded = {v for v in verts if uf.find(v) != v}
    by_prefix: dict[str, list[str]] = {}
    for v in verts:
        if v not in seeded:
            by_prefix.setdefault(_schema_prefix(v), []).append(v)
    for grp in by_prefix.values():
        for other in grp[1:]:
            uf.union(grp[0], other)
    roots = sorted({uf.find(v) for v in verts})
    root_idx = {r: i for i, r in enumerate(roots)}
    seed_membership = [root_idx[uf.find(v)] for v in verts]

    mem = _run_multiplex(verts, coarse_layers, gamma=gamma, seed_membership=seed_membership)
    return [set(c) for c in _communities(verts, mem)]


def _has_internal_structure(community: set[str], struct: dict[str, list[tuple[str, str, float]]]) -> bool:
    """True iff the community holds ≥1 internal STRUCTURAL asset↔asset edge — i.e. it is
    NOT held together only by a tag/agent prior. A tag never solo-creates a Domain
    (MV-D52), so a Leiden community with no internal structure is dropped."""
    for kind in STRUCTURAL_KINDS:
        for a, b, _w in struct.get(kind, []):
            if a in community and b in community:
                return True
    return False


# ── Stage 2: sub-domains from an EXPLICIT boundary (MV-D54) ─────────────────

# One derived sub-domain: (sorted members, boundary reason, governed slash bind or
# None, value (tag_key, tag_value) binding or None). At most one of bind/value_binding
# is set; a plain create sub carries neither.
_SubGroup = tuple[list[str], str, "_Bind | None", "tuple[str, str] | None"]


def _sorted_subgroups(groups: list[_SubGroup]) -> list[_SubGroup]:
    """Deterministic order (largest first, then first sorted member) so sub-domain
    ids/binding are stable across runs."""
    return sorted(groups, key=lambda g: (-len(g[0]), g[0][0] if g[0] else ""))


def _derive_subdomains(
    community: list[str],
    *,
    domain_tag_key: str | None,
    struct: dict[str, list[tuple[str, str, float]]],
    tag_members: dict[str, set[str]],
    tag_values: dict[str, dict[str, str]],
    mv_members: dict[str, set[str]],
) -> list[_SubGroup] | None:
    """Derive a Domain's sub-domains from an EXPLICIT boundary (Stage 2, MV-D54), in
    precedence: (1) governed slash sub-tags (``Domain/Sub``) → (2) a value-carrying
    tag's distinct values (``mvm_subdomain=fare_pricing``) → (3) schema-within-domain
    → (4) MV / FK component. Returns the sub-group descriptors for the FIRST rule that
    finds a boundary, or ``None`` when NONE apply (the caller then falls back to the
    finer Leiden split). Assets no sub-group covers stay direct members of the parent
    Domain (the parent always lists its full member set)."""
    sset = set(community)

    # (1) Governed slash sub-tags whose parent IS this Domain's tag (bind via the
    # global per-level ownership pass so two subs never claim the same governed tag).
    if domain_tag_key:
        slash = sorted(
            t for t in tag_members
            if "/" in t and t.split("/", 1)[0] == domain_tag_key and (tag_members[t] & sset)
        )
        if slash:
            sub_sets = [tag_members[t] & sset for t in slash]
            binds = _bind_level(sub_sets, slash, tag_members)
            return _sorted_subgroups(
                [(sorted(sub), f"sub-tag: {t}", bind, None)
                 for t, sub, bind in zip(slash, sub_sets, binds)]
            )

    # (2) A value-carrying tag: one sub-domain per distinct governed value (≥2 values
    # over the Domain's assets to be a boundary). A reuse of the tag with that value.
    for tk in sorted(tag_values):
        if tk == domain_tag_key:
            continue
        by_value: dict[str, set[str]] = {}
        for fqn, val in tag_values[tk].items():
            if fqn in sset:
                by_value.setdefault(val, set()).add(fqn)
        if len(by_value) >= 2:
            return _sorted_subgroups(
                [(sorted(mem), f"{tk}={val}", None, (tk, val)) for val, mem in by_value.items()]
            )

    # (3) Schema-within-domain: group the Domain's assets by schema; a multi-schema
    # Domain splits one sub-domain per schema.
    by_schema: dict[str, set[str]] = {}
    for fqn in community:
        by_schema.setdefault(_schema_prefix(fqn), set()).add(fqn)
    if len(by_schema) >= 2:
        return _sorted_subgroups(
            [(sorted(mem), f"schema: {sk}", None, None) for sk, mem in by_schema.items()]
        )

    # (4) MV / FK component within the Domain: ≥2 FK components (or ≥2 MV source sets).
    jk = [(a, b, w) for (a, b, w) in struct.get("join_key", []) if a in sset and b in sset]
    fk_comps = [c for c in _components(sorted(sset), jk) if len(c) >= 2] if jk else []
    if len(fk_comps) >= 2:
        return _sorted_subgroups(
            [(sorted(c), "foreign-key component within domain", None, None) for c in fk_comps]
        )
    mv_groups = [(mv, mem & sset) for mv, mem in sorted(mv_members.items()) if len(mem & sset) >= 2]
    if len(mv_groups) >= 2:
        return _sorted_subgroups(
            [(sorted(mem), f"metric view: {mv}", None, None) for mv, mem in mv_groups]
        )

    return None


def cluster(
    signal_graph: dict[str, Any],
    *,
    identity: Sequence[Any] | None = None,
    company: str | None = None,
    namer: Namer | None = None,
    facet_tiebreaker: Callable[[str], bool | None] | None = None,
    gamma_coarse: float = GAMMA_COARSE,
    gamma_fine: float = GAMMA_FINE,
) -> list[DomainProposal]:
    """Cluster the fused signal graph into a Domain -> Sub-Domain proposal tree.

    ``identity`` is 17d's ER verdicts (duck-typed ``.verdict``/``.members``) used to
    collapse duplicate tags to canonical seeds; ``namer`` is injected for tests (the
    real job passes :func:`default_namer`). ``facet_tiebreaker`` is an optional injected
    facet/aboutness resolver for genuinely ambiguous tag names (degrades, MV-D43).
    Rules-first (MV-D53): decisive signals group deterministically, Leiden handles the
    remainder. Deterministic and offline.
    """
    canon_tag = _tag_canonical_map(identity)

    # ── Collect asset<->asset structural signals + tag/agent/MV/schema memberships. ─
    asset_type: dict[str, str] = {}
    for nd in signal_graph.get("nodes", []):
        if _is_asset(nd["id"]):
            asset_type[_fqn_of(nd["id"])] = nd.get("kind", "table")

    struct: dict[str, list[tuple[str, str, float]]] = {k: [] for k in STRUCTURAL_KINDS}
    tag_members: dict[str, set[str]] = {}     # canonical tag_key -> seeded asset fqns
    # Per-asset governed VALUE of a value-carrying tag (Stage 2, MV-D54): tag_key ->
    # {asset_fqn -> tag_value}. A tag whose assignments carry distinct values
    # (``mvm_subdomain=fare_pricing`` ...) names sub-domains by value.
    tag_values: dict[str, dict[str, str]] = {}
    agent_members: dict[str, set[str]] = {}
    mv_members: dict[str, set[str]] = {}      # metric-view fqn -> source asset fqns (R3)
    schema_members: dict[str, set[str]] = {}  # schema key -> asset fqns (R4)
    for e in signal_graph.get("edges", []):
        kind = e.get("kind", "")
        a = _canon_node(e["src"], canon_tag)
        b = _canon_node(e["dst"], canon_tag)
        w = float(e.get("weight", 1.0) or 1.0)
        if kind in STRUCTURAL_KINDS and _is_asset(a) and _is_asset(b):
            struct[kind].append((_fqn_of(a), _fqn_of(b), w))
        elif kind == "tag_assignment" and _is_tag(a) and _is_asset(b):
            tag_members.setdefault(_fqn_of(a), set()).add(_fqn_of(b))
            tv = e.get("tag_value")
            if tv is not None and str(tv) != "":
                tag_values.setdefault(_fqn_of(a), {})[_fqn_of(b)] = str(tv)
        elif kind == "agent_scope" and _is_asset(b):
            agent_members.setdefault(_fqn_of(a), set()).add(_fqn_of(b))
        elif kind == "mv_membership" and _is_asset(b):
            mv_members.setdefault(_fqn_of(a), set()).add(_fqn_of(b))
        elif kind == "schema_affinity" and _is_asset(b):
            schema_members.setdefault(_fqn_of(a), set()).add(_fqn_of(b))

    # Facet routing (MV-D51): a FACET tag names an attribute OF data, not a business
    # area — drop it from the tag priors so it neither seeds nor binds a Domain.
    tag_members = {
        k: m for k, m in tag_members.items()
        if not transforms.is_facet_tag(k, tiebreaker=facet_tiebreaker)
    }
    # A facet tag never names a sub-domain either (MV-D51) — drop its per-asset values.
    tag_values = {k: v for k, v in tag_values.items() if k in tag_members}
    # A curated domain tag = an aboutness top-level tag that acts as a governance domain
    # (it has ≥1 sub-tag under the Domain/Sub convention) → the R1 authoritative rule.
    curated_domain_keys = {t.split("/", 1)[0] for t in tag_members if "/" in t}

    # Asset vertex universe (everything the layers, hubs, or rules touch).
    assets: set[str] = set(asset_type)
    for pairs in struct.values():
        for a, b, _w in pairs:
            assets.add(a)
            assets.add(b)
    for grp in list(tag_members.values()) + list(agent_members.values()) \
            + list(mv_members.values()) + list(schema_members.values()):
        assets |= grp
    vertices = sorted(assets)
    if not vertices:
        return []

    lineage_pairs = struct["lineage_adjacency"]
    coquery_pairs = struct["co_query"]

    # ── Rules-first Domain grouping (MV-D53); Leiden on the remainder only. ──────
    rule_groups, remainder = _rules_first_partition(
        vertices, struct=struct, tag_members=tag_members, agent_members=agent_members,
        mv_members=mv_members, schema_members=schema_members,
        curated_domain_keys=curated_domain_keys,
    )
    domains_with_reason: list[tuple[list[str], str]] = [
        (sorted(members), reason) for members, reason in rule_groups
    ]
    for comm in _leiden_communities(
        remainder, struct=struct, tag_members=tag_members,
        agent_members=agent_members, gamma=gamma_coarse,
    ):
        # A leftover community with no internal structure is tag-only — a tag never
        # solo-creates a Domain (MV-D52), so it is dropped rather than surfaced.
        if _has_internal_structure(comm, struct):
            domains_with_reason.append((sorted(comm), "grouped by graph community detection"))
    # Deterministic order (largest first, then first sorted member) so downstream
    # binding + ids are stable across runs.
    domains_with_reason.sort(key=lambda mr: (-len(mr[0]), mr[0][0] if mr[0] else ""))

    domain_sets = [set(m) for m, _ in domains_with_reason]
    domain_level_tags = [t for t in tag_members if "/" not in t]
    domain_binds = _bind_level(domain_sets, domain_level_tags, tag_members)

    proposals: list[DomainProposal] = []
    for (community, reason), bind in zip(domains_with_reason, domain_binds):
        dom = _make_proposal(
            community, parent_id=None, bind=bind, reason=reason, tag_members=tag_members,
            agent_members=agent_members, lineage_pairs=lineage_pairs,
            coquery_pairs=coquery_pairs, namer=namer, company=company,
        )
        proposals.append(dom)

        # ── Sub-domains from an EXPLICIT boundary first (Stage 2, MV-D54); the finer
        # Leiden split is the FALLBACK ONLY when no explicit boundary exists. ────────
        explicit = _derive_subdomains(
            community,
            domain_tag_key=dom.tag_key if dom.tag_decision in ("reuse", "reassign") else None,
            struct=struct, tag_members=tag_members, tag_values=tag_values, mv_members=mv_members,
        )
        if explicit is not None:
            # Rules (2)-(4) only return when they find ≥2 groups; rule (1) may name a
            # single governed sub-domain (the rest stays direct in the parent Domain).
            for members, reason, sbind, vbind in explicit:
                proposals.append(_make_proposal(
                    members, parent_id=dom.domain_id, bind=sbind, reason=reason,
                    tag_members=tag_members, agent_members=agent_members, lineage_pairs=lineage_pairs,
                    coquery_pairs=coquery_pairs, namer=namer, company=company, value_binding=vbind,
                ))
            continue

        # Fallback: the finer Leiden split over STRUCTURAL layers (finer γ, fixed seed).
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
                sub, parent_id=dom.domain_id, bind=sbind, reason="split by structure",
                tag_members=tag_members, agent_members=agent_members, lineage_pairs=lineage_pairs,
                coquery_pairs=coquery_pairs, namer=namer, company=company,
            ))
    return proposals


def _make_proposal(
    assets: list[str],
    *,
    parent_id: str | None,
    bind: _Bind | None,
    reason: str,
    tag_members: dict[str, set[str]],
    agent_members: dict[str, set[str]],
    lineage_pairs: list[tuple[str, str, float]],
    coquery_pairs: list[tuple[str, str, float]],
    namer: Namer | None,
    company: str | None,
    value_binding: tuple[str, str] | None = None,
) -> DomainProposal:
    """Build one Domain/Sub-Domain proposal from its asset set + the level's tag
    binding, attaching evidence (MV-D35). ``reason`` is the plain grouping explanation
    (MV-D53) stamped into ``evidence`` so every assignment shows WHY it grouped.
    ``value_binding`` (Stage 2, MV-D54) names a sub-domain from a value-carrying
    tag's ``(tag_key, tag_value)`` — a reuse of that governed tag+value; it takes
    precedence over ``bind`` and the LLM namer (the governed value IS the name)."""
    members = tuple(sorted(assets))
    s = set(members)
    is_sub = parent_id is not None

    tag_decision: Literal["reuse", "create", "reassign"] = "create"
    tag_key: str | None = None
    tag_value: str | None = None
    conflict: dict | None = None

    if value_binding is not None:
        tag_key, tag_value = value_binding
        tag_decision = "reuse"
    elif bind is not None:
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
        "reason": reason,
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
