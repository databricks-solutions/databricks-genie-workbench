"""Industry-reference alignment (MV-D58, §9) — the naming/description/hypothesis layer
that hybrid-matches the estate's DISCOVERED domains against a Vibe **industry reference
model** and emits TYPED correspondences + gap-domain hypotheses.

**The one rule (MV-D58 "align, don't conform").** Membership comes from the graph and is
fixed before this runs. Alignment never reshapes the estate — it only *names*,
*describes*, and *hypothesizes*, as a **T2 industry-canonical** prior. Every leaf it emits
is provenance-gated (:class:`~genie_space_optimizer.ontology.context_pack.Provenanced`);
the ``rank.py`` provenance ladder decides whether any of it applies, and a T2/T3 name
**never** outranks a T0/curated fact (MV-D38/D35). This module writes NOTHING and touches
no structure — it returns an :class:`Alignment` the caller (``materialize.py``) folds into
``rank.apply_alignment`` + the run report.

**Four ordered passes (deterministic; fixed seed anchors — same estate + same reference ⇒
same output):**

1. **string** — discovered domain name/member-stems vs reference name + synonyms
   (:func:`~genie_space_optimizer.ontology.similarity.keyword_score` + token Jaccard).
2. **embedding-seed-anchor** — for domains string didn't catch, cosine of the discovered
   surface text against each reference domain's **fixed seed anchors** (via an injected
   ``embedder``; absent ⇒ this pass is skipped, MV-D43). Catches renamed-but-equivalent.
3. **structural propagation** — a matched anchor pulls its FK/join neighbours: an unmatched
   estate domain adjacent to a matched one takes the reference-neighbour it minimally
   overlaps, typed ``derived``.
4. **semantic sanity** — a match that is lexically/embedding-close but structurally
   incoherent (pure embedding, no shared token, no structural support) is REJECTED and
   recorded as an explicit ``not-equivalent`` correspondence, never a silent drop.

Every emitted correspondence is exactly one of ``exact`` / ``narrower`` / ``broader`` /
``derived`` / ``not-equivalent`` (MV-D60 map-not-merge). Reference domains with no coherent
correspondence surface as **gap hypotheses** — ranked BELOW every graph-backed proposal and
NEVER auto-created (MV-D38). The :attr:`Alignment.aligned_reference` is emitted in the shape
the §10 eval harness (``eval_harness.compute_precision_recall_f1``) reads.

Degrade-not-hang (MV-D43): a missing / empty reference model, no surfaced domains, or a
failing embedder ⇒ an empty :class:`Alignment` (no leaves, no hypotheses) and the run
completes. Off by default (MV-D44): the caller only runs this when the ``industry_alignment``
tier is enabled and a model loads.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.ontology import reference_models, similarity
from genie_space_optimizer.ontology.context_pack import Provenanced

logger = logging.getLogger(__name__)

# The five typed relations (MV-D60). Every emitted correspondence carries exactly one.
RELATIONS: frozenset[str] = frozenset({"exact", "narrower", "broader", "derived", "not-equivalent"})

# ── Fixed matching thresholds (module constants ⇒ deterministic; same estate + same
# reference ⇒ same output). Tuned conservative: a suggestion layer must not over-claim. ──
MIN_STRING_SCORE = 0.60       # a string candidate needs at least this name/surface score
EXACT_STRING_FLOOR = 0.90     # at/above this (or a normalized name/synonym hit) ⇒ `exact`
MIN_EMBEDDING_COSINE = 0.62   # an embedding candidate needs at least this cosine
SANITY_STRING_FLOOR = 0.34    # below this AND pure-embedding AND no structural support ⇒ reject
MIN_PROP_OVERLAP = 1          # a propagated (derived) candidate needs ≥ this token overlap

# Tokenizer stopwords — the same business-noise words the rank naming prior strips, so a
# reference name like "Revenue" matches a "Revenue Data Domain" cluster.
_STOPWORDS: frozenset[str] = frozenset(
    {"the", "and", "of", "for", "data", "domain", "business", "core", "team", "group", "a", "an"}
)
_NONWORD = re.compile(r"[^0-9a-z]+")


def _tokens(text: str) -> set[str]:
    """Meaningful lowercase tokens (len > 2, non-stopword)."""
    return {t for t in _NONWORD.split(str(text or "").lower()) if len(t) > 2 and t not in _STOPWORDS}


def _norm_name(text: str) -> str:
    """A normalized name key for exact-equality (lowercase, collapse non-word to space)."""
    return " ".join(t for t in _NONWORD.split(str(text or "").lower()) if t)


def _slug(name: str) -> str:
    """A stable reference-domain id for the harness (lowercase, non-word → underscore)."""
    return "_".join(t for t in _NONWORD.split(str(name or "").lower()) if t) or "ref"


# ── Reference model (the T2 industry-canonical prior) ────────────────────────


@dataclass(frozen=True)
class ReferenceDomain:
    """One reference-model domain: vocabulary (name/synonyms/seed_anchors) + structure
    (neighbors — the reference FK/join adjacency the propagation pass walks)."""

    name: str
    synonyms: tuple[str, ...] = ()
    description: str = ""
    seed_anchors: tuple[str, ...] = ()
    neighbors: tuple[str, ...] = ()

    @property
    def slug(self) -> str:
        return _slug(self.name)


@dataclass(frozen=True)
class ReferenceModel:
    """A loaded Vibe industry reference model — a T2 industry-canonical vocabulary + structure.
    Never a structural writer (MV-D58)."""

    model_id: str
    label: str
    source_url: str | None
    as_of: str
    tier: str
    domains: tuple[ReferenceDomain, ...]

    def is_empty(self) -> bool:
        return not self.domains


# A loader: ``(reference_model) -> model_dict | None``. Injectable so the job can point at
# the real ``lakehouse-industry-data-models`` repo/volume; the default reads bundled dicts.
ReferenceLoader = Callable[[str | None], "Mapping[str, Any] | None"]


def load_reference_model(
    reference_model: str | None, *, loader: ReferenceLoader | None = None,
) -> ReferenceModel | None:
    """Load the reference model for ``reference_model`` (id / alias / NAICS), or ``None``.

    ``loader`` (injected by the job for the real repo) takes precedence; the default reads
    the bundled Python models (:mod:`reference_models`). Returns ``None`` — the no-op degrade
    (MV-D43) — for a blank/unknown id, an unloadable model, or a model with zero usable
    domains. NEVER raises: any loader error degrades to ``None``."""
    if not str(reference_model or "").strip():
        return None
    load = loader or reference_models.bundled_model
    try:
        raw = load(reference_model)
    except Exception as e:  # noqa: BLE001 — a broken loader degrades to no alignment
        logger.info("industry-alignment reference load degraded (%s): %s", reference_model, e)
        return None
    if not isinstance(raw, Mapping):
        return None
    domains: list[ReferenceDomain] = []
    for d in raw.get("domains") or []:
        if not isinstance(d, Mapping):
            continue
        name = str(d.get("name") or "").strip()
        if not name:
            continue
        domains.append(
            ReferenceDomain(
                name=name,
                synonyms=tuple(str(s).strip() for s in (d.get("synonyms") or []) if str(s).strip()),
                description=str(d.get("description") or "").strip(),
                seed_anchors=tuple(str(s).strip() for s in (d.get("seed_anchors") or []) if str(s).strip()),
                neighbors=tuple(str(s).strip() for s in (d.get("neighbors") or []) if str(s).strip()),
            )
        )
    if not domains:
        return None
    # Deterministic domain order (by name) so passes + output are stable.
    domains.sort(key=lambda rd: rd.name.lower())
    return ReferenceModel(
        model_id=str(raw.get("model_id") or reference_models._norm(reference_model) or "reference"),
        label=str(raw.get("label") or raw.get("model_id") or "").strip(),
        source_url=(str(raw.get("source_url")).strip() or None) if raw.get("source_url") else None,
        as_of=str(raw.get("as_of") or "").strip(),
        tier=str(raw.get("tier") or "T2").strip() or "T2",
        domains=tuple(domains),
    )


# ── Correspondence + result types ────────────────────────────────────────────


@dataclass(frozen=True)
class Correspondence:
    """One discovered ↔ reference typed link. ``relation`` is exactly one of :data:`RELATIONS`.
    ``provenance`` is the :class:`Provenanced` leaf dict (tier / source_url / source_kind /
    as_of) so L6 ranking treats it like any pack fact."""

    discovered_domain_id: str
    discovered_name: str
    reference_name: str
    reference_id: str
    relation: str
    score: float
    match_pass: str  # "string" | "embedding" | "structural" | "sanity"
    provenance: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "discovered_domain_id": self.discovered_domain_id,
            "discovered_name": self.discovered_name,
            "reference_name": self.reference_name,
            "reference_id": self.reference_id,
            "relation": self.relation,
            "score": round(float(self.score), 6),
            "match_pass": self.match_pass,
            "provenance": self.provenance,
        }


@dataclass(frozen=True)
class Alignment:
    """The result of aligning one estate against one reference model. All lists are sorted
    (deterministic). ``correspondences`` includes every relation type (``not-equivalent``
    among them); ``aligned_reference`` is the §10 harness-shaped view (equivalent-family
    relations only — ``not-equivalent`` is excluded from the harness's match set)."""

    reference_model: str
    correspondences: list[Correspondence] = field(default_factory=list)
    gap_hypotheses: list[dict[str, Any]] = field(default_factory=list)
    aligned_reference: dict[str, Any] = field(default_factory=dict)

    def is_empty(self) -> bool:
        return not self.correspondences and not self.gap_hypotheses


def _empty_alignment(model_id: str) -> Alignment:
    return Alignment(reference_model=model_id, aligned_reference={
        "reference_model": model_id, "domains": [], "alignments": [], "gaps": [],
    })


# ── Discovered-domain surface (deterministic, from the estate — robust to naming) ───


def _member_stems(members: Sequence[str], *, cap: int = 24) -> set[str]:
    """Schema/table stems of a domain's member FQNs (skip catalog) — the business-sense
    tokens. Bounded so a huge domain stays cheap + stable."""
    toks: set[str] = set()
    for m in sorted({str(x) for x in members})[:cap]:
        for part in str(m).split(".")[1:]:  # skip catalog; schema + table carry the sense
            toks |= _tokens(part)
    return toks


@dataclass(frozen=True)
class _Discovered:
    domain_id: str
    name: str
    parent_id: str | None
    name_norm: str
    tokens: frozenset[str]   # name tokens + member stems (the match surface)
    text: str                # embedding surface text (name + stems)


def _discovered_domains(
    domain_rows: Sequence[Mapping[str, Any]],
    members_by_domain: Mapping[str, Sequence[str]],
    *, load_evidence: Callable[[Mapping[str, Any]], Mapping[str, Any]],
) -> list[_Discovered]:
    """The surfaced discovered domains to align — deterministic (sorted by domain_id). A
    domain the gate did not surface (below-bar / diffuse / dismissed) is skipped, matching
    the naming prior's targeting: alignment names/corroborates only real domains."""
    out: list[_Discovered] = []
    for row in domain_rows:
        ev = load_evidence(row)
        if not ev.get("surfaced"):
            continue
        did = str(row.get("domain_id") or "")
        if not did:
            continue
        name = str(row.get("name") or "")
        stems = _member_stems(members_by_domain.get(did, ()))
        name_toks = _tokens(name)
        text = " ".join([name] + sorted(stems))
        out.append(_Discovered(
            domain_id=did, name=name, parent_id=(str(row.get("parent_id")) or None) if row.get("parent_id") else None,
            name_norm=_norm_name(name), tokens=frozenset(name_toks | stems), text=text.strip() or name,
        ))
    out.sort(key=lambda d: d.domain_id)
    return out


# ── The passes ───────────────────────────────────────────────────────────────


@dataclass
class _Cand:
    """A working candidate discovered→reference link before typing."""
    ref: ReferenceDomain
    string_score: float
    embed_cos: float
    overlap: int
    match_pass: str  # "string" | "embedding" | "structural"


# A discovered name fully containing a reference name (or vice-versa) is a strong match even
# when extra descriptor tokens dilute the Jaccard ("Aircraft Fleet" ⊇ "Fleet"). Scored just
# below the exact floor so it matches confidently without being mistyped as an exact identity.
_CONTAINMENT_SCORE = 0.85


def _string_score(d: _Discovered, rd: ReferenceDomain) -> tuple[float, int]:
    """String match score + token overlap of one discovered domain vs one reference domain.
    Score = max(best name/synonym keyword score, surface-token Jaccard, name-token containment)."""
    surface_names = [rd.name, *rd.synonyms]
    name_score = max((similarity.keyword_score(d.name, c) for c in surface_names), default=0.0)
    ref_toks = _tokens(rd.name)
    for s in rd.synonyms:
        ref_toks |= _tokens(s)
    overlap = len(d.tokens & ref_toks)
    union = len(d.tokens | ref_toks)
    jacc = (overlap / union) if union else 0.0
    # Name-token containment (either direction): a domain named "Aircraft Fleet" clearly
    # covers the reference "Fleet". Uses reference NAME tokens (not synonyms) so a single
    # incidental synonym token can't trigger it.
    ref_name_toks = _tokens(rd.name)
    d_name_toks = _tokens(d.name)
    contain = 0.0
    if ref_name_toks and ref_name_toks <= d.tokens:
        contain = _CONTAINMENT_SCORE
    elif d_name_toks and d_name_toks <= ref_toks:
        contain = _CONTAINMENT_SCORE
    return max(name_score, jacc, contain), overlap


def _is_exact(d: _Discovered, rd: ReferenceDomain, string_score: float) -> bool:
    """A normalized name/synonym hit, or a very high string score ⇒ exact."""
    if d.name_norm and d.name_norm in ({_norm_name(rd.name)} | {_norm_name(s) for s in rd.synonyms}):
        return True
    return string_score >= EXACT_STRING_FLOOR


def _embedding_candidates(
    discovered: Sequence[_Discovered], reference: ReferenceModel, embedder: Any,
) -> dict[str, dict[str, float]]:
    """``{domain_id: {ref_name: cosine}}`` from one batched embed of the discovered surface
    texts + each reference domain's fixed seed-anchor text. Degrades to ``{}`` on any embedder
    failure (MV-D43). Deterministic given the (deterministic) embedder."""
    if embedder is None:
        return {}
    ref_texts = [
        (rd.name, "; ".join([rd.name, *rd.seed_anchors]) or rd.name) for rd in reference.domains
    ]
    disc_texts = [d.text for d in discovered]
    try:
        vecs = embedder.embed([*disc_texts, *[t for _n, t in ref_texts]])
    except Exception as e:  # noqa: BLE001 — a dead embedder just skips the pass
        logger.info("industry-alignment embedding pass degraded: %s", e)
        return {}
    if not vecs or len(vecs) != len(disc_texts) + len(ref_texts):
        return {}
    disc_vecs = vecs[: len(disc_texts)]
    ref_vecs = vecs[len(disc_texts):]
    out: dict[str, dict[str, float]] = {}
    for d, dv in zip(discovered, disc_vecs):
        row: dict[str, float] = {}
        for (rname, _t), rv in zip(ref_texts, ref_vecs):
            row[rname] = round(similarity._cosine(dv, rv), 6)
        out[d.domain_id] = row
    return out


def _base_candidates(
    discovered: Sequence[_Discovered], reference: ReferenceModel, embedder: Any,
) -> dict[str, list[_Cand]]:
    """Passes (a)+(b): per discovered domain, the string + embedding candidates that clear
    their thresholds, strongest first. Deterministic."""
    embed = _embedding_candidates(discovered, reference, embedder)
    by_domain: dict[str, list[_Cand]] = {}
    for d in discovered:
        cands: list[_Cand] = []
        cos_by_ref = embed.get(d.domain_id, {})
        for rd in reference.domains:
            s_score, overlap = _string_score(d, rd)
            cos = float(cos_by_ref.get(rd.name, 0.0))
            if s_score >= MIN_STRING_SCORE:
                cands.append(_Cand(ref=rd, string_score=s_score, embed_cos=cos, overlap=overlap, match_pass="string"))
            elif cos >= MIN_EMBEDDING_COSINE:
                cands.append(_Cand(ref=rd, string_score=s_score, embed_cos=cos, overlap=overlap, match_pass="embedding"))
        # Strongest first: string beats embedding at a tie; then score; then name.
        cands.sort(key=lambda c: (0 if c.match_pass == "string" else 1, -_cand_score(c), c.ref.name.lower()))
        if cands:
            by_domain[d.domain_id] = cands
    return by_domain


def _cand_score(c: _Cand) -> float:
    return c.string_score if c.match_pass == "string" else c.embed_cos


def _structural_candidates(
    discovered: Sequence[_Discovered],
    reference: ReferenceModel,
    best_ref_by_domain: Mapping[str, ReferenceDomain],
    domain_adjacency: Mapping[str, Sequence[str]],
) -> dict[str, _Cand]:
    """Pass (c): a matched anchor pulls its FK/join neighbours. For each UNMATCHED estate
    domain adjacent to a matched one, take the matched domain's reference NEIGHBOURS and
    keep the reference neighbour the unmatched domain minimally overlaps (≥ MIN_PROP_OVERLAP
    tokens) — typed ``derived``. Deterministic. Empty when no adjacency (MV-D43)."""
    if not domain_adjacency:
        return {}
    disc_by_id = {d.domain_id: d for d in discovered}
    ref_by_name = {rd.name: rd for rd in reference.domains}
    out: dict[str, _Cand] = {}
    for d in discovered:
        if d.domain_id in best_ref_by_domain:
            continue  # already matched by string/embedding
        # Reference neighbours reachable from this domain's matched estate neighbours.
        neigh_refs: dict[str, ReferenceDomain] = {}
        for nid in sorted(set(domain_adjacency.get(d.domain_id, ()))):
            anchor_ref = best_ref_by_domain.get(nid)
            if anchor_ref is None:
                continue
            for rn in anchor_ref.neighbors:
                rd = ref_by_name.get(rn)
                if rd is not None:
                    neigh_refs[rd.name] = rd
        # Keep the neighbour reference with the strongest (still ≥ MIN_PROP_OVERLAP) overlap.
        best: _Cand | None = None
        for rd in sorted(neigh_refs.values(), key=lambda r: r.name.lower()):
            _s, overlap = _string_score(disc_by_id[d.domain_id], rd)
            if overlap < MIN_PROP_OVERLAP:
                continue
            if best is None or overlap > best.overlap:
                best = _Cand(ref=rd, string_score=_s, embed_cos=0.0, overlap=overlap, match_pass="structural")
        if best is not None:
            out[d.domain_id] = best
    return out


def _coherent(c: _Cand, *, structurally_supported: bool) -> bool:
    """Semantic-sanity pass (d): reject a lexically/embedding-close but structurally
    incoherent match. A string or structural candidate is coherent (it has real token
    evidence). A pure-embedding candidate is coherent only if it shares a token OR has
    structural support — else it is embedding-close noise and is rejected (→ not-equivalent)."""
    if c.match_pass in ("string", "structural"):
        return True
    # embedding pass
    if c.overlap >= 1 or c.string_score >= SANITY_STRING_FLOOR:
        return True
    return structurally_supported


# ── Relation typing (deterministic precedence) ───────────────────────────────


def _relation(
    d: _Discovered, c: _Cand,
    *, multi_ref: bool, shared_ref: bool,
) -> str:
    """Type a coherent candidate. Precedence: structural-only → ``derived``; exact-name →
    ``exact``; matches ≥2 reference domains → ``broader``; a sub-domain (parent_id) or one of
    several estate domains mapping to the same reference → ``narrower``; else ``narrower``
    (a partial, non-exact match is conservatively the narrower slice)."""
    if c.match_pass == "structural":
        return "derived"
    if _is_exact(d, c.ref, c.string_score):
        return "exact"
    if multi_ref:
        return "broader"
    if d.parent_id or shared_ref:
        return "narrower"
    return "narrower"


def _provenance_leaf(reference: ReferenceModel, as_of: str) -> dict[str, Any]:
    """The Provenanced envelope every correspondence + gap hypothesis carries (T2 industry
    model). ``confidence`` is left at the envelope default; the rank ladder, not a number,
    decides application."""
    return Provenanced(
        value=reference.model_id,
        tier=reference.tier or "T2",
        source_url=reference.source_url,
        source_kind="industry_model",
        as_of=reference.as_of or as_of,
    ).to_dict()


# ── The public entry point ───────────────────────────────────────────────────


def align(
    domain_rows: Sequence[Mapping[str, Any]],
    reference: ReferenceModel | None,
    *,
    members_by_domain: Mapping[str, Sequence[str]] | None = None,
    domain_adjacency: Mapping[str, Sequence[str]] | None = None,
    embedder: Any | None = None,
    as_of: str = "",
    load_evidence: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
) -> Alignment:
    """Align the estate's discovered domains against ``reference`` and return the typed
    correspondences + gap hypotheses + the harness-shaped aligned reference.

    Deterministic (fixed thresholds + seed anchors; sorted iteration). Read-only — mutates
    nothing, touches no structure. Degrades to an empty :class:`Alignment` (MV-D43) when the
    reference is missing/empty or there are no surfaced discovered domains. ``embedder``
    (``.embed(list[str]) -> list[list[float]]``) is optional; absent ⇒ passes (a),(c),(d)
    only. ``domain_adjacency`` (``{domain_id: [neighbour_domain_id]}``, from the FK/join
    graph) drives pass (c); absent ⇒ no structural propagation."""
    if reference is None or reference.is_empty():
        return _empty_alignment(reference.model_id if reference else "")
    members = members_by_domain or {}
    adjacency = domain_adjacency or {}
    _load = load_evidence or _default_load_evidence

    discovered = _discovered_domains(domain_rows, members, load_evidence=_load)
    if not discovered:
        return _empty_alignment(reference.model_id)

    # Passes (a)+(b): string + embedding candidates.
    base = _base_candidates(discovered, reference, embedder)
    best_base: dict[str, _Cand] = {did: cands[0] for did, cands in base.items()}
    best_ref_by_domain: dict[str, ReferenceDomain] = {
        did: c.ref for did, c in best_base.items() if _coherent(c, structurally_supported=False)
    }

    # Pass (c): structural propagation off the coherent anchors.
    structural = _structural_candidates(discovered, reference, best_ref_by_domain, adjacency)

    # Cardinality signals for typing: how many refs a domain matches (broader) + how many
    # domains map to one ref (narrower). Computed over the coherent base candidates.
    refs_per_domain: dict[str, set[str]] = {}
    domains_per_ref: dict[str, set[str]] = {}
    for did, cands in base.items():
        for c in cands:
            if _coherent(c, structurally_supported=False):
                refs_per_domain.setdefault(did, set()).add(c.ref.name)
    for did, c in best_base.items():
        if _coherent(c, structurally_supported=False):
            domains_per_ref.setdefault(c.ref.name, set()).add(did)

    prov = _provenance_leaf(reference, as_of)
    correspondences: list[Correspondence] = []
    matched_refs: set[str] = set()

    for d in discovered:
        base_c = best_base.get(d.domain_id)
        struct_c = structural.get(d.domain_id)
        if base_c is not None:
            supported = d.domain_id in structural
            if _coherent(base_c, structurally_supported=supported):
                multi = len(refs_per_domain.get(d.domain_id, set())) >= 2
                shared = len(domains_per_ref.get(base_c.ref.name, set())) >= 2
                rel = _relation(d, base_c, multi_ref=multi, shared_ref=shared)
                correspondences.append(_mk(d, base_c, rel, prov))
                matched_refs.add(base_c.ref.name)
            else:
                # Near-miss: recorded explicitly as not-equivalent (never a silent drop).
                correspondences.append(_mk(d, base_c, "not-equivalent", prov))
        elif struct_c is not None:
            correspondences.append(_mk(d, struct_c, "derived", prov))
            matched_refs.add(struct_c.ref.name)

    correspondences.sort(key=lambda c: (c.discovered_domain_id, c.reference_name))

    # Gap hypotheses: reference domains no coherent correspondence covered — ranked below
    # every graph-backed proposal, never auto-created (MV-D38).
    gap_hypotheses = _gap_hypotheses(reference, matched_refs, prov)
    aligned_reference = _aligned_reference(reference, correspondences, gap_hypotheses, as_of)
    return Alignment(
        reference_model=reference.model_id,
        correspondences=correspondences,
        gap_hypotheses=gap_hypotheses,
        aligned_reference=aligned_reference,
    )


def _mk(d: _Discovered, c: _Cand, relation: str, prov: dict[str, Any]) -> Correspondence:
    return Correspondence(
        discovered_domain_id=d.domain_id,
        discovered_name=d.name,
        reference_name=c.ref.name,
        reference_id=c.ref.slug,
        relation=relation,
        score=_cand_score(c) if c.match_pass != "structural" else float(c.overlap),
        match_pass=c.match_pass if relation != "not-equivalent" else "sanity",
        provenance=prov,
    )


def _gap_hypotheses(
    reference: ReferenceModel, matched_refs: set[str], prov: dict[str, Any],
) -> list[dict[str, Any]]:
    """One hypothesis per reference domain with NO coherent correspondence. Each is ranked
    below graph-backed proposals and is never a surfaced domain row (MV-D38)."""
    out: list[dict[str, Any]] = []
    for rd in reference.domains:
        if rd.name in matched_refs:
            continue
        out.append({
            "kind": "gap_hypothesis",
            "name": rd.name,
            "reference_id": rd.slug,
            "description": rd.description,
            "tier": prov["tier"],
            "source_url": prov["source_url"],
            "source_kind": prov["source_kind"],
            "as_of": prov["as_of"],
            "ranks_below_graph_proposals": True,
            "surfaced": False,
        })
    return sorted(out, key=lambda h: h["name"].lower())


def _aligned_reference(
    reference: ReferenceModel,
    correspondences: Sequence[Correspondence],
    gap_hypotheses: Sequence[Mapping[str, Any]],
    as_of: str,
) -> dict[str, Any]:
    """The §10 harness-shaped view (``eval_harness.compute_precision_recall_f1`` reads
    ``domains[].id`` + ``alignments[].discovered_id``/``reference_id``). Only equivalent-family
    relations (not ``not-equivalent``) become harness alignments — a rejected near-miss is
    NOT a match. Deterministic (sorted)."""
    alignments = [
        {
            "discovered_id": c.discovered_domain_id,
            "reference_id": c.reference_id,
            "relation": c.relation,
            "score": round(float(c.score), 6),
        }
        for c in correspondences
        if c.relation != "not-equivalent"
    ]
    alignments.sort(key=lambda a: (a["discovered_id"], a["reference_id"]))
    return {
        "reference_model": reference.model_id,
        "reference_label": reference.label,
        "as_of": reference.as_of or as_of,
        "source_url": reference.source_url,
        "domains": [
            {"id": rd.slug, "name": rd.name, "members": []} for rd in reference.domains
        ],
        "alignments": alignments,
        "gaps": [h["name"] for h in gap_hypotheses],
    }


def _default_load_evidence(row: Mapping[str, Any]) -> dict[str, Any]:
    """Parse a row's ``evidence`` JSON string into a dict (empty on any trouble) — matches
    the ``rank``/``materialize`` loaders so the surfaced-gate reads identically."""
    raw = row.get("evidence")
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}


__all__ = [
    "Alignment",
    "Correspondence",
    "RELATIONS",
    "ReferenceDomain",
    "ReferenceModel",
    "align",
    "load_reference_model",
]
