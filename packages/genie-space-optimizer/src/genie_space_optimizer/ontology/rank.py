"""L6 rank & trust gate (Phase 3d) — score every Domain / Sub-Domain / Page
proposal, firewall the unsafe/illegitimate, and mark what surfaces.

The governing rule is **MV-D35: facts lead, the score ranks.** Quality is already
binary-gated upstream (17e non-overlap, 17f corroboration), so this score is a
*demand / importance* ranking — never a "confidence in correctness," never rendered
as ``NN%``. It ORDERS the reviewer's queue and gates the sub-threshold noise.

Two jobs, both **pure, deterministic, LLM-free, and I/O-free** (the wheel runs on a
job cluster; the batch already computed the signals):

  1. **Score** (``score_proposals``) — the ``usage × lineage-centrality ×
     governance`` blend (a generalization of the MV-advisor's LYDS blend from
     single-MV proposals to every estate candidate). Reuses the MV-advisor tier
     thresholds + the evidence-coverage cap via ``transforms`` (the ONE tiering home),
     so a single-signal opinion cannot outrank a corroborated finding.
  2. **Firewall** — a candidate must pass ALL to *surface*:
     - **PII on proposed tag names** — ``leakage.LeakageOracle.tag_name_leaks``
       (the one oracle, delegating to ``er.pii_reject``): a Domain / Sub-Domain whose
       ``tag_key`` / ``tag_value`` name leaks PII is **blocked**, not surfaced.
     - **Policy conformance** — propose-only. A candidate whose evidence implies an
       instruction / card / Page *write* (there should be none) is rejected.
     - **Provenance ladder (MV-D38)** — SCAFFOLDED, DORMANT: a no-op pass today (all
       estate signal is T0 structural). The seam + the "T3 hint never outranks a T0
       fact" ordering become load-bearing when 17h reads external context. No external
       context is read here.
  3. **Mark surfaced** (``mark_surfaced``) — READS the ``genie_ont_suppressions``
     ledger rows (passed in; the wheel issues no ledger write) and marks any
     ``(kind, proposal_id)`` a curator already dismissed as ``surfaced=false`` so a
     re-run never resurfaces it (MV-D26). Blocked + sub-threshold + dismissed are all
     ``surfaced=false``, counted in the run report, and persisted (the full metastore
     set is re-MERGEd; a blocked row is kept, just not surfaced — §8).

This module writes NOTHING and holds **no reference to ``genie_ont_consents``** and
**no MERGE/INSERT/UPDATE** against any ledger table. ``materialize.py`` re-MERGEs the
augmented rows; the backend serve reads ``surfaced=true`` rows and tiers them with the
same ``transforms.tier_of`` (mirror-order == live-order).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.ontology import transforms

logger = logging.getLogger(__name__)

# ── The blend: usage × lineage-centrality × governance (MV-D35) ─────────────
# Weights sum to 1.0 so full-coverage evidence divides by 1.0 (the MV-advisor
# coverage-cap arithmetic, generalized). A partial-coverage proposal renormalizes
# over the factors actually measured, and the coverage cap (transforms.coverage_cap)
# bounds its tier so one weak signal cannot outrank a corroborated finding.
FACTOR_WEIGHTS: dict[str, float] = {"usage": 0.40, "centrality": 0.35, "governance": 0.25}

# Governance traffic-light ladder → a [0,1] factor value (governed > curated >
# ungoverned). Absence of any governance signal is UNAVAILABLE (drops from coverage),
# not "ungoverned" — the honest-gap discipline (architecture §5): a sparse estate
# must not be penalised as though its assets were positively ungoverned.
_GOVERNANCE_VALUE: dict[str, float] = {"governed": 1.0, "curated": 0.6, "ungoverned": 0.2}

# Provenance ladder tiers (MV-D38) — T0 internal-verified (system tables) is the
# strongest; T3 web-inferred the weakest. DORMANT here (all offline signal is T0); the
# strength order pins the seam for 17h ("a higher tier always wins").
PROVENANCE_TIER_STRENGTH: dict[str, int] = {"T0": 3, "T1": 2, "T2": 1, "T3": 0}


@dataclass(frozen=True)
class RankSignals:
    """The batch-precomputed inputs the blend reads — pure maps, no I/O.

    All three are keyed by asset FQN. ``usage`` and ``centrality`` are **already
    normalized to [0, 1]** by their producers (L2 ``query.history`` / ``billing.usage``
    demand-cost; ``graph.lineage_centrality`` degree centrality), so this module is
    pure arithmetic. ``governance`` maps an FQN to its rung
    (``governed``/``curated``/``ungoverned``). A proposal's factor is *present* when at
    least one of its assets appears in the corresponding map, else UNAVAILABLE (it
    leaves the blend and lowers coverage).
    """

    usage: Mapping[str, float] = field(default_factory=dict)
    centrality: Mapping[str, float] = field(default_factory=dict)
    governance: Mapping[str, str] = field(default_factory=dict)
    # Deprecation firewall input (Signal Authority Stage 2, MV-D94): the set of asset
    # FQNs tagged ``deprecated``. A proposal anchored on any of them is BLOCKED at the
    # firewall (never surfaced), the direct analogue of OntoRank steering away from a
    # stale source. Empty by default ⇒ no proposal is deprecation-blocked (today's bytes).
    deprecated: frozenset[str] = field(default_factory=frozenset)


# ── Firewalls (must pass ALL to surface) ────────────────────────────────────


def _default_oracle() -> Any:
    """A corpus-less ``LeakageOracle`` — enough for the intrinsic tag-name PII check
    (``tag_name_leaks`` is corpus-independent). Lazy-imported so ``rank`` stays
    importable without the optimization package's heavier deps at module scope."""
    from genie_space_optimizer.optimization.leakage import LeakageOracle

    return LeakageOracle()


def pii_name_reject(name: str | None, oracle: Any) -> tuple[bool, str]:
    """PII firewall on a proposed governed tag name — the ONE oracle
    (``LeakageOracle.tag_name_leaks`` → ``er.pii_reject``). ``(blocked, reason)``."""
    if not name:
        return False, ""
    return oracle.tag_name_leaks(str(name))


def policy_conform(kind: str, tag_decision: str | None, evidence: Mapping[str, Any]) -> tuple[bool, str]:
    """Propose-only conformance. ``(ok, reason)`` — ``ok=False`` rejects at the gate.

    Every Phase-3d candidate is propose-only: a Domain carries a ``tag_decision`` in
    ``{reuse, create, reassign}`` (all proposal strings, never a UC write) and a Page
    is a copy-ready draft. A candidate whose evidence carries an explicit write intent
    (``evidence["write_intent"]``) — or a Domain with a ``tag_decision`` outside the
    proposal vocabulary — implies a mutation and is rejected. There should be none;
    this is the gate that keeps it so (and the §11 firewall test's hook)."""
    if evidence.get("write_intent"):
        return False, "policy_write_intent"
    if kind != "page" and tag_decision not in (None, "reuse", "create", "reassign"):
        return False, f"policy_bad_tag_decision:{tag_decision}"
    return True, ""


def provenance_ladder(provenance_tier: str = "T0") -> tuple[bool, str]:
    """DORMANT provenance gate (MV-D38). A no-op **pass** today — all estate signal is
    T0 structural and external context (T1/T2/T3) is not read until 17h. The seam
    exists so 17h can make it load-bearing: a higher provenance tier always wins, and
    a T3 web-inferred vocabulary hint can never outrank a T0 lineage fact. Never reads
    external context here."""
    return True, "dormant"


def outranks(tier_a: str, tier_b: str) -> bool:
    """Whether provenance ``tier_a`` strictly outranks ``tier_b`` (T0 > T1 > T2 > T3).
    The invariant 17h enforces; the naming-prior + gap-hypothesis plug-points below make
    it load-bearing (a T3 web hint never outranks a T0 lineage fact)."""
    return PROVENANCE_TIER_STRENGTH.get(tier_a, -1) > PROVENANCE_TIER_STRENGTH.get(tier_b, -1)


# ── Plug-point 1 (17h Stage B): external Context Pack as a read-only naming prior ────
# The pack (MV-D38) is a PROVENANCED prior that steers L4/L5 cluster NAMING + gap
# hypotheses. It NEVER outranks a T0/curated fact: a governed-tag Domain (reuse/reassign
# — a human-asserted, T0/curated name) is never renamed by the pack; only a pure engine
# ``create`` cluster (whose name is an anchor-derived default, not a curated fact) may
# take a business-language name from the pack. A gap hypothesis ("industry has domain X,
# estate has none") is ranked BELOW every graph-backed proposal (it is never a surfaced
# Domain row; the run report carries it). This is the ONLY place the pack touches naming;
# ``pack=None`` (the estate-only default) makes every function here a no-op.

_STOPWORDS: frozenset[str] = frozenset(
    {"the", "and", "of", "for", "data", "domain", "business", "core", "team", "group"}
)


def _tokens(text: str) -> set[str]:
    """Meaningful lowercase tokens (len > 2, non-stopword) for prior/gap matching."""
    raw = "".join(c.lower() if (c.isalnum() or c.isspace()) else " " for c in str(text or ""))
    return {t for t in raw.split() if len(t) > 2 and t not in _STOPWORDS}


def _row_name_tokens(row: Mapping[str, Any], members: Sequence[str]) -> set[str]:
    """The tokens a pack name is matched against: the cluster's current name + its member
    schema/table stems (so an industry name like ``Revenue`` matches a revenue cluster)."""
    toks = _tokens(row.get("name") or "")
    for m in members:
        parts = str(m).split(".")
        for p in parts[1:]:  # skip catalog; schema + table carry the business sense
            toks |= _tokens(p)
    return toks


def apply_context_prior(
    domain_rows: list[dict[str, Any]],
    pack: Any | None,
    *,
    members_by_domain: Mapping[str, Sequence[str]] | None = None,
) -> int:
    """Apply the Context Pack's business-language names as PROVENANCED priors to ``create``
    Domain rows IN PLACE (plug-point 1, MV-D38). Returns the number of rows renamed.

    **Targeting (MV-D38 — tightened after the 2026-09-15 deploy-verify):** the prior only
    touches a domain the gate will SURFACE. A row whose ``evidence.surfaced`` is not true (a
    below-bar / diffuse / dev / migration / demo cluster the gate suppressed) is SKIPPED
    entirely — no rename, no recorded prior — so a pack name can never land on hidden junk
    (the observed "Loyalty & Mileage Plan Programs → Migration" mis-label). ``mark_surfaced``
    runs before this in ``materialize.py``, so ``surfaced`` is authoritative here.

    A curated governed-tag Domain (``tag_decision`` in ``{reuse, reassign}`` — a T0/curated
    name) is NEVER renamed: the pack name is recorded on ``evidence["rank"]["naming_prior"]``
    as corroborating provenance but the name is left untouched (T0/curated wins, MV-D35/D38).
    A pure engine ``create`` cluster (an anchor-derived default name, not a curated fact) may
    take a matching pack name; the prior + its provenance envelope are recorded on the rank
    block. The match itself is quality-gated (``_best_prior`` requires a meaningful overlap,
    not one incidental shared member stem). ``pack=None`` (the estate-only default) is a
    NO-OP — zero rows touched, byte-identical."""
    if pack is None:
        return 0
    priors = _pack_domain_priors(pack)
    if not priors:
        return 0
    members = members_by_domain or {}
    renamed = 0
    for row in domain_rows:
        ev = _load_evidence(row)
        # Targeting gate: the prior never touches an unsurfaced cluster (MV-D38). This is
        # the single load-bearing guard against naming hidden dev/migration/demo junk.
        if not ev.get("surfaced"):
            continue
        row_members = members.get(str(row.get("domain_id") or ""), ())
        row_toks = _row_name_tokens(row, row_members)
        match = _best_prior(priors, row_toks)
        if match is None:
            continue
        name, leaf = match
        rank = ev.get("rank")
        if not isinstance(rank, dict):
            rank = {}
        curated = str(row.get("tag_decision") or "") in ("reuse", "reassign")
        rank["naming_prior"] = {
            "value": name,
            "tier": str(leaf.get("tier") or "T2"),
            "source_url": leaf.get("source_url"),
            "source_kind": leaf.get("source_kind"),
            "as_of": leaf.get("as_of"),
            # A curated (T0) name always wins — record why the prior did NOT apply.
            "applied": not curated,
            "outranked_by": "curated" if curated else None,
        }
        ev["rank"] = rank
        if not curated:
            row["name"] = name
            renamed += 1
        row["evidence"] = json.dumps(ev, sort_keys=True)
    return renamed


def context_gap_hypotheses(pack: Any | None, surfaced_domain_names: Iterable[str]) -> list[dict[str, Any]]:
    """The pack's canonical-domain gap hypotheses ("industry has domain X, estate has
    none"), each a PROVENANCED prior ranked BELOW every graph-backed proposal (MV-D38).

    Returns ``[]`` when ``pack=None`` OR the confidence-gate suppressed the gap-check
    (``pack.gap_check_suppressed`` — a low-confidence industry guess, MV-D38 rail 6). A
    hypothesis is emitted only for a canonical domain whose name is NOT already covered by
    a surfaced Domain (token overlap). These are NEVER surfaced Domain rows — the caller
    carries them in the run report as informational, sourced, dated hints."""
    if pack is None or getattr(pack, "gap_check_suppressed", False):
        return []
    priors = _pack_domain_priors(pack)
    if not priors:
        return []
    covered: set[str] = set()
    for nm in surfaced_domain_names:
        covered |= _tokens(nm)
    out: list[dict[str, Any]] = []
    for name, leaf in priors:
        if _tokens(name) & covered:
            continue  # the estate already has a domain covering this concept
        out.append({
            "kind": "gap_hypothesis",
            "name": name,
            "tier": str(leaf.get("tier") or "T2"),
            "source_url": leaf.get("source_url"),
            "source_kind": leaf.get("source_kind"),
            "as_of": leaf.get("as_of"),
            # A gap ranks below EVERY graph-backed proposal — never a surfaced Domain.
            "ranks_below_graph_proposals": True,
            "surfaced": False,
        })
    return sorted(out, key=lambda h: h["name"].lower())


# ── Plug-point 1b (17h / §9, MV-D58): industry-reference alignment as a typed prior ──
# ``alignment.py`` runs the four-pass hybrid match (string → embedding seed anchors →
# structural propagation → semantic sanity) and hands this function the TYPED correspondences
# (``exact``/``narrower``/``broader``/``derived``/``not-equivalent``) as plain dicts (rank stays
# decoupled from the alignment module — no import). This records each correspondence on the
# domain's ``evidence.rank.alignment`` as a PROVENANCED prior and, like the pack naming prior,
# renames ONLY a surfaced pure-engine ``create`` cluster on an ``exact`` match — a curated
# governed-tag Domain (reuse/reassign — a T0/curated name) is NEVER renamed; the correspondence
# becomes corroborating evidence (``applied=false``, ``outranked_by="curated"``). Empty
# ``correspondences`` (alignment off / no match) is a NO-OP ⇒ byte-identical (MV-D44).


def apply_alignment(
    domain_rows: list[dict[str, Any]],
    correspondences: Sequence[Mapping[str, Any]],
) -> int:
    """Apply industry-reference typed correspondences to Domain rows IN PLACE (plug-point 1b,
    MV-D58). Returns the number of rows renamed.

    Each correspondence (a dict from ``alignment.Correspondence.to_dict``) is recorded on
    ``evidence.rank.alignment`` with its relation + provenance envelope. A row is renamed to
    the reference name ONLY when it is a surfaced pure-engine ``create`` cluster matched
    ``exact`` — the same T0/curated-wins discipline as :func:`apply_context_prior`: a curated
    Domain (``tag_decision`` in ``{reuse, reassign}``) keeps its name and the correspondence is
    recorded as corroborating evidence. A ``not-equivalent`` (rejected near-miss) is recorded
    too but never applied. No correspondences ⇒ a NO-OP (zero rows touched, byte-identical)."""
    if not correspondences:
        return 0
    by_domain: dict[str, Mapping[str, Any]] = {}
    for c in correspondences:
        did = str(c.get("discovered_domain_id") or "")
        if did and did not in by_domain:  # first (deterministically-sorted) correspondence wins
            by_domain[did] = c
    renamed = 0
    for row in domain_rows:
        corr = by_domain.get(str(row.get("domain_id") or ""))
        if corr is None:
            continue
        ev = _load_evidence(row)
        rank = ev.get("rank")
        if not isinstance(rank, dict):
            rank = {}
        curated = str(row.get("tag_decision") or "") in ("reuse", "reassign")
        relation = str(corr.get("relation") or "")
        prov = corr.get("provenance") if isinstance(corr.get("provenance"), Mapping) else {}
        applied = (not curated) and bool(ev.get("surfaced")) and relation == "exact"
        rank["alignment"] = {
            "reference_name": corr.get("reference_name"),
            "reference_id": corr.get("reference_id"),
            "relation": relation,
            "match_pass": corr.get("match_pass"),
            "score": corr.get("score"),
            "tier": str(prov.get("tier") or "T2"),
            "source_url": prov.get("source_url"),
            "source_kind": prov.get("source_kind"),
            "as_of": prov.get("as_of"),
            # A curated (T0) name always wins — record why alignment did NOT rename.
            "applied": applied,
            "outranked_by": "curated" if curated else None,
        }
        ev["rank"] = rank
        if applied:
            row["name"] = str(corr.get("reference_name") or row.get("name"))
            renamed += 1
        row["evidence"] = json.dumps(ev, sort_keys=True)
    return renamed


def _pack_domain_priors(pack: Any) -> list[tuple[str, dict[str, Any]]]:
    """``[(name, name_leaf)]`` from the pack's canonical domains (duck-typed on
    ``canonical_domain_names``; falls back to reading ``canonical_domains`` dicts)."""
    getter = getattr(pack, "canonical_domain_names", None)
    if callable(getter):
        try:
            return list(getter())
        except Exception:  # noqa: BLE001 — degrade to the raw-field read
            pass
    out: list[tuple[str, dict[str, Any]]] = []
    for cd in getattr(pack, "canonical_domains", []) or []:
        leaf = cd.get("name") if isinstance(cd, dict) else None
        if isinstance(leaf, dict) and str(leaf.get("value") or "").strip():
            out.append((str(leaf["value"]).strip(), leaf))
    return out


# A meaningful pack-name match needs more than one incidental shared token (e.g. a single
# member-table stem): a multi-word canonical name must overlap on ≥ this many tokens. A
# single-token canonical name ("Revenue") still matches on its one token (full coverage).
_MIN_PRIOR_OVERLAP = 2


def _best_prior(
    priors: Sequence[tuple[str, dict[str, Any]]],
    row_toks: set[str],
    *,
    min_overlap: int = _MIN_PRIOR_OVERLAP,
) -> tuple[str, dict[str, Any]] | None:
    """The pack name with the strongest MEANINGFUL token overlap with a cluster (deterministic
    tie-break by name); ``None`` when nothing clears the bar. The bar rejects a lone incidental
    token (the "Migration cluster has one `loyalty` table" false match): a multi-word name needs
    ``min_overlap`` shared tokens, while a single-token name qualifies on its one full token."""
    best: tuple[int, str, dict[str, Any]] | None = None
    for name, leaf in priors:
        name_toks = _tokens(name)
        overlap = len(name_toks & row_toks)
        if overlap <= 0:
            continue
        # Strong match: ≥ min_overlap shared tokens, OR a single-token name fully matched.
        if overlap < min_overlap and not (len(name_toks) == 1 and overlap >= 1):
            continue
        if best is None or overlap > best[0] or (overlap == best[0] and name.lower() < best[1].lower()):
            best = (overlap, name, leaf)
    return (best[1], best[2]) if best else None


# ── The blend ───────────────────────────────────────────────────────────────


def _factor(assets: Sequence[str], signal_map: Mapping[str, Any]) -> tuple[bool, float]:
    """``(present, value)`` for one factor over a proposal's assets. Present when any
    asset carries the signal; the value is the strongest (max) — the load-bearing
    asset speaks for the proposal (the MV-D35 anchor discipline)."""
    vals = [signal_map[a] for a in assets if a in signal_map]
    if not vals:
        return False, 0.0
    return True, max(float(v) for v in vals)


def _governance_factor(assets: Sequence[str], governance: Mapping[str, str]) -> tuple[bool, float]:
    """Governance factor: the best rung among the proposal's assets (governed >
    curated > ungoverned). Present only when a rung is known for ≥1 asset."""
    vals = [_GOVERNANCE_VALUE.get(str(governance[a]).lower(), 0.0) for a in assets if a in governance]
    if not vals:
        return False, 0.0
    return True, max(vals)


def blend(assets: Sequence[str], signals: RankSignals) -> dict[str, Any]:
    """The ``usage × centrality × governance`` blend + coverage-capped tier for one
    proposal's assets. Pure. Returns the rank evidence block (no ``surfaced`` — that is
    set once the firewalls + ledger have run)."""
    factors: dict[str, dict[str, Any]] = {}
    numerator = 0.0
    coverage = 0.0
    for name, (present, value) in (
        ("usage", _factor(assets, signals.usage)),
        ("centrality", _factor(assets, signals.centrality)),
        ("governance", _governance_factor(assets, signals.governance)),
    ):
        factors[name] = {"present": present, "value": round(value, 6)}
        if present:
            w = FACTOR_WEIGHTS[name]
            numerator += w * value
            coverage += w
    coverage = round(coverage, 6)
    score = 0.0 if coverage <= 0 else 100.0 * (numerator / coverage)
    tier, uncapped, capped = transforms.coverage_cap(score, coverage)
    return {
        "score": round(score, 6),
        "tier": tier,
        "uncapped_tier": uncapped,
        "tier_capped_by_coverage": capped,
        "evidence_coverage": coverage,
        "factors": factors,
        "provenance_tier": "T0",
    }


# ── Surfacing predicate: usage RANKS, it never GATES (MV-D93/D94) ───────────
# The blend (usage × centrality × governance) ORDERS the reviewer's queue, but the
# usage factor must never be the *reason* a cluster surfaces. A heavily-queried but
# ungoverned, structureless junk cluster (Migration, the GSO's own cost tables,
# dev/demo datasets — usage≈1.0, governance≈0.2) would otherwise clear the tier
# threshold on demand alone. The surfacing test recomputes the SAME coverage-normalized
# blend over the NON-USAGE factors only (centrality, governance): a cluster whose only
# present evidence is usage scores 0 there ⇒ no tier ⇒ not a surfacing basis (honest-gap,
# MV-D43). The DISPLAYED tier / score / coverage / factors keep the full usage-inclusive
# blend — usage still ranks. Touches neither ``blend`` nor any firewall (MV-D35).
_NON_USAGE_FACTORS: tuple[str, ...] = ("centrality", "governance")


def _surfacing_ok(factors: Mapping[str, Any]) -> bool:
    """Whether a proposal has a NON-USAGE surfacing basis (MV-D93/D94).

    Recompute the blend score over the present non-usage factors only (centrality,
    governance) with the SAME coverage-normalized formula as :func:`blend`
    (``100 * Σ w·value / Σ w`` over present non-usage factors; ``0`` when none present),
    then require a real tier. A cluster whose only present evidence is usage scores 0
    here ⇒ :func:`transforms.tier_of` returns ``None`` ⇒ it is not a surfacing basis
    (usage ranks, it never gates). Reuses ``FACTOR_WEIGHTS``; pure and deterministic."""
    numerator = 0.0
    coverage = 0.0
    for name in _NON_USAGE_FACTORS:
        factor = factors.get(name) or {}
        if factor.get("present"):
            w = FACTOR_WEIGHTS[name]
            numerator += w * float(factor.get("value") or 0.0)
            coverage += w
    score = 0.0 if coverage <= 0 else 100.0 * (numerator / coverage)
    return transforms.tier_of(score) is not None


# ── Row-level scoring (operates on the built Delta row dicts) ───────────────


def _load_evidence(row: dict[str, Any]) -> dict[str, Any]:
    """Parse a proposal row's ``evidence`` JSON string into a dict (empty on any
    trouble). The row builders store ``evidence`` as ``json.dumps(...)``."""
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


def _schema_of(fqn: str) -> str:
    """``catalog.schema`` of an asset fqn (the natural bigger home for a below-bar
    fragment's "add to existing domain" hint)."""
    parts = str(fqn).split(".")
    return ".".join(parts[:2]) if len(parts) >= 2 else str(fqn)


def _is_connected(evidence: Mapping[str, Any]) -> bool:
    """Whether a Domain proposal has a STRUCTURAL connection between its assets — a
    lineage spine, a co-query link, or an FK / metric-view / community grouping. A
    shared-schema-only group (edgeless assets that merely sit in one schema) is NOT
    connected, so ``domain_require_connection`` prunes it (the §Appendix A junk)."""
    if len(evidence.get("shared_spine") or []) >= 2:
        return True
    if int(evidence.get("co_query_count") or 0) > 0:
        return True
    reason = str(evidence.get("reason") or "").lower()
    return any(k in reason for k in ("foreign key", "foreign-key", "metric view", "community detection"))


def _is_curated_domain(evidence: Mapping[str, Any]) -> bool:
    """A human-curated governed-tag Domain (grouped by the R1 curated-domain-tag rule,
    MV-D53 precedence #1). Such a Domain is a human-asserted bounded context — legitimate
    by fiat — so the structural legitimacy bar must never gate it (Stage-3.1, §A.3)."""
    return str(evidence.get("reason") or "").lower().startswith("grouped by curated domain tag")


def _legitimacy_home(members: Sequence[str], evidence: Mapping[str, Any]) -> str:
    """The bigger home a below-bar fragment should fold into: its most common schema
    (deterministic, alphabetical tie-break), else the anchor's schema."""
    schemas = [_schema_of(m) for m in members]
    if schemas:
        counts: dict[str, int] = {}
        for s in schemas:
            counts[s] = counts.get(s, 0) + 1
        return max(sorted(counts), key=lambda s: counts[s])
    anchor = evidence.get("anchor")
    return _schema_of(str(anchor)) if anchor else "another domain"


def _apply_legitimacy_gate(
    row: dict[str, Any], evidence: dict[str, Any], members: Sequence[str], rank: dict[str, Any],
    *, min_tables: int, min_schemas: int, require_connection: bool,
) -> None:
    """The legitimacy bar (MV-D57), applied to a top-level Domain proposal in place: a
    below-bar group is KEPT but ``surfaced=false`` with an "add to existing domain"
    hint, never a standalone Domain. Sub-domains + reassign + pages are exempt (they
    already live inside a domain or name a governed conflict); a curated governed-tag
    Domain is likewise exempt — a human already asserted it as a bounded context
    (MV-D53 precedence #1), so it is legitimate by fiat (Stage-3.1, §A.3). Records the
    verdict on ``rank`` so the run report and the serve layer can read it."""
    if _is_curated_domain(evidence):
        rank["legitimate"] = True
        return
    n_tables = len({str(m) for m in members})
    n_schemas = len({_schema_of(m) for m in members})
    connected = _is_connected(evidence)
    ok, reason = transforms.legitimacy_ok(
        n_tables, n_schemas, connected,
        min_tables=min_tables, min_schemas=min_schemas, require_connection=require_connection,
    )
    rank["legitimate"] = ok
    if not ok:
        home = _legitimacy_home(members, evidence)
        rank["legitimacy_reason"] = reason
        evidence["gate_hint"] = f"add to existing domain: {home}"
        evidence["surfaced"] = False


def _apply_diffuseness_gate(
    row: dict[str, Any], evidence: dict[str, Any], members: Sequence[str], rank: dict[str, Any],
    *, max_schemas: int, min_home_concentration: float,
) -> None:
    """Gate-B (MV-D62), applied to a top-level structural Domain in place: a diffuse
    cross-schema hairball is KEPT but ``surfaced=false`` with a "split or attach" hint,
    never a standalone Domain — the last-resort presentation net (spec §2.2). Runs ONLY
    on a top-level, non-curated Domain whose origin is STRUCTURAL (``tag_decision ==
    'create'`` — an FK-component / community / shared-schema grouping); a curated
    governed-tag Domain (a human-asserted bounded context, MV-D53 #1) and a governed-tag
    REUSE Domain are never diffuse-gated, and sub-domains / reassign / pages are skipped
    by the ``kind == 'domain'`` guard at the call site (exactly like the legitimacy
    bar). Records the verdict on ``rank`` so the run report + serve layer can read it."""
    if _is_curated_domain(evidence):
        return
    if str(row.get("tag_decision") or "") != "create":
        return
    uniq = {str(m) for m in members}
    if not uniq:
        return
    schemas = [_schema_of(m) for m in uniq]
    n_schemas = len(set(schemas))
    home = max(schemas.count(s) for s in set(schemas))
    home_concentration = home / len(uniq)
    diffuse, reason = transforms.is_diffuse(
        n_schemas, home_concentration,
        max_schemas=max_schemas, min_home_concentration=min_home_concentration,
    )
    rank["diffuse"] = diffuse
    if diffuse:
        rank["diffuse_reason"] = reason
        evidence["surfaced"] = False


_PAGE_UNATTACHED_REASON = "page not attached to a surfaced domain"


def _apply_page_attachment_gate(row: dict[str, Any], surfaced_domain_ids: set[str]) -> None:
    """The Page-attachment gate (MV-D64), applied to a Page row in place AFTER
    ``_score_row`` set its tentative ``evidence["surfaced"]``: a Page whose ``domain_id``
    is empty — or points to a Domain that did NOT surface this run — is KEPT but
    ``surfaced=false`` with a ``surfaced_reason`` hint, never shown orphaned (spec §3.2).
    ``surfaced_domain_ids`` are the Domains that cleared their own gates this run
    (computed from the just-scored ``domain_rows``); Domains are scored first, so this
    set is known before the Page pass. Rides ``evidence`` — no new column (MV-D49) — and
    never deletes the row (the metastore re-MERGE carries the full set, §8)."""
    dom = str(row.get("domain_id") or "")
    if dom and dom in surfaced_domain_ids:
        return
    ev = _load_evidence(row)
    ev["surfaced"] = False
    ev["surfaced_reason"] = _PAGE_UNATTACHED_REASON
    row["evidence"] = json.dumps(ev, sort_keys=True)


def _domain_assets(row: dict[str, Any], evidence: Mapping[str, Any], members_by_domain: Mapping[str, Sequence[str]]) -> list[str]:
    """A Domain proposal's scoring assets: its member FQNs (17e membership) plus the
    lineage anchor / shared spine carried in evidence (the load-bearing spine)."""
    assets: set[str] = set(members_by_domain.get(row.get("domain_id", ""), ()))
    anchor = evidence.get("anchor")
    if anchor:
        assets.add(str(anchor))
    assets.update(str(a) for a in (evidence.get("shared_spine") or []))
    return sorted(assets)


def _page_assets(row: dict[str, Any]) -> list[str]:
    """A Page proposal's scoring assets: its Sources + Related (metric views, coded
    tables, serving Agents) — the artifacts the concept is anchored on."""
    assets: set[str] = set(str(a) for a in (row.get("source_fqns") or []))
    assets.update(str(a) for a in (row.get("related_fqns") or []))
    return sorted(assets)


def _score_row(
    row: dict[str, Any], *, kind: str, assets: Sequence[str], signals: RankSignals, oracle: Any,
    members: Sequence[str] = (), min_tables: int = transforms.DOMAIN_MIN_TABLES,
    min_schemas: int = transforms.DOMAIN_MIN_SCHEMAS,
    require_connection: bool = transforms.DOMAIN_REQUIRE_CONNECTION,
    max_diffuse_schemas: int = transforms.DOMAIN_MAX_DIFFUSE_SCHEMAS,
    min_home_concentration: float = transforms.DOMAIN_MIN_HOME_CONCENTRATION,
) -> None:
    """Score + firewall one proposal row in place: set ``score`` and write the rank
    block + a tentative ``surfaced`` flag into ``evidence`` (the ledger pass finalizes
    ``surfaced``). A blocked candidate keeps its row but never surfaces (§8). A
    top-level Domain below the legitimacy bar (MV-D57) is kept but not surfaced. The
    readable confidence band (MV-D56) is written into the rank block."""
    evidence = _load_evidence(row)
    rank = blend(assets, signals)

    blocked = False
    reason = ""
    # PII on proposed tag names (Domains only — Pages carry no tag name).
    if kind != "page":
        for name in (row.get("tag_key"), row.get("tag_value")):
            hit, why = pii_name_reject(name, oracle)
            if hit:
                blocked, reason = True, why
                break
    # Deprecation firewall (MV-D94): a proposal anchored on ANY deprecated asset is steered
    # away from — blocked, never surfaced even at a high raw score — mirroring the PII block.
    # Applies to Domains and Pages alike (a deprecated table poisons either).
    if not blocked and any(a in signals.deprecated for a in assets):
        blocked, reason = True, "deprecated_asset"
    if not blocked:
        ok, why = policy_conform(kind, row.get("tag_decision"), evidence)
        if not ok:
            blocked, reason = True, why
    if not blocked:
        ok, why = provenance_ladder(rank.get("provenance_tier", "T0"))
        if not ok:
            blocked, reason = True, why

    rank["blocked"] = blocked
    if blocked:
        rank["block_reason"] = reason
    # Honest confidence (MV-D56): band + signals present + gap, never a percent.
    rank["confidence"] = transforms.confidence_band(rank)
    row["score"] = rank["score"]
    evidence["rank"] = rank
    # Tentative: surfaced iff it cleared threshold AND passed every firewall AND has a
    # non-usage surfacing basis (usage RANKS, it never GATES — MV-D93/D94). The ledger
    # pass (mark_surfaced) may still flip it to false for a dismissed proposal.
    rank["surface_basis"] = "non_usage_evidence"
    evidence["surfaced"] = bool(
        rank["tier"] is not None and not blocked and _surfacing_ok(rank["factors"])
    )
    # Legitimacy bar (MV-D57) — top-level Domains only; below-bar rows are kept but
    # not surfaced, with an "add to existing domain" hint (§Appendix A junk pruning).
    if kind == "domain":
        _apply_legitimacy_gate(
            row, evidence, members, rank,
            min_tables=min_tables, min_schemas=min_schemas, require_connection=require_connection,
        )
        # Gate-B (MV-D62) — the diffuseness net, right AFTER the legitimacy bar: a diffuse
        # cross-schema structural hairball is kept but not surfaced (spec §2.2).
        _apply_diffuseness_gate(
            row, evidence, members, rank,
            max_schemas=max_diffuse_schemas, min_home_concentration=min_home_concentration,
        )
    row["evidence"] = json.dumps(evidence, sort_keys=True)


def score_proposals(
    domain_rows: list[dict[str, Any]],
    page_rows: list[dict[str, Any]],
    *,
    members_by_domain: Mapping[str, Sequence[str]] | None = None,
    signals: RankSignals | None = None,
    oracle: Any | None = None,
    min_tables: int = transforms.DOMAIN_MIN_TABLES,
    min_schemas: int = transforms.DOMAIN_MIN_SCHEMAS,
    require_connection: bool = transforms.DOMAIN_REQUIRE_CONNECTION,
    max_diffuse_schemas: int = transforms.DOMAIN_MAX_DIFFUSE_SCHEMAS,
    min_home_concentration: float = transforms.DOMAIN_MIN_HOME_CONCENTRATION,
    page_require_domain: bool = True,
) -> None:
    """Score + firewall every Domain / Sub-Domain / Page proposal **in place**.

    Deterministic and pure. Each row gets its ``score`` (0-100 blend) and an
    ``evidence["rank"]`` block (tier, uncapped tier, coverage, factors, blocked +
    reason, the readable ``confidence`` band) plus a tentative ``evidence["surfaced"]``.
    Blocked (firewall), sub-threshold, and below-legitimacy-bar (MV-D57) proposals are
    marked ``surfaced=false`` but KEPT in the list so the metastore-scoped re-MERGE
    carries the full set (§8). The legitimacy bar defaults come from config (MV-D57);
    a param-less call uses the shipped moderate defaults. The ledger pass
    (:func:`mark_surfaced`) runs after this.

    Domains are scored FIRST so the Page-attachment gate (MV-D64) can read the set of
    Domains that surfaced this run: a Page whose ``domain_id`` is empty or names a
    non-surfaced Domain is kept but ``surfaced=false`` (spec §3.2). ``page_require_domain``
    (config, default True) gates the whole rule so it can be disabled.
    """
    members = members_by_domain or {}
    sig = signals or RankSignals()
    oracle = oracle or _default_oracle()
    for row in domain_rows:
        ev = _load_evidence(row)
        _score_row(
            row, kind=transforms.proposal_kind_of(row), assets=_domain_assets(row, ev, members),
            signals=sig, oracle=oracle, members=members.get(str(row.get("domain_id") or ""), ()),
            min_tables=min_tables, min_schemas=min_schemas, require_connection=require_connection,
            max_diffuse_schemas=max_diffuse_schemas, min_home_concentration=min_home_concentration,
        )
    # The Domains that cleared their own gates this run (empty when nothing surfaced) —
    # the attach-to targets for the Page gate. Computed after the Domain pass, before the
    # Page pass (MV-D64).
    surfaced_domain_ids = {
        str(row.get("domain_id") or "") for row in domain_rows if _load_evidence(row).get("surfaced")
    }
    for row in page_rows:
        _score_row(row, kind="page", assets=_page_assets(row), signals=sig, oracle=oracle)
        if page_require_domain:
            _apply_page_attachment_gate(row, surfaced_domain_ids)


# ── Ledger read → surfaced (MV-D26) + run report ────────────────────────────


def _suppression_keys(suppressions: Sequence[Mapping[str, Any]]) -> set[tuple[str, str]]:
    """``{(proposal_kind, proposal_id)}`` from the ``genie_ont_suppressions`` rows the
    caller read. The wheel only READS the ledger — no write of any kind."""
    return {
        (str(s.get("proposal_kind") or ""), str(s.get("proposal_id") or ""))
        for s in suppressions or ()
        if s.get("proposal_id")
    }


def _finalize_row(row: dict[str, Any], *, kind: str, proposal_id: str, suppressed: set[tuple[str, str]], counts: dict[str, int]) -> None:
    """Apply the suppression ledger to one scored row and tally the run report."""
    ev = _load_evidence(row)
    rank = ev.get("rank") or {}
    if (kind, proposal_id) in suppressed:
        ev["surfaced"] = False
        rank["dismissed"] = True
        ev["rank"] = rank
        row["evidence"] = json.dumps(ev, sort_keys=True)

    if ev.get("surfaced"):
        counts["surfaced"] += 1
    elif rank.get("blocked"):
        counts["blocked"] += 1
    else:
        counts["suppressed"] += 1
    if rank.get("dismissed"):
        counts["dismissed"] += 1


def mark_surfaced(
    domain_rows: list[dict[str, Any]],
    page_rows: list[dict[str, Any]],
    suppressions: Sequence[Mapping[str, Any]] = (),
) -> dict[str, int]:
    """Read the suppression ledger and finalize ``surfaced`` on every scored row,
    then return the run report ``{surfaced, suppressed, blocked, dismissed}`` (MV-D26).

    A proposal a curator already dismissed — matched on ``(kind, proposal_id)`` at
    metastore grain — is set ``surfaced=false`` so the next serve never shows it (a
    rejected ``reassign`` stays suppressed on re-run: it is a ``reassign`` row and its
    suppression carries ``proposal_kind="reassign"``). The wheel issues a read-only
    ledger fetch upstream; this function receives the rows and holds no write.
    """
    suppressed = _suppression_keys(suppressions)
    counts = {"surfaced": 0, "suppressed": 0, "blocked": 0, "dismissed": 0}
    for row in domain_rows:
        _finalize_row(row, kind=transforms.proposal_kind_of(row), proposal_id=str(row.get("domain_id") or ""), suppressed=suppressed, counts=counts)
    for row in page_rows:
        _finalize_row(row, kind="page", proposal_id=str(row.get("page_id") or ""), suppressed=suppressed, counts=counts)
    return counts


__all__ = [
    "FACTOR_WEIGHTS",
    "PROVENANCE_TIER_STRENGTH",
    "RankSignals",
    "apply_alignment",
    "apply_context_prior",
    "blend",
    "context_gap_hypotheses",
    "mark_surfaced",
    "outranks",
    "pii_name_reject",
    "policy_conform",
    "provenance_ladder",
    "score_proposals",
]
