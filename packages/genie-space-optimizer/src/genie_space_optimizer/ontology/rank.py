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
from collections.abc import Mapping, Sequence
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
    The invariant 17h enforces; pinned dormant by the §11 seam test."""
    return PROVENANCE_TIER_STRENGTH.get(tier_a, -1) > PROVENANCE_TIER_STRENGTH.get(tier_b, -1)


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
    already live inside a domain or name a governed conflict). Records the verdict on
    ``rank`` so the run report and the serve layer can read it."""
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
    # Tentative: surfaced iff it cleared threshold AND passed every firewall. The
    # ledger pass (mark_surfaced) may still flip it to false for a dismissed proposal.
    evidence["surfaced"] = bool(rank["tier"] is not None and not blocked)
    # Legitimacy bar (MV-D57) — top-level Domains only; below-bar rows are kept but
    # not surfaced, with an "add to existing domain" hint (§Appendix A junk pruning).
    if kind == "domain":
        _apply_legitimacy_gate(
            row, evidence, members, rank,
            min_tables=min_tables, min_schemas=min_schemas, require_connection=require_connection,
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
        )
    for row in page_rows:
        _score_row(row, kind="page", assets=_page_assets(row), signals=sig, oracle=oracle)


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
    "blend",
    "mark_surfaced",
    "outranks",
    "pii_name_reject",
    "policy_conform",
    "provenance_ladder",
    "score_proposals",
]
