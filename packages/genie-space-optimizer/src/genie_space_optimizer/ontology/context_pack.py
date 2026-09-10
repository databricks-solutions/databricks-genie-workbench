"""External Context Pack resolver (Phase 4 Stage B, MV-D38/D44/D46/D47/D50) — wheel-native.

Resolves the enabled context sources **once per run at BATCH identity** (MV-D50) into the
architecture §6 ``ContextPack`` — a versioned, cached, self-validated INTERNAL artifact that
steers naming and Page Recent-context as a **read-only prior**. It never touches structure,
never writes a governed tag, and never outranks a T0/curated fact (MV-D38/D35).

The pack is resolved ONLY when the tier is enabled AND ≥1 source is available; with the
toggle off (default) the job never calls :func:`resolve_context_pack`, so a materialize run
is **byte-identical estate-only** (MV-D44). Every external leaf is a :class:`Provenanced`
(tier / source_url / source_kind / as_of / confidence / decay_weight). A T1–T3 number with no
``source_url`` is DROPPED (MV-D38 rail 2).

**Self-validation before it steers naming** (MV-D38, architecture §6 lifecycle):

* **firewall-by-class** — every pack section maps to a naming-family target; the
  :mod:`~genie_space_optimizer.ontology.context_firewall` carve proves none can reach a
  membership / measure / certification writer.
* **no-unsourced-numbers** — :meth:`LeakageOracle.unsourced_number_leaks` drops a figure
  leaf with no citable URL.
* **PII scan** — :meth:`LeakageOracle.tag_name_leaks` quarantines a tag-name-bound leaf
  (a canonical-domain name or a synonym) that would leak PII.
* **confidence-gate** — a low-confidence company→industry map (``gate_confidence < τ``)
  suppresses the canonical-domain gap-check (don't hallucinate a missing domain off a bad
  sector guess).

FAIL ⇒ drop the leaf / skip the pack. The two read-only plug-points that CONSUME the pack —
L4/L5 naming + gap hypotheses (``rank.py``) and Page Recent-context (:func:`apply_recent_context`)
— never let it reach structure.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from genie_space_optimizer.ontology import web_search
from genie_space_optimizer.ontology.context_firewall import (
    FORBIDDEN_TARGETS,
    influence_allows,
)

logger = logging.getLogger(__name__)

# The confidence-gate threshold (MV-D38 rail 6): an industry resolution below this
# suppresses the canonical-domain gap-check. Conservative default; the job may override.
DEFAULT_GATE_TAU = 0.5

# T3 web facts decay with age; T2 canonical models are stable; T0/T1 don't decay
# (MV-D38 rail 5). Applied at read time — the resolver stamps the initial weight.
_DECAY_BY_TIER: dict[str, float] = {"T0": 1.0, "T1": 1.0, "T2": 1.0, "T3": 0.8}

# The section → firewall target map (MV-D38): which naming-family target each pack
# section may steer. Used both to self-validate (positive guard) and to bind consumers.
SECTION_TARGET: dict[str, str] = {
    "industry": "naming",
    "canonical_domains": "naming",
    "lexicon": "synonym",
    "financial_context": "recent_context",
    "regulatory_notes": "recent_context",
    "competitors": "naming",
}


@dataclass(frozen=True)
class Provenanced:
    """The provenance envelope (architecture §6) — the atomic external leaf, so L6
    ranking treats pack facts and graph facts uniformly. ``source_url`` is REQUIRED for a
    T1–T3 number (else the leaf is dropped). ``decay_weight`` is recency-adjusted at read
    time; T3 web facts decay, T0/T1/T2 don't."""
    value: Any
    tier: str  # "T0" | "T1" | "T2" | "T3"
    source_url: str | None
    source_kind: str  # system_table|filing|industry_model|standards_body|web|llm_synthesis
    as_of: str
    confidence: float = 0.5
    decay_weight: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ContextPack:
    """The internal Context Pack (architecture §6) — the user never edits this. Resolved
    once per company, cached + versioned, passed as a read-only prior into L4/L5. Carries
    the industry resolution, the advisory ``canonical_domains`` template (``is_template``
    ⇒ NEVER structural), the retrieval-critical ``lexicon``, Page Recent-context inputs
    (``financial_context`` / ``regulatory_notes``), ``competitors``, and the safety/audit
    log (``egress_log`` / ``pii_findings``)."""

    pack_id: str
    company_key: str
    version: int
    content_hash: str
    generated_at: str
    status: str = "active"  # active | stale | superseded
    generated_by: dict[str, Any] = field(default_factory=dict)
    refresh_policy: dict[str, Any] = field(default_factory=lambda: {"ttl_days": 30, "financials_ttl_days": 7})
    industry: dict[str, Any] = field(default_factory=dict)
    canonical_domains: list[dict[str, Any]] = field(default_factory=list)
    lexicon: list[dict[str, Any]] = field(default_factory=list)
    financial_context: list[dict[str, Any]] = field(default_factory=list)
    regulatory_notes: list[dict[str, Any]] = field(default_factory=list)
    competitors: list[dict[str, Any]] = field(default_factory=list)
    egress_log: list[dict[str, Any]] = field(default_factory=list)
    pii_findings: list[dict[str, Any]] = field(default_factory=list)
    # Derived, internal: the confidence-gate verdict (MV-D38 rail 6). True ⇒ the L5
    # canonical-domain gap-check is suppressed (a low-confidence industry guess).
    gap_check_suppressed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "pack_id": self.pack_id,
            "company_key": self.company_key,
            "version": self.version,
            "content_hash": self.content_hash,
            "generated_at": self.generated_at,
            "status": self.status,
            "generated_by": self.generated_by,
            "refresh_policy": self.refresh_policy,
            "industry": self.industry,
            "canonical_domains": self.canonical_domains,
            "lexicon": self.lexicon,
            "financial_context": self.financial_context,
            "regulatory_notes": self.regulatory_notes,
            "competitors": self.competitors,
            "egress_log": self.egress_log,
            "pii_findings": self.pii_findings,
            "gap_check_suppressed": self.gap_check_suppressed,
        }

    # ── Read-only accessors the plug-points consume ─────────────────────────
    def canonical_domain_names(self) -> list[tuple[str, dict[str, Any]]]:
        """``[(name, name_leaf)]`` — the advisory business-language domain names + their
        provenance envelope. A pure read; the naming plug-point (``rank.py``) decides
        whether any becomes a name (never over a T0/curated fact)."""
        out: list[tuple[str, dict[str, Any]]] = []
        for cd in self.canonical_domains:
            leaf = cd.get("name") if isinstance(cd, dict) else None
            if isinstance(leaf, dict) and str(leaf.get("value") or "").strip():
                out.append((str(leaf["value"]).strip(), leaf))
        return out


# ── The oracle (self-validation) ────────────────────────────────────────────


def _default_oracle() -> Any:
    """A corpus-less ``LeakageOracle`` — enough for the intrinsic PII + unsourced-number
    rails (both corpus-independent). Lazy-imported so this module stays importable without
    the optimization package's heavier deps at module scope."""
    from genie_space_optimizer.optimization.leakage import LeakageOracle

    return LeakageOracle()


def _provenanced(
    value: Any, *, tier: str, source_url: str | None, source_kind: str, as_of: str,
    confidence: float = 0.5,
) -> Provenanced:
    return Provenanced(
        value=value, tier=tier, source_url=source_url, source_kind=source_kind,
        as_of=as_of, confidence=round(float(confidence), 6),
        decay_weight=_DECAY_BY_TIER.get(tier, 1.0),
    )


def _leaf_ok(leaf: Provenanced, *, target: str, oracle: Any, is_name_bound: bool,
             pii_findings: list[dict[str, Any]]) -> bool:
    """Self-validate ONE provenanced leaf (MV-D38). Returns True to keep it.

    Order (all must pass): firewall-by-class (the section's target must be a naming-family
    target, never a forbidden writer) → no-unsourced-numbers (a figure needs a URL) →
    PII scan on tag-name-bound text (quarantine a name/synonym that leaks PII)."""
    # firewall-by-class — the section's target is reachable by an external source, and NEVER
    # a membership/measure/certification writer (the structural carve, checked first).
    if target in FORBIDDEN_TARGETS or not influence_allows(_EXTERNAL_SHIM, target):
        return False
    # T0 leaves are internal-verified (no external rail); T1–T3 external leaves run the rails.
    if leaf.tier != "T0":
        text = leaf.value if isinstance(leaf.value, str) else json.dumps(leaf.value, default=str)
        hit, _why = oracle.unsourced_number_leaks(text, leaf.source_url)
        if hit:
            return False
        # Labeled + dated rail (§6 rail 3): a T1–T3 leaf must carry a citable source.
        if not (leaf.source_url and str(leaf.source_url).strip()):
            return False
    if is_name_bound:
        pii_hit, _reason = oracle.tag_name_leaks(str(leaf.value))
        if pii_hit:
            pii_findings.append({"field_path": target, "action": "blocked"})
            return False
    return True


# A minimal external-class shim for the firewall predicate (the resolver's leaves come
# from external sources; the firewall reads only ``.klass``).
class _ExternalShim:
    klass = "external"


_EXTERNAL_SHIM = _ExternalShim()


# ── The resolver (batch identity, once per run) ──────────────────────────────

# A search function: ``(query) -> list[web_search.WebResult]``. Injectable so the job
# wires the AI-Gateway ladder and tests drive deterministic results.
SearchFn = Callable[[str], "Sequence[web_search.WebResult]"]
# An LLM synthesis function: ``(prompt) -> str`` (JSON text). Optional; absent ⇒ the pack
# is built from search results alone (names from result titles, no synthesis).
LlmFn = Callable[[str], str]


def _company_key(company: str | None) -> str:
    """A stable, lowercased key for a company name (the pack's per-company grain)."""
    return "".join(ch for ch in (company or "").strip().lower() if ch.isalnum() or ch in ("-", "_", " ")).strip() or "unknown"


def _content_hash(pack_content: Mapping[str, Any]) -> str:
    """A deterministic SHA-256 over the validated pack CONTENT (excludes volatile envelope
    fields: pack_id / content_hash / generated_at / generated_by). Same estate + same
    enabled sources ⇒ same hash (the §11 determinism guard)."""
    volatile = {"pack_id", "content_hash", "generated_at", "generated_by"}
    stable = {k: v for k, v in pack_content.items() if k not in volatile}
    return hashlib.sha256(json.dumps(stable, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _default_search_fn(w: Any, enabled_providers: Iterable[str], hipaa_baa: bool) -> SearchFn:
    provs = tuple(enabled_providers)

    def _search(query: str) -> Sequence[web_search.WebResult]:
        return web_search.web_search(
            query, w=w, enabled_providers=provs, hipaa_baa=hipaa_baa,
        )

    return _search


def _synthesize(llm_fn: LlmFn | None, prompt: str) -> dict[str, Any]:
    """Call the injected LLM for JSON synthesis; degrade to ``{}`` on any failure
    (degrade-not-hang, MV-D43). Never raises."""
    if llm_fn is None:
        return {}
    try:
        raw = llm_fn(prompt)
    except Exception as e:  # noqa: BLE001 — synthesis is best-effort
        logger.info("context-pack synthesis degraded: %s", e)
        return {}
    if not raw or not isinstance(raw, str):
        return {}
    try:
        # Tolerate a fenced or prefixed JSON blob.
        start, end = raw.find("{"), raw.rfind("}")
        if start >= 0 and end > start:
            return json.loads(raw[start : end + 1])
    except (ValueError, TypeError):
        return {}
    return {}


def resolve_context_pack(
    *,
    company: str | None,
    enabled_providers: Iterable[str] = (),
    industry_hint: str | None = None,
    search_fn: SearchFn | None = None,
    llm_fn: LlmFn | None = None,
    oracle: Any | None = None,
    gate_tau: float = DEFAULT_GATE_TAU,
    as_of: str | None = None,
    w: Any = None,
    hipaa_baa: bool = False,
    version: int = 1,
    model: str | None = None,
) -> ContextPack | None:
    """Resolve the enabled sources ONCE into a validated :class:`ContextPack`, or ``None``.

    Returns ``None`` — the estate-only degrade (MV-D43/D44) — when: no provider is enabled;
    HIPAA/BAA is set; the ladder yields nothing; or every leaf fails self-validation (so the
    materialize run writes zero pack rows and stays estate-only). Deterministic: the same
    estate + same enabled sources (+ same injected search/LLM) yield the same ``content_hash``.

    This is the batch-identity resolver (MV-D50) — the caller owns identity (the injected
    ``w`` / ``search_fn`` / ``llm_fn``, MV-D65). It NEVER raises: web/LLM failures degrade to
    an empty or skipped pack."""
    providers = [str(p) for p in enabled_providers]
    if hipaa_baa or not providers:
        return None
    as_of = as_of or datetime.now(timezone.utc).date().isoformat()
    oracle = oracle or _default_oracle()
    search = search_fn if search_fn is not None else _default_search_fn(w, providers, hipaa_baa)
    ckey = _company_key(company)

    # ── Egress: one batched set of queries (industry / competitors / regulatory) ──
    egress_log: list[dict[str, Any]] = []
    results_by_topic: dict[str, list[web_search.WebResult]] = {}
    for topic, query in _queries(company, industry_hint):
        try:
            hits = list(search(query) or [])
        except Exception as e:  # noqa: BLE001 — a broken search degrades to no hits
            logger.info("context-pack search degraded (%s): %s", topic, e)
            hits = []
        results_by_topic[topic] = hits
        for h in hits:
            if h.url:
                egress_log.append({"url": h.url, "fetched_at": as_of, "sha256": _sha(h.url)})

    all_hits = [h for hits in results_by_topic.values() for h in hits]
    if not all_hits:
        return None  # estate-only: nothing resolved (MV-D43)

    # ── LLM synthesis (optional) → the pack sections, each a Provenanced leaf ──
    synth = _synthesize(llm_fn, _synthesis_prompt(company, all_hits))
    pii_findings: list[dict[str, Any]] = []

    industry = _build_industry(synth, results_by_topic.get("industry", []), as_of, oracle, pii_findings)
    gate_conf = float(industry.get("gate_confidence", 0.0) or 0.0)
    gap_suppressed = gate_conf < float(gate_tau)

    canonical_domains = _build_canonical_domains(synth, results_by_topic.get("industry", []), as_of, oracle, pii_findings)
    lexicon = _build_lexicon(synth, all_hits, as_of, oracle, pii_findings)
    financial_context = _build_financial_context(synth, results_by_topic.get("industry", []), as_of, oracle)
    regulatory_notes = _build_regulatory_notes(synth, results_by_topic.get("regulatory", []), as_of, oracle)
    competitors = _build_competitors(synth, results_by_topic.get("competitors", []), as_of, oracle, pii_findings)

    # Nothing survived self-validation ⇒ skip the pack (estate-only, no rows).
    if not any((industry.get("label"), canonical_domains, lexicon, financial_context, regulatory_notes, competitors)):
        return None

    content = {
        "company_key": ckey,
        "version": int(version),
        "status": "active",
        "refresh_policy": {"ttl_days": 30, "financials_ttl_days": 7},
        "industry": industry,
        "canonical_domains": canonical_domains,
        "lexicon": lexicon,
        "financial_context": financial_context,
        "regulatory_notes": regulatory_notes,
        "competitors": competitors,
        "egress_log": sorted(egress_log, key=lambda e: e["url"]),
        "pii_findings": pii_findings,
        "gap_check_suppressed": gap_suppressed,
    }
    chash = _content_hash(content)
    return ContextPack(
        pack_id=f"pack_{ckey}_{version}_{chash[:12]}",
        company_key=ckey,
        version=int(version),
        content_hash=chash,
        generated_at=datetime.now(timezone.utc).isoformat(),
        status="active",
        generated_by={"model": model or "estate-only", "egress_log_ref": f"pack_{ckey}_{version}"},
        refresh_policy=content["refresh_policy"],
        industry=industry,
        canonical_domains=canonical_domains,
        lexicon=lexicon,
        financial_context=financial_context,
        regulatory_notes=regulatory_notes,
        competitors=competitors,
        egress_log=content["egress_log"],
        pii_findings=pii_findings,
        gap_check_suppressed=gap_suppressed,
    )


def _sha(text: str) -> str:
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()


def _queries(company: str | None, industry_hint: str | None) -> list[tuple[str, str]]:
    """The batched query set (one egress burst). Deterministic order for a stable hash."""
    c = (company or "").strip() or (industry_hint or "").strip()
    if not c:
        return []
    q: list[tuple[str, str]] = [
        ("industry", f"{c} company industry sector business segments"),
        ("competitors", f"{c} main competitors"),
        ("regulatory", f"{c} industry regulatory requirements"),
    ]
    return q


def _synthesis_prompt(company: str | None, hits: Sequence[web_search.WebResult]) -> str:
    lines = [
        "You classify a company's industry and canonical business domains from public web results.",
        f"Company: {company or 'unknown'}",
        "Return STRICT JSON with keys: industry {label, naics, gics_sector, gate_confidence 0-1},",
        "canonical_domains [{name, description, source_url}], lexicon [{term, synonyms, synonym_class, source_url}],",
        "competitors [{name, source_url}]. Cite a source_url for every item from the results below.",
        "Results:",
    ]
    for h in hits[:12]:
        lines.append(f"- {h.title} | {h.url} | {h.snippet}")
    return "\n".join(lines)


def _first_url(results: Sequence[web_search.WebResult]) -> str | None:
    for r in results:
        if r.url:
            return r.url
    return None


def _build_industry(
    synth: Mapping[str, Any], results: Sequence[web_search.WebResult], as_of: str,
    oracle: Any, pii_findings: list[dict[str, Any]],
) -> dict[str, Any]:
    """The industry resolution — the ONLY field with an optional user confirm (§6). The
    label/codes are T2 industry-canonical leaves; a code with no source_url is dropped."""
    ind = synth.get("industry") if isinstance(synth, Mapping) else None
    ind = ind if isinstance(ind, Mapping) else {}
    src = str(ind.get("source_url") or "") or (_first_url(results) or "")
    label_val = str(ind.get("label") or "").strip()
    out: dict[str, Any] = {
        "user_confirmed": False,
        "gate_confidence": float(ind.get("gate_confidence", 0.4) or 0.4) if label_val else 0.0,
    }
    if label_val:
        leaf = _provenanced(label_val, tier="T2", source_url=src or None, source_kind="industry_model",
                            as_of=as_of, confidence=out["gate_confidence"])
        if _leaf_ok(leaf, target="naming", oracle=oracle, is_name_bound=True, pii_findings=pii_findings):
            out["label"] = leaf.to_dict()
    codes = {"naics": str(ind.get("naics") or ""), "gics_sector": str(ind.get("gics_sector") or "")}
    if (codes["naics"] or codes["gics_sector"]) and src:
        codes_leaf = _provenanced(codes, tier="T2", source_url=src, source_kind="standards_body",
                                  as_of=as_of, confidence=out["gate_confidence"])
        if _leaf_ok(codes_leaf, target="naming", oracle=oracle, is_name_bound=False, pii_findings=pii_findings):
            out["codes"] = codes_leaf.to_dict()
    return out


def _build_canonical_domains(
    synth: Mapping[str, Any], results: Sequence[web_search.WebResult], as_of: str,
    oracle: Any, pii_findings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """The advisory ``canonical_domains`` template (``is_template: true`` ⇒ NEVER
    structural). Each name/description is a T2 leaf; a PII / unsourced leaf is dropped."""
    raw = synth.get("canonical_domains") if isinstance(synth, Mapping) else None
    raw = raw if isinstance(raw, list) else []
    out: list[dict[str, Any]] = []
    for cd in raw:
        if not isinstance(cd, Mapping):
            continue
        name = str(cd.get("name") or "").strip()
        src = str(cd.get("source_url") or "") or (_first_url(results) or "")
        if not name or not src:
            continue
        name_leaf = _provenanced(name, tier="T2", source_url=src, source_kind="industry_model", as_of=as_of, confidence=0.6)
        if not _leaf_ok(name_leaf, target="naming", oracle=oracle, is_name_bound=True, pii_findings=pii_findings):
            continue
        desc = str(cd.get("description") or "").strip()
        desc_leaf = _provenanced(desc, tier="T2", source_url=src, source_kind="industry_model", as_of=as_of, confidence=0.6) if desc else None
        entry: dict[str, Any] = {"name": name_leaf.to_dict(), "is_template": True}
        if desc_leaf and _leaf_ok(desc_leaf, target="description", oracle=oracle, is_name_bound=False, pii_findings=pii_findings):
            entry["description"] = desc_leaf.to_dict()
        out.append(entry)
    return sorted(out, key=lambda e: str(e["name"]["value"]).lower())


def _build_lexicon(
    synth: Mapping[str, Any], hits: Sequence[web_search.WebResult], as_of: str,
    oracle: Any, pii_findings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Retrieval-critical synonyms (the 4 classes). Each synonym is PII-scanned before it
    could become a name; an unsourced synonym is dropped."""
    raw = synth.get("lexicon") if isinstance(synth, Mapping) else None
    raw = raw if isinstance(raw, list) else []
    out: list[dict[str, Any]] = []
    for lx in raw:
        if not isinstance(lx, Mapping):
            continue
        term = str(lx.get("term") or "").strip()
        src = str(lx.get("source_url") or "")
        if not term or not src:
            continue
        syns: list[dict[str, Any]] = []
        for s in (lx.get("synonyms") or []):
            s = str(s).strip()
            if not s:
                continue
            leaf = _provenanced(s, tier="T3", source_url=src, source_kind="web", as_of=as_of, confidence=0.4)
            if _leaf_ok(leaf, target="synonym", oracle=oracle, is_name_bound=True, pii_findings=pii_findings):
                syns.append(leaf.to_dict())
        if not syns:
            continue
        out.append({
            "term": term,
            "synonyms": syns,
            "synonym_class": str(lx.get("synonym_class") or "jargon"),
            "pii_scanned": True,
        })
    return sorted(out, key=lambda e: e["term"].lower())


def _build_financial_context(
    synth: Mapping[str, Any], results: Sequence[web_search.WebResult], as_of: str, oracle: Any,
) -> list[dict[str, Any]]:
    """Page Recent-context financial overlay — ``certify-no``; a value with no URL+date is
    dropped (the no-unsourced-numbers rail runs hard here)."""
    raw = synth.get("financial_context") if isinstance(synth, Mapping) else None
    raw = raw if isinstance(raw, list) else []
    out: list[dict[str, Any]] = []
    for fc in raw:
        if not isinstance(fc, Mapping):
            continue
        value = str(fc.get("value") or "").strip()
        src = str(fc.get("source_url") or "")
        concept = str(fc.get("concept") or "").strip()
        if not value or not concept:
            continue
        leaf = _provenanced(value, tier="T1", source_url=src or None, source_kind="filing", as_of=as_of, confidence=0.7)
        # The figure MUST be sourced (no-unsourced-numbers) — _leaf_ok drops it otherwise.
        if not _leaf_ok(leaf, target="recent_context", oracle=None or oracle, is_name_bound=False, pii_findings=[]):
            continue
        entry: dict[str, Any] = {"concept": concept, "period": str(fc.get("period") or ""), "value": leaf.to_dict()}
        if fc.get("segment"):
            entry["segment"] = str(fc["segment"])
        out.append(entry)
    return sorted(out, key=lambda e: (e["concept"].lower(), e.get("period", "")))


def _build_regulatory_notes(
    synth: Mapping[str, Any], results: Sequence[web_search.WebResult], as_of: str, oracle: Any,
) -> list[dict[str, Any]]:
    """Regulatory context — informational only (the rule derives from an internal
    measure/column). Each scope is a sourced T2 leaf."""
    raw = synth.get("regulatory_notes") if isinstance(synth, Mapping) else None
    raw = raw if isinstance(raw, list) else []
    out: list[dict[str, Any]] = []
    for rn in raw:
        if not isinstance(rn, Mapping):
            continue
        regime = str(rn.get("regime") or "").strip()
        scope = str(rn.get("scope") or "").strip()
        src = str(rn.get("source_url") or "") or (_first_url(results) or "")
        if not regime or not scope or not src:
            continue
        scope_leaf = _provenanced(scope, tier="T2", source_url=src, source_kind="standards_body", as_of=as_of, confidence=0.6)
        if not _leaf_ok(scope_leaf, target="recent_context", oracle=oracle, is_name_bound=False, pii_findings=[]):
            continue
        hint = [str(h) for h in (rn.get("applies_to_hint") or []) if str(h).strip()]
        out.append({"regime": regime, "scope": scope_leaf.to_dict(), "applies_to_hint": sorted(hint)})
    return sorted(out, key=lambda e: e["regime"].lower())


def _build_competitors(
    synth: Mapping[str, Any], results: Sequence[web_search.WebResult], as_of: str,
    oracle: Any, pii_findings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Competitor context — T3 hints only (never a name alone). Names are PII-scanned."""
    raw = synth.get("competitors") if isinstance(synth, Mapping) else None
    raw = raw if isinstance(raw, list) else []
    out: list[dict[str, Any]] = []
    for co in raw:
        if not isinstance(co, Mapping):
            continue
        name = str(co.get("name") or "").strip()
        src = str(co.get("source_url") or "") or (_first_url(results) or "")
        if not name or not src:
            continue
        segs: list[dict[str, Any]] = []
        for s in (co.get("public_segments") or []):
            s = str(s).strip()
            if not s:
                continue
            leaf = _provenanced(s, tier="T3", source_url=src, source_kind="web", as_of=as_of, confidence=0.3)
            if _leaf_ok(leaf, target="naming", oracle=oracle, is_name_bound=False, pii_findings=pii_findings):
                segs.append(leaf.to_dict())
        # The competitor name itself is name-bound (PII-scanned) but T3 (never a name alone).
        if oracle.tag_name_leaks(name)[0]:
            pii_findings.append({"field_path": "competitors", "action": "blocked"})
            continue
        out.append({"name": name, "public_segments": segs})
    return sorted(out, key=lambda e: e["name"].lower())


# ── Plug-point 2: Page Recent-context overlay (MV-D28 subsumed) ──────────────


def apply_recent_context(page_rows: list[dict[str, Any]], pack: ContextPack | None) -> int:
    """Attach a labeled / sourced / dated **Recent-context overlay** to matching Page rows,
    IN PLACE (plug-point 2, MV-D28 subsumed). Returns the number of pages annotated.

    The overlay rides ``evidence["recent_context"]`` (no new column, MV-D49) as
    ``{text, sources, as_of, certify: False}`` — informational only. It NEVER flips
    ``page.certify`` (``certify-no``) and NEVER touches ``body`` / structure. ``pack=None``
    (the default estate-only path) is a NO-OP: zero pages annotated, byte-identical."""
    if pack is None:
        return 0
    overlays = _recent_context_index(pack)
    if not overlays:
        return 0
    annotated = 0
    for row in page_rows:
        hay = _page_haystack(row)
        matched = [ov for concept, ov in overlays if concept in hay]
        if not matched:
            continue
        ev = _load_row_evidence(row)
        # Deterministic: sort overlays by their text so the blob is stable across runs.
        blocks = sorted({ov["text"]: ov for ov in matched}.values(), key=lambda o: o["text"])
        ev["recent_context"] = {
            "certify": False,  # certify-no (MV-D28): informational overlay, never certified
            "as_of": pack.generated_at[:10],
            "items": blocks,
            "disclaimer": (
                "Informational context summarised from public sources as of "
                f"{pack.generated_at[:10]}. Not certified operational data."
            ),
        }
        row["evidence"] = json.dumps(ev, sort_keys=True)
        annotated += 1
    return annotated


def _recent_context_index(pack: ContextPack) -> list[tuple[str, dict[str, Any]]]:
    """``[(concept_lowercased, overlay)]`` from financial_context + regulatory_notes — the
    labeled/sourced/dated overlay blocks a Page may carry."""
    out: list[tuple[str, dict[str, Any]]] = []
    for fc in pack.financial_context:
        concept = str(fc.get("concept") or "").strip()
        val = fc.get("value") or {}
        if not concept or not isinstance(val, Mapping):
            continue
        out.append((concept.lower(), {
            "text": f"{concept}: {val.get('value')}" + (f" ({fc.get('period')})" if fc.get("period") else ""),
            "sources": [val.get("source_url")] if val.get("source_url") else [],
            "kind": "financial",
        }))
    for rn in pack.regulatory_notes:
        regime = str(rn.get("regime") or "").strip()
        scope = rn.get("scope") or {}
        if not regime or not isinstance(scope, Mapping):
            continue
        out.append((regime.lower(), {
            "text": f"{regime}: {scope.get('value')}",
            "sources": [scope.get("source_url")] if scope.get("source_url") else [],
            "kind": "regulatory",
        }))
    return out


def _page_haystack(row: Mapping[str, Any]) -> str:
    parts = [str(row.get("title") or "")]
    parts += [str(s) for s in (row.get("synonyms") or [])]
    return " ".join(parts).lower()


def _load_row_evidence(row: Mapping[str, Any]) -> dict[str, Any]:
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


# ── Persistence rows (additive DDL, MV-D49) ──────────────────────────────────


def pack_rows(
    pack: ContextPack, *, metastore_id: str, workspace_id: str, run_id: str, as_of: str,
) -> dict[str, list[dict[str, Any]]]:
    """Build the ``genie_ont_context_pack`` header row + the ``genie_ont_context_sources``
    per-leaf citation rows (MV-D49 additive shapes, §6). One immutable pack row per
    ``(metastore_id, company_key, version)``; one source row per provenanced leaf with a
    ``source_url`` (the egress audit index)."""
    industry = pack.industry or {}
    codes = (industry.get("codes") or {}).get("value") if isinstance(industry.get("codes"), Mapping) else None
    industry_code = ""
    if isinstance(codes, Mapping):
        industry_code = str(codes.get("naics") or codes.get("gics_sector") or "")
    pack_row = {
        "metastore_id": metastore_id,
        "workspace_id": workspace_id,
        "company_key": pack.company_key,
        "version": int(pack.version),
        "pack_id": pack.pack_id,
        "content_hash": pack.content_hash,
        "status": pack.status,
        "industry_code": industry_code,
        "gate_confidence": float(industry.get("gate_confidence", 0.0) or 0.0),
        "pack_json": json.dumps(pack.to_dict(), sort_keys=True, default=str),
        "generated_at": pack.generated_at,
        "run_id": run_id,
        "as_of": as_of,
    }
    source_rows: list[dict[str, Any]] = []
    for field_path, leaf in _iter_sourced_leaves(pack):
        source_rows.append({
            "metastore_id": metastore_id,
            "workspace_id": workspace_id,
            "pack_id": pack.pack_id,
            "field_path": field_path,
            "tier": str(leaf.get("tier") or ""),
            "source_url": str(leaf.get("source_url") or ""),
            "source_kind": str(leaf.get("source_kind") or ""),
            "as_of": str(leaf.get("as_of") or as_of),
            "sha256": _sha(str(leaf.get("source_url") or "")),
            "run_id": run_id,
        })
    # Deterministic ordering for a stable MERGE source.
    source_rows.sort(key=lambda r: (r["field_path"], r["source_url"]))
    return {"context_pack_rows": [pack_row], "context_source_rows": source_rows}


def _iter_sourced_leaves(pack: ContextPack):
    """Yield ``(field_path, leaf_dict)`` for every provenanced leaf carrying a source_url."""
    def _emit(path: str, leaf: Any):
        if isinstance(leaf, Mapping) and leaf.get("source_url"):
            yield path, leaf

    industry = pack.industry or {}
    for k in ("label", "codes"):
        yield from _emit(f"industry.{k}", industry.get(k))
    for i, cd in enumerate(pack.canonical_domains):
        yield from _emit(f"canonical_domains[{i}].name", cd.get("name"))
        yield from _emit(f"canonical_domains[{i}].description", cd.get("description"))
    for i, lx in enumerate(pack.lexicon):
        for j, syn in enumerate(lx.get("synonyms") or []):
            yield from _emit(f"lexicon[{i}].synonyms[{j}]", syn)
    for i, fc in enumerate(pack.financial_context):
        yield from _emit(f"financial_context[{i}].value", fc.get("value"))
    for i, rn in enumerate(pack.regulatory_notes):
        yield from _emit(f"regulatory_notes[{i}].scope", rn.get("scope"))
    for i, co in enumerate(pack.competitors):
        for j, seg in enumerate(co.get("public_segments") or []):
            yield from _emit(f"competitors[{i}].public_segments[{j}]", seg)


__all__ = [
    "ContextPack",
    "DEFAULT_GATE_TAU",
    "Provenanced",
    "SECTION_TARGET",
    "apply_recent_context",
    "pack_rows",
    "resolve_context_pack",
]
