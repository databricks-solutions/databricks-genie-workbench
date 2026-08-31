"""L5 Page-miner engine (Phase 3c) — canonical-concept detectors → LLM draft →
retrieval-gate validation → deterministic concept-anchored ``PageCandidate``s.

Pages are **account-level (metastore-grain, MV-D49) artifacts keyed to a canonical
CONCEPT (17d ``genie_ont_identity.canonical_id``), not to a single artifact.** The
engine resolves every candidate signal (measure / coded column) to its canonical
concept, aggregates ALL artifacts across the metastore that resolve to the same
concept (including artifacts in different sub-domains), gates on CORROBORATION (how
many independent artifacts back the concept), drafts each candidate's body with the
LLM, validates against the retrieval gates, and emits archetype-tagged Page
proposals with copy-ready Related/Sources.

Design discipline (architecture §5, ``page-archetypes.md``, ``genie-retrieval-notes.md``):

  * **Deterministic detectors, LLM prose only.** ``archetype``, ``source_fqns``,
    ``certify``, and the ``page_id`` are all deterministic; the injected drafter
    writes body prose only and never feeds the ``page_id`` (so idempotency holds as
    prose drifts). The ``page_id`` is a fingerprint of concept-level signals
    (``canonical_id``, ``archetype``, sorted key identifiers) — never the home
    ``domain_id`` (a concept keeps one Page even if it moves sub-domains) and never
    the LLM body.
  * **Reuse, do not fork.** ``mv_fingerprint`` (expression fingerprint + shape
    detection) is the ONLY measure comparator; ``er.canonical_id_of`` /
    ``er.pii_reject`` and ``transforms.token_set_sig`` are the ONLY identity/PII
    primitives; ``similarity.keyword_score`` is the ONLY dedupe scorer; the drafting
    LLM and the ``ask_genie`` routing validator are injected (lazy + degrade). No new
    comparator, no new similarity backend.
  * **Degrade, never block (MV-D43).** A drafter that is absent or raises yields a
    deterministic evidence-derived stub body + ``certify=false``; a per-candidate
    detector/validation error is logged and that candidate is skipped; an
    unreachable routing validator marks the Page ``unvalidated``.

This module writes NOTHING. It proposes ``PageCandidate``s; the materializer expands
them into ``genie_ont_pages`` rows and MERGEs them (metastore-scoped). It never
proposes a governed tag and never writes an Agent instruction (MV-D27); the
contradiction gate is READ-ONLY.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Literal, Mapping, Sequence

from genie_space_optimizer.ontology import er, similarity, transforms

logger = logging.getLogger(__name__)

Archetype = Literal[
    "Routing", "Disambiguation", "Guardrail", "Taxonomy",
    "Method", "Cross-domain", "Defaults", "Rule",
]

# Title prefixes (the eight archetypes; the prefix is part of the format — it is how
# the archetype is read back, page-archetypes.md §"Format template").
_TITLE_PREFIX: dict[str, str] = {a: f"[{a}]" for a in (
    "Routing", "Disambiguation", "Guardrail", "Taxonomy",
    "Method", "Cross-domain", "Defaults", "Rule",
)}

# Retrieval-gate thresholds (genie-retrieval-notes.md §"Synonyms").
_MIN_SYNONYMS = 3
_MIN_SYNONYM_CLASSES = 3       # of the four classes
# Estate corroboration gate (MV-D35, MV-D49): >=2 independent artifacts → full +
# certify-eligible; exactly 1 → low-confidence + certify=false.
_CORROBORATION_FULL = 2
# Coded-column cardinality ceiling for a [Taxonomy] page (a code list, not free text).
_TAXONOMY_MAX_CARDINALITY = 40

_BACKTICK_RE = re.compile(r"`([^`]+)`")
# A rule sentence that opens with one of these bare pronouns is chunk-unsafe — it
# leans on the title or a previous bullet for its subject (retrieval note rule 1).
_BARE_PRONOUN_RE = re.compile(r"^\s*(it|its|this|that|these|those|they|them|their)\b", re.IGNORECASE)
# Aggregate-shaped fragment inside prose (the contradiction gate lifts these to
# fingerprint them with mv_fingerprint — instructions are prose, not statements).
_AGG_FRAGMENT_RE = re.compile(r"\b(?:SUM|AVG|COUNT|MIN|MAX)\s*\([^()]*\)", re.IGNORECASE)


# ── Input signals (wheel-side dataclasses; NOT API models) ──────────────────


@dataclass(frozen=True)
class MeasureSignal:
    """One governed metric-view measure that expresses a concept.

    ``mv_fqn`` and ``source_fqns`` are member assets (identifier-gated); ``agent_fqns``
    are the serving Genie Agent(s) (Discover Related). ``domain_id`` is the signal's
    home sub-domain (provenance for picking the Page's home; never in the page_id)."""

    mv_fqn: str
    name: str
    expression: str = ""
    fmt: str = ""                       # measure format string (e.g. "0.00%", "$#,##0")
    source_fqns: tuple[str, ...] = ()
    agent_fqns: tuple[str, ...] = ()
    comment: str = ""
    domain_id: str = ""

    @property
    def ref(self) -> str:
        return f"{self.mv_fqn}.{self.name}"


@dataclass(frozen=True)
class ColumnSignal:
    """One low-cardinality coded column (a [Taxonomy] candidate)."""

    table_fqn: str
    column: str
    comment: str = ""
    distinct_values: tuple[str, ...] = ()
    governed: bool = False              # a governed code list → certify-eligible
    agent_fqns: tuple[str, ...] = ()
    domain_id: str = ""

    @property
    def ref(self) -> str:
        return f"{self.table_fqn}.{self.column}"


@dataclass(frozen=True)
class PageCandidate:
    """A concept-anchored Page proposal (maps 1:1 onto the genie_ont_pages columns;
    ``canonical_id`` / ``corroboration`` / ``confidence`` ride in ``evidence`` JSON —
    no new DDL, §4). ``score`` is written NULL/0.0 by the materializer — L6 ranking
    is 17g."""

    page_id: str
    canonical_id: str
    domain_id: str
    archetype: Archetype
    title: str
    body: str
    synonyms: tuple[str, ...]
    related_fqns: tuple[str, ...]
    source_fqns: tuple[str, ...]
    corroboration: int
    certify: bool
    evidence: dict
    confidence: float


# ── Concept aggregation (the anchor: 17d canonical_id, MV-D49) ──────────────


@dataclass
class _Concept:
    """All artifacts across the metastore that resolve to one canonical concept."""

    canonical_id: str
    measures: list[MeasureSignal] = field(default_factory=list)
    columns: list[ColumnSignal] = field(default_factory=list)

    def _all_agents(self) -> set[str]:
        out: set[str] = set()
        for m in self.measures:
            out.update(m.agent_fqns)
        for c in self.columns:
            out.update(c.agent_fqns)
        return out

    def contributing_artifacts(self) -> list[str]:
        """The independent artifacts backing the concept — distinct metric views /
        coded tables / serving Agents. Their count is the corroboration (MV-D35)."""
        arts: set[str] = {m.mv_fqn for m in self.measures}
        arts |= {c.table_fqn for c in self.columns}
        arts |= self._all_agents()
        return sorted(arts)

    def corroboration(self) -> int:
        return len(self.contributing_artifacts())

    def home_domain(self) -> str:
        """The sub-domain of the concept's strongest membership (most signals);
        deterministic tie-break by sorted domain_id."""
        counts = Counter(
            s.domain_id for s in (*self.measures, *self.columns) if s.domain_id
        )
        if not counts:
            return ""
        top = max(counts.values())
        return sorted(k for k, v in counts.items() if v == top)[0]


def _identity_index(verdicts: Sequence[Any]) -> dict[str, str]:
    """member_ref -> canonical_id, from 17d's ER merge verdicts (the anchor). Only
    merged groups contribute a shared canonical_id; the resolver falls back to a
    name-derived canonical id for refs the map does not cover."""
    out: dict[str, str] = {}
    for v in verdicts or []:
        cid = getattr(v, "canonical_id", None)
        for ref in getattr(v, "members", ()) or ():
            if cid:
                out[str(ref)] = cid
    return out


def resolve_canonical_id(ref: str, name: str, index: Mapping[str, str]) -> str:
    """Resolve a signal to its canonical concept. Prefers the 17d identity map (the
    anchor); otherwise derives a deterministic concept id from the normalized name
    (``transforms.token_set_sig`` → ``er.canonical_id_of`` — REUSE, no new scheme), so
    two artifacts naming the same concept collapse even across sub-domains."""
    if ref in index:
        return index[ref]
    return er.canonical_id_of([transforms.token_set_sig(name)])


def aggregate_concepts(
    measures: Sequence[MeasureSignal],
    columns: Sequence[ColumnSignal],
    index: Mapping[str, str],
) -> list[_Concept]:
    """Group all signals across the metastore by canonical concept (MV-D49). Order is
    deterministic (sorted by canonical_id)."""
    by_cid: dict[str, _Concept] = {}
    for m in measures:
        cid = resolve_canonical_id(m.ref, m.name, index)
        by_cid.setdefault(cid, _Concept(cid)).measures.append(m)
    for c in columns:
        cid = resolve_canonical_id(c.ref, c.column, index)
        by_cid.setdefault(cid, _Concept(cid)).columns.append(c)
    return [by_cid[k] for k in sorted(by_cid)]


# ── Deterministic id ────────────────────────────────────────────────────────


def page_id_of(canonical_id: str, archetype: str, key_ids: Sequence[str]) -> str:
    """``pg_<sha256(canonical_id | archetype | sorted key ids)>`` — concept-anchored
    deterministic signals ONLY (never domain_id, never the LLM body), so a concept
    keeps one stable Page across runs and across sub-domain moves (§7; the 17g
    suppression ledger needs the id invariant)."""
    payload = canonical_id + "|" + archetype + "|" + "|".join(sorted(key_ids))
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"pg_{digest[:16]}"


# ── sqlglot-backed measure inspection (reuse mv_fingerprint; degrade on parse) ─


def _measure_shapes(expr: str) -> tuple[Any, ...]:
    """ShapeMatch tuple for a measure expression (reuse mv_fingerprint), or () on any
    parse failure (MV-D43)."""
    if not expr:
        return ()
    from genie_space_optimizer.optimization import mv_fingerprint as mvfp
    for sql in (expr, f"SELECT {expr}", f"SELECT {expr} AS m FROM t"):
        try:
            got = mvfp.shapes_in_statement(sql)
        except Exception:  # noqa: BLE001 — degrade, never block
            got = ()
        if got:
            return got
    return ()


def _canonical_measure(expr: str) -> str:
    """Literal-erased, qualifier-stripped canonical TEXT of a measure expression
    (reuse mv_fingerprint.canonicalize_expr), or "" on parse failure. We compare
    canonical text — never the expression-grained hash, which must not leave
    mv_fingerprint (MV-D10) — so same/different-definition is detected without
    persisting a fingerprint key."""
    if not expr:
        return ""
    from genie_space_optimizer.optimization import mv_fingerprint as mvfp
    try:
        return mvfp.canonicalize_expr(expr)
    except Exception:  # noqa: BLE001
        return ""


def _leading_aggregate(expr: str) -> str:
    from genie_space_optimizer.optimization import mv_fingerprint as mvfp
    try:
        measures = mvfp.extract_measures(f"SELECT {expr} AS m FROM t")
    except Exception:  # noqa: BLE001
        return ""
    return measures[0].aggregate.upper() if measures else ""


def _is_ratio_measure(m: MeasureSignal) -> tuple[bool, tuple[Any, ...]]:
    """A measure is a [Guardrail] candidate when it is percentage-formatted, has a
    RATIO / PCT_OF_TOTAL shape, or is an AVG over a division (never average a rate)."""
    shapes = _measure_shapes(m.expression)
    ratio_shapes = tuple(
        s for s in shapes if getattr(s, "kind", "") in ("RATIO", "PCT_OF_TOTAL")
    )
    pct_format = "%" in (m.fmt or "")
    avg_of_rate = _leading_aggregate(m.expression) == "AVG" and "/" in (m.expression or "")
    return bool(ratio_shapes or pct_format or avg_of_rate), ratio_shapes


# ── Synonyms (deterministic in 17f — identifiers + comments + instructions) ─


_ACRONYM_RE = re.compile(r"^[A-Z][A-Z0-9]{1,7}$")
_STOP = frozenset({"the", "a", "an", "of", "for", "and", "or", "to", "in", "on", "by", "is"})


def _name_variants(name: str) -> list[str]:
    """Abbreviation-variant class: separator/case spellings of the concept name
    (``on_time`` → ``on time`` / ``ontime``)."""
    base = name.strip()
    out = [base]
    if "_" in base:
        out.append(base.replace("_", " "))
        out.append(base.replace("_", ""))
    if "-" in base:
        out.append(base.replace("-", " "))
        out.append(base.replace("-", ""))
    seen: set[str] = set()
    uniq: list[str] = []
    for v in out:
        k = v.casefold()
        if v and k not in seen:
            seen.add(k)
            uniq.append(v)
    return uniq


def _classify_term(term: str, name_tokens: set[str]) -> str | None:
    """Classify a vocabulary term into one of the four synonym classes, or None."""
    t = term.strip()
    if not t:
        return None
    if _ACRONYM_RE.match(t):
        return "acronym"
    words = [w for w in re.split(r"\s+", t) if w]
    if len(words) >= 2:
        return "casual"                 # a casual multi-word phrasing
    tok = t.casefold()
    if tok in _STOP or tok in name_tokens:
        return None
    return "jargon"                     # an internal single-word alias


def derive_synonyms(
    name: str, vocab: Sequence[str],
) -> tuple[tuple[str, ...], frozenset[str]]:
    """Deterministically derive synonyms + their classes from the concept name and a
    vocabulary bag (member identifiers, column comments, existing instruction text).
    Returns ``(synonyms, classes)``; the four classes are industry acronyms / casual
    language / internal jargon / abbreviation variants (genie-retrieval-notes.md).
    PII-echoing terms are dropped (er.pii_reject)."""
    name_tokens = {t for t in re.split(r"[^0-9a-zA-Z]+", name.casefold()) if t}
    synonyms: list[str] = []
    classes: set[str] = set()
    seen: set[str] = set()

    def _add(term: str, cls: str) -> None:
        k = term.casefold()
        if not term or k in seen or er.pii_reject(term):
            return
        seen.add(k)
        synonyms.append(term)
        classes.add(cls)

    for v in _name_variants(name):
        _add(v, "abbreviation")
    for raw in vocab:
        for candidate in re.split(r"[;,/]", str(raw or "")):
            cls = _classify_term(candidate, name_tokens)
            if cls:
                _add(candidate.strip(), cls)
    return tuple(synonyms), frozenset(classes)


def _concept_vocab(concept: _Concept, instructions: Sequence[str]) -> list[str]:
    """The deterministic vocabulary bag for a concept's synonyms/prose."""
    bag: list[str] = []
    for m in concept.measures:
        bag.append(m.name)
        if m.comment:
            bag.append(m.comment)
    for c in concept.columns:
        bag.append(c.column)
        if c.comment:
            bag.append(c.comment)
        bag.extend(c.distinct_values)
    bag.extend(instructions)
    return bag


# ── Body drafting (deterministic stub baseline + injected LLM prose) ────────


def _bt(identifier: str) -> str:
    return f"`{identifier}`"


def _stub_body(spec: "_DraftSpec") -> str:
    """A deterministic, evidence-derived body — the MV-D43 degrade baseline and the
    identifier-gate floor. Names real backticked identifiers inline (chunk-safe +
    specific by construction), so it always passes the structural gates."""
    lines = [f"Description: {spec.description}", "", "Definition:", f"  {spec.definition}"]
    if spec.rules:
        lines.append("")
        lines.append("Rules:")
        for r in spec.rules:
            lines.append(f"  - {r}")
    return "\n".join(lines)


def _draft_body(spec: "_DraftSpec", drafter: Callable[[dict], str] | None) -> tuple[str, bool]:
    """Draft the body. Returns ``(body, llm_ok)``: the injected drafter writes prose
    from the spec facts; if it is absent or raises, the deterministic stub is used and
    ``llm_ok`` is False (→ certify=false downgrade, §6)."""
    stub = _stub_body(spec)
    if drafter is None:
        return stub, False
    try:
        body = drafter(spec.facts())
    except Exception as exc:  # noqa: BLE001 — LLM down → stub, run still succeeds
        logger.info("ontology page drafting failed (%s); deterministic stub", exc)
        return stub, False
    body = (body or "").strip()
    return (body or stub), bool(body)


# ── Draft spec (what a detector emits; the LLM fills prose from its facts) ──


@dataclass(frozen=True)
class _DraftSpec:
    archetype: Archetype
    canonical_id: str
    domain_id: str
    concept_name: str
    title: str
    description: str
    definition: str
    rules: tuple[str, ...]
    key_ids: tuple[str, ...]          # sorted deterministic identifiers → page_id
    synonyms: tuple[str, ...]
    synonym_classes: frozenset[str]
    related_fqns: tuple[str, ...]
    source_fqns: tuple[str, ...]
    corroboration: int
    certify_shape: bool               # archetype/shape is authoritative (pre-corroboration)
    confidence: float
    evidence: dict
    nl_question: str = ""             # for optional [Routing] ask_genie validation

    def facts(self) -> dict:
        """The structured facts handed to the drafter — prose only, no structure."""
        return {
            "archetype": self.archetype,
            "title": self.title,
            "concept": self.concept_name,
            "description": self.description,
            "definition": self.definition,
            "rules": list(self.rules),
            "synonyms": list(self.synonyms),
            "sources": list(self.source_fqns),
            "related": list(self.related_fqns),
        }


# ── Retrieval gates (validator-enforced — §6) ───────────────────────────────


def _backticked(text: str) -> list[str]:
    return _BACKTICK_RE.findall(text or "")


def identifier_gate(body: str, source_fqns: Sequence[str], universe: frozenset[str]) -> tuple[bool, list[str]]:
    """Every backticked identifier in the body AND every Source FQN must EXIST in the
    member universe — an invented column/table fails (§6, the 17e naming discipline
    transposed). Returns ``(ok, invented)``."""
    invented = [i for i in _backticked(body) if i not in universe]
    invented += [f for f in source_fqns if f not in universe]
    return (not invented), invented


def _rule_lines(body: str) -> list[str]:
    """The Rules-section bullet sentences of a body (for chunk-safe/specificity)."""
    out: list[str] = []
    in_rules = False
    for raw in (body or "").splitlines():
        line = raw.strip()
        if line.lower().startswith("rules:"):
            in_rules = True
            continue
        if in_rules:
            if line.endswith(":") and not line.startswith("-"):
                break                    # next section
            if line.startswith("-"):
                out.append(line.lstrip("- ").strip())
    return out


def _definition_lines(body: str) -> list[str]:
    out: list[str] = []
    in_def = False
    for raw in (body or "").splitlines():
        line = raw.strip()
        if line.lower().startswith("definition:"):
            in_def = True
            continue
        if in_def:
            if line.endswith(":") and not line.startswith("-"):
                break
            if line:
                out.append(line.lstrip("- ").strip())
    return out


def chunk_safe_gate(body: str) -> bool:
    """Every rule sentence must stand alone — no bare pronoun opening a rule (retrieval
    rule 1). A rule leaning on the title/a previous bullet for its subject fails."""
    return not any(_BARE_PRONOUN_RE.match(r) for r in _rule_lines(body))


def specificity_gate(body: str) -> bool:
    """≥1 backticked identifier (or literal formula) in the Definition AND in each
    Rules bullet (retrieval rule 2 — vague guidance is invisible to the extractor)."""
    if not any(_backticked(l) for l in _definition_lines(body)):
        return False
    return all(_backticked(r) for r in _rule_lines(body))


def synonyms_gate(synonyms: Sequence[str], classes: frozenset[str]) -> bool:
    """≥3 synonyms spanning ≥3 of the four classes (retrieval rule 3)."""
    return len(synonyms) >= _MIN_SYNONYMS and len(classes) >= _MIN_SYNONYM_CLASSES


def contradicts_instructions(spec: _DraftSpec, measures: Sequence[MeasureSignal], instructions: Sequence[str]) -> bool:
    """READ-ONLY contradiction gate (MV-D35): reuse the mv_fingerprint expression
    comparator (NOT a new comparator). A hit is when an existing instruction names the
    concept but asserts a measure whose canonical fingerprint DIFFERS from every
    fingerprint the Page's Sources carry — a same-term/different-definition conflict.
    Never writes back; the caller downgrades the candidate to CONFLICT for 17g."""
    page_defs = {d for d in (_canonical_measure(m.expression) for m in measures) if d}
    if not page_defs:
        return False
    name_l = spec.concept_name.casefold()
    for text in instructions or ():
        if name_l not in (text or "").casefold():
            continue
        # Reuse the mv_fingerprint canonicalizer on any aggregate-shaped fragment the
        # instruction asserts for this concept (prose is not a parseable statement, so
        # we lift the fragments first). A canonical definition the Page's Sources do
        # not carry is a same-term/different-definition contradiction.
        for frag in _AGG_FRAGMENT_RE.findall(text or ""):
            d = _canonical_measure(frag)
            if d and d not in page_defs:
                return True
    return False


# ── Identifier universe (the identifier gate's allowlist) ───────────────────


def build_universe(
    measures: Sequence[MeasureSignal],
    columns: Sequence[ColumnSignal],
    members: Sequence[str],
) -> frozenset[str]:
    """Every real identifier the miner may backtick / cite as a Source — member
    assets, metric-view + measure pointers, source tables, coded-column pointers,
    serving Agents. Anything outside this set is invented and fails the gate."""
    u: set[str] = set(members)
    for m in measures:
        u.update({m.mv_fqn, m.ref, m.name, *m.source_fqns, *m.agent_fqns})
    for c in columns:
        u.update({c.table_fqn, c.ref, c.column, *c.agent_fqns})
    return frozenset(u)


# ── Deterministic per-archetype detectors (LLM-free) ────────────────────────


def _distinct_definitions(measures: Sequence[MeasureSignal]) -> set[str]:
    """The distinct canonical definitions among a concept's measures (≥2 → a genuine
    same-term/different-expression conflict → [Disambiguation])."""
    return {(_canonical_measure(m.expression) or m.expression) for m in measures if (m.expression or "")}


def _measure_detector(concept: _Concept, instructions: Sequence[str]) -> _DraftSpec | None:
    """One measure concept → exactly ONE Page: Disambiguation on a genuine expression
    conflict, else Guardrail for a rate/percentage, else Routing (the canonical
    answer). All corroborating measures aggregate into Sources."""
    ms = sorted(concept.measures, key=lambda m: m.ref)
    if not ms:
        return None
    name = ms[0].name
    cid = concept.canonical_id
    dom = concept.home_domain()
    corr = concept.corroboration()
    agents = sorted({a for m in ms for a in m.agent_fqns})
    mv_sources = sorted({m.mv_fqn for m in ms} | {s for m in ms for s in m.source_fqns})
    synonyms, classes = derive_synonyms(name, _concept_vocab(concept, instructions))
    key_ids = tuple(sorted(m.ref for m in ms))
    base_ev = {"contributing_artifacts": concept.contributing_artifacts()}

    defs = _distinct_definitions(ms)
    if len(defs) >= 2:                         # same term, several valid answers
        rules = tuple(
            f"When asked for \"{name}\", {_bt(m.ref)} answers the {m.mv_fqn.split('.')[-1]} grain; "
            f"confirm which is meant before writing SQL."
            for m in ms
        )
        return _DraftSpec(
            archetype="Disambiguation", canonical_id=cid, domain_id=dom, concept_name=name,
            title=f"{_TITLE_PREFIX['Disambiguation']} {name}",
            description=f"\"{name}\" resolves to several governed measures — pick the grain the question means.",
            definition=(
                f"\"{name}\" is defined differently across the estate: "
                + ", ".join(_bt(m.ref) for m in ms)
                + ". These are not interchangeable — choose by the question's grain."
            ),
            rules=rules, key_ids=key_ids, synonyms=synonyms, synonym_classes=classes,
            related_fqns=tuple(agents), source_fqns=tuple(mv_sources), corroboration=corr,
            certify_shape=True, confidence=0.8,
            evidence={**base_ev, "conflicting_definitions": sorted(defs)},
        )

    ratio = next((m for m in ms if _is_ratio_measure(m)[0]), None)
    if ratio is not None:                      # never average a rate
        _, ratio_shapes = _is_ratio_measure(ratio)
        rules = (
            f"Never average {_bt(ratio.ref)} across rows or periods; recompute it from its "
            f"numerator and denominator inside {_bt(ratio.mv_fqn)}.",
        )
        return _DraftSpec(
            archetype="Guardrail", canonical_id=cid, domain_id=dom, concept_name=name,
            title=f"{_TITLE_PREFIX['Guardrail']} {name}",
            description=f"{name} is a non-additive rate — recompute from numerator/denominator, never average.",
            definition=(
                f"{_bt(ratio.ref)} is a ratio measure evaluated inside {_bt(ratio.mv_fqn)}; "
                f"averaging pre-computed rates gives a wrong answer."
            ),
            rules=rules, key_ids=key_ids, synonyms=synonyms, synonym_classes=classes,
            related_fqns=tuple(agents), source_fqns=tuple(mv_sources), corroboration=corr,
            certify_shape=True, confidence=0.75,
            evidence={**base_ev, "shapes": [getattr(s, "kind", "") for s in ratio_shapes], "fmt": ratio.fmt},
        )

    primary = ms[0]                            # the canonical routing answer
    rules = (
        f"Route \"{name}\" to {_bt(primary.ref)}; do not hand-write the aggregate over "
        f"{_bt(primary.mv_fqn)}'s source tables.",
    )
    return _DraftSpec(
        archetype="Routing", canonical_id=cid, domain_id=dom, concept_name=name,
        title=f"{_TITLE_PREFIX['Routing']} {name}",
        description=f"{name} — answer from the governed metric view {primary.mv_fqn}.",
        definition=(
            f"For \"{name}\", answer from {_bt(primary.mv_fqn)} using its {_bt(primary.name)} measure — "
            f"never a raw aggregate over its source tables."
        ),
        rules=rules, key_ids=key_ids, synonyms=synonyms, synonym_classes=classes,
        related_fqns=tuple(agents), source_fqns=tuple(mv_sources), corroboration=corr,
        certify_shape=True, confidence=0.7,
        evidence={**base_ev, "canonical_definition": _canonical_measure(primary.expression)},
        nl_question=f"what is {name.replace('_', ' ')}",
    )


def _taxonomy_detector(concept: _Concept, instructions: Sequence[str]) -> _DraftSpec | None:
    """Coded columns for one concept → a [Taxonomy] Page decoding the code list.
    Certify only for a governed code list (else no — page-archetypes.md)."""
    coded = [
        c for c in concept.columns
        if c.distinct_values and len(c.distinct_values) <= _TAXONOMY_MAX_CARDINALITY
    ]
    if not coded:
        return None
    coded = sorted(coded, key=lambda c: c.ref)
    col = coded[0]
    cid = concept.canonical_id
    dom = concept.home_domain()
    corr = concept.corroboration()
    agents = sorted({a for c in coded for a in c.agent_fqns})
    sources = sorted({c.table_fqn for c in coded})
    vocab = _concept_vocab(concept, instructions)
    synonyms, classes = derive_synonyms(col.column, vocab)
    key_ids = tuple(sorted(c.ref for c in coded))
    values = ", ".join(col.distinct_values[:12])
    return _DraftSpec(
        archetype="Taxonomy", canonical_id=cid, domain_id=dom, concept_name=col.column,
        title=f"{_TITLE_PREFIX['Taxonomy']} {col.column}",
        description=f"{col.column} is a coded column — decode its values before filtering or grouping.",
        definition=(
            f"{_bt(col.ref)} holds a fixed code list ({values}). Decode each code to its business "
            f"meaning; do not treat the raw codes as labels."
        ),
        rules=(), key_ids=key_ids, synonyms=synonyms, synonym_classes=classes,
        related_fqns=tuple(agents), source_fqns=tuple(sources), corroboration=corr,
        certify_shape=all(c.governed for c in coded), confidence=0.5,
        evidence={"contributing_artifacts": concept.contributing_artifacts(),
                  "distinct_values": list(col.distinct_values), "governed": all(c.governed for c in coded)},
    )


def detect_concept(concept: _Concept, instructions: Sequence[str]) -> list[_DraftSpec]:
    """All Page specs a concept yields (measures → one measure Page; coded columns → a
    Taxonomy Page). [Method]/[Cross-domain]/[Defaults]/[Rule] are signal-gated and
    dormant in the offline slice — their unambiguous signals (method families,
    join-spine, standard filters, structural breaks) are not among the offline
    reader's inputs, so "No signal → nothing" (§1.1)."""
    specs: list[_DraftSpec] = []
    m = _measure_detector(concept, instructions)
    if m is not None:
        specs.append(m)
    t = _taxonomy_detector(concept, instructions)
    if t is not None:
        specs.append(t)
    return specs


# ── Draft → validate → certify → PageCandidate ──────────────────────────────


def _validate_routing(spec: _DraftSpec, routing_validator: Callable[[str, str], bool] | None) -> bool | None:
    """Optional ask_genie confirmation for a [Routing] Page — does the NL question
    resolve to the intended measure? Returns True/False, or None ("unvalidated") when
    the validator is absent or unreachable (degrade, never block — MV-D43)."""
    if spec.archetype != "Routing" or routing_validator is None or not spec.key_ids:
        return None
    try:
        return bool(routing_validator(spec.nl_question, spec.key_ids[0]))
    except Exception as exc:  # noqa: BLE001 — Genie unreachable on the job → unvalidated
        logger.info("ontology routing validation failed (%s); marking unvalidated", exc)
        return None


def _finalize(
    spec: _DraftSpec,
    concept: _Concept,
    universe: frozenset[str],
    instructions: Sequence[str],
    *,
    workspace_id: str,
    drafter: Callable[[dict], str] | None,
    routing_validator: Callable[[str, str], bool] | None,
    oracle: Any | None,
) -> PageCandidate | None:
    """Draft the body, run the retrieval + identifier + contradiction gates, decide
    certify/confidence, and emit the concept-anchored PageCandidate — or None if it
    fails a hard gate even as the deterministic stub."""
    body, llm_ok = _draft_body(spec, drafter)

    # Identifier gate (hard): an invented backtick / Source degrades to the stub; a
    # stub that still cites a non-member Source is dropped.
    ok, invented = identifier_gate(body, spec.source_fqns, universe)
    if not ok:
        body, llm_ok = _stub_body(spec), False
        ok2, invented2 = identifier_gate(body, spec.source_fqns, universe)
        if not ok2:
            logger.info("ontology page %s dropped — Source not in members: %s", spec.title, invented2)
            return None

    # Structural gates (chunk-safe + specificity): degrade to the stub, which passes
    # both by construction; if even the stub fails, drop.
    if not (chunk_safe_gate(body) and specificity_gate(body)):
        body, llm_ok = _stub_body(spec), False
        if not (chunk_safe_gate(body) and specificity_gate(body)):
            return None

    # Page-body leakage firewall (extended LeakageOracle) — a body echoing a benchmark
    # answer degrades to the stub; a leaking stub is dropped. No-op when no corpus.
    leaked = False
    if oracle is not None and getattr(oracle, "contains_page_leak", None) is not None:
        if oracle.contains_page_leak(body)[0]:
            body, llm_ok, leaked = _stub_body(spec), False, True
            if oracle.contains_page_leak(body)[0]:
                logger.info("ontology page %s dropped — body matched leakage corpus", spec.title)
                return None

    syn_ok = synonyms_gate(spec.synonyms, spec.synonym_classes)
    corroborated = spec.corroboration >= _CORROBORATION_FULL
    conflict = contradicts_instructions(spec, concept.measures, instructions)
    routing_validated = _validate_routing(spec, routing_validator)

    # Certify (§6): authoritative shape AND ≥2 corroboration AND synonyms cover AND no
    # contradiction AND a trustworthy (LLM- or stub-, identifier-valid) draft. A single
    # artifact, a synonym-short concept, a conflict, or an LLM-down draft is certify=false.
    certify = bool(spec.certify_shape and corroborated and syn_ok and not conflict and llm_ok)

    confidence = spec.confidence
    if not corroborated:
        confidence *= 0.5
    if not syn_ok:
        confidence *= 0.6
    if conflict:
        confidence *= 0.4
    if not llm_ok:
        confidence *= 0.8

    evidence = {
        **spec.evidence,
        "detector": spec.archetype,
        "canonical_id": spec.canonical_id,
        "corroboration": spec.corroboration,
        "synonym_classes": sorted(spec.synonym_classes),
        "body_source": "llm" if llm_ok else "stub",
        "status": "CONFLICT" if conflict else "OK",
        "routing_validated": routing_validated,
        "leak_degraded": leaked,
        "low_confidence": (not corroborated) or (not syn_ok),
        "gate_results": {
            "identifier": True, "chunk_safe": True, "specificity": True,
            "synonyms": syn_ok, "corroborated": corroborated, "contradiction": conflict,
        },
    }
    if invented:
        evidence["invented_identifiers"] = sorted(invented)

    return PageCandidate(
        page_id=page_id_of(spec.canonical_id, spec.archetype, spec.key_ids),
        canonical_id=spec.canonical_id,
        domain_id=spec.domain_id,
        archetype=spec.archetype,
        title=spec.title,
        body=body,
        synonyms=spec.synonyms,
        related_fqns=spec.related_fqns,
        source_fqns=spec.source_fqns,
        corroboration=spec.corroboration,
        certify=certify,
        evidence=evidence,
        confidence=round(confidence, 4),
    )


# ── Best-effort Page-vs-Page dedupe (name/synonym only — no Page read API) ──


def flag_duplicates(candidates: Sequence[PageCandidate]) -> None:
    """Best-effort dedupe (architecture §5 flagged asymmetry): Pages have no read API,
    so this is name/synonym heuristics only — it FLAGS a likely duplicate in evidence
    (reusing similarity.keyword_score) and NEVER silently merges. Mutates evidence in
    place (the dict is mutable though the dataclass is frozen)."""
    for i in range(len(candidates)):
        for j in range(i + 1, len(candidates)):
            a, b = candidates[i], candidates[j]
            if a.archetype != b.archetype:
                continue
            if similarity.keyword_score(a.title, b.title) >= 0.9:
                a.evidence.setdefault("possible_duplicate_of", []).append(b.page_id)
                b.evidence.setdefault("possible_duplicate_of", []).append(a.page_id)


def mine_pages(
    *,
    measures: Sequence[MeasureSignal] = (),
    columns: Sequence[ColumnSignal] = (),
    identity_verdicts: Sequence[Any] = (),
    members: Sequence[str] = (),
    instructions: Sequence[str] = (),
    workspace_id: str = "",
    drafter: Callable[[dict], str] | None = None,
    routing_validator: Callable[[str, str], bool] | None = None,
    oracle: Any | None = None,
) -> list[PageCandidate]:
    """Mine archetype Page proposals for every canonical concept in the metastore.

    Resolve each signal to its 17d ``canonical_id`` (the anchor), aggregate all
    artifacts that resolve to the same concept (across sub-domains), run the
    deterministic detectors, draft + validate each candidate, dedupe best-effort, and
    return concept-anchored ``PageCandidate``s (stable ``page_id``s). Per-concept and
    per-candidate errors are logged and skipped (MV-D43); the caller MERGEs the result
    metastore-scoped. Deterministic and offline."""
    index = _identity_index(identity_verdicts)
    concepts = aggregate_concepts(measures, columns, index)
    universe = build_universe(measures, columns, members)

    out: dict[str, PageCandidate] = {}
    for concept in concepts:
        try:
            specs = detect_concept(concept, instructions)
        except Exception as exc:  # noqa: BLE001 — skip this concept, keep the run
            logger.info("ontology page detection failed for %s (%s)", concept.canonical_id, exc)
            continue
        for spec in specs:
            try:
                cand = _finalize(
                    spec, concept, universe, instructions, workspace_id=workspace_id,
                    drafter=drafter, routing_validator=routing_validator, oracle=oracle,
                )
            except Exception as exc:  # noqa: BLE001 — skip this candidate, keep the run
                logger.info("ontology page finalize failed for %s (%s)", spec.title, exc)
                continue
            if cand is not None:
                out[cand.page_id] = cand

    candidates = sorted(out.values(), key=lambda c: (c.archetype, c.page_id))
    flag_duplicates(candidates)
    return candidates


# ── Default LLM drafter (lazy backend import; degrades to the stub) ─────────


def default_page_drafter(model: str | None = None) -> Callable[[dict], str]:
    """A body drafter backed by ``call_serving_endpoint`` — the ONLY LLM path, for
    body PROSE only (never structure/identifiers/the page_id). Lazily imports the
    backend LLM client so this module stays importable on a job cluster without
    ``backend`` on the path; on any failure it returns ``""`` so ``_draft_body`` falls
    back to the deterministic stub + ``certify=false`` (MV-D43). The precedent is
    ``cluster.default_namer`` / ``er.default_adjudicator``."""

    def _draft(facts: dict) -> str:
        try:
            from backend.services.llm_utils import call_serving_endpoint
            chosen = model
            if chosen:
                from backend.services.model_catalog import validate_chat_model
                chosen = validate_chat_model(chosen)
        except Exception:  # noqa: BLE001 — backend/LLM not reachable here → stub
            return ""
        prompt = (
            "You write the BODY PROSE of a governed Genie ontology Page. You are given "
            "the archetype, concept, a one-line description, a definition, rules, and "
            "the exact backticked identifiers to use. Rewrite them as a clear Page body "
            "with 'Description:', 'Definition:', and (if any) 'Rules:' sections. Every "
            "rule must name its metric/table inline (chunk-safe) and keep every "
            "backticked identifier EXACTLY as given — never invent a table, column, or "
            "measure, and never add identifiers not listed.\n\n"
            f"Archetype: {facts.get('archetype')}\nConcept: {facts.get('concept')}\n"
            f"Description: {facts.get('description')}\nDefinition: {facts.get('definition')}\n"
            f"Rules: {facts.get('rules')}\nAllowed identifiers (Sources): {facts.get('sources')}\n"
        )
        try:
            resp = call_serving_endpoint([{"role": "user", "content": prompt}], model=chosen, max_tokens=400)
        except Exception as exc:  # noqa: BLE001 — degrade, never block the run
            logger.info("ontology page drafting call failed: %s", exc)
            return ""
        return (resp or "").strip()

    return _draft
