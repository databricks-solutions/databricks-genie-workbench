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
import json
import logging
import re
from collections import Counter
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Callable, Literal, Mapping, Sequence

from genie_space_optimizer.ontology import er, similarity, transforms

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

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
# Bounded batch auto-drafting (MV-D66): the batch LLM-drafts only the "super sure" set —
# certify-eligible + corroboration ≥ MIN, top by confidence, hard-capped at MAX. Everyone
# else carries the deterministic stub; the on-demand / bulk prose (Steps 3–4) is pulled by
# the curator. MAX=0 ⇒ a pure-stub (zero-LLM) batch. Both are config-overridable (MV-D57).
PAGE_AUTODRAFT_MIN_CORROBORATION = 3
PAGE_AUTODRAFT_MAX_PAGES = 50
# Bounded worker cap for the Pass-C super-sure draft fan-out (MV-D67). k<=1 ⇒ sequential.
PAGE_AUTODRAFT_MAX_WORKERS = 4

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
class CommentSignal:
    """One business term carried in a table/column COMMENT — a broadened Page trigger
    (MV-D55). Modelled on ``ColumnSignal``: ``fqn`` is the asset the comment lives on
    (a table or a ``table.column``); ``term`` is the business term the comment names;
    ``comment`` is its free text. ``agent_fqns`` are serving Agents (Discover Related);
    ``domain_id`` is the signal's home sub-domain (provenance for the source-majority
    fallback; never in the ``page_id``). Its ``canonical_id`` is derived from ``term``
    with the SAME scheme as every other signal (``token_set_sig`` → ``canonical_id_of``),
    so a comment collapses onto the concept its term names — corroborating a measure /
    coded column that shares the concept, or standing alone as a comment-primary
    trigger."""

    fqn: str
    term: str
    comment: str = ""
    agent_fqns: tuple[str, ...] = ()
    domain_id: str = ""

    @property
    def ref(self) -> str:
        return self.fqn


@dataclass(frozen=True)
class HistorySignal:
    """DORMANT trigger seam (MV-D55) — a recurring Genie-history disambiguation would
    land here as a broadened trigger. Like the four dormant archetypes
    ([Method]/[Cross-domain]/[Defaults]/[Rule]), the offline slice reads NO Genie
    history, so there is no detector and no job reader for it: "no input → nothing
    mined." The dataclass + the accepted-but-unconsumed ``mine_pages(history=…)`` seam
    exist only so the trigger surface is named; they mine nothing until a later stage
    wires a detector and a (deploy-gated) Genie-history reader."""

    question: str
    term: str = ""
    agent_fqns: tuple[str, ...] = ()
    domain_id: str = ""


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
    comments: list[CommentSignal] = field(default_factory=list)

    def _all_agents(self) -> set[str]:
        out: set[str] = set()
        for m in self.measures:
            out.update(m.agent_fqns)
        for c in self.columns:
            out.update(c.agent_fqns)
        for cm in self.comments:
            out.update(cm.agent_fqns)
        return out

    def contributing_artifacts(self) -> list[str]:
        """The independent artifacts backing the concept — distinct metric views /
        coded tables / commented assets / serving Agents. Their count is the
        corroboration (MV-D35); a comment on a distinct asset is an independent artifact
        exactly like a measure or a coded column."""
        arts: set[str] = {m.mv_fqn for m in self.measures}
        arts |= {c.table_fqn for c in self.columns}
        arts |= {cm.fqn for cm in self.comments}
        arts |= self._all_agents()
        return sorted(arts)

    def corroboration(self) -> int:
        return len(self.contributing_artifacts())

    def home_domain(self) -> str:
        """The sub-domain of the concept's strongest membership (most signals);
        deterministic tie-break by sorted domain_id. This is the SIGNAL-derived fallback
        (MV-D55): source-majority attachment (``_resolve_home_domain``) overrides it when
        an ``asset_domain`` map is threaded in."""
        counts = Counter(
            s.domain_id for s in (*self.measures, *self.columns, *self.comments) if s.domain_id
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
    comments: Sequence[CommentSignal] = (),
) -> list[_Concept]:
    """Group all signals across the metastore by canonical concept (MV-D49). Order is
    deterministic (sorted by canonical_id). Comment signals resolve by their ``term``
    with the SAME identity scheme, so a comment naming a measure/column's concept
    aggregates into it (corroborating), and a comment naming an otherwise-unseen term
    forms a comment-primary concept."""
    by_cid: dict[str, _Concept] = {}
    for m in measures:
        cid = resolve_canonical_id(m.ref, m.name, index)
        by_cid.setdefault(cid, _Concept(cid)).measures.append(m)
    for c in columns:
        cid = resolve_canonical_id(c.ref, c.column, index)
        by_cid.setdefault(cid, _Concept(cid)).columns.append(c)
    for cm in comments:
        cid = resolve_canonical_id(cm.ref, cm.term, index)
        by_cid.setdefault(cid, _Concept(cid)).comments.append(cm)
    return [by_cid[k] for k in sorted(by_cid)]


def _resolve_home_domain(
    concept: _Concept, source_fqns: Sequence[str], asset_domain: Mapping[str, str],
) -> str:
    """The Page's home sub-domain = the domain of the MAJORITY of its SOURCE assets
    (MV-D55), via the injected ``asset_domain`` (source_fqn → domain_id) map computed
    THIS run; deterministic tie-break by sorted domain_id. An EMPTY map — or Sources none
    of which the map covers — falls back to the concept's signal-derived
    ``home_domain()`` (the pre-Stage-4 behaviour). ``page_id`` never depends on this."""
    if asset_domain:
        counts = Counter(asset_domain[s] for s in source_fqns if s in asset_domain)
        if counts:
            top = max(counts.values())
            return sorted(k for k, v in counts.items() if v == top)[0]
    return concept.home_domain()


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
    for cm in concept.comments:
        bag.append(cm.term)
        if cm.comment:
            bag.append(cm.comment)
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


# Section parsing tolerant of markdown decoration (## / ** / #) and bullet styles
# (-, *, +, 1.). The deterministic stub emits plain 'Definition:' / 'Rules:' labels, but
# an LLM drafter (default_page_drafter) tends to wrap them as '**Definition:**' or
# '## Rules:'. Normalizing here keeps the retrieval gates (specificity / chunk-safe)
# honest regardless of cosmetic formatting WITHOUT loosening what they require — a real
# backticked identifier still has to be present. Backticks and inner text are preserved.
_SECTION_LABELS = frozenset({"description", "definition", "rules"})
_MD_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")


def _md_unwrap(line: str) -> str:
    """Strip leading ATX-heading / blockquote markers and surrounding bold/italic
    emphasis so a decorated label ('## Definition:', '**Rules:**') is recognizable.
    Preserves backticks and inner text."""
    s = line.strip()
    s = re.sub(r"^[>\s]*#{1,6}\s+", "", s)   # '## ' heading prefix
    s = re.sub(r"^[*_]+", "", s)              # leading bold/italic run
    s = re.sub(r"[*_]+$", "", s)              # trailing bold/italic run
    return s.strip()


def _section_label(line: str) -> str | None:
    """Return 'description'/'definition'/'rules' when this line is a (possibly
    markdown-decorated) section header — the label alone, optionally followed by ':'.
    A content line that merely contains a colon is NOT a header."""
    s = _md_unwrap(line).rstrip(":").strip().lower()
    return s if s in _SECTION_LABELS else None


def _section_lines(body: str, want: str, *, bullets_only: bool) -> list[str]:
    """Non-empty content lines of the ``want`` section, markdown bullet markers stripped.
    The section ends at the next recognized section header. ``bullets_only`` keeps only
    bullet lines (the historical Rules behavior); False keeps every non-empty line (the
    historical Definition behavior). An inline 'Label: text' header contributes its text
    when not bullets_only."""
    out: list[str] = []
    in_section = False
    for raw in (body or "").splitlines():
        label = _section_label(raw)
        if label is not None:
            if label == want:
                in_section = True
                if not bullets_only:
                    unwrapped = _md_unwrap(raw)
                    inline = unwrapped[len(want):].lstrip(": ").strip() \
                        if unwrapped.lower().startswith(want) else ""
                    if inline:
                        out.append(inline)
                continue
            if in_section:
                break                     # next section header ends this one
            continue
        if not in_section:
            continue
        stripped = raw.strip()
        if bullets_only and not _MD_BULLET_RE.match(stripped):
            continue
        content = _MD_BULLET_RE.sub("", stripped).strip()
        if content:
            out.append(content)
    return out


def _rule_lines(body: str) -> list[str]:
    """The Rules-section bullet sentences of a body (for chunk-safe/specificity)."""
    return _section_lines(body, "rules", bullets_only=True)


def _definition_lines(body: str) -> list[str]:
    """The Definition-section sentences of a body (for specificity)."""
    return _section_lines(body, "definition", bullets_only=False)


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
    comments: Sequence[CommentSignal] = (),
) -> frozenset[str]:
    """Every real identifier the miner may backtick / cite as a Source — member
    assets, metric-view + measure pointers, source tables, coded-column pointers,
    commented assets, serving Agents. Anything outside this set is invented and fails
    the gate."""
    u: set[str] = set(members)
    for m in measures:
        u.update({m.mv_fqn, m.ref, m.name, *m.source_fqns, *m.agent_fqns})
    for c in columns:
        u.update({c.table_fqn, c.ref, c.column, *c.agent_fqns})
    for cm in comments:
        u.update({cm.fqn, cm.term, *cm.agent_fqns})
    return frozenset(u)


# ── Deterministic per-archetype detectors (LLM-free) ────────────────────────


def _distinct_definitions(measures: Sequence[MeasureSignal]) -> set[str]:
    """The distinct canonical definitions among a concept's measures (≥2 → a genuine
    same-term/different-expression conflict → [Disambiguation])."""
    return {(_canonical_measure(m.expression) or m.expression) for m in measures if (m.expression or "")}


def _measure_detector(
    concept: _Concept, instructions: Sequence[str], asset_domain: Mapping[str, str],
) -> _DraftSpec | None:
    """One measure concept → exactly ONE Page: Disambiguation on a genuine expression
    conflict, else Guardrail for a rate/percentage, else Routing (the canonical
    answer). All corroborating measures aggregate into Sources."""
    ms = sorted(concept.measures, key=lambda m: m.ref)
    if not ms:
        return None
    name = ms[0].name
    cid = concept.canonical_id
    corr = concept.corroboration()
    agents = sorted({a for m in ms for a in m.agent_fqns})
    mv_sources = sorted({m.mv_fqn for m in ms} | {s for m in ms for s in m.source_fqns})
    dom = _resolve_home_domain(concept, mv_sources, asset_domain)
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


def _taxonomy_detector(
    concept: _Concept, instructions: Sequence[str], asset_domain: Mapping[str, str],
) -> _DraftSpec | None:
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
    corr = concept.corroboration()
    agents = sorted({a for c in coded for a in c.agent_fqns})
    sources = sorted({c.table_fqn for c in coded})
    dom = _resolve_home_domain(concept, sources, asset_domain)
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


def _comment_detector(
    concept: _Concept,
    instructions: Sequence[str],
    asset_domain: Mapping[str, str],
    coded_by_fqn: Mapping[str, ColumnSignal],
    def_by_fqn: Mapping[str, frozenset[str]],
) -> _DraftSpec | None:
    """A business term carried in table/column COMMENTs → at most one broadened-trigger
    Page (MV-D55): the SAME term on ≥2 assets whose canonical measure definitions
    CONFLICT → [Disambiguation]; else a comment term sitting on a CODED column →
    [Taxonomy]; else nothing (a lone descriptive comment with no coded/conflict signal
    mines no Page — "no signal → nothing"). Codedness (``coded_by_fqn``) and conflicting
    definitions (``def_by_fqn``) come from the estate-wide signal maps; the detector
    reuses the SAME comparator (``_canonical_measure`` fingerprints, computed upstream)
    and identity scheme — it invents nothing. ``detect_concept`` suppresses a comment
    Page whose archetype the measure/taxonomy detectors already produced for the concept,
    so comments never duplicate a Page they merely corroborate."""
    cms = sorted(concept.comments, key=lambda c: c.fqn)
    if not cms:
        return None
    cid = concept.canonical_id
    corr = concept.corroboration()
    term = cms[0].term
    agents = sorted({a for cm in cms for a in cm.agent_fqns})
    synonyms, classes = derive_synonyms(term, _concept_vocab(concept, instructions))

    # [Disambiguation] — the same term on ≥2 distinct assets carrying conflicting
    # canonical measure definitions (read from the estate-wide fingerprint map).
    conflict_assets = sorted({cm.fqn for cm in cms if def_by_fqn.get(cm.fqn)})
    conflict_defs: set[str] = set()
    for f in conflict_assets:
        conflict_defs |= set(def_by_fqn.get(f, ()))
    if len(conflict_assets) >= 2 and len(conflict_defs) >= 2:
        dom = _resolve_home_domain(concept, conflict_assets, asset_domain)
        rules = tuple(
            f"When asked for \"{term}\", {_bt(f)} carries its own definition of the term; "
            f"confirm which asset is meant before writing SQL."
            for f in conflict_assets
        )
        return _DraftSpec(
            archetype="Disambiguation", canonical_id=cid, domain_id=dom, concept_name=term,
            title=f"{_TITLE_PREFIX['Disambiguation']} {term}",
            description=f"\"{term}\" names conflicting definitions across the estate — confirm which is meant.",
            definition=(
                f"The business term \"{term}\" appears on "
                + ", ".join(_bt(f) for f in conflict_assets)
                + " with conflicting definitions — these are not interchangeable."
            ),
            rules=rules, key_ids=tuple(conflict_assets), synonyms=synonyms, synonym_classes=classes,
            related_fqns=tuple(agents), source_fqns=tuple(conflict_assets), corroboration=corr,
            certify_shape=True, confidence=0.8,
            evidence={"contributing_artifacts": concept.contributing_artifacts(),
                      "conflicting_definitions": sorted(conflict_defs), "trigger": "comment"},
        )

    # [Taxonomy] — the comment term sits on a coded column (a decode Page keyed to the
    # BUSINESS term the comment names, distinct from a bare column-name Taxonomy Page).
    coded_hits = sorted(cm.fqn for cm in cms if cm.fqn in coded_by_fqn)
    if coded_hits:
        cols = [coded_by_fqn[f] for f in coded_hits]
        sources = sorted({c.table_fqn for c in cols})
        dom = _resolve_home_domain(concept, sources, asset_domain)
        values = ", ".join(cols[0].distinct_values[:12])
        return _DraftSpec(
            archetype="Taxonomy", canonical_id=cid, domain_id=dom, concept_name=term,
            title=f"{_TITLE_PREFIX['Taxonomy']} {term}",
            description=f"\"{term}\" is stored as a coded column — decode its values before filtering or grouping.",
            definition=(
                f"The business term \"{term}\" is held as a fixed code list in "
                + ", ".join(_bt(c.ref) for c in cols)
                + f" ({values}). Decode each code to its business meaning."
            ),
            rules=(), key_ids=tuple(coded_hits), synonyms=synonyms, synonym_classes=classes,
            related_fqns=tuple(agents), source_fqns=tuple(sources), corroboration=corr,
            certify_shape=all(c.governed for c in cols), confidence=0.5,
            evidence={"contributing_artifacts": concept.contributing_artifacts(),
                      "distinct_values": list(cols[0].distinct_values),
                      "governed": all(c.governed for c in cols), "trigger": "comment"},
        )
    return None


def detect_concept(
    concept: _Concept,
    instructions: Sequence[str],
    *,
    asset_domain: Mapping[str, str] | None = None,
    coded_by_fqn: Mapping[str, ColumnSignal] | None = None,
    def_by_fqn: Mapping[str, frozenset[str]] | None = None,
) -> list[_DraftSpec]:
    """All Page specs a concept yields (measures → one measure Page; coded columns → a
    Taxonomy Page; comment terms → a broadened-trigger Page). A comment Page whose
    archetype a measure/taxonomy detector already produced for the concept is dropped —
    comments then only corroborate that Page, never duplicate it.
    [Method]/[Cross-domain]/[Defaults]/[Rule] are signal-gated and dormant in the
    offline slice — their unambiguous signals (method families, join-spine, standard
    filters, structural breaks) are not among the offline reader's inputs, so "No signal
    → nothing" (§1.1); the same holds for :class:`HistorySignal` (no detector)."""
    asset_domain = asset_domain or {}
    coded_by_fqn = coded_by_fqn or {}
    def_by_fqn = def_by_fqn or {}
    specs: list[_DraftSpec] = []
    produced: set[str] = set()
    m = _measure_detector(concept, instructions, asset_domain)
    if m is not None:
        specs.append(m)
        produced.add(m.archetype)
    t = _taxonomy_detector(concept, instructions, asset_domain)
    if t is not None:
        specs.append(t)
        produced.add(t.archetype)
    cm = _comment_detector(concept, instructions, asset_domain, coded_by_fqn, def_by_fqn)
    if cm is not None and cm.archetype not in produced:
        specs.append(cm)
    return specs


# ── Draft → validate → certify → PageCandidate ──────────────────────────────


# One-line, archetype-keyed reason for why each SOURCE asset backs the Page (MV-D55).
_SOURCE_WHY: dict[str, str] = {
    "Routing": "Backs this metric — the governed answer for the concept.",
    "Guardrail": "Backs this rate — recompute it from its numerator and denominator here.",
    "Disambiguation": "One of the conflicting definitions this page reconciles.",
    "Taxonomy": "Holds the coded values this page decodes.",
}
_RELATED_WHY = "Serving Genie Agent that answers questions about this concept."


def _asset_why(spec: _DraftSpec) -> dict[str, str]:
    """A deterministic one-line "why this asset" for every Source and Related FQN
    (MV-D55). Rides in ``evidence`` — it never restructures the ``source_fqns`` /
    ``related_fqns`` tuples. Sources read from the archetype; Related are the serving
    Agents."""
    src_reason = _SOURCE_WHY.get(spec.archetype, "Backs this page.")
    why: dict[str, str] = {f: src_reason for f in spec.source_fqns}
    for f in spec.related_fqns:
        why[f] = _RELATED_WHY
    return why


def _compute_facts_hash(spec: _DraftSpec) -> str:
    """Compute a deterministic SHA256 hash of spec.facts() (the prose inputs) so Step 2
    can detect when a preserved body has gone stale (MV-D66). The hash is a 32-char hex
    string that rides in evidence.facts_hash; a re-run with different facts will carry a
    different hash, allowing the merge to set evidence.body_stale=true."""
    facts = spec.facts()
    facts_json = json.dumps(facts, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(facts_json.encode()).hexdigest()


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

    # Certify (§6, MV-D66): a DETERMINISTIC curator recommendation — authoritative shape
    # AND ≥2 corroboration AND synonyms cover AND no contradiction. Independent of body
    # source: the stub already passes every safety gate (identifier / chunk-safe /
    # specificity), so ``llm_ok`` is prose polish, not correctness, and no longer gates the
    # recommendation. ``llm_ok`` still rides ``evidence.body_source`` and still scales
    # ``confidence`` (below). A single artifact, a synonym-short concept, or a conflict is
    # certify=false. Nothing auto-certifies — a human approves (consent ledger, Phase 5).
    certify = bool(spec.certify_shape and corroborated and syn_ok and not conflict)

    confidence = spec.confidence
    if not corroborated:
        confidence *= 0.5
    if not syn_ok:
        confidence *= 0.6
    if conflict:
        confidence *= 0.4
    if not llm_ok:
        confidence *= 0.8

    # Step 2 (MV-D66): compute facts_hash so preserved bodies can detect staleness.
    # The hash rides in evidence JSON (no DDL); it lets the merge compare incoming vs
    # stored facts and set body_stale=true if they differ.
    facts_hash = _compute_facts_hash(spec)

    evidence = {
        **spec.evidence,
        "detector": spec.archetype,
        "canonical_id": spec.canonical_id,
        "corroboration": spec.corroboration,
        "synonym_classes": sorted(spec.synonym_classes),
        "body_source": "llm" if llm_ok else "stub",
        "facts_hash": facts_hash,
        "status": "CONFLICT" if conflict else "OK",
        "routing_validated": routing_validated,
        "leak_degraded": leaked,
        "low_confidence": (not corroborated) or (not syn_ok),
        "asset_why": _asset_why(spec),
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


def _coded_column_index(columns: Sequence[ColumnSignal]) -> dict[str, ColumnSignal]:
    """Estate-wide map ``coded-column ref → ColumnSignal`` (low-cardinality only) — the
    codedness lookup for the comment detector's [Taxonomy] branch."""
    return {
        c.ref: c for c in columns
        if c.distinct_values and len(c.distinct_values) <= _TAXONOMY_MAX_CARDINALITY
    }


def _definition_index(measures: Sequence[MeasureSignal]) -> dict[str, frozenset[str]]:
    """Estate-wide map ``asset fqn → {canonical measure definitions}`` (reusing the ONLY
    comparator, ``_canonical_measure`` / ``mv_fingerprint``) — the conflicting-definition
    lookup for the comment detector's [Disambiguation] branch. An asset carrying two
    differently-defined measures maps to a two-element set (a same-asset conflict); the
    detector also unions across the ≥2 assets a term spans."""
    out: dict[str, set[str]] = {}
    for m in measures:
        d = _canonical_measure(m.expression) or (m.expression or "")
        if not d:
            continue
        for f in (m.mv_fqn, *m.source_fqns):
            out.setdefault(f, set()).add(d)
    return {f: frozenset(defs) for f, defs in out.items()}


def _canonical_body(drafted: str, spec: "_DraftSpec") -> tuple[str, str]:
    """Reassemble an LLM draft into the canonical plain-text skeleton (MV-D70),
    guaranteeing the Definition axis of ``specificity_gate`` WITHOUT loosening it. Keeps the
    LLM ``Description`` + ``Rules`` verbatim, but when ``_definition_lines(drafted)`` carries
    NO backticked identifier (opus's dominant 4.1h failure mode — it paraphrases the id away)
    it substitutes ``spec.definition``, which is evidence-derived and already cites a real,
    in-universe identifier. The substituted id is genuine, so the gate stays honest.

    Returns ``(body, definition_source)`` with ``definition_source`` ∈ {"llm","deterministic"}:
    "llm" when the drafted Definition is kept, "deterministic" when it falls back."""
    desc_lines = _section_lines(drafted, "description", bullets_only=False)
    description = " ".join(desc_lines).strip() or spec.description
    def_lines = _definition_lines(drafted)
    if any(_backticked(l) for l in def_lines):
        definition, definition_source = " ".join(def_lines).strip(), "llm"
    else:
        definition, definition_source = spec.definition, "deterministic"
    lines = [f"Description: {description}", "", "Definition:", f"  {definition}"]
    rule_lines = _rule_lines(drafted)
    if rule_lines:
        lines.append("")
        lines.append("Rules:")
        lines.extend(f"  - {r}" for r in rule_lines)
    return "\n".join(lines), definition_source


def _autodraft(
    cand: PageCandidate,
    spec: "_DraftSpec",
    universe: frozenset[str],
    drafter: Callable[[dict], str],
    oracle: Any | None,
) -> PageCandidate | None:
    """Pass C (MV-D66 + MV-D70): try to upgrade a selected certify Page's deterministic stub
    to an LLM-drafted body. Calls the injected ``drafter`` on ``spec.facts()``, reassembles
    the draft into the canonical plain-text skeleton (:func:`_canonical_body`) — keeping the
    LLM ``Description`` + ``Rules`` but guaranteeing the Definition carries a real, in-universe
    backticked identifier (falling back to ``spec.definition`` when opus paraphrased it away) —
    then runs the SAME identifier / chunk-safe / specificity / leakage gates ``_finalize``
    runs, on the REASSEMBLED body.

    On success returns a new candidate with that body, ``evidence.body_source="llm_auto"`` and
    ``evidence.definition_source`` ∈ {"llm","deterministic"}, clearing any ``autodraft_reject``.
    Otherwise returns ``None`` — keep the stub (degrade, MV-D43) — and records the first failing
    reason in ``evidence.autodraft_reject`` (Prong 3 observability; ``empty`` covers a
    missing/raising/empty drafter). Only an attempted (selected super-sure) Page carries the
    marker. ``certify`` / ``confidence`` are unchanged (computed deterministically in Pass A)."""
    try:
        body = drafter(spec.facts())
    except Exception as exc:  # noqa: BLE001 — LLM down → keep stub, run still succeeds
        logger.info("ontology page auto-draft failed for %s (%s); keeping stub", spec.title, exc)
        cand.evidence["autodraft_reject"] = "empty"
        return None
    body = (body or "").strip()
    if not body:
        cand.evidence["autodraft_reject"] = "empty"
        return None

    reassembled, definition_source = _canonical_body(body, spec)

    ok, _invented = identifier_gate(reassembled, spec.source_fqns, universe)
    if not ok:
        cand.evidence["autodraft_reject"] = "identifier"
        return None
    if not chunk_safe_gate(reassembled):
        cand.evidence["autodraft_reject"] = "chunk_safe"
        return None
    if not specificity_gate(reassembled):
        cand.evidence["autodraft_reject"] = "specificity"
        return None
    if oracle is not None and getattr(oracle, "contains_page_leak", None) is not None:
        if oracle.contains_page_leak(reassembled)[0]:
            cand.evidence["autodraft_reject"] = "leakage"
            return None

    cand.evidence["body_source"] = "llm_auto"
    cand.evidence["definition_source"] = definition_source
    cand.evidence.pop("autodraft_reject", None)
    return replace(cand, body=reassembled)


def mine_pages(
    *,
    measures: Sequence[MeasureSignal] = (),
    columns: Sequence[ColumnSignal] = (),
    comments: Sequence[CommentSignal] = (),
    history: Sequence[HistorySignal] = (),
    identity_verdicts: Sequence[Any] = (),
    members: Sequence[str] = (),
    instructions: Sequence[str] = (),
    asset_domain: Mapping[str, str] | None = None,
    workspace_id: str = "",
    drafter: Callable[[dict], str] | None = None,
    routing_validator: Callable[[str, str], bool] | None = None,
    oracle: Any | None = None,
    page_autodraft_min_corroboration: int = PAGE_AUTODRAFT_MIN_CORROBORATION,
    page_autodraft_max_pages: int = PAGE_AUTODRAFT_MAX_PAGES,
    page_autodraft_max_workers: int = PAGE_AUTODRAFT_MAX_WORKERS,
) -> list[PageCandidate]:
    """Mine archetype Page proposals for every canonical concept in the metastore.

    Resolve each signal to its 17d ``canonical_id`` (the anchor), aggregate all
    artifacts that resolve to the same concept (across sub-domains) — measures, coded
    columns AND business-term comments (MV-D55) — run the deterministic detectors, attach
    each Page to the domain of the MAJORITY of its Source assets (via ``asset_domain``,
    an empty map falling back to the signal home), and return concept-anchored
    ``PageCandidate``s (stable ``page_id``s).

    Drafting is BOUNDED (MV-D66): the LLM is called for at most
    ``page_autodraft_max_pages`` Pages, never per-candidate. **Pass A** finalizes EVERY
    candidate deterministically (``drafter=None`` → stub body, ``llm_ok=False``) — because
    ``certify`` no longer needs ``llm_ok`` (§3.1), every strong Page already gets its
    correct ``certify`` / ``confidence`` / stub ``body`` / ``evidence``
    (``body_source="stub"``). **Pass B** selects the "super sure" set — ``certify`` AND
    ``corroboration ≥ page_autodraft_min_corroboration``, sorted by ``confidence`` desc
    (the pre-L6 ranking proxy — ``score`` is written later by the materializer), tie-break
    ``page_id``, capped at ``page_autodraft_max_pages``. **Pass C** drafts only those (see
    :func:`_autodraft`). ``drafter=None`` or ``page_autodraft_max_pages=0`` ⇒ a fully
    deterministic all-stub run; ``certify`` still lights up, so offline tests need no LLM.

    ``history`` is the DORMANT :class:`HistorySignal` seam — accepted so the trigger
    surface is named, consumed by no detector (mines nothing). Per-concept and
    per-candidate errors are logged and skipped (MV-D43); the caller MERGEs the result
    metastore-scoped. Deterministic and offline."""
    _ = history  # dormant seam (MV-D55): named, not mined — no Genie-history detector.
    asset_domain = dict(asset_domain or {})
    index = _identity_index(identity_verdicts)
    concepts = aggregate_concepts(measures, columns, index, comments)
    universe = build_universe(measures, columns, members, comments)
    coded_by_fqn = _coded_column_index(columns)
    def_by_fqn = _definition_index(measures)

    # ── Pass A: deterministic finalize-all (no LLM). certify/confidence/evidence correct.
    out: dict[str, PageCandidate] = {}
    specs_by_page_id: dict[str, "_DraftSpec"] = {}
    for concept in concepts:
        try:
            specs = detect_concept(
                concept, instructions, asset_domain=asset_domain,
                coded_by_fqn=coded_by_fqn, def_by_fqn=def_by_fqn,
            )
        except Exception as exc:  # noqa: BLE001 — skip this concept, keep the run
            logger.info("ontology page detection failed for %s (%s)", concept.canonical_id, exc)
            continue
        for spec in specs:
            try:
                cand = _finalize(
                    spec, concept, universe, instructions, workspace_id=workspace_id,
                    drafter=None, routing_validator=routing_validator, oracle=oracle,
                )
            except Exception as exc:  # noqa: BLE001 — skip this candidate, keep the run
                logger.info("ontology page finalize failed for %s (%s)", spec.title, exc)
                continue
            if cand is not None:
                out[cand.page_id] = cand
                specs_by_page_id[cand.page_id] = spec

    candidates = sorted(out.values(), key=lambda c: (c.archetype, c.page_id))

    # ── Pass B + C: bounded auto-drafting of the "super sure" set (MV-D66).
    if drafter is not None and page_autodraft_max_pages > 0:
        eligible = [
            c for c in candidates
            if c.certify and int(c.evidence.get("corroboration", 0)) >= page_autodraft_min_corroboration
        ]
        eligible.sort(key=lambda c: (-c.confidence, c.page_id))
        selected = eligible[:page_autodraft_max_pages]
        # Fan out only the pure LLM drafter calls under a bounded pool (MV-D67); apply the
        # results by page_id so the written snapshot is identical for any worker count.
        upgrades = transforms.run_bounded(
            selected,
            lambda c: _autodraft(c, specs_by_page_id[c.page_id], universe, drafter, oracle),
            max_workers=page_autodraft_max_workers,
        )
        for cand, upgraded in zip(selected, upgrades):
            if upgraded is not None:
                out[cand.page_id] = upgraded
        candidates = sorted(out.values(), key=lambda c: (c.archetype, c.page_id))

    flag_duplicates(candidates)
    return candidates


# ── Default LLM drafter (lazy backend import; degrades to the stub) ─────────


def default_page_drafter(
    model: str | None = None, w: "WorkspaceClient | None" = None,
) -> Callable[[dict], str]:
    """A body drafter backed by the wheel-native LLM client
    (:func:`genie_space_optimizer.common.llm.call_llm_core`) — the ONLY LLM path,
    for body PROSE only (never structure/identifiers/the page_id). The identity
    ``w`` is INJECTED (the job passes its ``run_as`` client; the app its OBO
    client). On any failure it returns ``""`` so ``_draft_body`` falls back to the
    deterministic stub + ``certify=false`` (MV-D43). The precedent is
    ``cluster.default_namer`` / ``er.default_adjudicator``."""

    def _draft(facts: dict) -> str:
        try:
            from genie_space_optimizer.common.llm import call_llm_core
        except Exception:  # noqa: BLE001 — client unavailable here → stub
            return ""
        # Allowed identifiers = the page's Sources (guaranteed ⊆ the member universe for a
        # certify page, so identifier_gate can't flag them). Related FQNs are NOT offered
        # to the model because they are not guaranteed to be in the universe.
        allowed = list(facts.get("sources") or [])
        # The output MUST satisfy the retrieval gates deterministically (specificity =
        # a backticked identifier in the Definition AND in every Rules bullet; chunk-safe =
        # no leading pronoun; identifier = only Allowed identifiers). The gate parsers key
        # off literal 'Description:'/'Definition:'/'Rules:' line labels, so markdown headers
        # would make the sections parse empty (4.1h) — hence the strict PLAIN-TEXT contract.
        prompt = (
            "You write ONLY the body PROSE of a governed Genie ontology Page. Output PLAIN "
            "TEXT in EXACTLY this shape — three sections, each label on its OWN line, "
            "verbatim, in this order:\n"
            "Description: <one or two sentences>\n"
            "Definition: <one or two sentences>\n"
            "Rules:\n"
            "- <rule one>\n"
            "- <rule two>\n\n"
            "HARD FORMAT RULES (a violation makes the Page unusable):\n"
            "- Use NO markdown whatsoever: no '#', no '*', no '**', no headings, no bold, "
            "no numbered lists. Rules bullets start with '- ' only.\n"
            "- The 'Definition:' line AND every '- ' rule bullet MUST each contain at least "
            "one Allowed identifier below, wrapped in `backticks`, EXACTLY as given.\n"
            "- Use ONLY the Allowed identifiers; never invent, abbreviate, or rename a "
            "table/column/measure, and never backtick anything not in the list.\n"
            "- Each rule must name its metric/table inline; never open a rule with a bare "
            "pronoun (it/this/that/they).\n\n"
            "WORKED EXAMPLE — note the backticked Source in the Definition line (this is the "
            "one part most often dropped; the Definition MUST contain at least one backticked "
            "Allowed identifier):\n"
            "Description: Total cost is the governed spend roll-up for the period.\n"
            "Definition: Total cost is the governed roll-up computed from `catalog.schema.mv`.\n"
            "Rules:\n"
            "- Answer total cost from `catalog.schema.mv`; never re-aggregate its source tables.\n\n"
            f"Archetype: {facts.get('archetype')}\nConcept: {facts.get('concept')}\n"
            f"Description (source): {facts.get('description')}\n"
            f"Definition (source): {facts.get('definition')}\n"
            f"Rules (source): {facts.get('rules')}\n"
            f"Allowed identifiers: {allowed}\n"
        )
        try:
            text, _ = call_llm_core(
                w, messages=[{"role": "user", "content": prompt}], model=model, max_tokens=400,
            )
        except Exception as exc:  # noqa: BLE001 — degrade, never block the run
            logger.info("ontology page drafting call failed: %s", exc)
            return ""
        return (text or "").strip()

    return _draft
