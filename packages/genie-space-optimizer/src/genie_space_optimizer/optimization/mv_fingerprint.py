"""SQL canonicalization and fingerprinting for the metric view advisor.

POV Parts 2 and 3: the advisor's core signal (**Y**) is that the same aggregate
expression keeps being re-derived in generated SQL and in query history. Turning
"keeps being re-derived" into a countable fact needs a canonical form that
collapses cosmetic variation — aliases, whitespace, case, predicate order,
literal values — while preserving every difference that makes two measures
genuinely different measures.

Two fingerprint levels exist in this feature and they are permanently distinct
(MV-D10):

- :func:`expr_fingerprint` — **expression**-grained, defined here. It counts
  recurrence inside a corpus scan and does nothing else. It is never written to
  a Delta column and never used as an upsert key.
- ``mv_state.mv_candidate_fingerprint`` — **candidate**-grained: the MV-D7
  idempotency key ``sha256(space_id | canonical_measure_expr |
  sorted_source_set)`` and the ``genie_opt_artifacts.content_hash``
  cross-reference. That is the only thing this feature calls a "dedup
  fingerprint". It consumes :attr:`MeasureRef.canonical_expr` from here as its
  ``canonical_measure_expr`` argument, so canonicalization lives in exactly one
  place and the two levels cannot drift apart.

**Inverted contract with the firewall.** ``leakage.canonicalize_sql``
*preserves* literals — it hunts verbatim benchmark text, so the values are the
evidence. This module *erases* them: two queries differing only in a date are
one shape, and a literal that reaches shipped metric-view metadata is a
firewall violation. Both functions canonicalize SQL and neither is a better
version of the other; they are not interchangeable.

Consequences of literal erasure, worth knowing before consuming a fingerprint:

- ``SUM(CASE WHEN status = 'F' THEN 1 END)`` and the same shape over ``'O'``
  share one fingerprint. A generator must recover concrete predicate values
  from profiling, never by reading them back out of a canonical form — there is
  nothing left there to read.
- Boolean and NULL keywords survive (``is_current = TRUE`` stays verbatim).
  They carry no value, and erasing them would merge opposite filters.
- Where canonicalization cannot prove two expressions are the same shape it
  emits two fingerprints rather than one. Recurrence therefore under-counts
  rather than over-counts, which can only delay a proposal — never fabricate
  one.

Unlike its neighbours in this package, sqlglot is imported at module scope
rather than inside each function: this module is nothing but sqlglot, so a lazy
import would only defer an ``ImportError`` to a less obvious place.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from typing import Any

import sqlglot
from sqlglot import expressions as exp

logger = logging.getLogger(__name__)


# ── Vocabularies ─────────────────────────────────────────────────────────

DIALECT = "databricks"
"""Every parse and every render pins this dialect. A fingerprint produced under
another dialect is not comparable with one produced here."""

MEASURE_AGGREGATES: frozenset[str] = frozenset({"SUM", "COUNT", "AVG", "MIN", "MAX"})
"""Aggregates the advisor will propose as metric-view measures. Anything else
(``PERCENTILE``, ``COLLECT_LIST``, …) is left alone: it either has no additive
metric-view form or needs a human to decide the grain."""

STRING_PLACEHOLDER = "?s"
NUMERIC_PLACEHOLDER = "?n"
"""Literal stand-ins. They are rendered bare (no quotes), so a canonical form
containing a quote character is a firewall failure by construction."""

SHAPE_RATIO = "RATIO"
SHAPE_CONDITIONAL_COUNT = "CONDITIONAL_COUNT"
SHAPE_PCT_OF_TOTAL = "PCT_OF_TOTAL"

SHAPE_KINDS: tuple[str, ...] = (SHAPE_RATIO, SHAPE_CONDITIONAL_COUNT, SHAPE_PCT_OF_TOTAL)

SHAPE_GUIDANCE: dict[str, str] = {
    SHAPE_RATIO: (
        "Emit atomic measures for numerator and denominator, plus a "
        "MEASURE()-composed derived measure. Never re-type the aggregates."
    ),
    SHAPE_CONDITIONAL_COUNT: (
        "Emit COUNT(1) FILTER (WHERE <condition>) rather than "
        "SUM(CASE WHEN <condition> THEN 1 END)."
    ),
    SHAPE_PCT_OF_TOTAL: (
        "Emit a Fixed-LOD dimension (SUM(x) OVER ()) read with ANY_VALUE(). "
        "NEVER MEASURE()/MEASURE() — that always yields 1.0."
    ),
}
"""Per-shape generation mandate from the MV-D8 quality standard. The generator
(Prompt 5.5) reads these rather than re-deriving the rule, so the shape
detector and the renderer cannot disagree about what a shape means."""

_WHITESPACE_RE = re.compile(r"\s+")


# ── Result types ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class MeasureRef:
    """One aggregate expression found in a statement.

    ``canonical_expr`` is what ``mv_state.mv_candidate_fingerprint`` expects as
    its ``canonical_measure_expr``. Table qualifiers are stripped from it —
    table identity travels in ``source_tables``, which is also what the MV-D7
    key hashes as its sorted source set, so an alias-qualified and an
    unqualified spelling of the same measure land on one candidate row.

    ``representative_expr`` is the literal-preserving render form of the same
    node (MV-D29): the source a generator emits into an executable body, where
    ``canonical_expr`` has erased ``1 - l_discount`` to ``?n - l_discount``. It
    is *never* an identity: ``fingerprint`` and every dedup/scoring key read
    ``canonical_expr`` only, so two measures differing only in a literal still
    share one fingerprint while their ``representative_expr`` differs.
    """

    canonical_expr: str
    fingerprint: str
    aggregate: str
    representative_expr: str = ""
    source_columns: tuple[str, ...] = ()
    source_tables: tuple[str, ...] = ()
    is_windowed: bool = False
    is_distinct: bool = False
    has_unresolved_columns: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DimensionRef:
    """One GROUP BY key. ``is_expression`` marks a derived key such as
    ``DATE_TRUNC('month', o_orderdate)``, which becomes a metric-view dimension
    expression rather than a bare column."""

    canonical_expr: str
    fingerprint: str
    source_columns: tuple[str, ...] = ()
    source_tables: tuple[str, ...] = ()
    is_expression: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FilterRef:
    """One WHERE or HAVING conjunct. Cross-table equalities are excluded — they
    are join keys, and :func:`extract_join_keys` claims them instead."""

    canonical_expr: str
    fingerprint: str
    clause: str
    operator: str
    source_columns: tuple[str, ...] = ()
    source_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class JoinKeyRef:
    """One column-to-column equality. ``left``/``right`` are sorted, so join
    orientation does not produce two fingerprints for one relationship.
    ``origin`` distinguishes an explicit ``JOIN … ON`` from the implicit
    comma-join equalities the classic TPC-H statements use."""

    canonical_expr: str
    fingerprint: str
    left: str
    right: str
    tables: tuple[str, ...] = ()
    origin: str = "on"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ShapeMatch:
    """A recurring pattern the generator consumes.

    ``target_form`` is the metric-view expression the generator should emit, and
    ``fingerprint`` identifies the shape *by that form* rather than by the
    spelling found in the corpus. So ``SUM(CASE WHEN c THEN 1 END)`` and
    ``SUM(CASE WHEN c THEN 1 ELSE 0 END)`` are one shape with one fingerprint —
    they ask for the same ``COUNT(1) FILTER`` measure, and counting them
    separately would halve the recurrence of the thing being proposed.

    Statement-level detection yields ``recurrence == 1``;
    :func:`classify_shapes` merges matches across a corpus and fills the
    recurrence and provenance fields.
    """

    kind: str
    canonical_expr: str
    fingerprint: str
    guidance: str
    target_form: str = ""
    components: tuple[tuple[str, str], ...] = ()
    source_columns: tuple[str, ...] = ()
    source_tables: tuple[str, ...] = ()
    recurrence: int = 1
    provenance_ids: tuple[str, ...] = ()
    first_seen: str | None = None
    last_seen: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["components"] = dict(self.components)
        return payload


CURATED_PROVENANCE_KIND = "curated"
"""``Provenance.kind`` value that marks an occurrence as coming from a *curated*
source (a trusted-asset SQL, a curated SQL snippet, a GSO-applied patch) rather
than from generated benchmark SQL.

Curated-ness is carried as an explicit recorded kind, never inferred from a
``provenance_ids`` prefix (MV-D17). The prefix route is available — the ids do
survive :meth:`_Bucket.freeze` and ``mv_scoring.TRUSTED_ASSET_SOURCE_PREFIX``
already establishes the convention — but making a string prefix structural to
scoring is exactly the inference this codebase avoids everywhere else
(``LineageOverlap.reference_kind``, ``Provenance.kind``, the MV-D15 status
vocabulary all record a kind rather than deduce one). The corpus scan counts how
many *distinct curated sources* re-derived each expression into
:attr:`FingerprintRecurrence.curated_provenance_count`, which ``mv_scoring``
reads as the MV-D17 provenance up-weight.
"""


@dataclass(frozen=True)
class Provenance:
    """Where one statement came from. ``kind`` is free-form by design — the
    corpus mixes benchmark-generated SQL with ``system.query.history`` rows and
    the scanner has no reason to police the vocabulary. The one kind it *does*
    read is :data:`CURATED_PROVENANCE_KIND`, which the aggregation counts
    separately (MV-D17)."""

    id: str
    kind: str = ""
    seen_at: str | None = None


@dataclass(frozen=True)
class FingerprintRecurrence:
    """One canonical expression and how often the corpus re-derived it.

    ``recurrence`` counts occurrences (a statement using a measure twice counts
    twice); ``provenance_count`` counts the distinct sources that used it, which
    is the number that matters for demand — sixty occurrences from one query is
    not a recurring measure.

    ``curated_provenance_count`` is a strict subset of ``provenance_count``: the
    distinct sources whose :attr:`Provenance.kind` was
    :data:`CURATED_PROVENANCE_KIND`. It is what ``mv_scoring`` reads to up-weight
    Y for expressions a human (or a prior GSO patch) curated, as opposed to ones
    only the benchmark generator produced (MV-D17). It is deliberately separate
    from the breadth question ``provenance_count`` would answer — damping raw
    recurrence by distinct-source breadth is a distinct, deferred fix (MV-D17).
    """

    fingerprint: str
    canonical_expr: str
    kind: str
    recurrence: int
    provenance_ids: tuple[str, ...]
    provenance_count: int
    curated_provenance_count: int = 0
    first_seen: str | None = None
    last_seen: str | None = None
    source_columns: tuple[str, ...] = ()
    source_tables: tuple[str, ...] = ()
    shapes: tuple[str, ...] = ()
    representative_expr: str = ""
    """MV-D29: the literal-preserving render source, captured from the FIRST
    occurrence of this fingerprint in the scan (deterministic — corpus order is
    fixed). Populated for measures; the other kinds leave it ``""``. It is a
    render source only — ``canonical_expr`` remains the sole identity."""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CorpusScan:
    """Result of :func:`corpus_scan`.

    ``parse_failures`` is a first-class field rather than a log line: a scan
    that silently dropped half its corpus and a scan that found nothing look
    identical from the outside otherwise.
    """

    measures: tuple[FingerprintRecurrence, ...] = ()
    dimensions: tuple[FingerprintRecurrence, ...] = ()
    filters: tuple[FingerprintRecurrence, ...] = ()
    join_keys: tuple[FingerprintRecurrence, ...] = ()
    shapes: tuple[ShapeMatch, ...] = ()
    statements_scanned: int = 0
    parse_failures: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "measures": [m.to_dict() for m in self.measures],
            "dimensions": [d.to_dict() for d in self.dimensions],
            "filters": [f.to_dict() for f in self.filters],
            "join_keys": [j.to_dict() for j in self.join_keys],
            "shapes": [s.to_dict() for s in self.shapes],
            "statements_scanned": self.statements_scanned,
            "parse_failures": self.parse_failures,
        }


# ── Parsing ──────────────────────────────────────────────────────────────


def parse_statement(sql: str) -> exp.Expression | None:
    """Parse ``sql`` into a Databricks-dialect AST, or ``None``.

    Unparseable SQL is a fact about the corpus, not an error to raise: query
    history and LLM-generated SQL both contain statements sqlglot cannot read,
    and one of them must not abort a scan.
    """
    if not isinstance(sql, str) or not sql.strip():
        return None
    try:
        return sqlglot.parse_one(sql, read=DIALECT)
    except Exception as exc:  # noqa: BLE001 - any parse error is just a skip
        logger.debug("mv_fingerprint: unparseable SQL skipped (%s)", exc)
        return None


def _parse_expression(expr: str) -> exp.Expression | None:
    if not isinstance(expr, str) or not expr.strip():
        return None
    try:
        return sqlglot.parse_one(expr, read=DIALECT)
    except Exception as exc:  # noqa: BLE001
        logger.debug("mv_fingerprint: unparseable expression skipped (%s)", exc)
        return None


def _render(node: exp.Expression) -> str:
    """Render to a single lower-cased line. Keyword casing and pretty-printing
    are the last cosmetic differences left at this point."""
    text = node.sql(dialect=DIALECT, comments=False, pretty=False)
    return _WHITESPACE_RE.sub(" ", text).strip().lower()


def _has_ancestor(node: exp.Expression, kinds: type | tuple[type, ...]) -> bool:
    parent = node.parent
    while parent is not None:
        if isinstance(parent, kinds):
            return True
        parent = parent.parent
    return False


def _enclosing_select(node: exp.Expression) -> exp.Expression | None:
    parent = node.parent
    while parent is not None:
        if isinstance(parent, exp.Select):
            return parent
        parent = parent.parent
    return None


# ── Canonicalization passes ──────────────────────────────────────────────


def _resolve_projection_refs(tree: exp.Expression) -> None:
    """Rewrite GROUP BY / ORDER BY references to the projection they mean.

    ``GROUP BY 1`` is an ordinal *literal*, so it must be resolved before
    literal erasure or the grain dissolves into ``GROUP BY ?n``. Alias
    references (``GROUP BY revenue``) are resolved for the same reason
    canonicalization drops output aliases at all: two identical queries that
    named the same column differently are one shape.
    """
    for select in tree.find_all(exp.Select):
        projections = list(select.expressions)
        if not projections:
            continue

        aliases: dict[str, exp.Expression] = {}
        for projection in projections:
            if isinstance(projection, exp.Alias):
                name = projection.alias.lower()
                inner = projection.this
                # An alias that merely renames the column it selects carries no
                # expression to substitute, and substituting would rewrite a
                # legitimate source-column reference into itself.
                if name and not (isinstance(inner, exp.Column) and inner.name.lower() == name):
                    aliases[name] = inner

        def resolve(node: exp.Expression | None) -> exp.Expression | None:
            if node is None:
                return None
            if isinstance(node, exp.Literal) and not node.is_string:
                try:
                    index = int(str(node.this))
                except (TypeError, ValueError):
                    return None
                if 1 <= index <= len(projections):
                    target = projections[index - 1]
                    inner = target.this if isinstance(target, exp.Alias) else target
                    return inner.copy()
                return None
            if isinstance(node, exp.Column) and not node.table:
                match = aliases.get(node.name.lower())
                if match is not None:
                    return match.copy()
            return None

        group = select.args.get("group")
        if isinstance(group, exp.Group):
            for key in list(group.expressions):
                resolved = resolve(key)
                if resolved is not None:
                    key.replace(resolved)

        order = select.args.get("order")
        if isinstance(order, exp.Order):
            for ordered in list(order.expressions):
                resolved = resolve(ordered.this if isinstance(ordered, exp.Ordered) else ordered)
                if resolved is not None and isinstance(ordered, exp.Ordered):
                    ordered.this.replace(resolved)


def _strip_output_aliases(tree: exp.Expression) -> None:
    """Drop ``AS name`` from projections. Output names are presentation; two
    queries differing only in them are the same shape."""
    for select in tree.find_all(exp.Select):
        for projection in list(select.expressions):
            if isinstance(projection, exp.Alias):
                projection.replace(projection.this.copy())


def _relation_order(tree: exp.Expression) -> dict[str, str]:
    """Map every relation name/alias to ``t1..tn`` by first appearance."""
    order: dict[str, str] = {}

    def register(name: str | None) -> None:
        key = (name or "").lower()
        if key and key not in order:
            order[key] = f"t{len(order) + 1}"

    for node in tree.walk():
        if isinstance(node, exp.CTE):
            register(node.alias_or_name)
        elif isinstance(node, exp.Table):
            register(node.alias or node.name)
        elif isinstance(node, exp.Subquery):
            register(node.alias)
    return order


def _rename_relations(tree: exp.Expression) -> None:
    """Apply :func:`_relation_order`. Positional names make two statements that
    differ only in how they spelled their aliases render identically."""
    order = _relation_order(tree)
    if not order:
        return

    for node in tree.find_all(exp.CTE):
        canonical = order.get((node.alias_or_name or "").lower())
        if canonical:
            node.set("alias", exp.TableAlias(this=exp.to_identifier(canonical)))

    for node in tree.find_all(exp.Table):
        canonical = order.get((node.alias or node.name or "").lower())
        if canonical:
            node.set("alias", exp.TableAlias(this=exp.to_identifier(canonical)))

    for node in tree.find_all(exp.Subquery):
        canonical = order.get((node.alias or "").lower())
        if canonical:
            node.set("alias", exp.TableAlias(this=exp.to_identifier(canonical)))

    for column in tree.find_all(exp.Column):
        canonical = order.get((column.table or "").lower())
        if canonical:
            column.set("table", exp.to_identifier(canonical))


def _normalize_identifiers(tree: exp.Expression) -> None:
    """Lower-case and unquote every identifier, so ``` `Revenue` ``` and
    ``revenue`` are one name."""
    for identifier in tree.find_all(exp.Identifier):
        if isinstance(identifier.this, str):
            identifier.set("this", identifier.this.lower())
        identifier.set("quoted", False)


def _normalize_temporal_units(tree: exp.Expression) -> exp.Expression:
    """Fold a function's date-part unit into its name.

    ``DATE_TRUNC('month', d)`` carries its grain in a ``unit`` argument that
    sqlglot renders quoted. Left alone it survives as a quoted token, which no
    reviewer or firewall grep can tell from a data literal; erased with the
    literals it merges monthly with daily. Folding it into the function name —
    ``timestamp_trunc_month(d)`` — keeps the grain distinction *and* keeps the
    canonical form free of quote characters.

    Deepest-first, so a unit function nested inside another is rewritten before
    its parent copies it.
    """
    for node in reversed(list(tree.walk())):
        if not isinstance(node, exp.Func):
            continue
        unit = node.args.get("unit")
        if not isinstance(unit, (exp.Var, exp.Literal)):
            continue
        raw = f"{node.sql_name()}_{unit.name or unit.this}".lower()
        replacement = exp.Anonymous(
            this=re.sub(r"[^a-z0-9_]", "_", raw),
            expressions=[
                value.copy()
                for key, value in node.args.items()
                if key != "unit" and isinstance(value, exp.Expression)
            ],
        )
        if node.parent is None:
            return replacement
        node.replace(replacement)
    return tree


def _erase_literals(tree: exp.Expression) -> exp.Expression:
    """Replace string and numeric literals with bare typed placeholders.

    Booleans and NULL survive: they carry no value, and merging
    ``is_current = TRUE`` with ``= FALSE`` would destroy a real distinction.
    Literals inside a type declaration (``DECIMAL(10, 2)``) survive for the
    same reason — they are part of the type, not data.

    The star in ``COUNT(*)`` is erased to the same placeholder as the ``1`` in
    ``COUNT(1)``, because those are one measure written two ways. A bare
    ``SELECT *`` is untouched.
    """

    def transform(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Literal) and not _has_ancestor(node, exp.DataType):
            tag = STRING_PLACEHOLDER if node.is_string else NUMERIC_PLACEHOLDER
            return exp.Var(this=tag)
        if isinstance(node, exp.Star) and isinstance(node.parent, exp.AggFunc):
            return exp.Var(this=NUMERIC_PLACEHOLDER)
        return node

    return tree.transform(transform, copy=False)


def _flatten_boolean_parens(tree: exp.Expression) -> None:
    """Unwrap parentheses around ``AND`` so a whole conjunction is one flat
    chain. ``NOT (a AND b)`` is left alone — there the parens are the meaning."""
    for paren in list(tree.find_all(exp.Paren)):
        if isinstance(paren.this, exp.And) and not isinstance(paren.parent, exp.Not):
            paren.replace(paren.this)


def _sort_conjunctions(tree: exp.Expression) -> None:
    """Sort each ``AND`` chain lexicographically.

    Runs last, after literals and identifiers are already normalized, so the
    sort keys are the final canonical text. Only the top of each chain is
    processed: ``flatten()`` collapses the nested ``And`` nodes beneath it.
    """
    chains = [node for node in tree.find_all(exp.And) if not isinstance(node.parent, exp.And)]
    for chain in reversed(chains):
        conjuncts = sorted(
            (conjunct.copy() for conjunct in chain.flatten()),
            key=lambda node: _render(node),
        )
        if len(conjuncts) > 1:
            chain.replace(exp.and_(*conjuncts, copy=False))


def _canonicalize_tree(
    tree: exp.Expression, *, strip_qualifiers: bool, erase_literals: bool = True
) -> exp.Expression:
    """The full pass order. Ordinal resolution must precede literal erasure and
    conjunct sorting must follow everything else; the rest is independent.

    ``erase_literals`` defaults to ``True`` — the firewall default that every
    canonical form (fingerprint identity, dedup key) depends on. It is turned
    off for exactly one consumer, :func:`render_expr` (MV-D29): the render
    source needs a *literal-preserving* form so a structural constant such as
    the ``1`` in ``1 - l_discount`` survives into an executable body. That form
    never feeds identity, scoring, or dedup, and it is gated by ``LeakageOracle``
    before it can reach shipped YAML.
    """
    tree = tree.copy()
    _resolve_projection_refs(tree)
    _strip_output_aliases(tree)
    if strip_qualifiers:
        for column in tree.find_all(exp.Column):
            column.set("table", None)
            column.set("db", None)
            column.set("catalog", None)
    else:
        _rename_relations(tree)
    _normalize_identifiers(tree)
    tree = _normalize_temporal_units(tree)
    if erase_literals:
        tree = _erase_literals(tree)
    _flatten_boolean_parens(tree)
    _sort_conjunctions(tree)
    return tree


def canonicalize_sql_ast(sql: str) -> str:
    """Return the canonical text of a whole statement, or ``""``.

    Collapses alias spelling (relations become ``t1..tn`` by first appearance),
    output aliases, GROUP BY ordinals and alias references, identifier case and
    quoting, AND-predicate order, literal values, keyword case, comments, and
    whitespace.

    The inverse of ``leakage.canonicalize_sql``, which preserves literals
    because verbatim text is exactly what the firewall looks for. This function
    guarantees the opposite: no string or numeric literal survives, so its
    output is safe to log and to hash. It returns *text* rather than a digest —
    :func:`expr_fingerprint` hashes it, and ``mv_state`` needs the text.
    """
    tree = parse_statement(sql)
    if tree is None:
        return ""
    try:
        return _render(_canonicalize_tree(tree, strip_qualifiers=False))
    except Exception as exc:  # noqa: BLE001 - a canonicalization gap is a skip
        logger.debug("mv_fingerprint: canonicalization failed (%s)", exc)
        return ""


def canonicalize_expr(expr: str | exp.Expression, *, strip_qualifiers: bool = True) -> str:
    """Return the canonical text of a single expression, or ``""``.

    Table qualifiers are stripped by default, which is what makes
    ``SUM(l.l_extendedprice * (1 - l.l_discount))``,
    ``SUM(li.l_extendedprice * (1 - li.l_discount))`` and the unqualified
    spelling one measure. Table identity is not lost — it travels in
    :attr:`MeasureRef.source_tables` and is hashed separately by the MV-D7 key,
    where it belongs.

    Literals are erased here exactly as in :func:`canonicalize_sql_ast`, and for
    the opposite reason to ``leakage.canonicalize_sql`` keeping them.
    """
    tree = _parse_expression(expr) if isinstance(expr, str) else expr
    if tree is None:
        return ""
    try:
        return _render(_canonicalize_tree(tree, strip_qualifiers=strip_qualifiers))
    except Exception as exc:  # noqa: BLE001
        logger.debug("mv_fingerprint: expression canonicalization failed (%s)", exc)
        return ""


def render_expr(expr: str | exp.Expression, *, strip_qualifiers: bool = True) -> str:
    """Return a *literal-preserving* render form of a single expression (MV-D29).

    Same normalization as :func:`canonicalize_expr` — qualifiers stripped by
    default so the result references the metric view's ``source:`` columns,
    identifiers and temporal units normalized, conjunctions flattened and
    sorted — **except that literals are kept**. So ``SUM(l.l_extendedprice *
    (1 - l.l_discount))`` renders as ``SUM(l_extendedprice * (1 - l_discount))``,
    an expression that a ``CREATE VIEW`` can actually execute, where
    :func:`canonicalize_expr` would emit ``SUM(l_extendedprice * (?n -
    l_discount))``.

    **This is not a canonical form and must never be treated as one.** It is
    only a render source: it never feeds :func:`expr_fingerprint`, the MV-D7
    dedup key, or scoring. Because it can carry a benchmark/PII predicate
    literal, a consumer MUST pass it through ``LeakageOracle`` before it reaches
    a shipped body and drop the candidate if it matches — the firewall moves
    from erasure-by-construction to an actual gate (MV-D29). Returns ``""`` when
    the expression does not parse, exactly like :func:`canonicalize_expr`.
    """
    tree = _parse_expression(expr) if isinstance(expr, str) else expr
    if tree is None:
        return ""
    try:
        return _render(
            _canonicalize_tree(
                tree, strip_qualifiers=strip_qualifiers, erase_literals=False
            )
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("mv_fingerprint: expression render failed (%s)", exc)
        return ""


def _text_fingerprint(text: str) -> str:
    """``sha256`` of already-canonical text. For values assembled by this module
    (a sorted join pair, a rewrite form) rather than parsed from a corpus."""
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def expr_fingerprint(expr: str | exp.Expression) -> str:
    """Return ``sha256`` of the canonical expression text, or ``""``.

    **This is not a dedup key and must never be persisted as one** (MV-D10).
    It is expression-grained and exists to count recurrence inside a corpus
    scan. The candidate-level key — the one that goes into
    ``genie_opt_mv_candidates.dedup_fingerprint`` and
    ``genie_opt_artifacts.content_hash`` — is
    ``mv_state.mv_candidate_fingerprint``, which additionally binds the space
    and the source set. Persisting this value under either column would create
    a second key that collides across spaces and across source sets.
    """
    return _text_fingerprint(canonicalize_expr(expr))


# ── Column and table resolution ──────────────────────────────────────────


def _table_fqn(table: exp.Table) -> str:
    parts = [part for part in (table.catalog, table.db, table.name) if part]
    return ".".join(part.lower() for part in parts)


def _relation_map(tree: exp.Expression) -> tuple[dict[str, str], tuple[str, ...]]:
    """Return ``{alias_or_name: fqn}`` and every table FQN in the statement.

    Built from the *original* tree, before canonicalization renames anything —
    positional aliases are for rendering, while source attribution needs the
    real names.
    """
    by_alias: dict[str, str] = {}
    tables: list[str] = []
    for table in tree.find_all(exp.Table):
        fqn = _table_fqn(table)
        if not fqn:
            continue
        if fqn not in tables:
            tables.append(fqn)
        if table.alias:
            by_alias[table.alias.lower()] = fqn
        by_alias.setdefault(table.name.lower(), fqn)
    return by_alias, tuple(tables)


def _attribute_columns(
    node: exp.Expression,
    by_alias: Mapping[str, str],
    tables: tuple[str, ...],
) -> tuple[tuple[str, ...], tuple[str, ...], bool]:
    """Resolve a node's columns to ``(columns, tables, has_unresolved)``.

    An unqualified column in a single-table statement is attributed to that
    table. In a multi-table statement it stays unattributed and flips the
    unresolved flag: guessing which table a bare column came from is how a
    measure gets proposed against the wrong source, and the caller can decide
    whether to fall back to profiling.
    """
    columns: set[str] = set()
    resolved: set[str] = set()
    unresolved = False

    for column in node.find_all(exp.Column):
        name = column.name.lower()
        if not name:
            continue
        columns.add(name)
        qualifier = (column.table or "").lower()
        if qualifier:
            fqn = by_alias.get(qualifier)
            if fqn:
                resolved.add(fqn)
            else:
                unresolved = True
        elif len(tables) == 1:
            resolved.add(tables[0])
        else:
            unresolved = True

    return tuple(sorted(columns)), tuple(sorted(resolved)), unresolved


# ── Extraction ───────────────────────────────────────────────────────────


def _outermost_aggregates(tree: exp.Expression) -> list[exp.AggFunc]:
    """Aggregates not nested inside another aggregate, filtered to the
    proposable set. ``SUM(SUM(x)) OVER ()`` contributes its outer aggregate
    once, not twice.

    ORDER BY and GROUP BY positions are skipped: an aggregate there is a
    reference to a projection, and ordinal resolution has just put a copy of
    that projection in it. Counting both would double every measure in a
    ``GROUP BY 1 ORDER BY 2`` statement.
    """
    found: list[exp.AggFunc] = []
    for node in tree.find_all(exp.AggFunc):
        if node.sql_name() not in MEASURE_AGGREGATES:
            continue
        if _has_ancestor(node, (exp.AggFunc, exp.Order, exp.Group)):
            continue
        found.append(node)
    return found


def extract_measures(sql: str) -> tuple[MeasureRef, ...]:
    """Extract aggregate expressions, each with its source columns and tables.

    Includes ``SUM(CASE WHEN … THEN 1 END)`` shapes and aggregates carrying a
    ``FILTER`` clause (the clause is part of the measure and stays in the
    canonical text). A windowed aggregate is reported with ``is_windowed`` set
    and canonicalized *without* its window spec: the underlying demand is still
    ``sum(x)``, so it shares a fingerprint with the plain form while the flag
    tells a caller it cannot be a plain additive measure.
    """
    tree = parse_statement(sql)
    if tree is None:
        return ()

    by_alias, tables = _relation_map(tree)
    resolved = tree.copy()
    _resolve_projection_refs(resolved)

    measures: list[MeasureRef] = []
    for aggregate in _outermost_aggregates(resolved):
        node: exp.Expression = aggregate
        if isinstance(aggregate.parent, exp.Filter):
            node = aggregate.parent

        canonical = canonicalize_expr(node)
        if not canonical:
            continue

        columns, sources, unresolved = _attribute_columns(node, by_alias, tables)
        measures.append(
            MeasureRef(
                canonical_expr=canonical,
                fingerprint=expr_fingerprint(node),
                aggregate=aggregate.sql_name(),
                representative_expr=render_expr(node),
                source_columns=columns,
                source_tables=sources,
                is_windowed=isinstance(aggregate.parent, exp.Window),
                is_distinct=bool(aggregate.find(exp.Distinct)),
                has_unresolved_columns=unresolved,
            )
        )
    return tuple(measures)


def extract_dimensions(sql: str) -> tuple[DimensionRef, ...]:
    """Extract the GROUP BY sets. ``GROUP BY 1`` and ``GROUP BY <alias>`` are
    resolved to the projections they reference first, so an ordinal query and a
    spelled-out one produce the same dimensions."""
    tree = parse_statement(sql)
    if tree is None:
        return ()

    by_alias, tables = _relation_map(tree)
    resolved = tree.copy()
    _resolve_projection_refs(resolved)

    dimensions: list[DimensionRef] = []
    seen: set[str] = set()
    for group in resolved.find_all(exp.Group):
        for key in group.expressions:
            canonical = canonicalize_expr(key)
            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            columns, sources, _ = _attribute_columns(key, by_alias, tables)
            dimensions.append(
                DimensionRef(
                    canonical_expr=canonical,
                    fingerprint=expr_fingerprint(key),
                    source_columns=columns,
                    source_tables=sources,
                    is_expression=not isinstance(key, exp.Column),
                )
            )
    return tuple(dimensions)


def statement_grain(sql: str) -> str:
    """Fingerprint of a statement's grain — its sorted GROUP BY set — or ``""``
    when the statement is ungrouped. Two aggregates share a grain when they sit
    in statements with the same value here, which is what makes a
    :data:`SHAPE_RATIO` safe to compose."""
    dimensions = extract_dimensions(sql)
    if not dimensions:
        return ""
    material = ",".join(sorted(dimension.canonical_expr for dimension in dimensions))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _is_cross_table_equality(
    node: exp.Expression,
    by_alias: Mapping[str, str],
    tables: tuple[str, ...],
) -> tuple[str, str] | None:
    """Return the resolved column pair when ``node`` equates two columns from
    two different tables."""
    if not isinstance(node, exp.EQ):
        return None
    left, right = node.this, node.expression
    if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
        return None

    def qualified(column: exp.Column) -> str | None:
        qualifier = (column.table or "").lower()
        fqn = by_alias.get(qualifier) if qualifier else (tables[0] if len(tables) == 1 else None)
        if not fqn:
            return None
        return f"{fqn}.{column.name.lower()}"

    left_name, right_name = qualified(left), qualified(right)
    if not left_name or not right_name:
        return None
    if left_name.rsplit(".", 1)[0] == right_name.rsplit(".", 1)[0]:
        return None
    first, second = sorted((left_name, right_name))
    return first, second


def _conjuncts(node: exp.Expression | None) -> list[exp.Expression]:
    if node is None:
        return []
    if isinstance(node, exp.And):
        return list(node.flatten())
    return [node]


def extract_filters(sql: str) -> tuple[FilterRef, ...]:
    """Extract WHERE and HAVING conjuncts.

    Cross-table equalities are omitted — they are join keys, and reporting them
    twice would let a join condition inflate a filter's recurrence.
    """
    tree = parse_statement(sql)
    if tree is None:
        return ()

    by_alias, tables = _relation_map(tree)
    resolved = tree.copy()
    _resolve_projection_refs(resolved)

    filters: list[FilterRef] = []
    for clause_name, clause_type in (("where", exp.Where), ("having", exp.Having)):
        for clause in resolved.find_all(clause_type):
            for conjunct in _conjuncts(clause.this):
                if _is_cross_table_equality(conjunct, by_alias, tables):
                    continue
                canonical = canonicalize_expr(conjunct)
                if not canonical:
                    continue
                columns, sources, _ = _attribute_columns(conjunct, by_alias, tables)
                filters.append(
                    FilterRef(
                        canonical_expr=canonical,
                        fingerprint=expr_fingerprint(conjunct),
                        clause=clause_name,
                        operator=type(conjunct).__name__.upper(),
                        source_columns=columns,
                        source_tables=sources,
                    )
                )
    return tuple(filters)


def extract_join_keys(sql: str) -> tuple[JoinKeyRef, ...]:
    """Extract column-to-column equalities from ``JOIN … ON`` and from
    WHERE-clause cross-table equalities.

    The WHERE path is not an extra: the classic TPC-H statements comma-join and
    equate in WHERE, so a detector that only reads ON clauses finds no joins in
    most of the corpus it was written for.
    """
    tree = parse_statement(sql)
    if tree is None:
        return ()

    by_alias, tables = _relation_map(tree)
    keys: list[JoinKeyRef] = []
    seen: set[tuple[str, str]] = set()

    def collect(node: exp.Expression | None, origin: str) -> None:
        for conjunct in _conjuncts(node):
            pair = _is_cross_table_equality(conjunct, by_alias, tables)
            if pair is None or pair in seen:
                continue
            seen.add(pair)
            left, right = pair
            canonical = f"{left} = {right}"
            keys.append(
                JoinKeyRef(
                    canonical_expr=canonical,
                    fingerprint=_text_fingerprint(canonical),
                    left=left,
                    right=right,
                    tables=tuple(sorted({left.rsplit(".", 1)[0], right.rsplit(".", 1)[0]})),
                    origin=origin,
                )
            )

    for join in tree.find_all(exp.Join):
        collect(join.args.get("on"), "on")
    for where in tree.find_all(exp.Where):
        collect(where.this, "where")
    return tuple(keys)


# ── Shape classification ─────────────────────────────────────────────────


def _windowed_aggregate(node: exp.Expression) -> exp.Window | None:
    for window in node.find_all(exp.Window):
        if isinstance(window.this, exp.AggFunc):
            return window
    return None


def _conditional_count_condition(aggregate: exp.AggFunc) -> exp.Expression | None:
    """Return the condition of a ``SUM/COUNT(CASE WHEN c THEN 1 [ELSE 0] END)``.

    Requires a single WHEN and a numeric THEN, which is what makes the shape a
    *count* rather than a conditional sum of some other column — the latter has
    no ``COUNT(1) FILTER`` form.
    """
    if aggregate.sql_name() not in {"SUM", "COUNT"}:
        return None
    case = aggregate.this
    if not isinstance(case, exp.Case):
        return None
    ifs = case.args.get("ifs") or []
    if len(ifs) != 1:
        return None
    then = ifs[0].args.get("true")
    if not isinstance(then, exp.Literal) or then.is_string:
        return None
    default = case.args.get("default")
    if default is not None and not (
        isinstance(default, exp.Null)
        or (isinstance(default, exp.Literal) and not default.is_string and str(default.this) == "0")
    ):
        return None
    return ifs[0].this


def shapes_in_statement(sql: str) -> tuple[ShapeMatch, ...]:
    """Detect :data:`SHAPE_KINDS` in one statement.

    Runs against the pre-erasure AST, because the detectors read literal values
    (``THEN 1``, ``ELSE 0``) that canonicalization is about to remove.
    ``PCT_OF_TOTAL`` is tested before ``RATIO``: a windowed denominator is a
    percent-of-total, and calling it a plain ratio is what produces the
    ``MEASURE()/MEASURE()`` form that always evaluates to 1.0.
    """
    tree = parse_statement(sql)
    if tree is None:
        return ()

    by_alias, tables = _relation_map(tree)
    resolved = tree.copy()
    _resolve_projection_refs(resolved)

    matches: list[ShapeMatch] = []

    def add(
        kind: str,
        node: exp.Expression,
        components: dict[str, str],
        target_form: str | None = None,
    ) -> None:
        canonical = canonicalize_expr(node)
        if not canonical:
            return
        form = target_form or canonical
        columns, sources, _ = _attribute_columns(node, by_alias, tables)
        matches.append(
            ShapeMatch(
                kind=kind,
                canonical_expr=canonical,
                fingerprint=_text_fingerprint(form),
                guidance=SHAPE_GUIDANCE[kind],
                target_form=form,
                components=tuple(sorted(components.items())),
                source_columns=columns,
                source_tables=sources,
            )
        )

    for division in resolved.find_all(exp.Div):
        if _has_ancestor(division, (exp.Order, exp.Group)):
            continue
        numerator, denominator = division.this, division.expression
        window = _windowed_aggregate(denominator)
        if window is not None:
            add(
                SHAPE_PCT_OF_TOTAL,
                division,
                {
                    "numerator": canonicalize_expr(numerator),
                    "windowed_total": canonicalize_expr(window),
                },
            )
            continue
        if _windowed_aggregate(numerator) is not None:
            continue
        if numerator.find(exp.AggFunc) is None or denominator.find(exp.AggFunc) is None:
            continue
        if _enclosing_select(numerator) is not _enclosing_select(denominator):
            continue
        add(
            SHAPE_RATIO,
            division,
            {
                "numerator": canonicalize_expr(numerator),
                "denominator": canonicalize_expr(denominator),
            },
        )

    for aggregate in _outermost_aggregates(resolved):
        condition = _conditional_count_condition(aggregate)
        if condition is None:
            continue
        canonical_condition = canonicalize_expr(condition)
        rewrite = f"count(1) filter (where {canonical_condition})"
        add(
            SHAPE_CONDITIONAL_COUNT,
            aggregate,
            {"condition": canonical_condition, "rewrite": rewrite},
            target_form=rewrite,
        )

    return tuple(matches)


# ── Corpus scanning ──────────────────────────────────────────────────────


def _coerce_provenance(value: Any) -> Provenance:
    if isinstance(value, Provenance):
        return value
    if isinstance(value, Mapping):
        return Provenance(
            id=str(value.get("id") or ""),
            kind=str(value.get("kind") or ""),
            seen_at=(str(value["seen_at"]) if value.get("seen_at") else None),
        )
    return Provenance(id=str(value or ""))


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


class _Bucket:
    """Mutable accumulator behind one :class:`FingerprintRecurrence`."""

    __slots__ = (
        "canonical_expr",
        "columns",
        "curated_provenance_ids",
        "first_seen",
        "first_ts",
        "fingerprint",
        "kind",
        "last_seen",
        "last_ts",
        "provenance_ids",
        "recurrence",
        "representative_expr",
        "shapes",
        "tables",
    )

    def __init__(
        self,
        fingerprint: str,
        canonical_expr: str,
        kind: str,
        representative_expr: str = "",
    ) -> None:
        self.fingerprint = fingerprint
        self.canonical_expr = canonical_expr
        self.kind = kind
        # MV-D29: captured once, from the first occurrence that created this
        # bucket — a later occurrence with the same fingerprint (hence the same
        # shape) does not overwrite it, so the representative is stable.
        self.representative_expr = representative_expr
        self.recurrence = 0
        self.provenance_ids: set[str] = set()
        self.curated_provenance_ids: set[str] = set()
        self.columns: set[str] = set()
        self.tables: set[str] = set()
        self.shapes: set[str] = set()
        self.first_seen: str | None = None
        self.last_seen: str | None = None
        self.first_ts: datetime | None = None
        self.last_ts: datetime | None = None

    def observe(
        self,
        provenance: Provenance,
        columns: Iterable[str] = (),
        tables: Iterable[str] = (),
    ) -> None:
        self.recurrence += 1
        if provenance.id:
            self.provenance_ids.add(provenance.id)
            if provenance.kind == CURATED_PROVENANCE_KIND:
                self.curated_provenance_ids.add(provenance.id)
        self.columns.update(columns)
        self.tables.update(tables)

        stamp = provenance.seen_at
        if not stamp:
            return
        parsed = _parse_timestamp(stamp)
        # Comparable timestamps sort by value; unparseable ones fall back to
        # string order so a mixed corpus still yields a stable window.
        if self.first_seen is None:
            self.first_seen = self.last_seen = stamp
            self.first_ts = self.last_ts = parsed
            return
        if _before(stamp, parsed, self.first_seen, self.first_ts):
            self.first_seen, self.first_ts = stamp, parsed
        if _before(self.last_seen, self.last_ts, stamp, parsed):
            self.last_seen, self.last_ts = stamp, parsed

    def freeze(self) -> FingerprintRecurrence:
        return FingerprintRecurrence(
            fingerprint=self.fingerprint,
            canonical_expr=self.canonical_expr,
            kind=self.kind,
            recurrence=self.recurrence,
            provenance_ids=tuple(sorted(self.provenance_ids)),
            provenance_count=len(self.provenance_ids),
            curated_provenance_count=len(self.curated_provenance_ids),
            first_seen=self.first_seen,
            last_seen=self.last_seen,
            source_columns=tuple(sorted(self.columns)),
            source_tables=tuple(sorted(self.tables)),
            shapes=tuple(sorted(self.shapes)),
            representative_expr=self.representative_expr,
        )


def _before(
    left: str | None,
    left_ts: datetime | None,
    right: str | None,
    right_ts: datetime | None,
) -> bool:
    if left_ts is not None and right_ts is not None:
        return left_ts < right_ts
    return (left or "") < (right or "")


def _rank(buckets: Iterable[_Bucket]) -> tuple[FingerprintRecurrence, ...]:
    frozen = [bucket.freeze() for bucket in buckets]
    frozen.sort(key=lambda row: (-row.recurrence, -row.provenance_count, row.fingerprint))
    return tuple(frozen)


def _iter_entries(corpus: Iterable[Any]) -> Iterable[tuple[str, Provenance]]:
    for entry in corpus:
        if isinstance(entry, str):
            yield entry, Provenance(id="")
            continue
        try:
            sql, provenance = entry
        except (TypeError, ValueError):
            logger.debug("mv_fingerprint: skipping malformed corpus entry")
            continue
        yield sql, _coerce_provenance(provenance)


def corpus_scan(corpus: Iterable[Any]) -> CorpusScan:
    """Scan ``(sql, provenance)`` pairs into recurrence-ranked fingerprints.

    ``provenance`` may be a plain id string, a :class:`Provenance`, or a mapping
    with ``id`` / ``kind`` / ``seen_at``; a bare SQL string is accepted as an
    entry with no provenance. Results are ranked by recurrence, then by distinct
    provenance count, then by fingerprint — total and deterministic, so two
    scans of one corpus produce byte-identical output.
    """
    measures: dict[str, _Bucket] = {}
    dimensions: dict[str, _Bucket] = {}
    filters: dict[str, _Bucket] = {}
    join_keys: dict[str, _Bucket] = {}
    shapes: dict[str, ShapeMatch] = {}

    scanned = 0
    failures = 0

    for sql, provenance in _iter_entries(corpus):
        if parse_statement(sql) is None:
            failures += 1
            continue
        scanned += 1

        statement_shapes = shapes_in_statement(sql)
        shape_kinds_by_expr: dict[str, set[str]] = {}
        for match in statement_shapes:
            for _, component in match.components:
                shape_kinds_by_expr.setdefault(component, set()).add(match.kind)
            shape_kinds_by_expr.setdefault(match.canonical_expr, set()).add(match.kind)

            existing = shapes.get(match.fingerprint)
            if existing is None:
                shapes[match.fingerprint] = replace(
                    match,
                    provenance_ids=(provenance.id,) if provenance.id else (),
                    first_seen=provenance.seen_at,
                    last_seen=provenance.seen_at,
                )
                continue
            merged_ids = set(existing.provenance_ids)
            if provenance.id:
                merged_ids.add(provenance.id)
            first, last = existing.first_seen, existing.last_seen
            if provenance.seen_at:
                first_ts, last_ts = _parse_timestamp(first), _parse_timestamp(last)
                stamp_ts = _parse_timestamp(provenance.seen_at)
                if first is None or _before(provenance.seen_at, stamp_ts, first, first_ts):
                    first = provenance.seen_at
                if last is None or _before(last, last_ts, provenance.seen_at, stamp_ts):
                    last = provenance.seen_at
            shapes[match.fingerprint] = replace(
                existing,
                recurrence=existing.recurrence + 1,
                provenance_ids=tuple(sorted(merged_ids)),
                first_seen=first,
                last_seen=last,
            )

        for measure in extract_measures(sql):
            bucket = measures.setdefault(
                measure.fingerprint,
                _Bucket(
                    measure.fingerprint,
                    measure.canonical_expr,
                    "measure",
                    measure.representative_expr,
                ),
            )
            bucket.observe(provenance, measure.source_columns, measure.source_tables)
            bucket.shapes.update(shape_kinds_by_expr.get(measure.canonical_expr, ()))

        for dimension in extract_dimensions(sql):
            bucket = dimensions.setdefault(
                dimension.fingerprint,
                _Bucket(dimension.fingerprint, dimension.canonical_expr, "dimension"),
            )
            bucket.observe(provenance, dimension.source_columns, dimension.source_tables)

        for filter_ref in extract_filters(sql):
            bucket = filters.setdefault(
                filter_ref.fingerprint,
                _Bucket(filter_ref.fingerprint, filter_ref.canonical_expr, "filter"),
            )
            bucket.observe(provenance, filter_ref.source_columns, filter_ref.source_tables)

        for join_key in extract_join_keys(sql):
            bucket = join_keys.setdefault(
                join_key.fingerprint,
                _Bucket(join_key.fingerprint, join_key.canonical_expr, "join_key"),
            )
            bucket.observe(provenance, (), join_key.tables)

    ranked_shapes = sorted(
        shapes.values(),
        key=lambda match: (-match.recurrence, -len(match.provenance_ids), match.fingerprint),
    )

    return CorpusScan(
        measures=_rank(measures.values()),
        dimensions=_rank(dimensions.values()),
        filters=_rank(filters.values()),
        join_keys=_rank(join_keys.values()),
        shapes=tuple(ranked_shapes),
        statements_scanned=scanned,
        parse_failures=failures,
    )


def classify_shapes(corpus: Iterable[Any]) -> tuple[ShapeMatch, ...]:
    """Tag the recurring shapes in a corpus, ranked like :func:`corpus_scan`.

    Takes the same entries as :func:`corpus_scan` and returns its ``shapes``
    field. Use :func:`shapes_in_statement` for one statement.
    """
    return corpus_scan(corpus).shapes


__all__ = [
    "CURATED_PROVENANCE_KIND",
    "DIALECT",
    "MEASURE_AGGREGATES",
    "NUMERIC_PLACEHOLDER",
    "SHAPE_CONDITIONAL_COUNT",
    "SHAPE_GUIDANCE",
    "SHAPE_KINDS",
    "SHAPE_PCT_OF_TOTAL",
    "SHAPE_RATIO",
    "STRING_PLACEHOLDER",
    "CorpusScan",
    "DimensionRef",
    "FilterRef",
    "FingerprintRecurrence",
    "JoinKeyRef",
    "MeasureRef",
    "Provenance",
    "ShapeMatch",
    "canonicalize_expr",
    "canonicalize_sql_ast",
    "classify_shapes",
    "corpus_scan",
    "expr_fingerprint",
    "extract_dimensions",
    "extract_filters",
    "extract_join_keys",
    "extract_measures",
    "parse_statement",
    "render_expr",
    "shapes_in_statement",
    "statement_grain",
]
