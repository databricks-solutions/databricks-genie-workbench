"""Metric view YAML rendering and its static validator (MV-D8).

This module is the **only** place in the codebase that renders metric view YAML.
The advisor phase inside the optimize task and the backend's OBO create path
both call :func:`generate` and :func:`validate`; neither assembles YAML itself.
Nothing else in the package emits YAML at all — there is no ``yaml.dump`` call
outside this file — so "one renderer" is a property that can be checked rather
than a convention that can drift.

Two halves, with a deliberate split of authority between them:

* :func:`generate` decides *what to emit* from a scored candidate plus
  :class:`MvProfiling`, which carries the physical facts (which columns exist
  where, which keys are proven unique, which runtime capabilities the probe
  found). Every rule it applies is normative — see the module constants and
  MV-D8. It never queries anything: profiling is an input, so generation is
  pure and therefore testable without a warehouse.
* :func:`validate` re-checks emitted YAML *statically*. It is not a formality
  after generation — it is also the gate for YAML this module did not write
  (a reviewer's edit, an LLM-proposed ``update_mv_yaml`` body), which is the
  case that actually needs a lint.

Restated in-repo rather than cited by external path, per the Prompt 5.5 rule
that reference material is a source and not an authority:

**The left-head-of-``on`` rule.** Every first-level join's ``on`` must join the
main relation to that join's own alias: the left side of each column-to-column
equality resolves to ``source``, and the right side to the join's alias. At
depth *d* the left side resolves to the *parent* join's alias instead. A join
whose ``on`` references a *sibling* alias is a transitive join — metric views
reject it, and it is the single most common way a hand-written snowflake join
fails. Multi-hop relationships are expressed with nested ``joins:``, a
denormalized column, or a subquery source; never by chaining siblings.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

import sqlglot
import yaml
from sqlglot import expressions as exp

from genie_space_optimizer.common.config import (
    MV_ADVISOR_GENERATED_BY,
    MV_CAPABILITY_FIELDS_AGG_WINDOW_OFFSET,
    MV_CAPABILITY_NESTED_JOINS,
    MV_COMMENT_ECHO_THRESHOLD,
    MV_COMMENT_SECTIONS,
    MV_ECHO_CHECK_COMPARED,
    MV_ECHO_CHECK_NOT_COMPARED,
    MV_FORMAT_TYPE_CORRECTIONS,
    MV_FORMAT_TYPES,
    MV_JOIN_STRATEGY_DENORMALIZED,
    MV_JOIN_STRATEGY_DIRECT,
    MV_JOIN_STRATEGY_NESTED,
    MV_JOIN_STRATEGY_SUBQUERY,
    MV_SYNONYM_MAX_CHARS,
    MV_SYNONYMS_MAX,
    MV_SYNONYMS_MIN,
    MV_UNSUPPORTED_JOIN_FIELDS,
    MV_UNSUPPORTED_TOP_LEVEL_FIELDS,
    MV_YAML_VERSION,
)
from genie_space_optimizer.optimization.mv_fingerprint import (
    SHAPE_CONDITIONAL_COUNT,
    SHAPE_PCT_OF_TOTAL,
    SHAPE_RATIO,
    ShapeMatch,
    extract_measures,
)
from genie_space_optimizer.optimization.mv_scoring import (
    VERDICT_CONFLICT,
    VERDICT_PROPOSE,
    DedupOutcome,
    MetricViewCandidate,
)

__all__ = [
    "CapabilityRow",
    "CapabilityRows",
    "ColumnFacts",
    "GeneratedMetricView",
    "JoinHop",
    "KeyUniqueness",
    "MvProfiling",
    "RequestedAttribute",
    "UNIQUENESS_EXACT",
    "UNIQUENESS_SAMPLED",
    "UNIQUENESS_UC_CONSTRAINT",
    "UNIQUENESS_UNKNOWN",
    "ValidationReport",
    "create_ddl",
    "generate",
    "validate",
]


# ── Capability input contract ────────────────────────────────────────────


@runtime_checkable
class CapabilityRow(Protocol):
    """The three fields this module reads off an entitlement-probe capability row.

    Declared here, in the consumer, so the dependency arrow stays backend →
    engine: the backend's ``MvCapabilityRow`` satisfies this structurally and
    nothing in this package imports it. ``test_mv_capability_row_satisfies_the_gso_protocol``
    in the backend suite is what makes that a checked claim rather than a hope —
    it fails if the model drops or renames any of the three.

    Deliberately minimal. A capability row carries labels, versions and
    remediation detail too; widening this Protocol to match would couple the
    engine to fields it never reads.
    """

    @property
    def capability(self) -> str: ...

    @property
    def status(self) -> str: ...

    @property
    def optional(self) -> bool: ...


CapabilityRows = Mapping[str, str] | Sequence[CapabilityRow | Mapping[str, Any]]
"""What :func:`validate` accepts: the probe's flat ``results`` map, its typed
rows, or those rows as the plain dicts a persisted payload round-trips as."""


# ── Uniqueness evidence ──────────────────────────────────────────────────

UNIQUENESS_EXACT = "EXACT"
UNIQUENESS_UC_CONSTRAINT = "UC_CONSTRAINT"
UNIQUENESS_SAMPLED = "SAMPLED"
UNIQUENESS_UNKNOWN = "UNKNOWN"

PROVING_UNIQUENESS_KINDS: frozenset[str] = frozenset({UNIQUENESS_EXACT})
"""Only an exact full-relation count proves a key unique.

``SAMPLED`` is explicitly not proof, and this is the module's most consequential
single decision. GSO's existing data profile is ``TABLESAMPLE``-bounded
(``preflight._collect_data_profile``), so a sampled distinct count equal to a
sampled row count says nothing about the other 99.99% of a large dimension.
Treating it as proof would emit ``rely.at_most_one_match: true`` on a key that
fans out, and because that hint is *unvalidated at runtime* the result is not an
error — it is a silently inflated ``SUM``. ``UC_CONSTRAINT`` is excluded for the
same reason at one remove: Unity Catalog primary keys are informational and
unenforced, so a declared key is a statement of intent, not a measurement.

The consequence is deliberate and worth stating plainly: with sampled profiling
alone the ladder never reaches the nested rung and ``rely`` is never emitted.
Callers that want either must supply an exact count."""


@dataclass(frozen=True)
class KeyUniqueness:
    """Evidence about whether one column uniquely identifies rows of one table.

    ``kind`` records *how* the evidence was obtained, because the strength of
    the claim is a property of the method and not of the numbers. ``proven`` is
    the only thing generation reads.
    """

    table: str
    column: str
    kind: str = UNIQUENESS_UNKNOWN
    row_count: int | None = None
    distinct_count: int | None = None
    detail: str = ""

    @property
    def proven(self) -> bool:
        """True only for an exact count that actually came back 1:1."""
        if self.kind not in PROVING_UNIQUENESS_KINDS:
            return False
        if self.row_count is None or self.distinct_count is None:
            return False
        return self.row_count > 0 and self.row_count == self.distinct_count

    @property
    def reason(self) -> str:
        """Why the key was or was not treated as unique, for the evidence trail."""
        if self.proven:
            return (
                f"{self.table}.{self.column} exact count {self.distinct_count}"
                f"/{self.row_count} distinct"
            )
        if self.kind == UNIQUENESS_EXACT:
            return (
                f"{self.table}.{self.column} exact count shows duplicates "
                f"({self.distinct_count}/{self.row_count})"
            )
        return f"{self.table}.{self.column} uniqueness evidence is {self.kind}, not proof"


# ── Physical facts ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class ColumnFacts:
    """One column of one relation, as Unity Catalog describes it."""

    name: str
    data_type: str = ""
    comment: str = ""


@dataclass(frozen=True)
class JoinHop:
    """One requested hop from the fact table toward an attribute.

    ``parent`` is ``None`` for a hop off the fact table and otherwise names the
    alias this hop hangs from, which is what makes a chain a chain. The chain's
    depth is what selects a ladder rung; a single hop is not a multi-hop problem
    and gets a plain first-level join.
    """

    alias: str
    table: str
    left_key: str
    right_key: str
    parent: str | None = None
    is_current_column: str | None = None
    description: str = ""


@dataclass(frozen=True)
class RequestedAttribute:
    """A grouping column the proposal wants, resolved to where it physically is.

    ``hop_alias`` is ``None`` when the column is on the fact table.
    ``denormalized_on`` names a *first-hop* alias that also carries this
    attribute, which is what makes ladder rung 1 available: if the value is
    already sitting on the near dimension, the far hop is unnecessary.
    """

    name: str
    column: str
    hop_alias: str | None = None
    denormalized_on: str | None = None
    denormalized_column: str | None = None
    comment: str = ""
    display_name: str = ""
    synonyms: tuple[str, ...] = ()


@dataclass(frozen=True)
class MeasureRequest:
    """One measure to emit, before shape expansion.

    ``expr`` is a raw aggregate over unqualified fact columns (the form the
    corpus scan produced). Generation qualifies the columns with ``source.``
    itself rather than trusting an incoming qualifier.
    """

    name: str
    expr: str
    comment: str = ""
    display_name: str = ""
    format_type: str = ""
    currency_code: str = "USD"
    decimal_places: int | None = None
    synonyms: tuple[str, ...] = ()


@dataclass(frozen=True)
class MvProfiling:
    """Everything physically true that generation is allowed to rely on.

    Assembled by the caller (the advisor phase) from GSO's existing profiling
    plus the entitlement probe's capability rows. Nothing in here is inferred by
    this module: if a fact is absent, generation takes the conservative branch
    rather than guessing, which is why every optional field defaults to the
    pessimistic value.
    """

    source_table: str
    table_columns: Mapping[str, tuple[ColumnFacts, ...]] = field(default_factory=dict)
    uniqueness: Mapping[tuple[str, str], KeyUniqueness] = field(default_factory=dict)
    hops: tuple[JoinHop, ...] = ()
    attributes: tuple[RequestedAttribute, ...] = ()
    measures: tuple[MeasureRequest, ...] = ()
    capabilities: Mapping[str, str] = field(default_factory=dict)
    domain: str = ""
    row_counts: Mapping[str, int] = field(default_factory=dict)

    def columns_of(self, table: str) -> dict[str, ColumnFacts]:
        return {c.name.lower(): c for c in self.table_columns.get(table, ())}

    def has_column(self, table: str, column: str) -> bool:
        return column.lower() in self.columns_of(table)

    def column_comment(self, table: str, column: str) -> str:
        facts = self.columns_of(table).get(column.lower())
        return facts.comment if facts else ""

    def hop(self, alias: str | None) -> JoinHop | None:
        if not alias:
            return None
        for h in self.hops:
            if h.alias == alias:
                return h
        return None

    def hop_depth(self, alias: str | None) -> int:
        """1 for a hop off the fact table, 2 for its child, and so on."""
        depth = 0
        cursor = self.hop(alias)
        seen: set[str] = set()
        while cursor is not None and cursor.alias not in seen:
            seen.add(cursor.alias)
            depth += 1
            cursor = self.hop(cursor.parent)
        return depth

    def is_unique(self, table: str, column: str) -> KeyUniqueness:
        return self.uniqueness.get(
            (table, column), KeyUniqueness(table=table, column=column)
        )

    def capability(self, name: str) -> str:
        return str(self.capabilities.get(name, UNIQUENESS_UNKNOWN)).upper()

    def capability_granted(self, name: str) -> bool:
        """``UNKNOWN`` is unavailable for optional capabilities (MV-D13).

        A SQL warehouse reports no DBR version, so the probe cannot decide the
        floor and says ``UNKNOWN``. Reading that as "probably fine" is how the
        generator emits YAML the runtime cannot plan.
        """
        return self.capability(name) == "GRANTED"


# ── Results ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class GeneratedMetricView:
    """One generation outcome.

    ``verdict`` reuses the dedup gate's vocabulary so a caller has one status
    field to branch on across scoring and generation. A ``CONFLICT`` carries no
    YAML by construction: the additive-measure rule is not a warning that can be
    shipped with a note, it means the requested measure cannot be expressed
    against this source.
    """

    verdict: str = VERDICT_PROPOSE
    yaml_text: str = ""
    definition: Mapping[str, Any] = field(default_factory=dict)
    join_strategy: str = MV_JOIN_STRATEGY_DIRECT
    strategy_reason: str = ""
    evidence: Mapping[str, Any] = field(default_factory=dict)
    conflicts: tuple[Mapping[str, Any], ...] = ()
    rejections: tuple[str, ...] = ()
    echo_check: str = MV_ECHO_CHECK_NOT_COMPARED

    @property
    def ok(self) -> bool:
        return self.verdict == VERDICT_PROPOSE and bool(self.yaml_text)

    @property
    def echo_checked(self) -> bool:
        """True only when a corpus was actually compared against."""
        return self.echo_check == MV_ECHO_CHECK_COMPARED


@dataclass(frozen=True)
class ValidationReport:
    """Static validation outcome.

    ``downgrade_to`` is the interesting field: a capability the runtime may not
    have is not an error in the YAML, it is a reason to re-render at a lower
    ladder rung. Returning "unplannable, try subquery" beats both failing the
    run and shipping YAML that cannot execute.

    ``echo_check`` reports whether the BEST FOR leakage comparison ran, because
    the oracle is optional and a check that compared nothing must not read as a
    check that found nothing.
    """

    ok: bool = True
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    downgrade_to: str | None = None
    echo_check: str = MV_ECHO_CHECK_NOT_COMPARED

    @property
    def echo_checked(self) -> bool:
        """True only when a corpus was actually compared against."""
        return self.echo_check == MV_ECHO_CHECK_COMPARED


# ── YAML emission ────────────────────────────────────────────────────────


class _Block(str):
    """A string to emit as a literal block scalar."""


class _MvDumper(yaml.SafeDumper):
    """Indents sequence items under their key, matching the published examples."""

    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
        super().increase_indent(flow=flow, indentless=False)


def _represent_block(dumper: yaml.SafeDumper, data: _Block) -> Any:
    return dumper.represent_scalar("tag:yaml.org,2002:str", str(data), style="|")


_MvDumper.add_representer(_Block, _represent_block)


def _dump(definition: Mapping[str, Any]) -> str:
    """Render a definition mapping to YAML text.

    Key ordering is insertion order (``sort_keys=False``) because the emitted
    document is read by humans in review: ``version`` / ``comment`` / ``source``
    first, then joins, dimensions, measures. ``width`` is set high so PyYAML
    never line-wraps a join condition, which would still parse but reads as if
    the expression were truncated.
    """
    return yaml.dump(
        _to_plain(definition),
        Dumper=_MvDumper,
        sort_keys=False,
        default_flow_style=False,
        width=10_000,
        allow_unicode=True,
    )


def _to_plain(value: Any) -> Any:
    """Convert nested mappings/sequences to plain dict/list, preserving _Block."""
    if isinstance(value, _Block):
        return value
    if isinstance(value, Mapping):
        return {str(k): _to_plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_plain(v) for v in value]
    return value


# ── Naming and metadata helpers ──────────────────────────────────────────

_IDENT_SPLIT_RE = re.compile(r"[_\W]+")
_COLUMN_PREFIX_RE = re.compile(r"^(?:[a-z]_|dim_|fact_|f_|d_)", re.IGNORECASE)

_CURRENCY_HINTS = ("price", "revenue", "cost", "amount", "spend", "sales", "charge", "fee")
_PERCENT_HINTS = ("rate", "pct", "percent", "share", "ratio", "margin")
_DATE_HINTS = ("date", "day", "month", "year", "week", "quarter")

_SOURCE_ALIAS = "fact"
"""Alias for the fact relation inside a subquery source. Never appears in an
emitted ``expr`` — the semantic layer always refers to the source as ``source``."""


def _words(identifier: str) -> list[str]:
    return [w for w in _IDENT_SPLIT_RE.split(identifier or "") if w]


def _display_name(identifier: str) -> str:
    words = _words(identifier)
    return " ".join(w.capitalize() for w in words) if words else identifier


def _synonyms_for(
    identifier: str,
    *,
    concept: str = "",
    extra: Sequence[str] = (),
) -> tuple[str, ...]:
    """Derive between ``MV_SYNONYMS_MIN`` and ``MV_SYNONYMS_MAX`` synonyms.

    Deterministic and boring on purpose: spaced form, de-prefixed form, and
    concept-qualified form cover the ways an analyst actually renames a column
    in a question. Padding is preferred over emitting fewer than the floor,
    because a field below the floor fails validation and a run that fails
    validation on a metadata detail has wasted the whole generation.
    """
    candidates: list[str] = []

    def _add(value: str) -> None:
        cleaned = " ".join(str(value or "").split()).lower()
        if not cleaned or len(cleaned) > MV_SYNONYM_MAX_CHARS:
            return
        if cleaned not in candidates:
            candidates.append(cleaned)

    for value in extra:
        _add(value)

    words = _words(identifier)
    _add(" ".join(words))
    stripped = _COLUMN_PREFIX_RE.sub("", identifier or "")
    if stripped and stripped != identifier:
        _add(" ".join(_words(stripped)))
    if len(words) > 1:
        _add(" ".join(words[1:]))
        _add(words[-1])
    if concept:
        _add(f"{concept} {' '.join(words)}")
        _add(concept)

    for suffix in ("value", "field", "attribute"):
        if len(candidates) >= MV_SYNONYMS_MIN:
            break
        _add(f"{' '.join(words) or identifier} {suffix}")

    return tuple(candidates[:MV_SYNONYMS_MAX])


def _format_block(
    *,
    name: str,
    expr: str,
    explicit_type: str = "",
    currency_code: str = "USD",
    decimal_places: int | None = None,
) -> dict[str, Any]:
    """Build a ``format`` block, rejecting any type outside the closed set."""
    format_type = (explicit_type or _infer_format_type(name, expr)).strip().lower()
    if format_type not in MV_FORMAT_TYPES:
        correction = MV_FORMAT_TYPE_CORRECTIONS.get(format_type)
        hint = f" — use '{correction}'" if correction else ""
        raise ValueError(
            f"measure '{name}': format.type '{format_type}' is not one of "
            f"{sorted(MV_FORMAT_TYPES)}{hint}"
        )

    block: dict[str, Any] = {"type": format_type}
    if format_type == "currency":
        block["currency_code"] = currency_code
        block["decimal_places"] = {"type": "exact", "places": 2 if decimal_places is None else decimal_places}
        block["abbreviation"] = "compact"
    elif format_type == "percentage":
        block["decimal_places"] = {"type": "exact", "places": 1 if decimal_places is None else decimal_places}
    elif format_type == "number":
        if decimal_places is None:
            block["decimal_places"] = {"type": "all"}
        else:
            block["decimal_places"] = {"type": "exact", "places": decimal_places}
        block["abbreviation"] = "compact"
    elif format_type == "byte":
        block["decimal_places"] = {"type": "exact", "places": 1 if decimal_places is None else decimal_places}
        block["abbreviation"] = "compact"
    return block


def _infer_format_type(name: str, expr: str) -> str:
    haystack = f"{name} {expr}".lower()
    if any(h in haystack for h in _PERCENT_HINTS):
        return "percentage"
    if any(h in haystack for h in _CURRENCY_HINTS):
        return "currency"
    return "number"


# ── The multi-hop ladder ─────────────────────────────────────────────────


@dataclass(frozen=True)
class _JoinPlan:
    """A chosen ladder rung together with the joins and column map it implies."""

    strategy: str
    reason: str
    joins: tuple[dict[str, Any], ...] = ()
    source: str = ""
    # attribute name -> the qualified expression that reads it
    column_map: Mapping[str, str] = field(default_factory=dict)
    evidence: Mapping[str, Any] = field(default_factory=dict)


def _plan_joins(profiling: MvProfiling) -> _JoinPlan:
    """Pick a ladder rung and build the joins for it.

    The order is fixed by MV-D8 and is a preference order, not a fallback chain
    of equals: rung 1 removes a hop entirely, rung 2 keeps the hop but needs
    both a runtime floor and proof of 1:1, and rung 3 always works but moves
    the join out of the semantic layer where a reviewer can see it.
    """
    max_depth = max((profiling.hop_depth(h.alias) for h in profiling.hops), default=0)

    if max_depth <= 1:
        joins = tuple(_render_join(h, profiling, parent_scope="source") for h in profiling.hops)
        return _JoinPlan(
            strategy=MV_JOIN_STRATEGY_DIRECT,
            reason="all attributes reachable through first-level joins; no multi-hop decision needed",
            joins=joins,
            source=profiling.source_table,
            column_map=_direct_column_map(profiling),
            evidence={"max_hop_depth": max_depth},
        )

    denormalized = _try_denormalized(profiling)
    if denormalized is not None:
        return denormalized

    nested = _try_nested(profiling, max_depth=max_depth)
    if nested is not None:
        return nested

    return _subquery_source(profiling, max_depth=max_depth)


def _deep_attributes(profiling: MvProfiling) -> tuple[RequestedAttribute, ...]:
    return tuple(
        a for a in profiling.attributes if profiling.hop_depth(a.hop_alias) > 1
    )


def _first_hop_of(profiling: MvProfiling, alias: str | None) -> JoinHop | None:
    """Walk up to the hop that hangs directly off the fact table."""
    cursor = profiling.hop(alias)
    seen: set[str] = set()
    while cursor is not None and cursor.parent and cursor.alias not in seen:
        seen.add(cursor.alias)
        cursor = profiling.hop(cursor.parent)
    return cursor


def _try_denormalized(profiling: MvProfiling) -> _JoinPlan | None:
    """Rung 1: every far attribute is also present on its first-hop dimension."""
    deep = _deep_attributes(profiling)
    if not deep:
        return None

    resolved: dict[str, str] = {}
    for attribute in deep:
        first_hop = _first_hop_of(profiling, attribute.hop_alias)
        alias = attribute.denormalized_on or (first_hop.alias if first_hop else None)
        column = attribute.denormalized_column or attribute.column
        if not alias or not attribute.denormalized_on:
            return None
        hop = profiling.hop(alias)
        if hop is None or profiling.hop_depth(alias) != 1:
            return None
        if not profiling.has_column(hop.table, column):
            return None
        resolved[attribute.name] = f"{alias}.{column}"

    kept = tuple(h for h in profiling.hops if profiling.hop_depth(h.alias) == 1)
    joins = tuple(_render_join(h, profiling, parent_scope="source") for h in kept)
    column_map = _direct_column_map(profiling)
    column_map.update(resolved)
    return _JoinPlan(
        strategy=MV_JOIN_STRATEGY_DENORMALIZED,
        reason=(
            "every attribute beyond the first hop is also carried on the first-hop "
            "dimension, so the deeper hops are unnecessary"
        ),
        joins=joins,
        source=profiling.source_table,
        column_map=column_map,
        evidence={"denormalized_attributes": sorted(resolved)},
    )


def _try_nested(profiling: MvProfiling, *, max_depth: int) -> _JoinPlan | None:
    """Rung 2: nested joins, gated on DBR >= 17.1 *and* proven 1:1 intermediates."""
    if not profiling.capability_granted(MV_CAPABILITY_NESTED_JOINS):
        return None

    unproven: list[str] = []
    proofs: list[str] = []
    for hop in profiling.hops:
        evidence = profiling.is_unique(hop.table, hop.right_key)
        if evidence.proven:
            proofs.append(evidence.reason)
        else:
            unproven.append(evidence.reason)
    if unproven:
        return None

    tree: dict[str, dict[str, Any]] = {}
    for hop in profiling.hops:
        parent_scope = hop.parent or "source"
        tree[hop.alias] = _render_join(hop, profiling, parent_scope=parent_scope)
    roots: list[dict[str, Any]] = []
    for hop in profiling.hops:
        node = tree[hop.alias]
        if hop.parent and hop.parent in tree:
            tree[hop.parent].setdefault("joins", []).append(node)
        else:
            roots.append(node)

    return _JoinPlan(
        strategy=MV_JOIN_STRATEGY_NESTED,
        reason=(
            "nested joins are available on the probed runtime and every intermediate "
            "key is proven 1:1, so the chain can stay in the semantic layer"
        ),
        joins=tuple(roots),
        source=profiling.source_table,
        column_map=_nested_column_map(profiling),
        evidence={"max_hop_depth": max_depth, "uniqueness_proofs": proofs},
    )


def _subquery_source(profiling: MvProfiling, *, max_depth: int) -> _JoinPlan:
    """Rung 3: pre-join the chain in the source subquery.

    Always available, which is why it is last rather than first. Two things it
    must do that a naive pre-join would not: apply the ``is_current`` guard for
    every SCD2 relation explicitly, and enforce uniqueness *inside* the subquery
    so the pre-join cannot fan the fact table out. Uniqueness is enforced with a
    ``GROUP BY`` on the join key rather than assumed from profiling — this rung
    exists precisely because uniqueness could not be proven.
    """
    ordered = _hops_in_dependency_order(profiling)
    select_parts = [f"{_SOURCE_ALIAS}.*"]
    attribute_columns: dict[str, str] = {}

    for attribute in profiling.attributes:
        if not attribute.hop_alias:
            continue
        hop = profiling.hop(attribute.hop_alias)
        if hop is None:
            continue
        alias_column = f"{attribute.name}"
        select_parts.append(f"{hop.alias}.{attribute.column} AS {alias_column}")
        attribute_columns[attribute.name] = f"source.{alias_column}"

    lines = [f"SELECT {', '.join(select_parts)}", f"FROM {profiling.source_table} AS {_SOURCE_ALIAS}"]
    for hop in ordered:
        parent = _SOURCE_ALIAS if not hop.parent else hop.parent
        keyed = _deduplicated_relation(hop, profiling)
        condition = f"{parent}.{hop.left_key} = {hop.alias}.{hop.right_key}"
        lines.append(f"LEFT JOIN {keyed} AS {hop.alias} ON {condition}")

    column_map = {
        a.name: f"source.{a.column}" for a in profiling.attributes if not a.hop_alias
    }
    column_map.update(attribute_columns)

    return _JoinPlan(
        strategy=MV_JOIN_STRATEGY_SUBQUERY,
        reason=(
            "nested joins are unavailable or an intermediate key is not proven 1:1, "
            "so the chain is pre-joined in the source with uniqueness enforced there"
        ),
        joins=(),
        source="\n".join(lines),
        column_map=column_map,
        evidence={
            "max_hop_depth": max_depth,
            "nested_capability": profiling.capability(MV_CAPABILITY_NESTED_JOINS),
            "unproven_keys": [
                profiling.is_unique(h.table, h.right_key).reason
                for h in profiling.hops
                if not profiling.is_unique(h.table, h.right_key).proven
            ],
        },
    )


def _deduplicated_relation(hop: JoinHop, profiling: MvProfiling) -> str:
    """One dimension relation, current-only and one row per key.

    ``GROUP BY`` on the join key with ``MAX`` over the payload is the portable
    way to guarantee at most one match without a window function; the payload
    columns are attribute values, so collapsing duplicates is a deliberate
    last-writer choice recorded in the NOTE section of the comment.
    """
    payload = sorted(
        {
            a.column
            for a in profiling.attributes
            if a.hop_alias == hop.alias and a.column != hop.right_key
        }
    )
    projected = [hop.right_key] + [f"MAX({c}) AS {c}" for c in payload]
    where = ""
    if hop.is_current_column:
        where = f" WHERE {hop.is_current_column} = true"
    return (
        f"(SELECT {', '.join(projected)} FROM {hop.table}{where} "
        f"GROUP BY {hop.right_key})"
    )


def _hops_in_dependency_order(profiling: MvProfiling) -> tuple[JoinHop, ...]:
    ordered: list[JoinHop] = []
    remaining = list(profiling.hops)
    placed: set[str] = set()
    while remaining:
        progressed = False
        for hop in list(remaining):
            if not hop.parent or hop.parent in placed:
                ordered.append(hop)
                placed.add(hop.alias)
                remaining.remove(hop)
                progressed = True
        if not progressed:
            ordered.extend(remaining)
            break
    return tuple(ordered)


def _render_join(hop: JoinHop, profiling: MvProfiling, *, parent_scope: str) -> dict[str, Any]:
    """One join entry.

    Emits ``name``, ``source`` and a quoted ``on`` only. ``join_type`` and
    ``table`` are never emitted — the first is unsupported and the second is the
    wrong key name for the relation, and both fail at create time.
    """
    condition = f"{parent_scope}.{hop.left_key} = {hop.alias}.{hop.right_key}"
    if hop.is_current_column:
        condition += f" AND {hop.alias}.{hop.is_current_column} = true"

    entry: dict[str, Any] = {
        "name": hop.alias,
        "source": hop.table,
        "on": condition,
    }
    evidence = profiling.is_unique(hop.table, hop.right_key)
    if evidence.proven:
        entry["rely"] = {"at_most_one_match": True}
    return entry


def _direct_column_map(profiling: MvProfiling) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for attribute in profiling.attributes:
        if attribute.hop_alias:
            mapping[attribute.name] = f"{attribute.hop_alias}.{attribute.column}"
        else:
            mapping[attribute.name] = f"source.{attribute.column}"
    return mapping


def _nested_column_map(profiling: MvProfiling) -> dict[str, str]:
    """Nested attributes are read through the full alias path."""
    mapping: dict[str, str] = {}
    for attribute in profiling.attributes:
        if not attribute.hop_alias:
            mapping[attribute.name] = f"source.{attribute.column}"
            continue
        path: list[str] = []
        cursor = profiling.hop(attribute.hop_alias)
        seen: set[str] = set()
        while cursor is not None and cursor.alias not in seen:
            seen.add(cursor.alias)
            path.append(cursor.alias)
            cursor = profiling.hop(cursor.parent)
        mapping[attribute.name] = ".".join(reversed(path) if path else []) + f".{attribute.column}"
    return mapping


# ── Measures and shapes ──────────────────────────────────────────────────


def _qualify_source_columns(expr: str, source_columns: set[str]) -> str:
    """Qualify bare fact columns with ``source.``, leaving qualified ones alone.

    Done on the parsed tree rather than by regex because a textual substitution
    cannot tell the column ``sum`` from the function ``SUM``, and metric views
    reject an unqualified column reference.
    """
    try:
        tree = sqlglot.parse_one(expr, read="databricks")
    except Exception:
        return expr
    for column in tree.find_all(exp.Column):
        if column.table:
            continue
        if column.name.lower() in source_columns:
            column.set("table", exp.to_identifier("source"))
    return tree.sql(dialect="databricks")


def _measure_entry(
    request: MeasureRequest,
    *,
    expr: str,
    concept: str,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "name": request.name,
        "expr": expr,
        "comment": request.comment or f"{_display_name(request.name)} for {concept or 'this domain'}.",
        "display_name": request.display_name or _display_name(request.name),
        "format": _format_block(
            name=request.name,
            expr=expr,
            explicit_type=request.format_type,
            currency_code=request.currency_code,
            decimal_places=request.decimal_places,
        ),
        "synonyms": list(
            _synonyms_for(request.name, concept=concept, extra=request.synonyms)
        ),
    }
    return entry


def _shape_measures(
    shapes: Sequence[ShapeMatch],
    *,
    source_columns: set[str],
    concept: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Expand recurring shapes into (dimensions, measures) entries.

    The per-shape mandate is read from ``SHAPE_GUIDANCE`` rather than restated
    here, so the shape detector and this renderer cannot disagree about what a
    shape means. Each branch below implements exactly the form that guidance
    names.
    """
    dimensions: list[dict[str, Any]] = []
    measures: list[dict[str, Any]] = []

    for shape in shapes:
        components = dict(shape.components)
        if shape.kind == SHAPE_RATIO:
            numerator = components.get("numerator") or ""
            denominator = components.get("denominator") or ""
            if not numerator or not denominator:
                continue
            base = _shape_base_name(shape, concept)
            num_name = f"{base}_numerator"
            den_name = f"{base}_denominator"
            for name, component in ((num_name, numerator), (den_name, denominator)):
                measures.append(
                    _measure_entry(
                        MeasureRequest(name=name, expr=component),
                        expr=_qualify_source_columns(component, source_columns),
                        concept=concept,
                    )
                )
            measures.append(
                _measure_entry(
                    MeasureRequest(
                        name=base,
                        expr="",
                        format_type="percentage" if "rate" in base else "number",
                        comment=(
                            f"{_display_name(base)}, composed from "
                            f"{num_name} and {den_name} so a change to either flows through."
                        ),
                    ),
                    expr=f"MEASURE(`{num_name}`) / MEASURE(`{den_name}`)",
                    concept=concept,
                )
            )
        elif shape.kind == SHAPE_CONDITIONAL_COUNT:
            condition = components.get("condition") or ""
            if not condition:
                continue
            name = _shape_base_name(shape, concept)
            qualified = _qualify_source_columns(condition, source_columns)
            measures.append(
                _measure_entry(
                    MeasureRequest(name=name, expr="", format_type="number"),
                    expr=f"COUNT(1) FILTER (WHERE {qualified})",
                    concept=concept,
                )
            )
        elif shape.kind == SHAPE_PCT_OF_TOTAL:
            base_expr = components.get("measure") or components.get("numerator") or ""
            if not base_expr:
                continue
            base = _shape_base_name(shape, concept)
            atomic = f"{base}_base"
            lod = f"{base}_grand_total"
            qualified = _qualify_source_columns(base_expr, source_columns)
            measures.append(
                _measure_entry(
                    MeasureRequest(name=atomic, expr=base_expr),
                    expr=qualified,
                    concept=concept,
                )
            )
            dimensions.append(
                {
                    "name": lod,
                    "expr": f"{qualified} OVER ()",
                    "comment": (
                        "Fixed level-of-detail grand total, computed independently of the "
                        "query GROUP BY so a share-of-total measure has a coarser "
                        "denominator to divide by."
                    ),
                    "display_name": _display_name(lod),
                    "synonyms": list(_synonyms_for(lod, concept=concept)),
                }
            )
            measures.append(
                _measure_entry(
                    MeasureRequest(
                        name=base,
                        expr="",
                        format_type="percentage",
                        comment=(
                            f"Share of the grand total. Reads the fixed-LOD dimension "
                            f"{lod} with ANY_VALUE; composing two MEASURE() calls here "
                            f"would resolve both at the query grain and always return 1.0."
                        ),
                    ),
                    expr=f"MEASURE(`{atomic}`) / ANY_VALUE(`{lod}`)",
                    concept=concept,
                )
            )

    return dimensions, measures


def _shape_base_name(shape: ShapeMatch, concept: str) -> str:
    """A stable measure name for a shape.

    Prefers a name the shape already carries; otherwise derives one from the
    concept and the shape kind, because a fingerprint hex string is not a name a
    reviewer can read in a Genie answer.
    """
    components = dict(shape.components)
    explicit = (components.get("name") or "").strip()
    if explicit:
        return re.sub(r"\W+", "_", explicit).strip("_").lower()
    stem = re.sub(r"\W+", "_", concept or shape.kind).strip("_").lower()
    suffix = {
        SHAPE_RATIO: "rate",
        SHAPE_CONDITIONAL_COUNT: "matching_count",
        SHAPE_PCT_OF_TOTAL: "pct_of_total",
    }.get(shape.kind, "measure")
    return f"{stem}_{suffix}" if stem else suffix


# ── Structured comment ───────────────────────────────────────────────────


def _build_comment(
    *,
    concept: str,
    source_table: str,
    domain: str,
    dimensions: Sequence[Mapping[str, Any]],
    measures: Sequence[Mapping[str, Any]],
    plan: _JoinPlan,
    profiling: MvProfiling,
    dedup: DedupOutcome | None,
) -> tuple[str, tuple[str, ...]]:
    """Render the eight-section comment and return it with its BEST FOR lines.

    BEST FOR is synthesized from the concept and the emitted field names — it is
    a paraphrase by construction, because a candidate carries benchmark
    *question ids* and no benchmark text, so there is no verbatim string
    available to copy even accidentally. The returned lines are what the leakage
    check inspects.
    """
    dimension_names = [str(d.get("name") or "") for d in dimensions]
    measure_names = [str(m.get("name") or "") for m in measures]
    subject = concept or _display_name(source_table.split(".")[-1])

    best_for = _best_for_lines(subject, dimension_names, measure_names)
    not_for = _not_for_line(dedup, source_table)

    joins_text = (
        ", ".join(
            f"{j['name']} ({profiling.hop(str(j['name'])).description or 'dimension attributes'})"
            for j in plan.joins
            if profiling.hop(str(j.get("name")))
        )
        or "none"
    )
    if plan.strategy == MV_JOIN_STRATEGY_SUBQUERY:
        joins_text = (
            "pre-joined in the source subquery, deduplicated on the join key ("
            + ", ".join(h.alias for h in profiling.hops)
            + ")"
        )

    sections = {
        "PURPOSE": f"{subject} metrics over {source_table.split('.')[-1]}",
        "BEST FOR": " | ".join(best_for),
        "NOT FOR": not_for,
        "DIMENSIONS": ", ".join(dimension_names) or "none",
        "MEASURES": ", ".join(measure_names) or "none",
        "SOURCE": f"{source_table.split('.')[-1]}"
        + (f" ({domain} domain)" if domain else ""),
        "JOINS": joins_text,
        "NOTE": _note_line(plan),
    }

    body = "\n\n".join(f"{name}: {sections[name]}" for name in MV_COMMENT_SECTIONS)
    return body, tuple(best_for)


def _best_for_lines(
    subject: str,
    dimension_names: Sequence[str],
    measure_names: Sequence[str],
) -> list[str]:
    """Paraphrased intents, built only from what the view can actually answer.

    Every line is derived from an emitted measure and an emitted dimension, so
    the section cannot promise a question the view would fail. The time-trend
    line in particular is added only when a dimension is actually temporal —
    a BEST FOR that advertises a trend on a view with no date column is worse
    than a shorter list, because Genie will select the view and then miss.
    """
    primary = _display_name(measure_names[0]).lower() if measure_names else subject.lower()
    slices = [_display_name(d).lower() for d in dimension_names[:3]]

    lines = [f"{primary} overall"]
    lines.extend(f"{primary} by {slice_name}" for slice_name in slices)

    if any(hint in d.lower() for d in dimension_names for hint in _DATE_HINTS):
        lines.append(f"{primary} trend over time")
    if len(lines) < 3 and slices:
        lines.append(f"{primary} ranked by {slices[0]}")

    return lines[:6]


def _not_for_line(dedup: DedupOutcome | None, source_table: str) -> str:
    """Cross-reference the adjacent metric view when the dedup gate found one."""
    alternatives = tuple(dedup.alternatives) if dedup else ()
    for alternative in alternatives:
        pointer = str(alternative.get("pointer") or alternative.get("mv_fqn") or "").strip()
        if pointer:
            adjacent = pointer.rsplit(".", 1)[0] if "." in pointer else pointer
            return (
                f"Metrics already governed elsewhere — overlapping definitions live in "
                f"{adjacent} (use that view instead)"
            )
    return (
        f"Row-level inspection or record lookup (query {source_table} directly instead)"
    )


def _note_line(plan: _JoinPlan) -> str:
    if plan.strategy == MV_JOIN_STRATEGY_SUBQUERY:
        return (
            "Dimension attributes are pre-joined and deduplicated on the join key, so a "
            "duplicated dimension row contributes one value rather than fanning out the fact"
        )
    if plan.strategy == MV_JOIN_STRATEGY_DENORMALIZED:
        return "Far-dimension attributes are read from the denormalized copy on the near dimension"
    if plan.strategy == MV_JOIN_STRATEGY_NESTED:
        return "Snowflake attributes are reached through nested joins; every intermediate key is 1:1"
    return "Joined dimensions are current-version only where the dimension is versioned"


# ── Generation ───────────────────────────────────────────────────────────


def generate(
    candidate: MetricViewCandidate,
    profiling: MvProfiling,
    *,
    shapes: Sequence[ShapeMatch] = (),
    dedup: DedupOutcome | None = None,
    oracle: Any = None,
    w: Any = None,
) -> GeneratedMetricView:
    """Render one metric view definition, or explain why it cannot be rendered.

    ``candidate`` and ``profiling`` are the positional contract MV-D8 names.
    The keyword arguments are the three optional inputs that sharpen the output
    without changing what it is: recurring ``shapes`` to expand, the ``dedup``
    outcome so NOT FOR can point at the adjacent view, and a leakage ``oracle``
    to re-check shipped comment lines.

    Returns a result rather than raising for the two rejection paths that are
    *expected* outcomes rather than bugs — an additive measure that aggregates a
    joined dimension column (CONFLICT), and a rejected format type or an echoed
    comment line (no YAML plus a reason). Callers check ``ok``.
    """
    source_columns = {name for name in profiling.columns_of(profiling.source_table)}
    concept = (candidate.concept or "").strip()

    conflicts = _additive_measure_conflicts(candidate, profiling, source_columns)
    if conflicts:
        return GeneratedMetricView(
            verdict=VERDICT_CONFLICT,
            join_strategy=MV_JOIN_STRATEGY_DIRECT,
            strategy_reason="not planned: the requested measure cannot be expressed against this source",
            conflicts=conflicts,
            rejections=tuple(str(c["reason"]) for c in conflicts),
        )

    plan = _plan_joins(profiling)

    try:
        dimensions = _dimension_entries(profiling, plan, concept=concept)
        measures = [
            _measure_entry(
                request,
                expr=_qualify_source_columns(request.expr, source_columns),
                concept=concept,
            )
            for request in profiling.measures
        ]
        shape_dimensions, shape_measures = _shape_measures(
            shapes, source_columns=source_columns, concept=concept
        )
    except ValueError as exc:
        return GeneratedMetricView(
            join_strategy=plan.strategy,
            strategy_reason=plan.reason,
            evidence=dict(plan.evidence),
            rejections=(str(exc),),
        )

    dimensions.extend(shape_dimensions)
    measures.extend(shape_measures)

    comment, best_for = _build_comment(
        concept=concept,
        source_table=profiling.source_table,
        domain=profiling.domain,
        dimensions=dimensions,
        measures=measures,
        plan=plan,
        profiling=profiling,
        dedup=dedup,
    )

    echoes, echo_compared = _comment_echoes(best_for, oracle=oracle)
    echo_check = MV_ECHO_CHECK_COMPARED if echo_compared else MV_ECHO_CHECK_NOT_COMPARED
    if echoes:
        return GeneratedMetricView(
            join_strategy=plan.strategy,
            strategy_reason=plan.reason,
            evidence=dict(plan.evidence),
            rejections=echoes,
            echo_check=echo_check,
        )

    definition: dict[str, Any] = {
        "version": MV_YAML_VERSION,
        "comment": _Block(comment + "\n"),
        "source": _Block(plan.source + "\n") if "\n" in plan.source else plan.source,
    }
    if plan.joins:
        definition["joins"] = [dict(j) for j in plan.joins]
    definition["dimensions"] = dimensions
    definition["measures"] = measures

    yaml_text = _dump(definition)
    # The oracle is not forwarded: the BEST FOR lines were just checked above
    # against the same corpus, and re-deriving them from the rendered comment
    # would run one comparison twice. `echo_check` below carries that result.
    report = validate(yaml_text, capabilities=profiling.capabilities)
    if not report.ok:
        return GeneratedMetricView(
            join_strategy=plan.strategy,
            strategy_reason=plan.reason,
            evidence=dict(plan.evidence),
            rejections=report.errors,
            echo_check=echo_check,
        )

    evidence = dict(plan.evidence)
    evidence.update(
        {
            "generated_by": MV_ADVISOR_GENERATED_BY,
            "join_strategy": plan.strategy,
            "join_strategy_reason": plan.reason,
            "benchmark_question_ids": list(candidate.benchmark_question_ids),
            "echo_check": echo_check,
        }
    )
    return GeneratedMetricView(
        verdict=VERDICT_PROPOSE,
        yaml_text=yaml_text,
        definition=definition,
        join_strategy=plan.strategy,
        strategy_reason=plan.reason,
        evidence=evidence,
        echo_check=echo_check,
    )


def _additive_measure_conflicts(
    candidate: MetricViewCandidate,
    profiling: MvProfiling,
    source_columns: set[str],
) -> tuple[Mapping[str, Any], ...]:
    """Reject measures that aggregate a joined dimension column.

    An additive measure must aggregate the fact. Aggregating a dimension column
    across a one-to-many join multiplies that column by the fan-out, so the
    number is wrong in a way no downstream check can see — which is why this is
    a CONFLICT for a reviewer to adjudicate rather than a warning.
    """
    requests = list(profiling.measures)
    if candidate.measure_expr:
        requests.append(MeasureRequest(name="candidate_measure", expr=candidate.measure_expr))

    conflicts: list[Mapping[str, Any]] = []
    for request in requests:
        if not request.expr:
            continue
        columns: set[str] = set()
        for measure in extract_measures(request.expr):
            columns.update(c.lower() for c in measure.source_columns)
        for column in sorted(columns):
            if column in source_columns:
                continue
            owner = _column_owner(profiling, column)
            if owner is None:
                continue
            conflicts.append(
                {
                    "measure": request.name,
                    "column": column,
                    "joined_table": owner,
                    "reason": (
                        f"measure '{request.name}' aggregates {column}, which lives on the "
                        f"joined dimension {owner} rather than on the source fact "
                        f"{profiling.source_table}"
                    ),
                }
            )
    return tuple(conflicts)


def _column_owner(profiling: MvProfiling, column: str) -> str | None:
    for hop in profiling.hops:
        if profiling.has_column(hop.table, column):
            return hop.table
    return None


def _dimension_entries(
    profiling: MvProfiling,
    plan: _JoinPlan,
    *,
    concept: str,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for attribute in profiling.attributes:
        expr = plan.column_map.get(attribute.name)
        if not expr:
            continue
        table = profiling.source_table
        hop = profiling.hop(attribute.hop_alias)
        if hop is not None:
            table = hop.table
        comment = (
            attribute.comment
            or profiling.column_comment(table, attribute.column)
            or f"{_display_name(attribute.name)} for slicing {concept or 'these metrics'}."
        )
        entries.append(
            {
                "name": attribute.name,
                "expr": expr,
                "comment": comment,
                "display_name": attribute.display_name or _display_name(attribute.name),
                "synonyms": list(
                    _synonyms_for(attribute.name, concept=concept, extra=attribute.synonyms)
                ),
            }
        )
    return entries


def _comment_echoes(lines: Sequence[str], *, oracle: Any) -> tuple[tuple[str, ...], bool]:
    """Reject any shipped comment line that echoes a benchmark question.

    Reuses ``LeakageOracle.contains_question`` with a higher threshold rather
    than adding a second matcher: one detection path means one place for the
    normalization rules to live. The oracle exposes booleans only, so this can
    check a line against the corpus without either this module or its caller
    ever seeing a benchmark question.

    Returns the rejections *and whether a comparison happened at all*. Without
    that second value an unconfigured oracle is indistinguishable from a clean
    corpus, and the caller would report a pass it never earned.
    """
    if oracle is None:
        return (), False
    contains = getattr(oracle, "contains_question", None)
    if not callable(contains):
        return (), False
    rejected: list[str] = []
    for line in lines:
        try:
            hit = contains(line, threshold=MV_COMMENT_ECHO_THRESHOLD)
        except TypeError:
            hit = contains(line)
        if hit:
            rejected.append(
                f"BEST FOR line '{line}' matches a benchmark question at or above "
                f"{MV_COMMENT_ECHO_THRESHOLD:.2f} normalized similarity"
            )
    return tuple(rejected), True


def _best_for_from_comment(definition: Mapping[str, Any]) -> tuple[str, ...]:
    """Recover the BEST FOR intents from an already-rendered comment.

    ``validate`` may be handed YAML this module did not render — foreign or
    LLM-authored text — where the only access to the intents is the comment
    itself. Emission joins them with ``|`` on one line, so parsing is the
    inverse of that join.
    """
    comment = definition.get("comment")
    if not isinstance(comment, str):
        return ()
    others = [f"{s}:" for s in MV_COMMENT_SECTIONS if s != "BEST FOR"]
    for block in comment.split("\n"):
        stripped = block.strip()
        if not stripped.startswith("BEST FOR:"):
            continue
        body = stripped[len("BEST FOR:"):]
        for marker in others:
            body = body.split(marker)[0]
        return tuple(part.strip() for part in body.split("|") if part.strip())
    return ()


# ── Static validation ────────────────────────────────────────────────────


def validate(
    yaml_text: str,
    *,
    capabilities: CapabilityRows = (),
    oracle: Any = None,
) -> ValidationReport:
    """Check emitted YAML statically. No warehouse, no network, no parse of data.

    Runs the seven checks MV-D8 requires: it parses, no unsupported field is
    present, every ``format.type`` is in the closed set, no join is transitive,
    synonym counts are within bounds, all eight comment sections exist, and the
    runtime capabilities the YAML depends on were actually observed.

    The capability check resolves to ``downgrade_to`` rather than an error: YAML
    that needs nested joins on a runtime that may not have them is not malformed,
    it is the wrong rung of the ladder, and the useful answer is which rung to
    fall back to.

    Passing an ``oracle`` additionally re-checks the BEST FOR lines recovered
    from the comment, which is the only echo check available for YAML this module
    did not render. Without one the report says ``NOT_COMPARED`` instead of
    quietly reporting a clean firewall.
    """
    errors: list[str] = []
    warnings: list[str] = []

    if not yaml_text or not str(yaml_text).strip():
        return ValidationReport(ok=False, errors=("YAML is empty",))

    try:
        definition = yaml.safe_load(yaml_text)
    except yaml.YAMLError as exc:
        return ValidationReport(ok=False, errors=(f"YAML does not parse: {exc}",))

    if not isinstance(definition, Mapping):
        return ValidationReport(
            ok=False, errors=("YAML top level is not a mapping",)
        )

    version = definition.get("version")
    if not isinstance(version, str) or version != MV_YAML_VERSION:
        errors.append(
            f"version must be the quoted string '{MV_YAML_VERSION}', got {version!r}"
        )

    for key in sorted(MV_UNSUPPORTED_TOP_LEVEL_FIELDS & set(definition)):
        errors.append(f"unsupported top-level field '{key}'")

    if not definition.get("source"):
        errors.append("missing required field 'source'")

    errors.extend(_validate_comment(definition))
    errors.extend(_validate_fields(definition))
    join_errors, join_warnings, needs_nested = _validate_joins(definition)
    errors.extend(join_errors)
    warnings.extend(join_warnings)

    echoes, echo_compared = _comment_echoes(
        _best_for_from_comment(definition), oracle=oracle
    )
    errors.extend(echoes)
    echo_check = MV_ECHO_CHECK_COMPARED if echo_compared else MV_ECHO_CHECK_NOT_COMPARED
    if not echo_compared:
        warnings.append(
            "the BEST FOR echo check did not run: no leakage corpus was supplied, "
            "so no comment line was compared against benchmark questions"
        )

    resolved = _capability_map(capabilities)
    downgrade: str | None = None
    if needs_nested and resolved.get(MV_CAPABILITY_NESTED_JOINS, "UNKNOWN") != "GRANTED":
        downgrade = MV_JOIN_STRATEGY_SUBQUERY
        warnings.append(
            "nested joins are present but the probed runtime does not report the "
            f"{MV_CAPABILITY_NESTED_JOINS} floor as granted — re-render at the "
            f"{MV_JOIN_STRATEGY_SUBQUERY} rung"
        )
    if "fields" in definition and resolved.get(
        MV_CAPABILITY_FIELDS_AGG_WINDOW_OFFSET, "UNKNOWN"
    ) != "GRANTED":
        warnings.append(
            "the 'fields' key requires the "
            f"{MV_CAPABILITY_FIELDS_AGG_WINDOW_OFFSET} floor; use 'dimensions' instead"
        )

    return ValidationReport(
        ok=not errors,
        errors=tuple(errors),
        warnings=tuple(warnings),
        downgrade_to=downgrade,
        echo_check=echo_check,
    )


def _validate_comment(definition: Mapping[str, Any]) -> list[str]:
    comment = definition.get("comment")
    if not isinstance(comment, str) or not comment.strip():
        return ["missing structured 'comment'"]
    missing = [
        section for section in MV_COMMENT_SECTIONS if f"{section}:" not in comment
    ]
    if missing:
        return [f"comment is missing required section(s): {', '.join(missing)}"]
    return []


def _validate_fields(definition: Mapping[str, Any]) -> list[str]:
    """Format-type and synonym lints over dimensions and measures."""
    errors: list[str] = []
    for key in ("dimensions", "fields", "measures"):
        entries = definition.get(key) or []
        if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
            continue
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            name = str(entry.get("name") or "<unnamed>")
            if not entry.get("expr"):
                errors.append(f"{key[:-1]} '{name}' has no expr")

            format_block = entry.get("format")
            if isinstance(format_block, Mapping):
                format_type = str(format_block.get("type") or "").strip().lower()
                if format_type not in MV_FORMAT_TYPES:
                    correction = MV_FORMAT_TYPE_CORRECTIONS.get(format_type)
                    hint = f" — use '{correction}'" if correction else ""
                    errors.append(
                        f"{key[:-1]} '{name}': format.type '{format_type}' is not one of "
                        f"{sorted(MV_FORMAT_TYPES)}{hint}"
                    )

            synonyms = entry.get("synonyms")
            if synonyms is None:
                continue
            if not isinstance(synonyms, Sequence) or isinstance(synonyms, (str, bytes)):
                errors.append(f"{key[:-1]} '{name}': synonyms must be a list")
                continue
            if not MV_SYNONYMS_MIN <= len(synonyms) <= MV_SYNONYMS_MAX:
                errors.append(
                    f"{key[:-1]} '{name}': {len(synonyms)} synonyms, expected between "
                    f"{MV_SYNONYMS_MIN} and {MV_SYNONYMS_MAX}"
                )
            for synonym in synonyms:
                if len(str(synonym)) > MV_SYNONYM_MAX_CHARS:
                    errors.append(
                        f"{key[:-1]} '{name}': synonym exceeds {MV_SYNONYM_MAX_CHARS} chars"
                    )
    return errors


def _validate_joins(
    definition: Mapping[str, Any],
) -> tuple[list[str], list[str], bool]:
    """Structural join lint plus the transitive-join detector.

    Returns ``(errors, warnings, contains_nested_joins)``.
    """
    errors: list[str] = []
    warnings: list[str] = []
    nested_present = False

    def _walk(joins: Any, parent_scope: str, depth: int) -> None:
        nonlocal nested_present
        if not isinstance(joins, Sequence) or isinstance(joins, (str, bytes)):
            return
        for join in joins:
            if not isinstance(join, Mapping):
                errors.append("join entry is not a mapping")
                continue
            alias = str(join.get("name") or "").strip()
            if not alias:
                errors.append("join is missing 'name' (the alias)")
                continue
            if not join.get("source"):
                errors.append(f"join '{alias}' is missing 'source'")
            for unsupported in sorted(MV_UNSUPPORTED_JOIN_FIELDS & set(join)):
                errors.append(
                    f"join '{alias}': unsupported field '{unsupported}'"
                    + (" — the relation key is 'source'" if unsupported == "table" else "")
                )

            on_clause = join.get("on")
            if on_clause:
                errors.extend(
                    _check_join_scope(
                        str(on_clause), alias=alias, parent_scope=parent_scope, warnings=warnings
                    )
                )
            elif not join.get("using"):
                errors.append(f"join '{alias}' has neither 'on' nor 'using'")

            children = join.get("joins")
            if children:
                nested_present = True
                _walk(children, parent_scope=alias, depth=depth + 1)

    _walk(definition.get("joins"), parent_scope="source", depth=1)
    return errors, warnings, nested_present


def _check_join_scope(
    on_clause: str,
    *,
    alias: str,
    parent_scope: str,
    warnings: list[str],
) -> list[str]:
    """The left-head-of-``on`` check, restated in this module's docstring.

    Every column-to-column equality in a join's ``on`` must connect the parent
    scope to this join's own alias. A reference to a *sibling* alias is a
    transitive join and is rejected.

    Operand order is a warning rather than an error. The rule is stated as
    "the left side resolves to source", and the strict reading would reject
    ``dim.pk = source.fk`` — which metric views accept and which is not the
    failure the rule exists to catch. Rejecting valid YAML over operand order
    would block correct proposals, so order is flagged and transitivity is
    failed.
    """
    condition = _parse_condition(on_clause)
    if condition is None:
        return [f"join '{alias}': 'on' does not parse as a SQL condition: {on_clause}"]

    errors: list[str] = []
    saw_join_equality = False

    for equality in condition.find_all(exp.EQ):
        left, right = equality.this, equality.expression
        if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
            continue
        left_scope = _column_scope(left)
        right_scope = _column_scope(right)
        if not left_scope or not right_scope:
            errors.append(
                f"join '{alias}': unqualified column in 'on' ({equality.sql(dialect='databricks')})"
            )
            continue

        saw_join_equality = True
        scopes = {left_scope, right_scope}
        if scopes != {parent_scope, alias}:
            foreign = sorted(scopes - {parent_scope, alias})
            errors.append(
                f"join '{alias}': transitive join — 'on' references {foreign or sorted(scopes)} "
                f"but a join at this level must connect '{parent_scope}' to '{alias}'. "
                "Use nested joins, a denormalized column, or a subquery source instead"
            )
        elif left_scope != parent_scope:
            warnings.append(
                f"join '{alias}': 'on' reads {left_scope} on the left; the documented form "
                f"puts '{parent_scope}' on the left"
            )

    if not saw_join_equality:
        errors.append(f"join '{alias}': 'on' has no column-to-column equality")
    return errors


def _parse_condition(text: str) -> exp.Expression | None:
    try:
        statement = sqlglot.parse_one(f"SELECT 1 FROM t WHERE {text}", read="databricks")
    except Exception:
        return None
    where = statement.args.get("where") if isinstance(statement, exp.Select) else None
    return where.this if where is not None else None


def _column_scope(column: exp.Column) -> str:
    """The dotted qualifier of a column reference, or ``''`` when unqualified.

    Nested join columns are referenced as ``parent.child.column``, so the scope
    is every part except the last rather than just ``table``.
    """
    parts = list(column.parts)
    if len(parts) < 2:
        return ""
    return ".".join(part.name for part in parts[:-1])


def _capability_map(capabilities: CapabilityRows) -> dict[str, str]:
    """Accept either the probe's flat results map or its typed capability rows.

    The rows are Pydantic models defined in the backend (``MvCapabilityRow``) and
    this package must not import the backend — the dependency runs backend to
    engine, never the reverse. :class:`CapabilityRow` is how that stays typed
    without inverting the arrow: the shape is declared here, the backend model
    satisfies it structurally, and a conformance test in the backend suite fails
    if it stops doing so.

    Plain mappings are still accepted because a persisted ``probe_results``
    payload round-trips as dicts, and re-hydrating models to read two fields
    would be ceremony.
    """
    if isinstance(capabilities, Mapping):
        return {str(k): str(v).upper() for k, v in capabilities.items()}

    resolved: dict[str, str] = {}
    for row in capabilities or ():
        if isinstance(row, Mapping):
            name = str(row.get("capability") or "")
            status = str(row.get("status") or "")
        else:
            name = str(getattr(row, "capability", "") or "")
            status = str(getattr(row, "status", "") or "")
        if name:
            resolved[name] = status.upper()
    return resolved


# ── DDL wrapper ──────────────────────────────────────────────────────────


def create_ddl(full_name: str, yaml_text: str, *, comment: str = "") -> str:
    """Wrap a rendered definition in the ``CREATE VIEW`` statement that installs it.

    Only the backend executes this, under the signed-in user's OBO client — the
    job runs as the service principal and never creates the object.

    A note on validating the wrapper: sqlglot has no grammar for
    ``WITH METRICS LANGUAGE YAML`` and parses the whole statement as an opaque
    ``Command``. So round-tripping it proves the YAML body survives intact, not
    that the DDL clause is well-formed; the body is what this module is
    responsible for and what :func:`validate` checks structurally.
    """
    body = yaml_text if yaml_text.endswith("\n") else yaml_text + "\n"
    statement = [f"CREATE VIEW {full_name}", "WITH METRICS", "LANGUAGE YAML"]
    if comment:
        statement.append("COMMENT '" + comment.replace("'", "''") + "'")
    statement.append("AS $$")
    statement.append(body + "$$")
    return "\n".join(statement)
