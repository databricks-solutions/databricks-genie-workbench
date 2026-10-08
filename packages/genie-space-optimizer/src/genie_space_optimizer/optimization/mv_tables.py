"""Resolve a spelled table name against the space's own identifiers (MV-D123).

Identity v2 keys a measure by its calculation and its fully qualified tables. A
statement may spell a table three-, two- or one-part, so before two table sets
can be compared each name is resolved to one of three states (Ruling 2):

- **resolved** — a lowercase, unquoted, three-part name. A three-part spelling
  resolves to itself; a two-part ``schema.table`` to the one space identifier
  whose last two parts match; a one-part ``table`` to the one space identifier
  with that leaf. Case and backticks never matter.
- :data:`TABLELESS` — no table is named.
- :data:`UNRESOLVED` — zero or several space identifiers match, or the name
  cannot be split into parts unambiguously (a part holding a dot, an unbalanced
  backtick, an empty part, more than three parts). A CTE name or a derived
  relation's alias is unresolved because no space identifier carries it.

Unresolved never matches anything. A table-less side matches only when the
caller says the sole-row rule holds — it joins the one row or set with its
calculation, and with two or more it joins none — which the comparison helpers
take as a required ``tableless_matches`` keyword.

The space's identifiers are ``data_sources.tables[].identifier`` and
``data_sources.metric_views[].identifier`` and nothing else (Ruling 3). With no
config at all, every name is read as written (Ruling 4).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from enum import Enum
from typing import Any, Final


class TableState(Enum):
    """The two ways a table name can fail to resolve to one full name."""

    TABLELESS = "tableless"
    UNRESOLVED = "unresolved"

    def __repr__(self) -> str:
        return self.name


TABLELESS: Final = TableState.TABLELESS
UNRESOLVED: Final = TableState.UNRESOLVED

ResolvedTables = frozenset[str] | TableState
"""A non-empty set of resolved names, :data:`TABLELESS` or :data:`UNRESOLVED`."""


def _name_parts(name: str) -> tuple[str, ...] | None:
    """The lowercase, unquoted parts of a dotted name, or None when unreadable.

    The split is backtick-aware, so a backticked ``a.b`` followed by ``.c`` is
    two parts. A part that still holds a dot after unquoting is refused: joined
    back with dots it would read as a different, three-part name.
    """
    # mv_yaml imports mv_fingerprint and mv_scoring at module scope; a
    # module-scope import here would be circular for either of them importing
    # this module.
    from genie_space_optimizer.optimization.mv_yaml import _split_name

    parts: list[str] = []
    for raw in _split_name(name):
        part = raw.strip()
        if len(part) >= 2 and part.startswith("`") and part.endswith("`"):
            part = part[1:-1].replace("``", "`")
        elif "`" in part:
            return None
        if not part or "." in part:
            return None
        parts.append(part.lower())
    if len(parts) > 3:
        return None
    return tuple(parts)


class TableResolver:
    """Maps a spelled table name to its full name, :data:`TABLELESS` or :data:`UNRESOLVED`.

    Build it with :meth:`from_config`. ``identifiers=None`` means no table list:
    every readable name resolves as written, lowercased and unquoted, which is
    what the extractor already produces for it (Ruling 4).
    """

    def __init__(self, identifiers: Iterable[str] | None) -> None:
        self._as_written = identifiers is None
        self._by_schema_leaf: dict[tuple[str, str], set[str]] = {}
        self._by_leaf: dict[str, set[str]] = {}
        for identifier in identifiers or ():
            parts = _name_parts(identifier) if isinstance(identifier, str) else None
            if parts is None or len(parts) != 3:
                continue
            full = ".".join(parts)
            self._by_schema_leaf.setdefault(parts[1:], set()).add(full)
            self._by_leaf.setdefault(parts[2], set()).add(full)

    @classmethod
    def from_config(cls, applied_config: Mapping[str, Any] | None) -> TableResolver:
        """The resolver over a space config's own identifiers (Ruling 3).

        ``None`` is no table list (Ruling 4). A mapping is always a table list,
        even when it names no tables, so a config missing ``data_sources``
        resolves only three-part names rather than falling back to as-written.
        """
        if applied_config is None:
            return cls(None)
        if not isinstance(applied_config, Mapping):
            raise TypeError("applied_config must be a mapping or None")
        sources = applied_config.get("data_sources")
        identifiers: list[str] = []
        if isinstance(sources, Mapping):
            for key in ("tables", "metric_views"):
                for entry in sources.get(key) or ():
                    if isinstance(entry, Mapping) and isinstance(
                        entry.get("identifier"), str
                    ):
                        identifiers.append(entry["identifier"])
        return cls(identifiers)

    @property
    def has_table_list(self) -> bool:
        """False when built with no table list, so every name reads as written (Ruling 4)."""
        return not self._as_written

    def resolve(self, name: str | None) -> str | TableState:
        """One spelled name's full name, :data:`TABLELESS` or :data:`UNRESOLVED`."""
        if name is None or not name.strip():
            return TABLELESS
        parts = _name_parts(name)
        if parts is None:
            return UNRESOLVED
        if len(parts) == 3 or self._as_written:
            return ".".join(parts)
        matches = (
            self._by_schema_leaf.get((parts[0], parts[1]))
            if len(parts) == 2
            else self._by_leaf.get(parts[0])
        )
        if not matches or len(matches) != 1:
            return UNRESOLVED
        return next(iter(matches))

    def resolve_all(self, names: Iterable[str | None]) -> ResolvedTables:
        """A statement's table set: the resolved names, :data:`TABLELESS` or :data:`UNRESOLVED`.

        Any unresolved name makes the whole set unresolved. Blank names are
        skipped, and a set that names no table is :data:`TABLELESS` — never an
        empty set.
        """
        if isinstance(names, str):
            raise TypeError("resolve_all takes a collection of names, not one name")
        resolved: set[str] = set()
        for name in names:
            state = self.resolve(name)
            if state is UNRESOLVED:
                return UNRESOLVED
            if isinstance(state, str):
                resolved.add(state)
        return frozenset(resolved) if resolved else TABLELESS


def _as_resolved(tables: ResolvedTables | Iterable[str]) -> ResolvedTables:
    if isinstance(tables, TableState):
        return tables
    if isinstance(tables, str):
        raise TypeError("expected a collection of resolved names, not one name")
    return TableResolver(None).resolve_all(tables)


def _match(
    left: ResolvedTables | Iterable[str],
    right: ResolvedTables | Iterable[str],
    tableless_matches: bool,
    sets_match: Callable[[frozenset[str], frozenset[str]], bool],
) -> bool:
    left_set, right_set = _as_resolved(left), _as_resolved(right)
    if left_set is UNRESOLVED or right_set is UNRESOLVED:
        return False
    if left_set is TABLELESS or right_set is TABLELESS:
        return tableless_matches
    return sets_match(left_set, right_set)


def same_fq_tables(
    left: ResolvedTables | Iterable[str],
    right: ResolvedTables | Iterable[str],
    *,
    tableless_matches: bool,
) -> bool:
    """Whether two resolved table sets name exactly the same tables.

    An unresolved side never matches. A table-less side matches exactly when
    ``tableless_matches`` is true: the caller passes whether the sole-row rule
    holds for that calculation. A plain collection of names is read as already
    resolved — only case and backticks are normalized — and an empty one is
    table-less.
    """
    return _match(left, right, tableless_matches, lambda a, b: a == b)


def fq_tables_overlap(
    left: ResolvedTables | Iterable[str],
    right: ResolvedTables | Iterable[str],
    *,
    tableless_matches: bool,
) -> bool:
    """Whether two resolved table sets share a table, under :func:`same_fq_tables`' rules."""
    return _match(left, right, tableless_matches, lambda a, b: not a.isdisjoint(b))


__all__ = [
    "TABLELESS",
    "UNRESOLVED",
    "ResolvedTables",
    "TableResolver",
    "TableState",
    "fq_tables_overlap",
    "same_fq_tables",
]
