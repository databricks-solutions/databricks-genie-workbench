"""PURE structural-signal extraction from ``information_schema`` + MV YAML (Stage 1,
MV-D52).

The strongest, most explainable grouping signals — foreign keys, shared join
columns, metric-view membership, and shared schema — all read from
``information_schema`` (``referential_constraints`` / ``key_column_usage`` /
``constraint_column_usage`` / ``columns`` / ``tables``) and the metric-view YAML the
17f estate reader already returns. No new dependency (MV-D45).

Every function here is **pure**: it takes plain row dicts (exactly what
``spark.sql(...).collect()`` yields, one dict per row) and returns plain edges / maps
that :func:`graph.build_signal_graph` accepts as opt-in kwargs. The job's Spark reader
issues the queries (allowlist-scoped, degrade-to-empty on any missing grant — MV-D43)
and hands the rows here, so the parsing stays offline-testable and the job stays thin.

No I/O, no ``backend.*`` / ``pyspark`` import, no governed-tag write of any kind.
"""

from __future__ import annotations

from fnmatch import fnmatchcase
from typing import Any, Iterable

# Shared-join-column proxy: a column whose name ends in one of these is a likely join
# key, so tables sharing it are a lower-confidence FK proxy (MV-D52, "strong"). Stage 3
# lifts this to settings; the constants live here for now.
#
# Stage 3.2 (MV-D61) DROPS ``_code``: a ``_code`` column (``country_code``,
# ``currency_code``, ``region_code``, ``postal_code``, …) is an enum / reference value,
# not a join key, and it was a top bridge that fused unrelated schemas (spec §1). Only
# ``_id`` / ``_key`` remain by default; ``_code`` is re-enabled only if an enterprise
# passes it back via ``domain_join_col_suffixes``.
JOIN_COLUMN_SUFFIXES: tuple[str, ...] = ("_id", "_key")
# A shared column touching more than this many tables is almost certainly a generic
# unit/dimension (the live estate's ``currency_code`` spans 17 tables) rather than a
# real join key — skip it so it does not fuse the whole estate into one blob.
MAX_TABLES_PER_SHARED_COLUMN = 15
# Stage 3.2 (MV-D61): a proxy column reaching more than this many distinct
# ``catalog.schema`` is a generic estate-wide bridge, not a bounded-context key — skip
# it (a declared FK is EXEMPT; only the proxy is span-capped). A real domain key stays
# within a domain's few schemas (spec §1/§2.1).
MAX_SCHEMAS_PER_SHARED_COLUMN = 2
# Stage 3.2 (MV-D61): generic surrogate / audit key names that collide across unrelated
# tables (an ``id`` / ``user_id`` in an infra table joins nothing meaningful to an
# airline table). Casefolded exact match; skipped outright. Threaded from config
# (``domain_join_col_denylist``) — this is the shipped default seed (spec §2.1.4).
GENERIC_JOIN_COLUMN_DENYLIST: frozenset[str] = frozenset(
    {"id", "user_id", "workspace_id", "category_id", "tenant_id", "account_id"}
)
# Proxy edges are weaker than a declared FK (which the clusterer treats as decisive).
SHARED_JOIN_WEIGHT = 0.5

# ``(catalog_key, schema_key)`` column-name variants across the information_schema row
# shapes this module consumes: ``table_*`` (columns / tables / key_column_usage /
# constraint_column_usage), ``catalog_name``/``schema_name`` (governed-tag assignments),
# and ``constraint_*`` (referential_constraints, keyed on the referencing schema).
_CATALOG_SCHEMA_KEYS: tuple[tuple[str, str], ...] = (
    ("table_catalog", "table_schema"),
    ("catalog_name", "schema_name"),
    ("constraint_catalog", "constraint_schema"),
)


def _fqn(row: dict[str, Any], cat: str, sch: str, tbl: str) -> str | None:
    parts = [row.get(cat), row.get(sch), row.get(tbl)]
    if not all(parts):
        return None
    return ".".join(str(p) for p in parts)


def _schema_of_fqn(fqn: str) -> str:
    """``catalog.schema`` of a dotted asset FQN (the schema-span key)."""
    parts = str(fqn).split(".")
    return ".".join(parts[:2]) if len(parts) >= 2 else str(fqn)


def _row_catalog_schema(row: dict[str, Any]) -> str | None:
    """``catalog.schema`` of a row across the information_schema shape variants, or
    ``None`` when the row carries no resolvable catalog+schema (kept, never judged)."""
    for cat_key, sch_key in _CATALOG_SCHEMA_KEYS:
        cat, sch = row.get(cat_key), row.get(sch_key)
        if cat and sch:
            return f"{cat}.{sch}"
    return None


def _schema_denylisted(cat_sch: str, entries: tuple[str, ...]) -> bool:
    """Whether ``catalog.schema`` matches a denylist entry — an exact ``catalog.schema``,
    a bare ``schema``, or an ``fnmatch`` glob (``e2e_*`` / ``*_dev``) against either.
    Case-insensitive + deterministic (``fnmatchcase`` on casefolded operands)."""
    schema = cat_sch.split(".", 1)[1] if "." in cat_sch else cat_sch
    cat_sch_cf, schema_cf = cat_sch.casefold(), schema.casefold()
    return any(
        fnmatchcase(cat_sch_cf, e.casefold()) or fnmatchcase(schema_cf, e.casefold())
        for e in entries
    )


def filter_denylisted_schemas(
    rows: Iterable[dict[str, Any]], *, denylist: Iterable[str] | None
) -> list[dict[str, Any]]:
    """Drop every row whose ``catalog.schema`` hits a non-business-schema denylist entry
    (MV-D61, the biggest lever, spec §2.1.1). Applied to EVERY row input before any edge
    is built, so a legitimate key that only *looked* cross-schema because it also touched
    infra collapses back to its true business schemas. A denylist entry is an exact
    ``catalog.schema``, a bare ``schema``, or an ``fnmatch`` glob (``e2e_*`` / ``*_dev``).
    A row with no resolvable catalog+schema is KEPT (never judged). Pure; empty/absent
    denylist returns the rows unchanged (today's behavior)."""
    entries = tuple(e.strip() for e in (denylist or []) if str(e).strip())
    if not entries:
        return list(rows)
    out: list[dict[str, Any]] = []
    for r in rows:
        cat_sch = _row_catalog_schema(r)
        if cat_sch is not None and _schema_denylisted(cat_sch, entries):
            continue
        out.append(r)
    return out


def _constraint_key(row: dict[str, Any], prefix: str) -> tuple[str, str, str] | None:
    cat = row.get(f"{prefix}_catalog")
    sch = row.get(f"{prefix}_schema")
    name = row.get(f"{prefix}_name")
    if cat is None or sch is None or name is None:
        return None
    return (str(cat), str(sch), str(name))


def fk_edges(
    referential_rows: Iterable[dict[str, Any]],
    key_column_rows: Iterable[dict[str, Any]],
    constraint_column_rows: Iterable[dict[str, Any]],
) -> list[tuple[str, str]]:
    """Reconstruct FK asset→asset pairs (``referencing_table``, ``referenced_table``).

    ``referential_constraints`` enumerates the FK constraints (it contains ONLY
    referential constraints). For each FK constraint:
    - the **referencing** (child) table is its entry in ``key_column_usage``;
    - the **referenced** (parent) table is its entry in ``constraint_column_usage``
      (falling back to the linked unique/PK constraint's ``key_column_usage`` row).
    Deterministic (sorted, de-duplicated); a self-referential FK is dropped (no
    edge). Missing/partial rows degrade to fewer edges, never an error.
    """
    # constraint (cat, sch, name) → table fqn
    kcu: dict[tuple[str, str, str], str] = {}
    for r in key_column_rows:
        ck = _constraint_key(r, "constraint")
        fqn = _fqn(r, "table_catalog", "table_schema", "table_name")
        if ck and fqn:
            kcu.setdefault(ck, fqn)
    ccu: dict[tuple[str, str, str], str] = {}
    for r in constraint_column_rows:
        ck = _constraint_key(r, "constraint")
        fqn = _fqn(r, "table_catalog", "table_schema", "table_name")
        if ck and fqn:
            ccu.setdefault(ck, fqn)

    edges: set[tuple[str, str]] = set()
    for r in referential_rows:
        fk = _constraint_key(r, "constraint")
        if fk is None:
            continue
        referencing = kcu.get(fk)
        referenced = ccu.get(fk)
        if referenced is None:
            uc = _constraint_key(r, "unique_constraint")
            if uc is not None:
                referenced = kcu.get(uc)
        if referencing and referenced and referencing != referenced:
            edges.add((referencing, referenced))
    return sorted(edges)


def shared_join_column_edges(
    column_rows: Iterable[dict[str, Any]],
    *,
    join_suffixes: tuple[str, ...] = JOIN_COLUMN_SUFFIXES,
    min_tables: int = 2,
    max_tables: int = MAX_TABLES_PER_SHARED_COLUMN,
    max_schemas: int = MAX_SCHEMAS_PER_SHARED_COLUMN,
    name_denylist: frozenset[str] = GENERIC_JOIN_COLUMN_DENYLIST,
) -> list[tuple[str, str, float, str]]:
    """Shared-join-column proxy edges (a lower-weight FK proxy, MV-D52).

    Tables that share a join-shaped column (name ends in one of ``join_suffixes``,
    default ``_id`` / ``_key`` — ``_code`` is dropped as a reference/enum, MV-D61) are
    likely joinable on it. Emitted as a **star** (sorted-first table → each other), so a
    column shared by *k* tables yields *k−1* edges (one connected component) rather than
    a *k²* clique. Three cuts keep the proxy from fusing the estate (MV-D61, spec §2.1):
    a column named in ``name_denylist`` (a generic surrogate/audit key) is skipped
    outright; a column touching more than ``max_tables`` tables is a generic unit; and a
    column whose tables span more than ``max_schemas`` distinct ``catalog.schema`` is a
    generic cross-schema bridge, not a bounded-context key. Each edge carries weight
    ``SHARED_JOIN_WEIGHT`` and source ``"shared_join_column"``.
    """
    suffixes = tuple(join_suffixes)
    denied = frozenset(str(n).casefold() for n in (name_denylist or ()))
    by_column: dict[str, set[str]] = {}
    for r in column_rows:
        col = r.get("column_name")
        if not col:
            continue
        name = str(col).casefold()
        if name in denied:
            continue
        if not name.endswith(suffixes):
            continue
        fqn = _fqn(r, "table_catalog", "table_schema", "table_name")
        if fqn:
            by_column.setdefault(name, set()).add(fqn)

    edges: list[tuple[str, str, float, str]] = []
    for _col, tables in sorted(by_column.items()):
        if not (min_tables <= len(tables) <= max_tables):
            continue
        # Per-column schema-span cap (MV-D61): a proxy reaching too many schemas is a
        # generic bridge, not a domain key. A declared FK is exempt (fk_edges above).
        if len({_schema_of_fqn(t) for t in tables}) > max_schemas:
            continue
        ordered = sorted(tables)
        hub = ordered[0]
        for other in ordered[1:]:
            edges.append((hub, other, SHARED_JOIN_WEIGHT, "shared_join_column"))
    return edges


def join_key_edges(
    referential_rows: Iterable[dict[str, Any]],
    key_column_rows: Iterable[dict[str, Any]],
    constraint_column_rows: Iterable[dict[str, Any]],
    column_rows: Iterable[dict[str, Any]],
    *,
    join_suffixes: tuple[str, ...] = JOIN_COLUMN_SUFFIXES,
    max_tables: int = MAX_TABLES_PER_SHARED_COLUMN,
    max_schemas: int = MAX_SCHEMAS_PER_SHARED_COLUMN,
    name_denylist: frozenset[str] = GENERIC_JOIN_COLUMN_DENYLIST,
) -> list[tuple]:
    """Combine declared FK edges (decisive) + shared-join-column proxy edges (strong)
    into the one ``join_key_edges`` list ``graph.build_signal_graph`` accepts. FK pairs
    default to source ``"foreign_key"``; proxies carry ``"shared_join_column"``. The
    proxy knobs (``join_suffixes`` / ``max_tables`` / ``max_schemas`` / ``name_denylist``,
    MV-D61) thread through to :func:`shared_join_column_edges`; ``fk_edges`` is unchanged
    — a declared FK is decisive and never span-capped."""
    out: list[tuple] = list(fk_edges(referential_rows, key_column_rows, constraint_column_rows))
    out += shared_join_column_edges(
        column_rows,
        join_suffixes=join_suffixes,
        max_tables=max_tables,
        max_schemas=max_schemas,
        name_denylist=name_denylist,
    )
    return out


def _clean_source_fqn(raw: Any) -> str | None:
    """A metric-view ``source`` is usually a table FQN (possibly back-ticked); a
    subquery source has no single table. Return the dotted FQN when the source is a
    plain identifier, else ``None`` (skip — no membership edge for a subquery source)."""
    if not isinstance(raw, str):
        return None
    s = raw.strip().replace("`", "")
    if not s or any(ch in s for ch in " \n\t(") or s.upper().startswith("SELECT"):
        return None
    return s if s.count(".") >= 1 else None


def mv_membership_map(yamls: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    """``{mv_fqn: [source_table_fqn]}`` from the 17f estate MV-YAML map.

    Reuses the existing ``estate_metric_view_yamls`` output (each value is a parsed
    metric-view YAML with a top-level ``source``). A subquery source (no single table)
    contributes no membership edge (MV-D43 degrade). An MV with no resolvable source is
    omitted so it never seeds an empty group."""
    out: dict[str, list[str]] = {}
    for mv_fqn, doc in (yamls or {}).items():
        if not isinstance(doc, dict):
            continue
        src = _clean_source_fqn(doc.get("source"))
        if src:
            out[str(mv_fqn)] = [src]
    return out


def schema_affinity_map(table_rows: Iterable[dict[str, Any]]) -> dict[str, list[str]]:
    """``{catalog.schema: [table_fqn, ...]}`` from ``information_schema.tables`` — the
    shared-schema grouping signal (assets in one schema, esp. a named business area,
    belong together). Deterministic (sorted); a schema with a single table still maps
    (the clusterer decides whether it is enough)."""
    by_schema: dict[str, set[str]] = {}
    for r in table_rows:
        cat, sch, tbl = r.get("table_catalog"), r.get("table_schema"), r.get("table_name")
        if not (cat and sch and tbl):
            continue
        by_schema.setdefault(f"{cat}.{sch}", set()).add(f"{cat}.{sch}.{tbl}")
    return {k: sorted(v) for k, v in sorted(by_schema.items())}
