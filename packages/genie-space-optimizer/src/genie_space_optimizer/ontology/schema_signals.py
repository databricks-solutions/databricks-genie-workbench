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
) -> list[tuple[str, str, tuple[str, ...]]]:
    """Reconstruct FK asset→asset pairs with their join column(s).

    Returns ``(referencing_table, referenced_table, join_columns)`` — the join
    columns are the referencing (child) side's ``key_column_usage.column_name`` rows for
    that FK constraint (MV-D88: name the key so the map reads "shares key ``route_id``",
    not a bare verb). ``referential_constraints`` enumerates the FK constraints (it
    contains ONLY referential constraints). For each FK constraint:
    - the **referencing** (child) table is its entry in ``key_column_usage``;
    - the **referenced** (parent) table is its entry in ``constraint_column_usage``
      (falling back to the linked unique/PK constraint's ``key_column_usage`` row).
    Deterministic (sorted, de-duplicated; columns unioned + sorted); a self-referential
    FK is dropped (no edge). Missing/partial rows degrade to fewer edges (and, when the
    ``column_name`` is absent, to an empty column tuple), never an error.
    """
    # constraint (cat, sch, name) → table fqn + its referencing join column(s)
    kcu: dict[tuple[str, str, str], str] = {}
    kcu_cols: dict[tuple[str, str, str], list[str]] = {}
    for r in key_column_rows:
        ck = _constraint_key(r, "constraint")
        fqn = _fqn(r, "table_catalog", "table_schema", "table_name")
        if ck and fqn:
            kcu.setdefault(ck, fqn)
        col = r.get("column_name")
        if ck and col:
            cols = kcu_cols.setdefault(ck, [])
            if str(col) not in cols:
                cols.append(str(col))
    ccu: dict[tuple[str, str, str], str] = {}
    for r in constraint_column_rows:
        ck = _constraint_key(r, "constraint")
        fqn = _fqn(r, "table_catalog", "table_schema", "table_name")
        if ck and fqn:
            ccu.setdefault(ck, fqn)

    # (referencing, referenced) → union of the FK's referencing join column(s). A pair
    # reachable via >1 constraint unions their columns so the label names every key.
    edges: dict[tuple[str, str], set[str]] = {}
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
            edges.setdefault((referencing, referenced), set()).update(kcu_cols.get(fk, []))
    return [(a, b, tuple(sorted(cols))) for (a, b), cols in sorted(edges.items())]


def shared_join_column_edges(
    column_rows: Iterable[dict[str, Any]],
    *,
    join_suffixes: tuple[str, ...] = JOIN_COLUMN_SUFFIXES,
    min_tables: int = 2,
    max_tables: int = MAX_TABLES_PER_SHARED_COLUMN,
    max_schemas: int = MAX_SCHEMAS_PER_SHARED_COLUMN,
    name_denylist: frozenset[str] = GENERIC_JOIN_COLUMN_DENYLIST,
) -> list[tuple[str, str, float, str, tuple[str, ...]]]:
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
    ``SHARED_JOIN_WEIGHT``, source ``"shared_join_column"``, and the shared column name
    (so the map can read "shares column ``booking_id``", MV-D88).
    """
    suffixes = tuple(join_suffixes)
    denied = frozenset(str(n).casefold() for n in (name_denylist or ()))
    # Group by casefolded name (dedup), keeping the first-seen ORIGINAL spelling for the
    # display label (the edge names the real column, not a casefolded proxy).
    by_column: dict[str, set[str]] = {}
    display_name: dict[str, str] = {}
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
            display_name.setdefault(name, str(col))

    edges: list[tuple[str, str, float, str, tuple[str, ...]]] = []
    for _col, tables in sorted(by_column.items()):
        if not (min_tables <= len(tables) <= max_tables):
            continue
        # Per-column schema-span cap (MV-D61): a proxy reaching too many schemas is a
        # generic bridge, not a domain key. A declared FK is exempt (fk_edges above).
        if len({_schema_of_fqn(t) for t in tables}) > max_schemas:
            continue
        ordered = sorted(tables)
        hub = ordered[0]
        cols = (display_name.get(_col, _col),)
        for other in ordered[1:]:
            edges.append((hub, other, SHARED_JOIN_WEIGHT, "shared_join_column", cols))
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
    into the one ``join_key_edges`` list ``graph.build_signal_graph`` accepts. Every item
    is the 5-slot ``(a, b, weight, source, columns)`` shape the graph reads by position:
    an FK is ``(a, b, None, "foreign_key", cols)`` (weight-less, decisive); a proxy is
    ``(hub, other, SHARED_JOIN_WEIGHT, "shared_join_column", (col,))``. ``columns`` names
    the join key(s) so the map reads "shares key ``route_id``" (MV-D88). The proxy knobs
    (``join_suffixes`` / ``max_tables`` / ``max_schemas`` / ``name_denylist``, MV-D61)
    thread through to :func:`shared_join_column_edges`; ``fk_edges`` is unchanged — a
    declared FK is decisive and never span-capped."""
    out: list[tuple] = [
        (a, b, None, "foreign_key", cols)
        for a, b, cols in fk_edges(referential_rows, key_column_rows, constraint_column_rows)
    ]
    out += shared_join_column_edges(
        column_rows,
        join_suffixes=join_suffixes,
        max_tables=max_tables,
        max_schemas=max_schemas,
        name_denylist=name_denylist,
    )
    return out


# MV-D105 co-query fan-out / weight knobs (module constants — NOT job parameters, so the
# job's public surface stays fixed). ``MAX`` caps each table's co-query partners (a hub
# table never explodes the graph); the ``HALFLIFE`` is the co-occurrence count at which
# the bounded weight reaches 0.5 (a saturating ``count / (count + HALFLIFE)`` in (0, 1)).
CO_QUERY_MAX_PARTNERS = 20
CO_QUERY_WEIGHT_HALFLIFE = 5.0


def _scoped_fqn(fqn: Any, cats: set[str], entries: tuple[str, ...]) -> str | None:
    """Lower-cased FQN if it resolves to ``catalog.schema.table``, sits in an allowlisted
    catalog (``cats``, lower-cased), and clears the ``schema_denylist`` (``entries``); else
    ``None`` (dropped). Shared by the system-table adjacency producers (MV-D105) so lineage
    and co-query apply identical scope + denylist discipline (mirroring
    :func:`filter_denylisted_schemas`)."""
    if not fqn:
        return None
    low = str(fqn).strip().lower()
    parts = low.split(".")
    if len(parts) < 3 or not all(parts[:3]):
        return None
    cat, sch = parts[0], parts[1]
    if cat not in cats:
        return None
    if entries and _schema_denylisted(f"{cat}.{sch}", entries):
        return None
    return low


def lineage_adjacency_edges(
    rows: Iterable[dict[str, Any]],
    *,
    allowlist: Iterable[str],
    denylist: Iterable[str] | None = None,
) -> list[tuple[str, str]]:
    """Turn ``system.access.table_lineage`` rows into deduped ``(source, target)``
    asset↔asset adjacency pairs — the ``lineage_adjacency`` backbone that
    :func:`graph.build_signal_graph` accepts as its positional ``lineage_edges`` (cluster
    weight 5.0, MV-D105 Phase 1).

    Each row carries ``source`` / ``target`` table full names (``catalog.schema.table``).
    A pair is kept only when BOTH endpoints are non-null, resolve to
    ``catalog.schema.table``, sit in an allowlisted catalog, and clear the non-business
    ``schema_denylist`` (MV-D61) — mirroring :func:`join_key_edges`' scope + denylist
    discipline so lineage never fuses an out-of-scope or infra schema into the map. FQNs
    are lower-cased; self-loops are dropped; the result is deduped + sorted
    (deterministic).

    Pure; empty rows OR an empty allowlist ⇒ ``[]`` — byte-identical to the pre-Phase-1
    lineage-empty stub (MV-D43), so a missing ``system.access`` grant (which the reader's
    ``_rows_safe`` degrades to ``[]``) leaves the graph unchanged rather than faked."""
    cats = {str(c).strip().lower() for c in (allowlist or []) if str(c).strip()}
    if not cats:
        return []
    entries = tuple(e.strip() for e in (denylist or []) if str(e).strip())

    seen: set[tuple[str, str]] = set()
    for r in rows or []:
        src, dst = _scoped_fqn(r.get("source"), cats, entries), _scoped_fqn(r.get("target"), cats, entries)
        if src is None or dst is None or src == dst:
            continue
        seen.add((src, dst))
    return sorted(seen)


def co_query_edges(
    rows: Iterable[dict[str, Any]],
    *,
    allowlist: Iterable[str],
    denylist: Iterable[str] | None = None,
    max_partners: int = CO_QUERY_MAX_PARTNERS,
    weight_halflife: float = CO_QUERY_WEIGHT_HALFLIFE,
) -> list[tuple[str, str, float]]:
    """Turn per-statement table-access rows into COUNT-weighted ``(a, b, weight)``
    co-query co-occurrence edges — the ``co_query`` "reinforce" layer
    :func:`graph.build_signal_graph` accepts (cluster weight 2.0, related why "Frequently
    queried together", MV-D105 Phase 2).

    Each row is one ``{statement_id, fqn}`` table access (``system.access.table_lineage``
    grouped by ``statement_id`` + ``source_table_full_name``). Two distinct tables read by
    the SAME statement co-occur once; the co-occurrence COUNT across statements is the raw
    strength. Endpoints are scope + denylist filtered exactly like
    :func:`lineage_adjacency_edges` (via :func:`_scoped_fqn`). The count is normalized to a
    BOUNDED, saturating weight ``count / (count + weight_halflife)`` in ``(0, 1)`` (a
    trafficked pair pulls harder, but no single pair dominates). Fan-out is CAPPED: each
    table keeps only its ``max_partners`` strongest partners (by count desc, then fqn), so
    a hub table cannot explode the graph — an edge survives only when it is in BOTH
    endpoints' top-K (mutual), so every node's co-query degree is bounded by ``max_partners``
    (a weak edge to a hub is dropped; the table keeps its lineage / join_key backbone). The
    result is deduped + sorted (deterministic).

    Pure; empty rows OR an empty allowlist OR no in-scope co-occurrence ⇒ ``[]`` (MV-D43),
    so a missing grant / an absent ``statement_id`` degrades to a byte-identical graph."""
    cats = {str(c).strip().lower() for c in (allowlist or []) if str(c).strip()}
    if not cats:
        return []
    entries = tuple(e.strip() for e in (denylist or []) if str(e).strip())

    # Group the in-scope tables read by each statement.
    by_stmt: dict[str, set[str]] = {}
    for r in rows or []:
        stmt = r.get("statement_id")
        fqn = _scoped_fqn(r.get("fqn"), cats, entries)
        if stmt is None or str(stmt) == "" or fqn is None:
            continue
        by_stmt.setdefault(str(stmt), set()).add(fqn)

    # Count co-occurrence over unordered (a < b) pairs within each statement.
    counts: dict[tuple[str, str], int] = {}
    for fqns in by_stmt.values():
        members = sorted(fqns)
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                counts[(members[i], members[j])] = counts.get((members[i], members[j]), 0) + 1
    if not counts:
        return []

    # Capped fan-out: each node keeps its top-``max_partners`` partners (count desc, fqn asc).
    partners: dict[str, list[tuple[int, str]]] = {}
    for (a, b), c in counts.items():
        partners.setdefault(a, []).append((c, b))
        partners.setdefault(b, []).append((c, a))
    keep_for: dict[str, set[str]] = {}
    for node, plist in partners.items():
        plist.sort(key=lambda t: (-t[0], t[1]))
        keep_for[node] = {p for _, p in plist[:max_partners]}

    out: list[tuple[str, str, float]] = []
    for (a, b), c in counts.items():
        if b in keep_for.get(a, ()) and a in keep_for.get(b, ()):
            out.append((a, b, round(c / (c + weight_halflife), 6)))
    return sorted(out)


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
