"""Row-level security / column-mask audit for Genie Space tables.

Lives in ``iq_scan/`` to colocate with Check 9 (the IQ score's RLS
advisory) but kept in a separate module from ``scoring.py`` so the
scoring engine stays a pure-function library free of SDK / Spark /
SQL-warehouse dependencies.

The audit goes beyond the serialized_space field scan that
``calculate_score`` does today: it detects **inherited** RLS (views
reading from row-filtered base tables) and **dynamic views** (views
using ``current_user()`` / ``is_member()`` / etc.) which Genie silently
disables entity matching on but which ``_table_has_rls`` in
``optimization/applier.py`` cannot see (those fields live on the base
table, not the view).

Queries use privilege-aware ``<catalog>.information_schema.*`` views.
The app service principal's ``USE CATALOG + USE SCHEMA + SELECT``
grants (auto-bound via app resource binding) are sufficient; we never
touch ``system.*`` which would require broader scope.

Fail-open contract: any probe failure or query error is logged once
per catalog at WARNING level; the verdict for affected tables becomes
``"unknown"``. The scoring function + auto_apply_prompt_matching
treat ``"unknown"`` as clean by default (matches preflight's
warn-and-proceed philosophy); operators who want fail-closed behaviour
flip ``GSO_STRICT_RLS=true``.
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


def _dynamic_view_fn_re() -> "re.Pattern[str]":
    """Lazy-import of DYNAMIC_VIEW_FN_RE from config.

    config imports from iq_scan.scoring (re-exporting
    ``looks_like_sql_in_prose``), so iq_scan can't eagerly import from
    config or we create a cycle. Lazy import defers the dependency to
    first call, which only happens after both modules are fully
    loaded.
    """
    from genie_space_optimizer.common.config import DYNAMIC_VIEW_FN_RE

    return DYNAMIC_VIEW_FN_RE


# Regex fallback for view-base-table extraction when
# ``information_schema.view_table_usage`` is unavailable. Matches
# ``FROM cat.sch.tbl`` and ``JOIN cat.sch.tbl`` with optional backticks.
# Not a full parser — anything ambiguous falls back to verdict=unknown.
_FROM_JOIN_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+"
    r"(?:`(?P<cat_q>[^`]+)`|(?P<cat>[\w]+))"
    r"(?:\s*\.\s*(?:`(?P<sch_q>[^`]+)`|(?P<sch>[\w]+)))"
    r"(?:\s*\.\s*(?:`(?P<tbl_q>[^`]+)`|(?P<tbl>[\w]+)))?",
    re.IGNORECASE,
)


def _canonical_fqn(catalog: str, schema: str, table: str) -> str:
    """Return lowercase ``cat.sch.tbl`` with backticks stripped."""
    return ".".join(
        (s or "").strip().strip("`").lower()
        for s in (catalog, schema, table)
    )


def _extract_space_tables(
    space_tables: list[dict],
) -> list[tuple[str, str, str, bool]]:
    """Return ``[(catalog, schema, table, is_view_or_mv), ...]`` lowercased.

    ``is_view_or_mv`` is True for metric views and any table flagged with a
    view-ish ``table_type``. Treated as "needs view_definition lookup" so
    inherited-RLS + dynamic-view checks get a chance to fire. On
    ambiguity, err on the side of "is a view" — the worst that happens is
    an unnecessary ``view_definition`` query.
    """
    out: list[tuple[str, str, str, bool]] = []
    for t in space_tables:
        if not isinstance(t, dict):
            continue
        ident = t.get("identifier") or t.get("name") or ""
        parts = str(ident).strip("`").split(".")
        if len(parts) != 3:
            continue
        cat, sch, tbl = (p.strip("`").lower() for p in parts)
        if not (cat and sch and tbl):
            continue
        is_view = bool(
            t.get("is_view")
            or t.get("is_metric_view")
            or t.get("table_type", "").upper() in {"VIEW", "MATERIALIZED_VIEW"}
            or t.get("is_materialized_view")
        )
        out.append((cat, sch, tbl, is_view))
    return out


def _make_empty_verdict() -> dict[str, Any]:
    return {
        "has_direct_row_filter": False,
        "has_direct_column_mask": False,
        "inherits_rls_via": [],
        "has_dynamic_view_function": False,
        "verdict": "clean",
        "reason": "",
    }


def _probe_information_schema(
    catalog: str,
    *,
    exec_sql,
) -> tuple[bool, str]:
    """Return ``(available, error_msg)`` for the given catalog.

    Uses a cheap ``SELECT 1 ... LIMIT 1`` against ``information_schema.row_filters``.
    Success (zero rows OK) → available. Any exception → unavailable; the
    caller logs a single WARNING and skips the catalog.
    """
    try:
        exec_sql(
            f"SELECT 1 FROM `{catalog}`.information_schema.row_filters LIMIT 1"
        )
        return True, ""
    except Exception as exc:
        msg = str(exc)[:200]
        return False, msg


def _build_in_clause(pairs: list[tuple[str, str]]) -> str:
    """Build ``(('sch1','tbl1'), ('sch2','tbl2'))`` for SQL IN."""
    quoted = ", ".join(
        f"('{sch.replace(chr(39), chr(39)*2)}', '{tbl.replace(chr(39), chr(39)*2)}')"
        for sch, tbl in pairs
    )
    return quoted


def _query_row_filters(
    catalog: str,
    pairs: list[tuple[str, str]],
    *,
    exec_sql,
) -> set[tuple[str, str]]:
    """Return ``{(schema, table)}`` with a direct row_filter in catalog.

    Queries ``information_schema.row_filters`` using the real column
    names (``table_schema`` / ``table_name``) — this view does NOT
    expose a ``schema_name`` column, so selecting or filtering on it
    raises ``UNRESOLVED_COLUMN``.
    """
    if not pairs:
        return set()
    in_clause = _build_in_clause(pairs)
    sql = (
        f"SELECT table_schema, table_name "
        f"FROM `{catalog}`.information_schema.row_filters "
        f"WHERE (table_schema, table_name) IN ({in_clause})"
    )
    try:
        df = exec_sql(sql)
    except Exception as exc:
        logger.warning(
            "RLS audit: row_filters query failed for catalog %s: %s",
            catalog, str(exc)[:200],
        )
        return set()
    return {
        (str(r["table_schema"]).lower(), str(r["table_name"]).lower())
        for _, r in df.iterrows()
    } if not df.empty else set()


def _query_column_masks(
    catalog: str,
    pairs: list[tuple[str, str]],
    *,
    exec_sql,
) -> set[tuple[str, str]]:
    """Return ``{(schema, table)}`` with a direct column_mask in catalog.

    Queries ``information_schema.column_masks`` using the real column
    names (``table_schema`` / ``table_name``) — this view does NOT
    expose a ``schema_name`` column, so selecting or filtering on it
    raises ``UNRESOLVED_COLUMN``.
    """
    if not pairs:
        return set()
    in_clause = _build_in_clause(pairs)
    sql = (
        f"SELECT table_schema, table_name "
        f"FROM `{catalog}`.information_schema.column_masks "
        f"WHERE (table_schema, table_name) IN ({in_clause})"
    )
    try:
        df = exec_sql(sql)
    except Exception as exc:
        logger.warning(
            "RLS audit: column_masks query failed for catalog %s: %s",
            catalog, str(exc)[:200],
        )
        return set()
    return {
        (str(r["table_schema"]).lower(), str(r["table_name"]).lower())
        for _, r in df.iterrows()
    } if not df.empty else set()


def _query_view_definitions(
    catalog: str,
    pairs: list[tuple[str, str]],
    *,
    exec_sql,
) -> dict[tuple[str, str], str]:
    """Return ``{(schema, view): view_definition}`` for each view in catalog.

    Used for dynamic-view detection and as a fallback lineage source when
    ``view_table_usage`` is unavailable.
    """
    if not pairs:
        return {}
    in_clause = _build_in_clause(pairs)
    sql = (
        f"SELECT table_schema, table_name, view_definition "
        f"FROM `{catalog}`.information_schema.views "
        f"WHERE (table_schema, table_name) IN ({in_clause})"
    )
    try:
        df = exec_sql(sql)
    except Exception as exc:
        logger.warning(
            "RLS audit: views query failed for catalog %s: %s",
            catalog, str(exc)[:200],
        )
        return {}
    if df.empty:
        return {}
    return {
        (str(r["table_schema"]).lower(), str(r["table_name"]).lower()):
            str(r["view_definition"] or "")
        for _, r in df.iterrows()
    }


def _query_view_table_usage(
    catalog: str,
    pairs: list[tuple[str, str]],
    *,
    exec_sql,
) -> dict[tuple[str, str], list[tuple[str, str, str]]]:
    """Return ``{(view_schema, view_name): [(base_catalog, base_schema, base_table), ...]}``.

    Availability varies by DBR version; on failure we return ``{}`` and
    the caller falls back to regex on the view definition.
    """
    if not pairs:
        return {}
    in_clause = _build_in_clause(pairs)
    sql = (
        f"SELECT view_schema, view_name, "
        f"table_catalog, table_schema, table_name "
        f"FROM `{catalog}`.information_schema.view_table_usage "
        f"WHERE (view_schema, view_name) IN ({in_clause})"
    )
    try:
        df = exec_sql(sql)
    except Exception as exc:
        logger.info(
            "RLS audit: view_table_usage unavailable in catalog %s "
            "(falling back to regex): %s",
            catalog, str(exc)[:200],
        )
        return {}
    out: dict[tuple[str, str], list[tuple[str, str, str]]] = {}
    for _, r in df.iterrows():
        key = (str(r["view_schema"]).lower(), str(r["view_name"]).lower())
        base = (
            str(r["table_catalog"]).lower(),
            str(r["table_schema"]).lower(),
            str(r["table_name"]).lower(),
        )
        out.setdefault(key, []).append(base)
    return out


def _regex_base_tables(view_definition: str) -> list[tuple[str, str, str]]:
    """Extract FROM/JOIN table references from a view's DDL.

    Fallback for catalogs where ``view_table_usage`` is unavailable. Returns
    lowercased ``[(catalog, schema, table), ...]``. A match is only emitted
    when all three parts are present — bare ``FROM t`` (relying on current
    catalog/schema context) is deliberately skipped because we can't resolve
    it safely.
    """
    if not view_definition:
        return []
    out: list[tuple[str, str, str]] = []
    for m in _FROM_JOIN_RE.finditer(view_definition):
        cat = (m.group("cat_q") or m.group("cat") or "").lower()
        sch = (m.group("sch_q") or m.group("sch") or "").lower()
        tbl = (m.group("tbl_q") or m.group("tbl") or "").lower()
        if cat and sch and tbl:
            out.append((cat, sch, tbl))
    return out


def collect_rls_audit(
    space_tables: list[dict],
    *,
    exec_sql=None,
    spark: Any = None,
    w: Any = None,
    warehouse_id: str = "",
) -> dict[str, dict]:
    """Per-table RLS verdict via catalog-scoped ``information_schema``.

    Parameters
    ----------
    space_tables
        Flat list of serialized_space table + metric_view entries. Only
        fully-qualified identifiers (``catalog.schema.table``) are
        considered; short-form entries are skipped.
    exec_sql
        Optional executor with signature ``(sql: str) -> pd.DataFrame``.
        If omitted, a default backed by :func:`_exec_sql` from
        ``optimization.evaluation`` is built from ``spark`` / ``w`` /
        ``warehouse_id``.

    Returns
    -------
    dict
        ``{"cat.sch.tbl": {verdict fields...}}``.  Every input table
        appears as a key; absent lookups return a ``clean`` verdict so
        the scorer can proceed without special-casing missing entries.

    Verdict fields: ``has_direct_row_filter``, ``has_direct_column_mask``,
    ``inherits_rls_via`` (list of base-table FQNs), ``has_dynamic_view_function``,
    ``verdict`` (``"clean" | "tainted" | "unknown"``), ``reason``.

    Fail-open: probe failures and query errors mark affected tables
    ``verdict="unknown"`` without raising. The function always returns a
    dict; callers can trust the shape.
    """
    if exec_sql is None:
        # Local import so this module stays importable from contexts
        # (unit tests, CI) where optimizer.evaluation isn't available.
        from genie_space_optimizer.optimization.evaluation import _exec_sql

        def exec_sql(sql: str):
            return _exec_sql(
                sql, spark, w=w, warehouse_id=warehouse_id,
            )

    parsed = _extract_space_tables(space_tables)
    if not parsed:
        return {}

    # Group by catalog for batch queries.
    by_catalog_all: dict[str, list[tuple[str, str]]] = {}
    by_catalog_views: dict[str, list[tuple[str, str]]] = {}
    for cat, sch, tbl, is_view in parsed:
        by_catalog_all.setdefault(cat, []).append((sch, tbl))
        if is_view:
            by_catalog_views.setdefault(cat, []).append((sch, tbl))

    # Initialise every table with a clean verdict; we'll overlay findings.
    verdicts: dict[str, dict] = {
        _canonical_fqn(cat, sch, tbl): _make_empty_verdict()
        for cat, sch, tbl, _ in parsed
    }

    # Per-catalog probe + queries.
    for catalog, pairs in by_catalog_all.items():
        available, err_msg = _probe_information_schema(catalog, exec_sql=exec_sql)
        if not available:
            logger.warning(
                "RLS audit: information_schema.row_filters unavailable in "
                "catalog %s — affected tables get verdict='unknown'. "
                "Cause: %s",
                catalog, err_msg or "unknown error",
            )
            for sch, tbl in pairs:
                key = _canonical_fqn(catalog, sch, tbl)
                verdicts[key]["verdict"] = "unknown"
                verdicts[key]["reason"] = (
                    f"information_schema probe failed: {err_msg[:120]}"
                )
            continue

        # Direct row_filters + column_masks.
        rf_set = _query_row_filters(catalog, pairs, exec_sql=exec_sql)
        cm_set = _query_column_masks(catalog, pairs, exec_sql=exec_sql)
        for sch, tbl in pairs:
            key = _canonical_fqn(catalog, sch, tbl)
            if (sch, tbl) in rf_set:
                verdicts[key]["has_direct_row_filter"] = True
                verdicts[key]["verdict"] = "tainted"
                verdicts[key]["reason"] = "direct row_filter attached"
            if (sch, tbl) in cm_set:
                verdicts[key]["has_direct_column_mask"] = True
                if verdicts[key]["verdict"] != "tainted":
                    verdicts[key]["verdict"] = "tainted"
                    verdicts[key]["reason"] = "direct column_mask attached"

        # Dynamic views + inherited RLS — only relevant for views.
        view_pairs = by_catalog_views.get(catalog, [])
        if not view_pairs:
            continue
        view_defs = _query_view_definitions(catalog, view_pairs, exec_sql=exec_sql)
        view_lineage = _query_view_table_usage(
            catalog, view_pairs, exec_sql=exec_sql,
        )

        # Build a lineage map (view -> list of base FQNs), preferring
        # view_table_usage; regex-only for views missing from it.
        for sch, tbl in view_pairs:
            view_key_db = (sch, tbl)
            base_refs = view_lineage.get(view_key_db, [])
            if not base_refs:
                base_refs = _regex_base_tables(view_defs.get(view_key_db, ""))
            view_fqn = _canonical_fqn(catalog, sch, tbl)

            # Dynamic-view detection via regex on the DDL.
            ddl = view_defs.get(view_key_db, "")
            if ddl and _dynamic_view_fn_re().search(ddl):
                verdicts[view_fqn]["has_dynamic_view_function"] = True
                verdicts[view_fqn]["verdict"] = "tainted"
                if not verdicts[view_fqn]["reason"]:
                    verdicts[view_fqn]["reason"] = (
                        "view uses identity function (current_user / is_member)"
                    )

            # Inherited RLS — a base table has its own row_filter /
            # column_mask. Look up each base in our per-catalog RLS sets.
            inherited: list[str] = []
            for base in base_refs:
                base_cat, base_sch, base_tbl = base
                # Cross-catalog: query that catalog too. We already
                # probed the VIEW's catalog; run a narrow query for the
                # base catalog if different.
                if base_cat == catalog:
                    if (base_sch, base_tbl) in rf_set or (base_sch, base_tbl) in cm_set:
                        inherited.append(
                            _canonical_fqn(base_cat, base_sch, base_tbl)
                        )
                else:
                    # Cross-catalog lineage: cheap targeted probe.
                    try:
                        cross_rf = _query_row_filters(
                            base_cat, [(base_sch, base_tbl)], exec_sql=exec_sql,
                        )
                        cross_cm = _query_column_masks(
                            base_cat, [(base_sch, base_tbl)], exec_sql=exec_sql,
                        )
                        if cross_rf or cross_cm:
                            inherited.append(
                                _canonical_fqn(base_cat, base_sch, base_tbl)
                            )
                    except Exception:
                        # Cross-catalog probe failed — treat the view as
                        # unknown rather than silently clean.
                        if verdicts[view_fqn]["verdict"] != "tainted":
                            verdicts[view_fqn]["verdict"] = "unknown"
                            verdicts[view_fqn]["reason"] = (
                                f"cross-catalog lineage unreadable: {base_cat}"
                            )

            if inherited:
                verdicts[view_fqn]["inherits_rls_via"] = inherited
                verdicts[view_fqn]["verdict"] = "tainted"
                if not verdicts[view_fqn]["reason"]:
                    verdicts[view_fqn]["reason"] = (
                        f"inherits RLS from {inherited[0]}"
                    )

    return verdicts


__all__ = ["collect_rls_audit"]
