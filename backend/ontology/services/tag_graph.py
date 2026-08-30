"""Governed-tag graph (SP, MV-D37).

Enumerate ``system.tags.governed_tags`` + allowed values, and join the
assignments from ``system.information_schema.{catalog,schema,table,column}_tags``
(``tag_name = tag_key``). Runs as the *service principal*
(`get_service_principal_client`) — system tables are not OBO-readable — with the
same in-process TTL cache and permission-error detection shape as
`backend/watch/services/system_tables.py`.

Phase 1 reads this live + TTL-cached (GenieWatch model). When the batch mirror
lands (Phase 2) the same route contract is served from the mirror table without
changing this module's public shape.

A tag ``acts_as_domain`` when it has no ``/`` in its key (a top-level Discover
domain tag); a ``{parent}/{child}`` key is a sub-domain (MV-D37 convention).
Whether a tag is *used* as a domain vs. a plain classification tag is a heuristic
in Phase 1 — see :func:`_classify`.

Scope is the catalog allowlist (MV-D42). An empty allowlist yields an empty
graph and queries nothing.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

from backend.services.auth import get_service_principal_client

logger = logging.getLogger(__name__)

# ─── In-process TTL cache (GenieWatch shape) ──────────────────────────────
_CACHE_TTL_SECONDS = 300
_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, tuple[float, Any]] = {}

# Observational accessibility signal for the tag_graph tier: None until a read
# runs, True on success, False on a permission error. Surfaced by /preflight.
_TAG_GRAPH_ACCESSIBLE: bool | None = None


def tag_graph_status() -> bool | None:
    """Last observed governed-tag accessibility (None=unknown, False=grant missing)."""
    return _TAG_GRAPH_ACCESSIBLE


def _looks_like_permission_error(msg: str) -> bool:
    m = (msg or "").lower()
    return any(
        s in m
        for s in (
            "permission denied",
            "does not have",
            "not authorized",
            "requires permission",
            "access denied",
            "insufficient privileges",
            "table or view not found",
        )
    )


def _warehouse_id() -> str:
    wh = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
    if not wh:
        raise RuntimeError("SQL_WAREHOUSE_ID env var is empty — tag-graph queries cannot run.")
    return wh


def _p(name: str, value: Any, value_type: str = "STRING") -> StatementParameterListItem:
    return StatementParameterListItem(name=name, value=str(value), type=value_type)


def _run(
    sql: str,
    parameters: list[StatementParameterListItem],
    *,
    track_health: bool,
    poll_total_seconds: int = 120,
) -> list[dict[str, Any]]:
    """Execute an SP statement; return rows as dicts (best-effort, cached)."""
    global _TAG_GRAPH_ACCESSIBLE
    client: WorkspaceClient = get_service_principal_client()
    resp = client.statement_execution.execute_statement(
        warehouse_id=_warehouse_id(),
        statement=sql,
        parameters=parameters,
        wait_timeout="50s",
    )
    statement_id = resp.statement_id if resp else None
    deadline = time.monotonic() + poll_total_seconds
    while resp and resp.status and resp.status.state in (
        StatementState.PENDING, StatementState.RUNNING,
    ):
        if time.monotonic() > deadline or not statement_id:
            logger.warning("tag-graph query %s timed out; returning [].", statement_id)
            return []
        time.sleep(2.0)
        resp = client.statement_execution.get_statement(statement_id=statement_id)

    if resp is None or resp.status is None:
        return []
    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error
        msg = err.message if err else resp.status.state
        if track_health and _looks_like_permission_error(str(msg)):
            _TAG_GRAPH_ACCESSIBLE = False
        logger.warning("tag-graph query %s ended in %s: %s", statement_id, resp.status.state, msg)
        return []

    if track_health:
        _TAG_GRAPH_ACCESSIBLE = True

    if not resp.result or not resp.result.data_array:
        return []
    schema = resp.manifest.schema if resp.manifest else None
    if schema is None or schema.columns is None:
        return []
    cols = [c.name for c in schema.columns]
    return [{cols[i]: row[i] for i in range(len(cols))} for row in resp.result.data_array]


def probe() -> bool:
    """Cheap SP read of the governed-tag catalog to resolve the tag_graph tier.

    Returns True if the SP can SELECT ``system.tags.governed_tags`` (tier ``ok``),
    False on a permission error (tier ``blocked``). Updates the observational
    accessibility signal as a side effect. Never raises.
    """
    try:
        _run("SELECT * FROM system.tags.governed_tags LIMIT 1", [], track_health=True)
    except Exception as e:  # noqa: BLE001 — preflight never raises
        logger.info("tag-graph probe failed: %s", e)
    return _TAG_GRAPH_ACCESSIBLE is True


def _tag_key(row: dict[str, Any]) -> str | None:
    for k in ("tag_name", "tag_key", "name", "key"):
        v = row.get(k)
        if v:
            return str(v)
    return None


def _allowed_values(row: dict[str, Any]) -> list[str]:
    """Extract allowed values from a governed_tags row across schema variants."""
    for k in ("allowed_values", "tag_values", "values"):
        v = row.get(k)
        if not v:
            continue
        if isinstance(v, list):
            return [str(x) for x in v if x is not None]
        if isinstance(v, str):
            # Comma- or JSON-array-ish string.
            s = v.strip().strip("[]")
            return [p.strip().strip('"').strip("'") for p in s.split(",") if p.strip()]
    return []


def _member_fqn(row: dict[str, Any]) -> str | None:
    cat = row.get("catalog_name")
    sch = row.get("schema_name")
    tbl = row.get("table_name")
    parts = [p for p in (cat, sch, tbl) if p]
    return ".".join(str(p) for p in parts) if parts else None


def build_graph(allowlist: list[str]) -> dict[str, Any]:
    """Enumerate governed tags + allowed values and their in-scope assignments.

    Returns a plain, JSON-friendly structure consumed by ``taxonomy`` and
    ``dedupe`` (kept as data, not a model, so those stay pure and unit-testable
    off fixtures)::

        {"tags": [{"tag_key", "allowed_values", "assignment_count", "members"}], "as_of"}

    An empty allowlist yields ``{"tags": [], ...}`` and queries nothing (MV-D42).
    """
    from datetime import datetime, timezone

    as_of = datetime.now(timezone.utc).isoformat()
    if not allowlist:
        return {"tags": [], "as_of": as_of}

    cache_key = "graph:" + "|".join(sorted(allowlist))
    with _CACHE_LOCK:
        entry = _CACHE.get(cache_key)
        if entry and time.monotonic() - entry[0] <= _CACHE_TTL_SECONDS:
            return entry[1]

    # 1) Governed-tag catalog (account-level; the substrate). Health tracked here
    #    because a permission error on this read is what blocks the tag_graph tier.
    catalog_rows = _run(
        "SELECT * FROM system.tags.governed_tags",
        [],
        track_health=True,
    )
    tags: dict[str, dict[str, Any]] = {}
    for r in catalog_rows:
        key = _tag_key(r)
        if not key:
            continue
        tags.setdefault(key, {"tag_key": key, "allowed_values": _allowed_values(r), "members": []})

    # 2) Assignments across grains, scoped to the allowlist (MV-D37 join on
    #    tag_name = tag_key). Not health-tracked — absence here means "no
    #    assignments", not "tier blocked".
    n = len(allowlist)
    binds = [_p(f"c{i}", allowlist[i % n]) for i in range(3 * n)]
    in_clause = lambda occ: ", ".join(f":c{i}" for i in range(occ * n, (occ + 1) * n))  # noqa: E731
    assign_sql = (
        "SELECT tag_name, catalog_name, schema_name, table_name FROM ("
        "  SELECT tag_name, catalog_name, schema_name, table_name "
        "    FROM system.information_schema.table_tags "
        f"   WHERE catalog_name IN ({in_clause(0)})"
        "  UNION ALL"
        "  SELECT tag_name, catalog_name, schema_name, CAST(NULL AS STRING) AS table_name "
        "    FROM system.information_schema.schema_tags "
        f"   WHERE catalog_name IN ({in_clause(1)})"
        "  UNION ALL"
        "  SELECT tag_name, catalog_name, CAST(NULL AS STRING) AS schema_name, CAST(NULL AS STRING) AS table_name "
        "    FROM system.information_schema.catalog_tags "
        f"   WHERE catalog_name IN ({in_clause(2)})"
        ")"
    )
    assign_rows = _run(assign_sql, binds, track_health=False)
    for r in assign_rows:
        key = _tag_key(r)
        fqn = _member_fqn(r)
        if not key or not fqn:
            continue
        # A tag may be assigned without appearing in the catalog read (or the
        # catalog read may be scoped differently) — include it so orphans/collisions
        # still surface.
        entry = tags.setdefault(key, {"tag_key": key, "allowed_values": [], "members": []})
        entry["members"].append({"fqn": fqn, "asset_type": "table"})

    out_tags = []
    for key, t in sorted(tags.items()):
        # De-dupe members by fqn (a table can carry the same tag at >1 grain).
        seen: set[str] = set()
        members = []
        for m in t["members"]:
            if m["fqn"] not in seen:
                seen.add(m["fqn"])
                members.append(m)
        out_tags.append({
            "tag_key": key,
            "allowed_values": t["allowed_values"],
            "assignment_count": len(members),
            "members": members,
        })

    result = {"tags": out_tags, "as_of": as_of}
    with _CACHE_LOCK:
        _CACHE[cache_key] = (time.monotonic(), result)
    return result
