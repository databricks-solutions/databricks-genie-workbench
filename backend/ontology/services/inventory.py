"""Inventory (OBO, MV-D43 fast-path).

Cheap ``system.information_schema`` counts that render in-request on first load.
Run as the *user* (`get_workspace_client`) so results are auto-filtered to what
they may see — no explicit grant required. This is the degrade-not-hang first
render: even a locked-down workspace gets the inventory tier.

Scope is the catalog allowlist (MV-D42). An **empty allowlist returns zero
counts and queries nothing** — the page then prompts the admin to choose
catalogs, never scanning the whole account.

Note: verify the metric-view ``table_type = 'METRIC_VIEW'`` filter against a live
workspace before relying on it (spec §8). Every count is best-effort and fails
open to 0 so a missing table or dialect quirk never breaks the fast-path.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

logger = logging.getLogger(__name__)


def _warehouse_id() -> str:
    wh = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
    if not wh:
        raise RuntimeError("SQL_WAREHOUSE_ID env var is empty — inventory queries cannot run.")
    return wh


def _run(client: WorkspaceClient, sql: str, parameters: list[StatementParameterListItem]) -> list[dict[str, Any]]:
    """Execute a short OBO statement and return rows as dicts (best-effort)."""
    resp = client.statement_execution.execute_statement(
        warehouse_id=_warehouse_id(),
        statement=sql,
        parameters=parameters,
        wait_timeout="30s",
    )
    statement_id = resp.statement_id if resp else None
    deadline = time.monotonic() + 40
    while resp and resp.status and resp.status.state in (
        StatementState.PENDING, StatementState.RUNNING,
    ):
        if time.monotonic() > deadline or not statement_id:
            logger.warning("ontology inventory query %s still running; giving up.", statement_id)
            return []
        time.sleep(1.0)
        resp = client.statement_execution.get_statement(statement_id=statement_id)

    if resp is None or resp.status is None or resp.status.state != StatementState.SUCCEEDED:
        return []
    if not resp.result or not resp.result.data_array:
        return []
    schema = resp.manifest.schema if resp.manifest else None
    if schema is None or schema.columns is None:
        return []
    cols = [c.name for c in schema.columns]
    return [{cols[i]: row[i] for i in range(len(cols))} for row in resp.result.data_array]


def _catalog_params(allowlist: list[str]) -> tuple[str, list[StatementParameterListItem]]:
    """Build a ``(:c0, :c1, ...)`` placeholder list + bound params for an IN clause."""
    placeholders = ", ".join(f":c{i}" for i in range(len(allowlist)))
    params = [
        StatementParameterListItem(name=f"c{i}", value=str(cat), type="STRING")
        for i, cat in enumerate(allowlist)
    ]
    return placeholders, params


def metric_view_count(client: WorkspaceClient, allowlist: list[str]) -> int:
    if not allowlist:
        return 0
    placeholders, params = _catalog_params(allowlist)
    sql = (
        "SELECT count(*) AS n FROM system.information_schema.tables "
        f"WHERE table_catalog IN ({placeholders}) AND table_type = 'METRIC_VIEW'"
    )
    try:
        rows = _run(client, sql, params)
        return int(rows[0].get("n") or 0) if rows else 0
    except Exception as e:  # noqa: BLE001 — fast-path never raises
        logger.info("metric_view_count failed: %s", e)
        return 0


def metric_view_fqns(client: WorkspaceClient, allowlist: list[str], limit: int = 2000) -> list[str]:
    """List metric-view FQNs in scope (OBO, auto-filtered). Empty allowlist → []."""
    if not allowlist:
        return []
    placeholders, params = _catalog_params(allowlist)
    sql = (
        "SELECT table_catalog, table_schema, table_name "
        "FROM system.information_schema.tables "
        f"WHERE table_catalog IN ({placeholders}) AND table_type = 'METRIC_VIEW' "
        f"ORDER BY table_catalog, table_schema, table_name LIMIT {int(limit)}"
    )
    try:
        rows = _run(client, sql, params)
        out: list[str] = []
        for r in rows:
            parts = [r.get("table_catalog"), r.get("table_schema"), r.get("table_name")]
            fqn = ".".join(str(p) for p in parts if p)
            if fqn:
                out.append(fqn)
        return out
    except Exception as e:  # noqa: BLE001 — fast-path never raises
        logger.info("metric_view_fqns failed: %s", e)
        return []


def governed_tag_count(client: WorkspaceClient, allowlist: list[str]) -> int:
    """Count distinct tag keys assigned within the allowlist (OBO, auto-filtered).

    A cheap coverage proxy over the OBO-readable assignment tables — the
    authoritative governed-tag catalog is the SP `tag_graph` read (`/tags`).
    """
    if not allowlist:
        return 0
    # The catalog IN clause appears once per assignment grain; give each
    # occurrence its own uniquely-named binds so the same allowlist is reused
    # across all three UNION branches.
    n = len(allowlist)
    binds: list[StatementParameterListItem] = [
        StatementParameterListItem(name=f"c{i}", value=str(allowlist[i % n]), type="STRING")
        for i in range(3 * n)
    ]
    in_clause = lambda occ: ", ".join(f":c{i}" for i in range(occ * n, (occ + 1) * n))  # noqa: E731
    sql = (
        "SELECT count(*) AS n FROM ("
        "  SELECT DISTINCT tag_name FROM system.information_schema.catalog_tags "
        f"   WHERE catalog_name IN ({in_clause(0)})"
        "  UNION"
        "  SELECT DISTINCT tag_name FROM system.information_schema.schema_tags "
        f"   WHERE catalog_name IN ({in_clause(1)})"
        "  UNION"
        "  SELECT DISTINCT tag_name FROM system.information_schema.table_tags "
        f"   WHERE catalog_name IN ({in_clause(2)})"
        ")"
    )
    try:
        rows = _run(client, sql, binds)
        return int(rows[0].get("n") or 0) if rows else 0
    except Exception as e:  # noqa: BLE001 — fast-path never raises
        logger.info("governed_tag_count failed: %s", e)
        return 0
