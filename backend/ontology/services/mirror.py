"""Lakebase mirror reads for the ontology snapshots (Phase 2).

Mirrors ``backend/services/gso_lakebase.py`` exactly — the one read interface,
**Delta-via-SQL-warehouse now, auto-upgrading to Lakebase synced tables later**.
Today ``gso_lakebase._SYNCED_TABLES_ENABLED`` is ``False``, so the synced-table
(Postgres) path is off and reads fall through to the source Delta tables via the
SQL warehouse (SP). When the flag flips and the three ``genie_ont_*`` snapshot
tables are provisioned as synced tables (registered in
``scripts/setup_synced_tables.py``), the same functions read Postgres and the
routers never see the difference.

Do NOT invent a new read path. These are read-only; no UC writes here.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import backend.services.gso_lakebase as _gso
from backend.services.auth import get_service_principal_client

logger = logging.getLogger(__name__)


def _gso_fqn(table: str) -> str:
    catalog = os.environ.get("GSO_CATALOG", "")
    schema = os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
    return f"{catalog}.{schema}.{table}"


def _synced_pool():
    """The Postgres synced-table pool, or None (synced disabled today — mirrors
    the exact gso_lakebase gate so both light up together on the flip)."""
    return _gso._get_pool()


def _delta_query(sql: str, params: list | None = None) -> list[dict[str, Any]]:
    """Read a snapshot Delta table via the SQL warehouse as the SP (best-effort).

    Returns [] on any failure (no warehouse, table absent, permission) so the
    reader swap degrades to the Phase-1 live path — never blocks or raises.
    """
    warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
    if not warehouse_id:
        return []
    from databricks.sdk.service.sql import StatementState

    try:
        client = get_service_principal_client()
        resp = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=sql, parameters=params or [], wait_timeout="30s",
        )
        statement_id = resp.statement_id if resp else None
        deadline = time.monotonic() + 40
        while resp and resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.monotonic() > deadline or not statement_id:
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
    except Exception as e:  # noqa: BLE001 — mirror never raises; degrade to live
        logger.info("mirror delta read failed: %s", e)
        return []


def _as_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(v) for v in parsed]
        except (ValueError, TypeError):
            pass
        s = value.strip().strip("[]")
        return [p.strip().strip('"').strip("'") for p in s.split(",") if p.strip()]
    return []


async def latest_run(metastore_id: str) -> dict[str, Any] | None:
    """Most recent run header for a metastore (any state), or None."""
    pool = _synced_pool()
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT * FROM "{_gso._GSO_PG_SCHEMA}"."genie_ont_runs_synced" '
                    "WHERE metastore_id = $1 ORDER BY started_at DESC LIMIT 1",
                    metastore_id,
                )
                return dict(row) if row else None
        except Exception:
            logger.info("mirror synced read failed for genie_ont_runs", exc_info=True)
            return None
    import asyncio
    rows = await asyncio.to_thread(
        _delta_query,
        f"SELECT * FROM {_gso_fqn('genie_ont_runs')} "
        f"WHERE metastore_id = '{metastore_id}' ORDER BY started_at DESC LIMIT 1",
    )
    return rows[0] if rows else None


async def latest_succeeded_run(metastore_id: str) -> dict[str, Any] | None:
    """Most recent *succeeded* run header (backs mirror freshness), or None."""
    pool = _synced_pool()
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT * FROM "{_gso._GSO_PG_SCHEMA}"."genie_ont_runs_synced" '
                    "WHERE metastore_id = $1 AND state = 'succeeded' ORDER BY as_of DESC LIMIT 1",
                    metastore_id,
                )
                return dict(row) if row else None
        except Exception:
            logger.info("mirror synced read failed for genie_ont_runs (succeeded)", exc_info=True)
            return None
    import asyncio
    rows = await asyncio.to_thread(
        _delta_query,
        f"SELECT * FROM {_gso_fqn('genie_ont_runs')} "
        f"WHERE metastore_id = '{metastore_id}' AND state = 'succeeded' ORDER BY as_of DESC LIMIT 1",
    )
    return rows[0] if rows else None


async def read_taxonomy_tree(metastore_id: str) -> dict[str, Any] | None:
    """The serialized taxonomy tree for a metastore (OntologyTaxonomy JSON), or None."""
    pool = _synced_pool()
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT tree FROM "{_gso._GSO_PG_SCHEMA}"."genie_ont_taxonomy_snapshot_synced" '
                    "WHERE metastore_id = $1",
                    metastore_id,
                )
            tree = row["tree"] if row else None
        except Exception:
            logger.info("mirror synced read failed for taxonomy", exc_info=True)
            return None
    else:
        import asyncio
        rows = await asyncio.to_thread(
            _delta_query,
            f"SELECT tree FROM {_gso_fqn('genie_ont_taxonomy_snapshot')} WHERE metastore_id = '{metastore_id}'",
        )
        tree = rows[0].get("tree") if rows else None
    if not tree:
        return None
    try:
        return json.loads(tree)
    except (ValueError, TypeError):
        return None


async def read_tag_graph(metastore_id: str) -> dict[str, Any] | None:
    """Reconstruct the tag-graph structure from the mirror rows, or None.

    Members are not stored per tag (they live in the taxonomy tree), so the
    reconstructed graph carries empty ``members`` — the tags-lens transforms
    (governed_tag_rows / collisions / cleanup) do not need them, so the route
    output is identical whether the graph came from the mirror or the live path.
    """
    pool = _synced_pool()
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                rows = [
                    dict(r) for r in await conn.fetch(
                        f'SELECT tag_key, allowed_values, assignment_count, dedupe_verdicts, as_of '
                        f'FROM "{_gso._GSO_PG_SCHEMA}"."genie_ont_tag_graph_synced" WHERE metastore_id = $1',
                        metastore_id,
                    )
                ]
        except Exception:
            logger.info("mirror synced read failed for tag_graph", exc_info=True)
            return None
    else:
        import asyncio
        rows = await asyncio.to_thread(
            _delta_query,
            f"SELECT tag_key, allowed_values, assignment_count, dedupe_verdicts, as_of "
            f"FROM {_gso_fqn('genie_ont_tag_graph')} WHERE metastore_id = '{metastore_id}'",
        )
    if not rows:
        return None
    as_of = max((str(r.get("as_of") or "") for r in rows), default="")
    tags = []
    for r in rows:
        if not r.get("tag_key"):
            continue
        tag: dict[str, Any] = {
            "tag_key": r.get("tag_key"),
            "allowed_values": _as_list(r.get("allowed_values")),
            "assignment_count": int(r.get("assignment_count") or 0),
            "members": [],
        }
        # Phase-3a: the embedding-backed per-tag dedupe verdicts (JSON), when present,
        # so the tags route can surface enriched collisions through the frozen contract.
        verdicts = r.get("dedupe_verdicts")
        if verdicts:
            try:
                tag["dedupe_verdicts"] = json.loads(verdicts) if isinstance(verdicts, str) else verdicts
            except (ValueError, TypeError):
                pass
        tags.append(tag)
    tags.sort(key=lambda t: t["tag_key"])
    return {"tags": tags, "as_of": as_of}
