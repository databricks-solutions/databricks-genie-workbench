"""IQ scoring service — thin wrapper around the GSO iq_scan package.

Historical note: the pure scoring logic (``calculate_score``,
``get_maturity_label``, ``_check``, ``_SQL_IN_TEXT_RE``, ``CONFIG_CHECK_COUNT``)
lived in this module until it was extracted to
``genie_space_optimizer.iq_scan.scoring`` so the GSO optimizer preflight can
share a single source of truth. This module now owns only the backend-specific
IO: UC metadata enrichment, Lakebase persistence, and the async
``scan_space`` orchestration.
"""

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from databricks.sdk.errors import NotFound

from backend.services.genie_client import get_genie_space, get_serialized_space
from backend.services.lakebase import save_scan_result, get_latest_score, get_latest_optimization_run

# Re-exported from the GSO iq_scan package so existing import paths keep working.
from genie_space_optimizer.iq_scan.scoring import (  # noqa: F401
    CONFIG_CHECK_COUNT,
    _SQL_IN_TEXT_RE,
    _check,
    calculate_score,
    get_maturity_label,
)

logger = logging.getLogger(__name__)


# Terminal GSO run statuses that indicate a completed optimization.
# Subset of auto_optimize._TERMINAL_RUN_STATUSES — only includes statuses
# where best_accuracy is meaningful for IQ scoring.
_GSO_TERMINAL = {"CONVERGED", "STALLED", "MAX_ITERATIONS"}

# Shared thread pool for UC metadata fetches — avoids per-scan pool churn.
_uc_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="uc-enrich")


def _parse_identifier(identifier: str) -> tuple[str, str, str]:
    """Parse a 3-part table identifier into (catalog, schema, table)."""
    parts = identifier.replace("`", "").split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return "", parts[0], parts[1]
    return "", "", parts[0] if parts else ""


def _enrich_with_uc_descriptions(space_data: dict, ws) -> int:
    """Fetch UC table/column descriptions and merge into *space_data* in-place.

    Only fills blanks — never overwrites existing ``description`` or ``comment``
    values in the Genie Space config.  Returns the number of enriched items.
    """
    ds = space_data.get("data_sources", {})
    all_sources = list(ds.get("tables", [])) + list(ds.get("metric_views", []))
    if not all_sources:
        return 0

    # Build (cat, sch, tbl) refs + index back to space_data items
    refs: list[tuple[str, str, str]] = []
    source_by_fqn: dict[str, dict] = {}
    for src in all_sources:
        ident = src.get("identifier", "")
        if not ident:
            continue
        cat, sch, tbl = _parse_identifier(ident)
        if cat and sch and tbl:
            fqn = f"{cat}.{sch}.{tbl}"
            refs.append((cat, sch, tbl))
            source_by_fqn[fqn] = src

    if not refs:
        return 0

    # Fetch UC metadata in parallel (sync SDK calls)
    table_infos: dict[str, object] = {}

    def _fetch_one(ref: tuple[str, str, str]):
        cat, sch, tbl = ref
        fqn = f"{cat}.{sch}.{tbl}"
        try:
            return fqn, ws.tables.get(full_name=fqn)
        except NotFound:
            logger.debug("UC table not found: %s", fqn)
            return fqn, None
        except Exception as exc:
            logger.warning("UC metadata fetch failed for %s: %s", fqn, exc)
            return fqn, None

    for fqn, info in _uc_pool.map(_fetch_one, refs):
        if info is not None:
            table_infos[fqn] = info

    if not table_infos:
        return 0

    enriched = 0

    for fqn, info in table_infos.items():
        src = source_by_fqn.get(fqn)
        if not src:
            continue

        # Enrich table-level description
        if not (src.get("description") or src.get("comment")):
            tbl_comment = getattr(info, "comment", None) or ""
            if tbl_comment:
                src["comment"] = tbl_comment
                enriched += 1

        # Enrich column-level descriptions
        uc_cols = {
            getattr(c, "name", "").lower(): getattr(c, "comment", None) or ""
            for c in (getattr(info, "columns", None) or [])
        }
        if not uc_cols:
            continue
        for col in src.get("column_configs", []) + src.get("columns", []):
            if col.get("description") or col.get("comment"):
                continue
            col_name = (col.get("column_name") or col.get("name", "")).lower()
            uc_comment = uc_cols.get(col_name, "")
            if uc_comment:
                col["comment"] = uc_comment
                enriched += 1

    return enriched


async def scan_space(space_id: str, user_token: Optional[str] = None) -> dict:
    """Fetch space config, calculate IQ score, and persist to Lakebase.

    Args:
        space_id: The Genie Space ID
        user_token: Optional user token for OBO auth (not used directly, SDK handles this)

    Returns:
        ScanResult dict with score, maturity, breakdown, checks, findings, next_steps
    """
    logger.info(f"Scanning space: {space_id}")

    try:
        space_data = get_serialized_space(space_id)
    except Exception as e:
        logger.error(f"Failed to fetch space {space_id}: {e}")
        raise ValueError(f"Cannot scan space {space_id}: {e}")

    # Enrich with UC descriptions so checks 2-3 reflect upstream metadata (#62)
    try:
        from backend.services.auth import get_workspace_client, run_in_context
        ws = get_workspace_client()
        loop = asyncio.get_event_loop()
        n = await loop.run_in_executor(None, run_in_context(_enrich_with_uc_descriptions, space_data, ws))
        if n:
            logger.info("Enriched %d descriptions from Unity Catalog for %s", n, space_id)
    except Exception as e:
        logger.warning("UC description enrichment skipped for %s: %s", space_id, e)

    # Fetch optimization runs from both sources concurrently
    async def _fetch_opt_run():
        try:
            return await get_latest_optimization_run(space_id)
        except Exception as e:
            logger.warning(f"Failed to fetch optimization run for {space_id}: {e}")
            return None

    async def _fetch_gso_runs():
        try:
            from backend.services import gso_lakebase
            runs = await gso_lakebase.load_gso_runs_for_space(space_id)
            # Delta table fallback when Lakebase synced tables are empty
            if not runs:
                catalog = os.environ.get("GSO_CATALOG", "")
                schema = os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
                wh_id = os.environ.get("GSO_WAREHOUSE_ID") or os.environ.get("SQL_WAREHOUSE_ID", "")
                if catalog and wh_id:
                    try:
                        from genie_space_optimizer.common.warehouse import sql_warehouse_query
                        from backend.services.auth import get_service_principal_client
                        ws = get_service_principal_client()
                        df = sql_warehouse_query(
                            ws, wh_id,
                            f"SELECT run_id, space_id, status, best_accuracy, completed_at, started_at "
                            f"FROM {catalog}.{schema}.genie_opt_runs "
                            f"WHERE space_id = '{space_id}' ORDER BY started_at DESC"
                        )
                        if not df.empty:
                            runs = df.to_dict(orient="records")
                    except Exception as e:
                        logger.warning(f"GSO Delta fallback failed for {space_id}: {e}")
            return runs or []
        except Exception as e:
            logger.warning(f"Failed to check GSO runs for {space_id}: {e}")
            return []

    optimization_run, gso_runs = await asyncio.gather(
        _fetch_opt_run(), _fetch_gso_runs()
    )

    # Use the best accuracy from either source
    for gso_run in gso_runs:  # already sorted most recent first
        status = str(gso_run.get("status", "")).upper()
        best_acc = gso_run.get("best_accuracy")
        if status in _GSO_TERMINAL and best_acc is not None:
            acc = float(best_acc)
            # GSO stores accuracy as percentage (0-100); normalize to 0.0-1.0
            if acc > 1.0:
                acc = acc / 100.0
            gso_as_opt = {
                "accuracy": acc,
                "created_at": gso_run.get("completed_at") or gso_run.get("started_at"),
            }
            if optimization_run is None or gso_as_opt["accuracy"] > optimization_run.get("accuracy", 0):
                optimization_run = gso_as_opt
            break  # only consider most recent terminal GSO run

    scan_result = calculate_score(space_data, optimization_run=optimization_run)
    scan_result["space_id"] = space_id

    # Persist to Lakebase
    try:
        await save_scan_result(space_id, scan_result)
        logger.info(f"Scan result saved for {space_id}: score={scan_result['score']}")
    except Exception as e:
        logger.warning(f"Failed to persist scan result for {space_id}: {e}")

    return scan_result
