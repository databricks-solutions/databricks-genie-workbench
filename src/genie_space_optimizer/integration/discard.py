"""Warehouse-backed discard operation for integration callers.

Uses the Statement Execution API for Delta state reads/writes and
the Genie REST API for config rollback -- no ``databricks-connect`` required.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.warehouse import (
    sql_warehouse_execute,
    wh_load_run,
)

from .config import IntegrationConfig
from .types import ActionResult

logger = logging.getLogger(__name__)


def discard_optimization(
    run_id: str,
    ws: WorkspaceClient,
    sp_ws: WorkspaceClient,
    config: IntegrationConfig,
) -> ActionResult:
    """Discard an optimization run and rollback config.  Needs OBO + SP.

    Uses SQL Warehouse for Delta state reads/writes.
    Rollback uses the Genie REST API (no Spark needed).

    Args:
        run_id: The optimization run ID to discard.
        ws: OBO-authenticated ``WorkspaceClient`` for the requesting user.
        sp_ws: Service-principal ``WorkspaceClient`` (fallback for Genie API).
        config: Integration configuration.

    Returns:
        :class:`ActionResult` with status ``"discarded"``.

    Raises:
        ValueError: If the run is not found or already in a terminal state.
    """
    run_data = wh_load_run(ws, config.warehouse_id, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise ValueError(f"Run not found: {run_id}")

    status = str(run_data.get("status") or "")
    if status in ("DISCARDED", "APPLIED"):
        raise ValueError(f"Run already {status.lower()}.")

    space_id = run_data.get("space_id", "")
    original_snapshot = run_data.get("config_snapshot")
    if isinstance(original_snapshot, str):
        try:
            original_snapshot = json.loads(original_snapshot)
        except (json.JSONDecodeError, TypeError):
            original_snapshot = {}

    if original_snapshot and isinstance(original_snapshot, dict):
        from genie_space_optimizer.optimization.applier import rollback

        apply_log = {"pre_snapshot": original_snapshot}
        client = _pick_genie_client(ws, sp_ws)
        rollback(apply_log, client, space_id)

    sql_warehouse_execute(
        ws,
        config.warehouse_id,
        f"UPDATE {config.catalog}.{config.schema_name}.genie_opt_runs "
        f"SET status = 'DISCARDED', "
        f"convergence_reason = 'user_discarded', "
        f"updated_at = current_timestamp() "
        f"WHERE run_id = '{run_id}'",
    )
    return ActionResult(
        status="discarded",
        run_id=run_id,
        message="Optimization discarded and patches rolled back.",
    )


def _pick_genie_client(
    ws: WorkspaceClient, sp_ws: WorkspaceClient,
) -> WorkspaceClient:
    """Pick the best client for Genie API calls (OBO preferred, SP fallback)."""
    from databricks.sdk.errors.platform import PermissionDenied

    try:
        ws.genie.list_spaces(page_size=1)
        return ws
    except PermissionDenied:
        logger.info("OBO token missing genie scope — falling back to SP client")
        return sp_ws
    except Exception:
        logger.warning(
            "Unexpected error probing OBO genie access — falling back to SP",
            exc_info=True,
        )
        return sp_ws
