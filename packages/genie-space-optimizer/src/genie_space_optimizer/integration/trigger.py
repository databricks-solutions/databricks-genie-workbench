"""Warehouse-first optimization trigger for integration callers.

This module does NOT require ``databricks-connect``.  All Delta state
operations use the Statement Execution API via the configured SQL Warehouse.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.warehouse import (
    sql_warehouse_execute,
    sql_warehouse_query,
    wh_create_run,
    wh_reconcile_active_runs,
)

from .config import IntegrationConfig
from .types import TriggerResult

logger = logging.getLogger(__name__)

_ACTIVE_RUN_STATUSES = frozenset({"QUEUED", "IN_PROGRESS"})


def trigger_optimization(
    space_id: str,
    ws: WorkspaceClient,
    sp_ws: WorkspaceClient,
    config: IntegrationConfig,
    *,
    user_email: str | None = None,
    user_name: str | None = None,
    apply_mode: str = "genie_config",
    levers: list[int] | None = None,
    deploy_target: str | None = None,
) -> TriggerResult:
    """Trigger a GSO optimization run using SQL Warehouse for state management.

    This function does NOT require Spark Connect.  All Delta state operations
    use the Statement Execution API via ``config.warehouse_id``.

    Args:
        space_id: The Genie Space ID to optimize.
        ws: OBO-authenticated ``WorkspaceClient`` for the requesting user.
        sp_ws: Service-principal ``WorkspaceClient`` for job submission.
        config: Integration configuration (catalog, schema, warehouse, etc.).
        user_email: Email of the requesting user (for audit trail).
        user_name: Display name fallback when email is unavailable.
        apply_mode: One of ``"genie_config"``, ``"uc_artifact"``, ``"both"``.
        levers: Subset of levers to run (default ``[1,2,3,4,5]``).
        deploy_target: Optional cross-environment deployment target URL.

    Returns:
        :class:`TriggerResult` with run_id, job_run_id, job_url, and status.
    """
    from genie_space_optimizer.backend.routes.spaces import SUPPORTED_APPLY_MODES
    from genie_space_optimizer.common.config import DEFAULT_LEVER_ORDER
    from genie_space_optimizer.common.genie_client import (
        fetch_space_config,
        sp_can_manage_space,
        user_can_edit_space,
    )

    requested_apply_mode = (apply_mode or "genie_config").strip().lower()
    if requested_apply_mode not in SUPPORTED_APPLY_MODES:
        raise ValueError(
            f"Unsupported apply_mode '{apply_mode}'. "
            f"Use one of: {sorted(SUPPORTED_APPLY_MODES)}"
        )

    caller_email = (user_email or user_name or "").lower()
    if not caller_email:
        raise ValueError(
            "Cannot determine the requesting user's identity. "
            "Provide user_email or user_name."
        )

    if not user_can_edit_space(ws, space_id, user_email=caller_email, acl_client=sp_ws):
        raise PermissionError(
            "You need CAN_EDIT or CAN_MANAGE permission on this "
            "Genie Space to start optimization."
        )

    runs_df = sql_warehouse_query(
        ws,
        config.warehouse_id,
        f"SELECT * FROM {config.catalog}.{config.schema_name}.genie_opt_runs "
        f"WHERE space_id = '{space_id}' ORDER BY started_at DESC",
    )

    if not runs_df.empty:
        if wh_reconcile_active_runs(
            ws, sp_ws, config.warehouse_id, runs_df,
            config.catalog, config.schema_name,
        ):
            runs_df = sql_warehouse_query(
                ws,
                config.warehouse_id,
                f"SELECT * FROM {config.catalog}.{config.schema_name}.genie_opt_runs "
                f"WHERE space_id = '{space_id}' ORDER BY started_at DESC",
            )

    if not runs_df.empty:
        active = runs_df[runs_df["status"].isin(list(_ACTIVE_RUN_STATUSES))]
        if not active.empty:
            active_id = active.iloc[0]["run_id"]
            raise RuntimeError(
                f"An optimization is already in progress for this space (run {active_id})"
            )

    prev_experiment: str | None = None
    if not runs_df.empty:
        terminal = runs_df[~runs_df["status"].isin(list(_ACTIVE_RUN_STATUSES))]
        if not terminal.empty:
            exp_val = terminal.iloc[0].get("experiment_name")
            if exp_val and str(exp_val) not in ("", "None", "nan"):
                prev_experiment = str(exp_val)

    if prev_experiment and not prev_experiment.startswith("/Shared/"):
        logger.warning("Ignoring legacy experiment path %s", prev_experiment)
        prev_experiment = None

    run_id = str(uuid.uuid4())

    space_snapshot: dict = {}
    snap_errors: list[str] = []
    for label, cli in [("OBO/user", ws), ("SP", sp_ws)]:
        try:
            space_snapshot = fetch_space_config(cli, space_id)
            logger.info("Captured space snapshot via %s client for %s", label, space_id)
            break
        except Exception as exc:
            snap_errors.append(f"{label}: {exc}")
            logger.info("Snapshot via %s failed for %s: %s", label, space_id, exc)

    if not space_snapshot:
        combined = "; ".join(snap_errors)
        raise RuntimeError(
            f"Cannot export Genie Space config for {space_id}. "
            f"Errors: {combined}"
        )

    title = str(space_snapshot.get("title", "") or "")
    domain = (
        re.sub(r"[^a-z0-9_]+", "_", title.lower().replace(" ", "_").replace("-", "_")).strip("_")
        if title
        else "default"
    )

    from genie_space_optimizer.backend.routes.spaces import fetch_uc_metadata_obo
    from genie_space_optimizer.common.uc_metadata import extract_genie_space_table_refs

    genie_refs = extract_genie_space_table_refs(space_snapshot) if space_snapshot else []
    try:
        obo_uc_metadata = fetch_uc_metadata_obo(
            ws,
            warehouse_id=config.warehouse_id,
            catalog=config.catalog,
            schema_name=config.schema_name,
            genie_table_refs=genie_refs or None,
        )
        if space_snapshot and obo_uc_metadata:
            space_snapshot["_prefetched_uc_metadata"] = obo_uc_metadata
    except Exception:
        logger.warning("OBO UC metadata prefetch failed for run %s", run_id, exc_info=True)

    from genie_space_optimizer.backend.routes.settings import get_sp_principal_aliases

    sp_aliases = get_sp_principal_aliases(sp_ws)
    if not sp_can_manage_space(sp_ws, space_id, sp_aliases):
        raise PermissionError(
            f"The service principal does not have CAN_MANAGE on Genie Space {space_id}."
        )

    from genie_space_optimizer.common.config import EXPERIMENT_PATH_TEMPLATE, format_mlflow_template

    experiment_name = prev_experiment or format_mlflow_template(
        EXPERIMENT_PATH_TEMPLATE, space_id=space_id, domain=domain,
    )

    levers_resolved = levers if levers else list(DEFAULT_LEVER_ORDER)
    levers_str = json.dumps(levers_resolved)

    wh_create_run(
        ws,
        config.warehouse_id,
        run_id=run_id,
        space_id=space_id,
        domain=domain,
        catalog=config.catalog,
        schema=config.schema_name,
        apply_mode=requested_apply_mode,
        levers=levers_resolved,
        triggered_by=caller_email,
        experiment_name=experiment_name,
        config_snapshot=space_snapshot if space_snapshot else None,
    )

    from genie_space_optimizer.backend.job_launcher import submit_optimization

    try:
        job_run_id, job_id = submit_optimization(
            sp_ws,
            job_id=config.job_id,
            run_id=run_id,
            space_id=space_id,
            domain=domain,
            catalog=config.catalog,
            schema=config.schema_name,
            apply_mode=requested_apply_mode,
            levers=levers_str,
            triggered_by=caller_email,
            experiment_name=experiment_name or "",
            deploy_target=deploy_target or "",
            warehouse_id=config.warehouse_id or "",
        )

        sql_warehouse_execute(
            ws,
            config.warehouse_id,
            f"UPDATE {config.catalog}.{config.schema_name}.genie_opt_runs "
            f"SET status = 'IN_PROGRESS', job_run_id = '{job_run_id}', "
            f"job_id = '{job_id}', "
            f"updated_at = current_timestamp() "
            f"WHERE run_id = '{run_id}'",
        )

        host = (sp_ws.config.host or "").rstrip("/")
        workspace_id: int | None = None
        if host:
            try:
                workspace_id = sp_ws.get_workspace_id()
            except Exception:
                workspace_id = None
        if host and workspace_id is not None:
            job_url = f"{host}/jobs/{job_id}/runs/{job_run_id}?o={workspace_id}"
        elif host:
            job_url = f"{host}/jobs/{job_id}/runs/{job_run_id}"
        else:
            job_url = None

        return TriggerResult(
            run_id=run_id,
            job_run_id=str(job_run_id),
            job_url=job_url,
            status="IN_PROGRESS",
        )

    except Exception as exc:
        logger.exception("Job submission failed for run %s", run_id)
        try:
            sql_warehouse_execute(
                ws,
                config.warehouse_id,
                f"UPDATE {config.catalog}.{config.schema_name}.genie_opt_runs "
                f"SET status = 'FAILED', "
                f"convergence_reason = 'job_submission_error: {str(exc)[:500]}', "
                f"updated_at = current_timestamp() "
                f"WHERE run_id = '{run_id}'",
            )
        except Exception:
            logger.warning("Failed to update run status to FAILED for %s", run_id)
        raise RuntimeError(f"Job submission failed: {exc}") from exc
