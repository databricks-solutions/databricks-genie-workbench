"""Warehouse-first optimization trigger for integration callers.

This module does NOT require ``databricks-connect``.  All Delta state
operations use the Statement Execution API via the configured SQL Warehouse.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.warehouse import (
    sql_warehouse_execute,
    sql_warehouse_query,
    wh_create_run,
    wh_ensure_optimization_tables,
    wh_reconcile_active_runs,
    wh_write_join_advice,
    wh_write_operator_guidance,
)

from .config import IntegrationConfig
from .types import TriggerResult

logger = logging.getLogger(__name__)

_ACTIVE_RUN_STATUSES = frozenset({"QUEUED", "IN_PROGRESS"})

_SUPPORTED_APPLY_MODES = {"genie_config", "uc_artifact", "both"}
_SUPPORTED_BENCHMARK_POLICIES = {"review_only", "repair_allowed"}


def _capture_config_snapshot(
    *,
    space_id: str,
    ws: WorkspaceClient,
    sp_ws: WorkspaceClient,
) -> dict:
    """Capture a non-empty Genie serialized_space snapshot, SP first."""
    from genie_space_optimizer.common.genie_client import (
        fetch_space_config,
        space_config_data_source_counts,
        space_config_has_data_sources,
    )

    space_snapshot: dict = {}
    snap_errors: list[str] = []
    for label, cli in [("SP", sp_ws), ("OBO/user", ws)]:
        try:
            candidate = fetch_space_config(cli, space_id)
        except Exception as exc:
            snap_errors.append(f"{label}: {exc}")
            logger.info("Snapshot via %s failed for %s: %s", label, space_id, exc)
            continue

        counts = space_config_data_source_counts(candidate)
        if not space_config_has_data_sources(candidate):
            msg = (
                f"{label}: exported serialized_space has no data sources "
                f"(tables={counts['tables']}, metric_views={counts['metric_views']}, "
                f"functions={counts['functions']})"
            )
            snap_errors.append(msg)
            logger.error("Rejecting empty Genie Agent snapshot for %s: %s", space_id, msg)
            continue

        space_snapshot = candidate
        logger.info(
            "Captured space snapshot via %s client for %s "
            "(tables=%d, metric_views=%d, functions=%d)",
            label,
            space_id,
            counts["tables"],
            counts["metric_views"],
            counts["functions"],
        )
        break

    if not space_snapshot:
        combined = "; ".join(snap_errors)
        raise RuntimeError(
            f"Cannot export non-empty Genie Agent config for {space_id}. "
            f"Errors: {combined}"
        )
    return space_snapshot


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
    target_accuracy: float | None = None,
    max_attempts: int | None = None,
    workload_warehouse_ids: list[str] | None = None,
    benchmark_policy: str = "repair_allowed",
    enable_metric_view_suggestions: bool = False,
    mv_action_mode: str = "suggest_only",
    mv_min_confidence: int | None = None,
    mv_attach_hook: Callable[[str], Any] | None = None,
    proposed_join_seeds: list[dict] | None = None,
    operator_guidance: str | None = None,
) -> TriggerResult:
    """Trigger a GSO optimization run using SQL Warehouse for state management.

    This function does NOT require Spark Connect.  All Delta state operations
    use the Statement Execution API via ``config.warehouse_id``.

    Args:
        space_id: The Genie Agent ID to optimize.
        ws: OBO-authenticated ``WorkspaceClient`` for the requesting user.
        sp_ws: Service-principal ``WorkspaceClient`` for job submission.
        config: Integration configuration (catalog, schema, warehouse, etc.).
        user_email: Email of the requesting user (for audit trail).
        user_name: Display name fallback when email is unavailable.
        apply_mode: One of ``"genie_config"``, ``"uc_artifact"``, ``"both"``.
        levers: Subset of levers to run (default ``[1,2,3,4,5]``).
        target_accuracy: Stop-early target accuracy on the 0–1 scale (default
            ``0.90`` when None — matches the job's databricks.yml param).
        max_attempts: SURGICAL hill-climb budget (default ``3`` when None); the
            attempt-1 coverage pass is a free probe that never consumes a slot.
        workload_warehouse_ids: Representative workload warehouses whose query
            history may be used as optional ranking evidence.
        benchmark_policy: ``review_only`` reviews and filters the existing live
            benchmark set without mutation; ``repair_allowed`` permits bounded
            generation, repair, and live benchmark updates.
        enable_metric_view_suggestions: gate the job's advisor phase (MV-D5).
        mv_action_mode: ``suggest_only`` or ``create_and_attach`` requested by the
            caller; the effective mode never upgrades and downgrades to
            ``suggest_only`` if the create hook attaches nothing (MV-D1).
        mv_min_confidence: advisor confidence floor (0–100) for the job phase.
        mv_attach_hook: backend-provided callback invoked with ``run_id`` after the
            run row exists and before submit. It performs the OBO metric view
            creates and returns an object exposing ``attach_views`` (list of
            identifiers), ``consent_id`` (probe_id, MV-D16), and ``action_mode``.
            The engine never imports the backend; the hook is the seam (MV-D20).
        proposed_join_seeds: operator-proposed candidate joins from the Semantic
            Blueprint's Join Advisor (§7), as JoinCandidate dicts. Persisted as a
            run-scoped ``operator_proposed_joins`` artifact the optimize loop reads
            and hands the LLM as candidate joins to VALIDATE and add itself — never
            a declared join_spec written here. Best-effort; a write failure does
            not fail the run.
        operator_guidance: free-text guidance the operator typed in the run-config
            panel for THIS run (§7). Persisted as a run-scoped ``operator_guidance``
            artifact the optimize loop injects into the LLM prompt as advice (not
            ground truth). Best-effort; a write failure does not fail the run.

    Returns:
        :class:`TriggerResult` with run_id, job_run_id, job_url, and status.
    """
    from genie_space_optimizer.common.config import DEFAULT_LEVER_ORDER
    from genie_space_optimizer.common.genie_client import (
        sp_can_manage_space,
        user_can_edit_space,
    )

    requested_apply_mode = (apply_mode or "genie_config").strip().lower()
    if requested_apply_mode not in _SUPPORTED_APPLY_MODES:
        raise ValueError(
            f"Unsupported apply_mode '{apply_mode}'. "
            f"Use one of: {sorted(_SUPPORTED_APPLY_MODES)}"
        )

    requested_benchmark_policy = (benchmark_policy or "repair_allowed").strip().lower()
    if requested_benchmark_policy not in _SUPPORTED_BENCHMARK_POLICIES:
        raise ValueError(
            f"Unsupported benchmark_policy '{benchmark_policy}'. "
            f"Use one of: {sorted(_SUPPORTED_BENCHMARK_POLICIES)}"
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
            "Genie Agent to start optimization."
        )

    wh_ensure_optimization_tables(
        sp_ws,
        config.warehouse_id,
        config.catalog,
        config.schema_name,
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

    # GSO v2 Phase 5 (D3): the ``experiment_name`` pointer column was scrubbed.
    # The job self-resolves a deterministic MLflow experiment path from
    # (space_id, domain) in ``preflight_persist_benchmark_corpus``, so re-runs land in
    # the same experiment without carrying a prior pointer forward.

    run_id = str(uuid.uuid4())

    space_snapshot = _capture_config_snapshot(space_id=space_id, ws=ws, sp_ws=sp_ws)

    title = str(space_snapshot.get("title", "") or "")
    domain = (
        re.sub(r"[^a-z0-9_]+", "_", title.lower().replace(" ", "_").replace("-", "_")).strip("_")
        if title
        else "default"
    )

    from genie_space_optimizer.common.uc_metadata import extract_genie_space_table_refs

    genie_refs = extract_genie_space_table_refs(space_snapshot) if space_snapshot else []
    try:
        from genie_space_optimizer.integration.uc_metadata import fetch_uc_metadata_obo

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

    from genie_space_optimizer.common.sp_permissions import get_sp_principal_aliases

    sp_aliases = get_sp_principal_aliases(sp_ws)
    if not sp_can_manage_space(sp_ws, space_id, sp_aliases):
        raise PermissionError(
            f"The service principal does not have CAN_MANAGE on Genie Agent {space_id}."
        )

    levers_resolved = levers if levers else list(DEFAULT_LEVER_ORDER)
    levers_str = json.dumps(levers_resolved)

    # Resolve the loop knobs to the job's databricks.yml defaults when the caller
    # omits them, and stringify for the Jobs run_now job_parameters (all job
    # params are strings). target_accuracy stays on the 0-1 scale the loop
    # expects (run_optimize normalizes <=1 to the 0-100 internal scale).
    target_accuracy_str = (
        f"{float(target_accuracy):g}" if target_accuracy is not None else "0.90"
    )
    max_attempts_str = str(int(max_attempts)) if max_attempts is not None else "3"
    workload_warehouse_ids_str = json.dumps(
        list(dict.fromkeys(
            str(value).strip()
            for value in (workload_warehouse_ids or [])
            if str(value).strip()
        ))[:20]
    )

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
        config_snapshot=space_snapshot if space_snapshot else None,
        llm_model=config.llm_model or None,
        benchmark_policy=requested_benchmark_policy,
    )

    # Join Advisor advice (§7): persist the operator-proposed candidate joins as a
    # run-scoped artifact for the optimize loop to validate and add itself. This is
    # advice, not a config edit — best-effort, so a write failure never fails the
    # run (the loop just sees no operator advice).
    if proposed_join_seeds:
        try:
            wh_write_join_advice(
                ws,
                config.warehouse_id,
                run_id=run_id,
                catalog=config.catalog,
                schema=config.schema_name,
                seeds=list(proposed_join_seeds),
            )
        except Exception:
            logger.warning(
                "Could not persist operator-proposed join seeds for run %s",
                run_id,
                exc_info=True,
            )

    # Operator free-text guidance (§7): persist the per-run advice as a run-scoped
    # ``operator_guidance`` artifact the optimize loop injects into the LLM prompt.
    # Advice, not a config edit — best-effort, so a write failure never fails the
    # run (the loop just sees no operator guidance).
    if operator_guidance and operator_guidance.strip():
        try:
            wh_write_operator_guidance(
                ws,
                config.warehouse_id,
                run_id=run_id,
                catalog=config.catalog,
                schema=config.schema_name,
                text=operator_guidance,
            )
        except Exception:
            logger.warning(
                "Could not persist operator guidance for run %s",
                run_id,
                exc_info=True,
            )

    # Metric view create-and-attach (MV-D1/D20): the object is created under the
    # user's OBO client by the backend hook, after the run row exists (so the
    # created-objects ledger FK is valid) and before submit (so the job receives
    # the identifiers to attach). The hook drops any suggestion that fails and
    # returns the effective mode; an empty attach set downgrades to suggest_only.
    mv_attach_views_str = ""
    mv_consent_id = ""
    effective_mv_action_mode = (mv_action_mode or "suggest_only").strip().lower()
    if (
        enable_metric_view_suggestions
        and effective_mv_action_mode == "create_and_attach"
        and mv_attach_hook is not None
    ):
        attach_views: list[str] = []
        try:
            handoff = mv_attach_hook(run_id)
            attach_views = list(getattr(handoff, "attach_views", []) or [])
            mv_consent_id = str(getattr(handoff, "consent_id", "") or "")
            hook_mode = str(getattr(handoff, "action_mode", "") or "").strip().lower()
            if hook_mode:
                effective_mv_action_mode = hook_mode
        except Exception:
            logger.warning(
                "Metric view create-and-attach hook failed for run %s; "
                "downgrading to suggest_only",
                run_id, exc_info=True,
            )
            attach_views = []
            effective_mv_action_mode = "suggest_only"
        if not attach_views:
            effective_mv_action_mode = "suggest_only"
        mv_attach_views_str = json.dumps(attach_views)

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
            warehouse_id=config.warehouse_id or "",
            llm_model=config.llm_model or "",
            target_accuracy=target_accuracy_str,
            max_attempts=max_attempts_str,
            workload_warehouse_ids=workload_warehouse_ids_str,
            benchmark_policy=requested_benchmark_policy,
            enable_metric_view_suggestions=(
                "true" if enable_metric_view_suggestions else "false"
            ),
            mv_action_mode=effective_mv_action_mode,
            mv_attach_views=mv_attach_views_str,
            mv_consent_id=mv_consent_id,
            mv_min_confidence=(
                str(int(mv_min_confidence)) if mv_min_confidence is not None else "75"
            ),
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

    # Submission succeeded, so this run must remain non-terminal even if the
    # first warehouse UPDATE fails.  Retry the idempotent handoff with the app
    # service principal; never misclassify a live job as a submission failure.
    handoff_sql = (
        f"UPDATE {config.catalog}.{config.schema_name}.genie_opt_runs "
        f"SET status = 'IN_PROGRESS', job_run_id = '{job_run_id}', "
        f"job_id = '{job_id}', "
        f"updated_at = current_timestamp() "
        f"WHERE run_id = '{run_id}'"
    )
    try:
        sql_warehouse_execute(ws, config.warehouse_id, handoff_sql)
    except Exception as first_exc:
        logger.warning(
            "OBO job handoff write failed for run %s; retrying with the service principal",
            run_id,
            exc_info=True,
        )
        try:
            sql_warehouse_execute(sp_ws, config.warehouse_id, handoff_sql)
        except Exception as recovery_exc:
            logger.exception(
                "Job %s was submitted for run %s but its tracking metadata could not be persisted",
                job_run_id,
                run_id,
            )
            raise RuntimeError(
                "Optimization job was submitted, but its tracking metadata could not "
                f"be recorded (run {run_id}, job run {job_run_id}). The run remains "
                "queued to block another optimization; retry after checking the job."
            ) from recovery_exc
        logger.info(
            "Recovered job handoff persistence for run %s after OBO failure: %s",
            run_id,
            type(first_exc).__name__,
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
