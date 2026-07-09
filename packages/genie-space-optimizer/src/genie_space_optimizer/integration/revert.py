"""Warehouse-backed revert operation for integration callers.

Reverts the **live Genie Space** to a chosen configuration captured during a
past optimization run:

* ``target="champion"`` — the run's champion iteration config
  (``genie_opt_iterations.config_json`` where ``is_champion = true``). This is
  the winning optimized config (or the baseline when the baseline was the
  champion — though in that case the caller usually offers the baseline
  button only).
* ``target="baseline"`` — the run's pre-run config (``config_snapshot``), i.e.
  what the space looked like before this optimization ran. Lets a user ditch
  a champion they don't like and jump back to the starting point.

Unlike :func:`genie_space_optimizer.integration.discard.discard_optimization`,
this is a pure config rollback — it does NOT flip the run's status to
``DISCARDED`` and is available for any past history entry regardless of its
terminal resolution. It exists so the Workbench "Optimization History" table
can offer per-entry "Revert to champion" / "Revert to baseline" affordances.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.warehouse import (
    sql_warehouse_query,
    wh_load_run,
)

from .config import IntegrationConfig
from .types import ActionResult

logger = logging.getLogger(__name__)

RevertTarget = Literal["champion", "baseline"]

# Non-terminal statuses — reverting to a still-running run's snapshot is racy
# (the active pipeline is mutating the live space), so callers should refuse.
_NON_TERMINAL_STATUSES = frozenset({"QUEUED", "IN_PROGRESS", "RUNNING"})


def revert_optimization(
    run_id: str,
    ws: WorkspaceClient,
    sp_ws: WorkspaceClient,
    config: IntegrationConfig,
    *,
    target: RevertTarget = "champion",
) -> ActionResult:
    """Revert the live Genie Space to a past run's captured configuration.

    Args:
        run_id: The optimization run whose configuration to revert to.
        ws: OBO-authenticated ``WorkspaceClient`` for the requesting user.
        sp_ws: Service-principal ``WorkspaceClient`` (fallback for Genie API).
        config: Integration configuration.
        target: Which configuration to revert to — ``"champion"`` (the winning
            iteration's full effective config) or ``"baseline"`` (the run's
            pre-run ``config_snapshot``).

    Returns:
        :class:`ActionResult` with status ``"reverted"``.

    Raises:
        ValueError: If the run is not found, still running, or has no captured
            configuration for the requested target.
        RuntimeError: If the Genie API rollback PATCH fails (incl. config
            validation failure).
    """
    run_data = wh_load_run(ws, config.warehouse_id, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise ValueError(f"Run not found: {run_id}")

    status = str(run_data.get("status") or "")
    if status.upper() in _NON_TERMINAL_STATUSES:
        raise ValueError(
            f"Cannot revert to a run that is still in progress (status={status}). "
            f"Wait for the run to finish first."
        )

    space_id = str(run_data.get("space_id") or "")
    if not space_id:
        raise ValueError("Run has no space_id; cannot target a Genie Space to revert.")

    if target == "baseline":
        target_config = _load_baseline_config(run_data)
        source = "run baseline (genie_opt_runs.config_snapshot)"
        missing_hint = (
            "This run has no baseline configuration snapshot to revert to."
        )
    else:
        target_config = _load_champion_config(
            ws, config.warehouse_id, config.catalog, config.schema_name, run_id,
        )
        source = "champion iteration (genie_opt_iterations.config_json)"
        missing_hint = (
            "This run's champion configuration is not available "
            "(the iteration config was not captured). "
            "Try reverting to the baseline instead."
        )

    if not target_config or not isinstance(target_config, dict):
        raise ValueError(missing_hint)

    from genie_space_optimizer.common.genie_client import patch_space_config

    client = _pick_genie_client(ws, sp_ws)
    try:
        patch_space_config(client, space_id, target_config)
    except ValueError as exc:
        # patch_space_config raises ValueError on serialized_space validation
        # failure — surface that as an unprocessable-config error (422), not a
        # 409 "conflict".
        logger.warning(
            "%s config for run %s failed validation before PATCH: %s",
            target, run_id, str(exc)[:200],
        )
        raise RuntimeError(
            f"The {target} configuration is invalid: {exc}"
        ) from exc

    logger.info(
        "Reverted live Genie Space %s to run %s's %s config (source=%s).",
        space_id, run_id, target, source,
    )
    return ActionResult(
        status="reverted",
        run_id=run_id,
        message=f"Genie Space reverted to this run's {target} configuration.",
    )


def _load_baseline_config(run_data: dict) -> dict | None:
    """Extract the run's pre-run config snapshot (iteration 0's config)."""
    raw = run_data.get("config_snapshot")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
        return parsed if isinstance(parsed, dict) else None
    if isinstance(raw, dict):
        return raw
    return None


def _load_champion_config(
    ws: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema_name: str,
    run_id: str,
) -> dict | None:
    """Load the champion iteration's full effective config from Delta.

    Selects ``config_json`` from the single ``genie_opt_iterations`` row
    stamped ``is_champion = true`` (rolled-back rows defensively excluded).
    Returns the parsed config dict, or ``None`` when:

    * the table predates the Phase-4 ``config_json`` / ``is_champion`` columns
      (``UNRESOLVED_COLUMN`` — legacy run);
    * no row is flagged champion;
    * the champion row's ``config_json`` is empty/unparseable.

    No baseline fallback here — the caller's ``target="baseline"`` path handles
    baseline reverts explicitly, so a missing champion config is surfaced as an
    error rather than silently reverting to the baseline.
    """
    safe_run = run_id.replace("'", "''")
    table = f"{catalog}.{schema_name}.genie_opt_iterations"
    sql = (
        f"SELECT config_json FROM {table} "
        f"WHERE run_id = '{safe_run}' "
        f"AND is_champion = true "
        f"AND (rolled_back IS NULL OR rolled_back = false) "
        f"ORDER BY iteration DESC LIMIT 1"
    )
    try:
        df = sql_warehouse_query(ws, warehouse_id, sql)
    except Exception as exc:
        if _looks_like_legacy_schema_error(exc):
            logger.info(
                "gso.revert.no_champion_col genie_opt_iterations is missing the "
                "Phase-4 config_json/is_champion columns for run %s. err=%s",
                run_id, str(exc)[:160],
            )
            return None
        logger.warning(
            "gso.revert.champion_query_failed run=%s err=%s",
            run_id, str(exc)[:200], exc_info=True,
        )
        return None

    if df is None or df.empty:
        return None
    raw = df.iloc[0].to_dict().get("config_json")
    if raw is None:
        return None
    # sql_warehouse_query may hand back a Databricks SDK ScalarDbValue / similar
    # wrapper around the JSON string — coerce to str before parsing.
    if not isinstance(raw, str):
        raw = getattr(raw, "value", None) or str(raw)
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("gso.revert.champion_config_unparseable run=%s", run_id)
        return None
    return parsed if isinstance(parsed, dict) else None


_LEGACY_COL_ERROR_MARKERS = (
    "UNRESOLVED_COLUMN",
    "cannot resolve",
    "config_json",
    "is_champion",
)


def _looks_like_legacy_schema_error(exc: BaseException) -> bool:
    """Detect a missing-column error from a pre-Phase-4 iterations table."""
    msg = str(exc)
    return any(marker in msg for marker in _LEGACY_COL_ERROR_MARKERS)


def _pick_genie_client(
    ws: WorkspaceClient, sp_ws: WorkspaceClient,
) -> WorkspaceClient:
    """Pick the best client for Genie API calls (OBO preferred, SP fallback).

    Mirrors the helper in ``integration.discard`` so revert reuses the same
    OBO-with-SP-fallback access pattern as the discard rollback.
    """
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
