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

import copy
import json
import logging
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.warehouse import (
    sql_warehouse_query,
    wh_load_run,
    wh_reconcile_active_runs,
)

from .config import IntegrationConfig
from .types import ActionResult

logger = logging.getLogger(__name__)

RevertTarget = Literal["champion", "baseline"]

# Non-terminal statuses — reverting to a still-running run's snapshot is racy
# (the active pipeline is mutating the live space), so callers should refuse.
_ACTIVE_RUN_STATUSES = frozenset({"QUEUED", "IN_PROGRESS", "RUNNING"})
_MISSING_DESCRIPTION = object()


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
    # Internal optimizer state is owned by the app service principal.  The OBO
    # caller is authorized against the target Space below; their UC grants must
    # not decide whether the app can read its own run history.
    run_data = wh_load_run(
        sp_ws, config.warehouse_id, run_id, config.catalog, config.schema_name,
    )
    if not run_data:
        raise ValueError(f"Run not found: {run_id}")

    status = str(run_data.get("status") or "")
    if status.upper() in _ACTIVE_RUN_STATUSES:
        raise ValueError(
            f"Cannot revert to a run that is still in progress (status={status}). "
            f"Wait for the run to finish first."
        )

    space_id = str(run_data.get("space_id") or "")
    if not space_id:
        raise ValueError("Run has no space_id; cannot target a Genie Space to revert.")

    from genie_space_optimizer.common.genie_client import user_can_edit_space

    if not user_can_edit_space(ws, space_id, acl_client=sp_ws):
        raise PermissionError(
            "You need CAN_EDIT or CAN_MANAGE permission on this Genie Space "
            "to revert its configuration."
        )

    _assert_no_active_space_runs(
        space_id=space_id,
        sp_ws=sp_ws,
        config=config,
    )

    if target == "baseline":
        target_snapshot = _load_baseline_config(run_data)
        target_config = target_snapshot
        target_description = _description_from_snapshot(target_snapshot)
        source = "run baseline (genie_opt_runs.config_snapshot)"
        missing_hint = (
            "This run has no baseline configuration snapshot to revert to."
        )
    else:
        target_config = _load_champion_config(
            sp_ws, config.warehouse_id, config.catalog, config.schema_name, run_id,
        )
        target_description = _load_champion_description(
            sp_ws, config.warehouse_id, config.catalog, config.schema_name, run_id,
        )
        source = "champion iteration (genie_opt_iterations.config_json)"
        missing_hint = (
            "This run's champion configuration is not available "
            "(the iteration config was not captured). "
            "Try reverting to the baseline instead."
        )

    target_config = _as_serialized_space_config(target_config)
    if not target_config or not isinstance(target_config, dict):
        raise ValueError(missing_hint)

    preferred_client = _pick_genie_client(ws, sp_ws)
    live_snapshot, client = _fetch_live_space_snapshot(
        space_id=space_id,
        clients=(preferred_client, ws, sp_ws),
    )
    live_config = _as_serialized_space_config(live_snapshot)
    if not live_config:
        raise RuntimeError(
            "Cannot safely revert without capturing the live Genie Space state."
        )
    live_description = _description_from_snapshot(live_snapshot)
    target_config = _preserve_live_benchmarks(
        target_config,
        space_id=space_id,
        live_config=live_config,
    )
    _apply_revert_state(
        client=client,
        space_id=space_id,
        target=target,
        run_id=run_id,
        target_config=target_config,
        target_description=target_description,
        live_config=live_config,
        live_description=live_description,
    )

    logger.info(
        "Reverted live Genie Space %s to run %s's %s config (source=%s).",
        space_id, run_id, target, source,
    )
    return ActionResult(
        status="reverted",
        run_id=run_id,
        message=f"Genie Space reverted to this run's {target} configuration.",
    )


def _assert_no_active_space_runs(
    *,
    space_id: str,
    sp_ws: WorkspaceClient,
    config: IntegrationConfig,
) -> None:
    """Reconcile and reject every active run for the target Space.

    The selected history row may be terminal while a newer run is mutating the
    same live Space.  Querying all rows closes that race window at the
    application boundary.  Both reads and reconciliation writes use the app SP.
    """
    safe_space = space_id.replace("'", "''")
    sql = (
        f"SELECT * FROM {config.catalog}.{config.schema_name}.genie_opt_runs "
        f"WHERE space_id = '{safe_space}' ORDER BY started_at DESC"
    )
    runs_df = sql_warehouse_query(sp_ws, config.warehouse_id, sql)
    active_before_reconcile = {
        str(row.get("run_id") or "")
        for _, row in runs_df.iterrows()
        if str(row.get("status") or "").upper() in _ACTIVE_RUN_STATUSES
    } if not runs_df.empty else set()
    if not runs_df.empty and wh_reconcile_active_runs(
        sp_ws,
        sp_ws,
        config.warehouse_id,
        runs_df,
        config.catalog,
        config.schema_name,
    ):
        runs_df = sql_warehouse_query(sp_ws, config.warehouse_id, sql)

    active: list[tuple[str, str]] = []
    if not runs_df.empty:
        for _, row in runs_df.iterrows():
            status = str(row.get("status") or "").upper()
            if status in _ACTIVE_RUN_STATUSES:
                active.append((str(row.get("run_id") or "unknown"), status))
                continue
            # The shared reconciler records a Jobs API lookup failure as a
            # FAILED row. That state is not proof that the job stopped, so a
            # row that was active at the start of this check remains a conflict
            # until a later reconciliation can observe an authoritative state.
            run_id = str(row.get("run_id") or "")
            reason = str(row.get("convergence_reason") or "")
            if run_id in active_before_reconcile and reason == "job_run_lookup_failed":
                active.append((run_id or "unknown", "STATE_UNVERIFIED"))
    if active:
        active_id, active_status = active[0]
        raise ValueError(
            "Cannot revert while an optimization is active for this Genie "
            f"Space (run {active_id}, status={active_status}). Wait for it to finish."
        )


def _as_serialized_space_config(config: dict | None) -> dict | None:
    """Return the parsed ``serialized_space`` object from a stored snapshot.

    History rows have existed in two shapes:

    * the correct parsed ``serialized_space`` object with top-level
      ``version`` / ``data_sources`` / ``config`` / ``instructions``;
    * a raw Genie Space API response where that object is nested under
      ``_parsed_space`` or ``serialized_space``.

    The PATCH client must receive the first shape.
    """
    if not isinstance(config, dict):
        return None

    parsed = config.get("_parsed_space")
    if isinstance(parsed, dict):
        return _with_default_serialized_space_version(parsed)

    serialized = config.get("serialized_space")
    if isinstance(serialized, dict):
        return _with_default_serialized_space_version(serialized)
    if isinstance(serialized, str) and serialized.strip():
        try:
            loaded = json.loads(serialized)
        except (json.JSONDecodeError, TypeError):
            return None
        if isinstance(loaded, dict):
            return _with_default_serialized_space_version(loaded)
        return None

    return _with_default_serialized_space_version(config)


def _with_default_serialized_space_version(config: dict) -> dict:
    """Backfill version for legacy projected history rows.

    Older ``config_json`` rows were projected from a parsed serialized-space
    object but accidentally dropped the required top-level ``version`` field.
    If the remaining shape is otherwise recognizable as serialized_space, use
    the current documented schema version.
    """
    if "version" in config:
        return config
    if any(key in config for key in ("data_sources", "instructions", "config", "benchmarks")):
        return {"version": 2, **config}
    return config


def _preserve_live_benchmarks(
    target_config: dict,
    *,
    space_id: str,
    live_config: dict,
) -> dict:
    """Keep revert from rolling benchmark ground truth backward.

    ``benchmarks`` is part of ``serialized_space``, but it is evaluation state,
    not the space configuration the History button is meant to restore. Because
    ``serialized_space`` PATCH is full-replacement, "skip benchmarks" means
    copying the live benchmark block into the outgoing payload unchanged.
    """
    target = copy.deepcopy(target_config)
    live_benchmarks = live_config.get("benchmarks")
    if isinstance(live_benchmarks, dict):
        target["benchmarks"] = copy.deepcopy(live_benchmarks)
        logger.info(
            "Preserved live benchmark block during revert for space %s "
            "(questions=%d).",
            space_id,
            len(live_benchmarks.get("questions", []) or []),
        )
    else:
        target.pop("benchmarks", None)
        logger.info(
            "Preserved empty live benchmark block during revert for space %s.",
            space_id,
        )
    return target


def _fetch_live_space_snapshot(
    *,
    space_id: str,
    clients: tuple[WorkspaceClient, ...],
) -> tuple[dict, WorkspaceClient]:
    """Capture the full live Space state and return the client that read it."""
    from genie_space_optimizer.common.genie_client import fetch_space_config

    errors: list[str] = []
    seen_clients: set[int] = set()
    for client in clients:
        if id(client) in seen_clients:
            continue
        seen_clients.add(id(client))
        try:
            snapshot = fetch_space_config(client, space_id)
            if _as_serialized_space_config(snapshot):
                return snapshot, client
            errors.append(f"{type(client).__name__}: empty serialized_space")
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {str(exc)[:160]}")
    raise RuntimeError(
        "Cannot safely revert without reading the live Genie Space state. "
        f"Fetch errors: {'; '.join(errors) or 'none'}"
    )


def _description_from_snapshot(config: dict | None) -> str | object:
    """Return exact top-level description, distinguishing absent from empty."""
    if not isinstance(config, dict) or "description" not in config:
        return _MISSING_DESCRIPTION
    value = config.get("description")
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(str(item) for item in value if item is not None)
    return str(value)


def _load_champion_description(
    ws: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema_name: str,
    run_id: str,
) -> str | object:
    """Load the post-enrichment top-level description for a champion revert.

    Description metadata was added after champion config capture existed.  A
    missing/malformed artifact therefore means a legacy run and intentionally
    preserves the live description.
    """
    safe_run = run_id.replace("'", "''")
    table = f"{catalog}.{schema_name}.genie_opt_artifacts"
    sql = (
        f"SELECT artifact_json FROM {table} "
        f"WHERE run_id = '{safe_run}' "
        f"AND artifact_kind = 'space_quality_enrichment' "
        f"ORDER BY created_at DESC LIMIT 1"
    )
    try:
        df = sql_warehouse_query(ws, warehouse_id, sql)
    except Exception:
        logger.info(
            "gso.revert.no_description_artifact run=%s; preserving live description",
            run_id,
            exc_info=True,
        )
        return _MISSING_DESCRIPTION
    if df is None or df.empty:
        return _MISSING_DESCRIPTION
    raw = df.iloc[0].to_dict().get("artifact_json")
    if raw is None:
        return _MISSING_DESCRIPTION
    if not isinstance(raw, str):
        raw = getattr(raw, "value", None) or str(raw)
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return _MISSING_DESCRIPTION
    if not isinstance(payload, dict) or not payload.get("description_present", False):
        return _MISSING_DESCRIPTION
    value = payload.get("description")
    return "" if value is None else str(value)


def _apply_revert_state(
    *,
    client: WorkspaceClient,
    space_id: str,
    target: RevertTarget,
    run_id: str,
    target_config: dict,
    target_description: str | object,
    live_config: dict,
    live_description: str | object,
) -> None:
    """Apply serialized state + description with best-effort compensation."""
    from genie_space_optimizer.common.genie_client import (
        patch_space_config,
        update_space_description,
    )

    try:
        patch_space_config(client, space_id, target_config)
    except ValueError as exc:
        logger.warning(
            "%s config for run %s failed validation before PATCH: %s",
            target, run_id, str(exc)[:200],
        )
        raise RuntimeError(f"The {target} configuration is invalid: {exc}") from exc
    except Exception as exc:
        raise RuntimeError(
            f"Failed to apply the {target} serialized configuration: {exc}"
        ) from exc

    if target_description is _MISSING_DESCRIPTION:
        return

    try:
        update_space_description(
            client, space_id, str(target_description),
        )
    except Exception as exc:
        compensation_errors: list[str] = []
        try:
            patch_space_config(client, space_id, live_config)
        except Exception as compensation_exc:
            compensation_errors.append(
                f"serialized_space: {type(compensation_exc).__name__}: "
                f"{str(compensation_exc)[:160]}"
            )
        try:
            # A missing live description is equivalent to the API's empty
            # description for compensation purposes.
            restore_description = (
                "" if live_description is _MISSING_DESCRIPTION
                else str(live_description)
            )
            update_space_description(client, space_id, restore_description)
        except Exception as compensation_exc:
            compensation_errors.append(
                f"description: {type(compensation_exc).__name__}: "
                f"{str(compensation_exc)[:160]}"
            )
        suffix = (
            " Compensation also failed: " + "; ".join(compensation_errors)
            if compensation_errors
            else " The original live state was restored."
        )
        raise RuntimeError(
            f"Failed to restore the {target} description; the revert was not "
            f"completed.{suffix}"
        ) from exc


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
