"""Warehouse-backed apply operation for integration callers.

Uses the Statement Execution API for all Delta state reads/writes --
no ``databricks-connect`` required.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.warehouse import (
    sql_warehouse_execute,
    sql_warehouse_query,
    wh_load_run,
)

from .config import IntegrationConfig
from .types import ActionResult

logger = logging.getLogger(__name__)

_TERMINAL_APPLY_STATUSES = frozenset({"CONVERGED", "STALLED", "MAX_ITERATIONS"})
_DEFERRED_UC_MODES = frozenset({"uc_artifact", "both"})


def apply_optimization(
    run_id: str,
    ws: WorkspaceClient,
    config: IntegrationConfig,
) -> ActionResult:
    """Apply an optimization run.  OBO client only (no SP needed).

    Uses SQL Warehouse for Delta state reads/writes.

    Args:
        run_id: The optimization run ID to apply.
        ws: OBO-authenticated ``WorkspaceClient`` for the requesting user.
        config: Integration configuration.

    Returns:
        :class:`ActionResult` with status ``"applied"``.

    Raises:
        ValueError: If the run is not found or not in a terminal status.
        RuntimeError: If deferred UC writes fail.
    """
    run_data = wh_load_run(ws, config.warehouse_id, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise ValueError(f"Run not found: {run_id}")

    status = str(run_data.get("status") or "")
    if status not in _TERMINAL_APPLY_STATUSES:
        raise ValueError(
            f"Cannot apply run in status {status}. "
            f"Must be one of: {sorted(_TERMINAL_APPLY_STATUSES)}"
        )

    requested_mode = str(run_data.get("apply_mode") or "genie_config").lower()

    if requested_mode in _DEFERRED_UC_MODES:
        iters_df = sql_warehouse_query(
            ws,
            config.warehouse_id,
            f"SELECT run_id, iteration, eval_scope, overall_accuracy "
            f"FROM {config.catalog}.{config.schema_name}.genie_opt_iterations "
            f"WHERE run_id = '{run_id}' AND eval_scope = 'full' "
            f"ORDER BY iteration ASC",
        )
        iters_rows = iters_df.to_dict("records") if not iters_df.empty else []
        baseline_score, best_score = _get_baseline_and_best(iters_rows)

        if baseline_score is None or best_score is None:
            raise RuntimeError(
                "Deferred UC writes require completed full-evaluation results "
                "(baseline and optimized scores)."
            )
        if best_score <= baseline_score:
            raise RuntimeError(
                "Deferred UC writes require a proven improvement in evaluation "
                f"(baseline={baseline_score:.1f}%, optimized={best_score:.1f}%)."
            )

    message = "Optimization applied successfully."
    if requested_mode in _DEFERRED_UC_MODES:
        message = (
            "Optimization applied. Note: deferred UC writes via the integration "
            "API are not yet supported; use the standalone app for UC write-back."
        )

    sql_warehouse_execute(
        ws,
        config.warehouse_id,
        f"UPDATE {config.catalog}.{config.schema_name}.genie_opt_runs "
        f"SET status = 'APPLIED', updated_at = current_timestamp() "
        f"WHERE run_id = '{run_id}'",
    )
    return ActionResult(status="applied", run_id=run_id, message=message)


def _get_baseline_and_best(
    iters_rows: list[dict],
) -> tuple[float | None, float | None]:
    """Extract baseline (iteration 0) and best accuracy from iteration rows."""
    baseline: float | None = None
    best: float | None = None
    for row in iters_rows:
        acc = row.get("overall_accuracy")
        if acc is None:
            continue
        try:
            val = float(acc)
        except (TypeError, ValueError):
            continue
        iteration = row.get("iteration")
        try:
            it_num = int(iteration)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if it_num == 0:
            baseline = val
        if best is None or val > best:
            best = val
    return baseline, best
