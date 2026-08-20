"""Champion selection for Genie Agent optimization runs (Delta-only).

GSO v2 (Phase 5, D3/D7): tracking and versioning are Delta-only. There is no
MLflow LoggedModel snapshot, no UC Model Registry version, and no per-mutation
MLflow run. The champion iteration is selected from ``genie_opt_iterations``
(highest accepted ``overall_accuracy``) and marked in Delta via
``mark_champion_iteration``. Rollback stays Delta-based — see
``applier.rollback`` (in-memory ``pre_snapshot`` re-PATCH) and
``integration.discard`` (``genie_opt_runs.config_snapshot`` re-PATCH).

The removed MLflow paths (``create_genie_model_version``,
``link_eval_scores_to_model``, ``rollback_to_model``, ``register_uc_model``,
``_GenieConfigSnapshot``, ``_register_uc_version``) were decommissioned in
Phase 5; cross-environment deploy is out of scope for that PR and will use the
official DAB ``genie_space`` resource in the future.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from genie_space_optimizer.optimization.champion import select_champion_row
from genie_space_optimizer.optimization.state import (
    load_iterations,
    load_run,
    mark_champion_iteration,
    update_run_status,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def promote_best_model(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> int | None:
    """Select and mark the champion iteration in Delta.

    Reads all iterations from Delta, picks the one with the highest
    ``overall_accuracy``, marks it as the Delta champion, and records
    ``best_iteration`` / ``best_accuracy`` on the run row.

    Returns the champion iteration number, or None on failure.
    """
    run_row = load_run(spark, run_id, catalog, schema)
    if not run_row:
        logger.error("Cannot promote: run %s not found", run_id)
        return None

    iterations_df = load_iterations(spark, run_id, catalog, schema)
    if iterations_df.empty:
        logger.warning("No iterations found for run %s", run_id)
        return None

    best_row = select_champion_row(iterations_df.to_dict("records"))
    if best_row is None:
        logger.warning(
            "No promotable full/enrichment iterations for run %s",
            run_id,
        )
        return None

    best_iteration = int(best_row.get("iteration", 0))
    best_accuracy = float(best_row.get("overall_accuracy", 0.0))
    best_eval_scope = str(best_row.get("eval_scope") or "full")

    # GSO v2 Phase 4 (D3): persist the champion in Delta. Reuses the selection
    # computed just above (no new selection logic). Best-effort inside
    # ``mark_champion_iteration`` — a write failure logs, never raises.
    mark_champion_iteration(
        spark, run_id, best_iteration,
        catalog=catalog, schema=schema, eval_scope=best_eval_scope,
    )

    update_run_status(
        spark, run_id, catalog, schema,
        best_iteration=best_iteration,
        best_accuracy=best_accuracy,
    )

    logger.info(
        "Champion: iter=%d (accuracy=%.1f%%, scope=%s) for run %s",
        best_iteration, best_accuracy, best_eval_scope, run_id,
    )
    return best_iteration
