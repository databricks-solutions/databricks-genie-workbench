"""Champion selection for Genie Space optimization runs (Delta-only).

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

    # Allow the post-enrichment iter-0 row (``eval_scope == "enrichment"``)
    # to be a candidate for champion alongside lever-loop iterations.
    # ``compute_run_scores`` already treats both as candidates for the UI
    # headline; selecting the matching iteration keeps the marked champion
    # consistent with the displayed ``optimizedScore``.
    full_evals = iterations_df[
        iterations_df["eval_scope"].isin(["full", "enrichment"])
    ]
    if full_evals.empty:
        full_evals = iterations_df

    # Tier 1.2: exclude rolled-back iterations from champion selection so the
    # run's stored ``best_accuracy`` reflects accepted state only. Baseline
    # (iteration 0 full) is never rolled back and remains the floor.
    if "rolled_back" in full_evals.columns:
        _rb_mask = full_evals["rolled_back"].fillna(False).astype(bool)
        _is_baseline = (
            (full_evals["iteration"].astype(int) == 0)
            & (full_evals["eval_scope"] == "full")
        )
        full_evals = full_evals[(~_rb_mask) | _is_baseline]

    if full_evals.empty:
        logger.warning(
            "No non-rolled-back full/enrichment iterations for run %s — "
            "falling back to all full/enrichment iterations",
            run_id,
        )
        full_evals = iterations_df[
            iterations_df["eval_scope"].isin(["full", "enrichment"])
        ]
        if full_evals.empty:
            full_evals = iterations_df

    best_idx = full_evals["overall_accuracy"].idxmax()
    best_row = full_evals.loc[best_idx]
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
