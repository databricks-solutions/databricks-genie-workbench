"""GSO synced table reads from Lakebase (PostgreSQL).

Synced tables live in the same catalog/schema as the source Delta tables,
with a `_synced` suffix (e.g. `genie_opt_runs_synced`).  In Postgres they
appear under the schema matching GSO_SCHEMA (default `genie_space_optimizer`).
"""

import logging
import os

import backend.services.lakebase as _lb

logger = logging.getLogger(__name__)

# Postgres schema where synced tables appear — matches the UC schema name.
_GSO_PG_SCHEMA = os.environ.get("GSO_SCHEMA", "genie_space_optimizer")

# Synced tables are created with this suffix in the same UC schema.
_SYNCED_SUFFIX = "_synced"

# Disabled until Databricks SDK supports Lakebase Autoscaling synced table
# creation. All reads fall through to Delta table queries via SQL Warehouse.
# Flip to True and redeploy once synced tables are provisioned.
_SYNCED_TABLES_ENABLED = False


def _get_pool():
    """Return the live Lakebase pool, or None if unavailable."""
    if not _SYNCED_TABLES_ENABLED:
        return None
    if not _lb._lakebase_available or _lb._pool is None:
        return None
    return _lb._pool


def _tbl(name: str) -> str:
    """Return the fully-qualified Postgres table reference for a synced table."""
    return f'"{_GSO_PG_SCHEMA}"."{name}{_SYNCED_SUFFIX}"'


async def load_gso_run(run_id: str) -> dict | None:
    """Load a single optimization run by ID."""
    pool = _get_pool()
    if pool is None:
        return None

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT * FROM {_tbl('genie_opt_runs')} WHERE run_id = $1",
                run_id,
            )
            return dict(row) if row else None
    except Exception:
        logger.warning("Lakebase query failed for genie_opt_runs", exc_info=True)
        return None


async def load_gso_runs_for_space(space_id: str) -> list[dict]:
    """Load all optimization runs for a space, most recent first."""
    pool = _get_pool()
    if pool is None:
        return []

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""SELECT run_id, space_id, status, started_at, completed_at,
                          best_accuracy, best_iteration, convergence_reason, triggered_by
                   FROM {_tbl('genie_opt_runs')}
                   WHERE space_id = $1
                   ORDER BY started_at DESC""",
                space_id,
            )
            return [dict(r) for r in rows]
    except Exception:
        logger.warning("Lakebase query failed for genie_opt_runs", exc_info=True)
        return []


async def load_gso_stages(run_id: str) -> list[dict]:
    """Load pipeline stages for a run."""
    pool = _get_pool()
    if pool is None:
        return []

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""SELECT * FROM {_tbl('genie_opt_stages')}
                   WHERE run_id = $1
                   ORDER BY started_at ASC""",
                run_id,
            )
            return [dict(r) for r in rows]
    except Exception:
        logger.warning("Lakebase query failed for genie_opt_stages", exc_info=True)
        return []


async def load_gso_iterations(run_id: str, *, include_rows_json: bool = False) -> list[dict]:
    """Load evaluation iterations for a run.

    By default excludes the large rows_json column for performance.
    Pass include_rows_json=True when per-question detail is needed.
    """
    pool = _get_pool()
    if pool is None:
        return []

    # Bug #2: evaluated_count / excluded_count / quarantined_benchmarks_json are
    # the denominator contract columns. If they're missing from the SELECT list
    # the frontend silently falls back to dividing by total_questions, which is
    # exactly the KPI-vs-tab-label mismatch that the bug exists to prevent.
    cols = "*" if include_rows_json else (
        "run_id, iteration, lever, eval_scope, timestamp, mlflow_run_id, model_id, "
        "overall_accuracy, total_questions, correct_count, "
        "evaluated_count, excluded_count, quarantined_benchmarks_json, "
        "scores_json, failures_json, "
        "remaining_failures, arbiter_actions_json, repeatability_pct, repeatability_json, "
        "thresholds_met, reflection_json"
    )
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""SELECT {cols} FROM {_tbl('genie_opt_iterations')}
                   WHERE run_id = $1
                   ORDER BY iteration ASC""",
                run_id,
            )
            return [dict(r) for r in rows]
    except Exception:
        logger.warning("Lakebase query failed for genie_opt_iterations", exc_info=True)
        return []


async def load_gso_patches(run_id: str) -> list[dict]:
    """Load optimization patches for a run."""
    pool = _get_pool()
    if pool is None:
        return []

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""SELECT * FROM {_tbl('genie_opt_patches')}
                   WHERE run_id = $1
                   ORDER BY iteration, lever, patch_index""",
                run_id,
            )
            return [dict(r) for r in rows]
    except Exception:
        logger.warning("Lakebase query failed for genie_opt_patches", exc_info=True)
        return []


async def load_gso_asi_results(run_id: str, iteration: int) -> list[dict]:
    """Load ASI (per-judge) evaluation results for a specific iteration."""
    pool = _get_pool()
    if pool is None:
        return []

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""SELECT * FROM {_tbl('genie_eval_asi_results')}
                   WHERE run_id = $1 AND iteration = $2""",
                run_id,
                iteration,
            )
            return [dict(r) for r in rows]
    except Exception:
        logger.warning("Lakebase query failed for genie_eval_asi_results", exc_info=True)
        return []


async def load_gso_iteration_rows(run_id: str, iteration: int, eval_scope: str | None = "full") -> str | None:
    """Load the rows_json column for a specific iteration and eval scope.

    If eval_scope is None, returns the first row with non-null rows_json
    for the given run_id and iteration (any scope).
    """
    pool = _get_pool()
    if pool is None:
        return None

    tbl = _tbl('genie_opt_iterations')
    try:
        async with pool.acquire() as conn:
            if eval_scope is not None:
                row = await conn.fetchrow(
                    f"SELECT rows_json FROM {tbl} "
                    "WHERE run_id = $1 AND iteration = $2 AND eval_scope = $3",
                    run_id,
                    iteration,
                    eval_scope,
                )
            else:
                row = await conn.fetchrow(
                    f"SELECT rows_json FROM {tbl} "
                    "WHERE run_id = $1 AND iteration = $2 AND rows_json IS NOT NULL",
                    run_id,
                    iteration,
                )
            return row["rows_json"] if row else None
    except Exception:
        logger.warning("Lakebase query failed for genie_opt_iterations (rows_json)", exc_info=True)
        return None


async def load_gso_suggestions(run_id: str) -> list[dict]:
    """Load optimization suggestions for a run."""
    pool = _get_pool()
    if pool is None:
        return []

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""SELECT * FROM {_tbl('genie_opt_suggestions')}
                   WHERE run_id = $1
                   ORDER BY created_at ASC""",
                run_id,
            )
            return [dict(r) for r in rows]
    except Exception:
        logger.warning("Lakebase query failed for genie_opt_suggestions", exc_info=True)
        return []
