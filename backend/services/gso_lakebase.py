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
                          best_accuracy, best_iteration, convergence_reason, triggered_by,
                          llm_model,
                          (config_snapshot IS NOT NULL AND length(config_snapshot) > 2) AS has_config_snapshot
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

    # Bug #2: evaluated_count / excluded_count are the denominator contract
    # columns. If they're missing from the SELECT list
    # the frontend silently falls back to dividing by total_questions, which is
    # exactly the KPI-vs-tab-label mismatch that the bug exists to prevent.
    cols = "*" if include_rows_json else (
        # GSO v2 Phase 5: genie_opt_iterations.mlflow_run_id / model_id columns
        # were scrubbed — selecting them would break on fresh post-Phase-5
        # tables. Downstream readers fall back to None (intended Phase-6 no-op).
        "run_id, iteration, lever, eval_scope, timestamp, "
        "overall_accuracy, total_questions, correct_count, "
        "evaluated_count, excluded_count, "
        "scores_json, failures_json, "
        "remaining_failures, "
        "thresholds_met, reflection_json, rolled_back, "
        # GSO v2 Phase 6: native official eval-run metadata surfaced by
        # /iterations (num_needs_review + eval_run_id/status).
        "num_needs_review, eval_run_id, eval_run_status"
    )
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""SELECT {cols} FROM {_tbl('genie_opt_iterations')}
                   WHERE run_id = $1
                   ORDER BY iteration ASC, timestamp ASC""",
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
                    "WHERE run_id = $1 AND iteration = $2 AND eval_scope = $3 "
                    "ORDER BY timestamp ASC LIMIT 1",
                    run_id,
                    iteration,
                    eval_scope,
                )
            else:
                row = await conn.fetchrow(
                    f"SELECT rows_json FROM {tbl} "
                    "WHERE run_id = $1 AND iteration = $2 AND rows_json IS NOT NULL "
                    "ORDER BY timestamp ASC LIMIT 1",
                    run_id,
                    iteration,
                )
            return row["rows_json"] if row else None
    except Exception:
        logger.warning("Lakebase query failed for genie_opt_iterations (rows_json)", exc_info=True)
        return None


async def load_gso_benchmark_mutations(run_id: str) -> list[dict]:
    """Load the benchmark provenance ledger for a run (GSO v2 Phase 6, §3.5).

    Rows record every benchmark question GSO added / removed / changed (or
    recommended for prune) in the live Genie Space. Synced reads are disabled
    today, so this falls through to the Delta SQL-warehouse fallback in the
    router.
    """
    pool = _get_pool()
    if pool is None:
        return []

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""SELECT run_id, question_id, op, before, after, reason, logged_at
                   FROM {_tbl('genie_opt_benchmark_mutations')}
                   WHERE run_id = $1
                   ORDER BY logged_at ASC""",
                run_id,
            )
            return [dict(r) for r in rows]
    except Exception:
        logger.warning("Lakebase query failed for genie_opt_benchmark_mutations", exc_info=True)
        return []
