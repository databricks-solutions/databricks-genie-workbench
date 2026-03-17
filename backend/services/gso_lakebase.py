"""GSO synced table reads from Lakebase (PostgreSQL)."""

import logging

from backend.services.lakebase import _pool, _lakebase_available

logger = logging.getLogger(__name__)


async def load_gso_run(run_id: str) -> dict | None:
    """Load a single optimization run by ID."""
    if not _lakebase_available or _pool is None:
        return None

    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM gso.genie_opt_runs WHERE run_id = $1",
            run_id,
        )
        return dict(row) if row else None


async def load_gso_runs_for_space(space_id: str) -> list[dict]:
    """Load all optimization runs for a space, most recent first."""
    if not _lakebase_available or _pool is None:
        return []

    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT run_id, space_id, status, started_at, completed_at,
                      best_accuracy, best_iteration, convergence_reason, triggered_by
               FROM gso.genie_opt_runs
               WHERE space_id = $1
               ORDER BY started_at DESC""",
            space_id,
        )
        return [dict(r) for r in rows]


async def load_gso_stages(run_id: str) -> list[dict]:
    """Load pipeline stages for a run."""
    if not _lakebase_available or _pool is None:
        return []

    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM gso.genie_opt_stages
               WHERE run_id = $1
               ORDER BY started_at ASC""",
            run_id,
        )
        return [dict(r) for r in rows]


async def load_gso_iterations(run_id: str) -> list[dict]:
    """Load evaluation iterations for a run."""
    if not _lakebase_available or _pool is None:
        return []

    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM gso.genie_opt_iterations
               WHERE run_id = $1
               ORDER BY iteration ASC""",
            run_id,
        )
        return [dict(r) for r in rows]


async def load_gso_patches(run_id: str) -> list[dict]:
    """Load optimization patches for a run."""
    if not _lakebase_available or _pool is None:
        return []

    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM gso.genie_opt_patches
               WHERE run_id = $1
               ORDER BY iteration, lever, patch_index""",
            run_id,
        )
        return [dict(r) for r in rows]


async def load_gso_asi_results(run_id: str, iteration: int) -> list[dict]:
    """Load ASI (per-judge) evaluation results for a specific iteration."""
    if not _lakebase_available or _pool is None:
        return []

    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM gso.genie_eval_asi_results
               WHERE run_id = $1 AND iteration = $2""",
            run_id,
            iteration,
        )
        return [dict(r) for r in rows]


async def load_gso_iteration_rows(run_id: str, iteration: int) -> str | None:
    """Load the rows_json column for a specific iteration."""
    if not _lakebase_available or _pool is None:
        return None

    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT rows_json FROM gso.genie_opt_iterations WHERE run_id = $1 AND iteration = $2",
            run_id,
            iteration,
        )
        return row["rows_json"] if row else None


async def load_gso_suggestions(run_id: str) -> list[dict]:
    """Load optimization suggestions for a run."""
    if not _lakebase_available or _pool is None:
        return []

    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM gso.genie_opt_suggestions
               WHERE run_id = $1
               ORDER BY created_at ASC""",
            run_id,
        )
        return [dict(r) for r in rows]
