"""Activity endpoint: recent optimization runs for the Dashboard.

Permission filtering: results are restricted to Genie Spaces where the
calling user has at least CAN_VIEW access.  Permission checks happen
**after** querying the Delta table (which returns ~20 rows), so only
the small set of unique space IDs in the result set are checked -- not
every space in the workspace.
"""

from __future__ import annotations

import logging

from ..core import Dependencies, create_router
from ..models import ActivityItem
from ..utils import ensure_utc_iso
from .._spark import get_spark
from ...common.accuracy import RunScores, compute_run_scores_by_run_id

router = create_router()
logger = logging.getLogger(__name__)


def _resolve_user_identity(ws, headers) -> tuple[str, set[str]]:
    """Resolve caller email and group memberships once."""
    user_email = (headers.user_email or headers.user_name or "").lower() or None
    user_groups: set[str] | None = None

    if not user_email:
        try:
            me = ws.current_user.me()
            user_email = (me.user_name or "").lower()
            if me.groups:
                user_groups = {g.display.lower() for g in me.groups if g.display}
        except Exception:
            user_email = ""

    if user_groups is None:
        try:
            me = ws.current_user.me()
            if me.groups:
                user_groups = {g.display.lower() for g in me.groups if g.display}
        except Exception:
            pass

    return user_email or "", user_groups or set()


def _check_access_for_spaces(
    ws, sp_ws, space_ids: set[str], user_email: str, user_groups: set[str],
) -> dict[str, str | None]:
    """Return access levels for a small set of space IDs (post-query filter)."""
    from genie_space_optimizer.common.genie_client import get_user_access_level

    result: dict[str, str | None] = {}
    for sid in space_ids:
        try:
            result[sid] = get_user_access_level(
                ws, sid,
                user_email=user_email,
                user_groups=user_groups,
                acl_client=sp_ws,
            )
        except Exception:
            result[sid] = None
    return result


@router.get("/activity", response_model=list[ActivityItem], operation_id="getActivity")
def get_activity(
    config: Dependencies.Config,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    headers: Dependencies.Headers,
    space_id: str | None = None,
    limit: int = 20,
):
    """Recent optimization runs for the Dashboard activity table.

    Results are filtered post-query to only include spaces where the
    calling user has at least CAN_VIEW permission.
    """
    from genie_space_optimizer.optimization.state import load_recent_activity

    try:
        spark = get_spark()
        df = load_recent_activity(
            spark, config.catalog, config.schema_name,
            space_id=space_id, limit=limit * 3,
        )
    except Exception:
        logger.debug("Delta tables not yet available, returning empty activity")
        return []

    if df.empty:
        return []

    user_email, user_groups = _resolve_user_identity(ws, headers)

    unique_space_ids = set(df["space_id"].dropna().unique())
    access_by_space = _check_access_for_spaces(
        ws, sp_ws, unique_space_ids, user_email, user_groups,
    )

    scores_by_run = _load_run_scores(
        spark, list(df["run_id"]), config.catalog, config.schema_name,
    )

    items: list[ActivityItem] = []
    for _, row in df.iterrows():
        row_space_id = row.get("space_id", "")
        if access_by_space.get(row_space_id) is None:
            continue
        run_id_val = row.get("run_id", "")
        scores = scores_by_run.get(run_id_val) or RunScores(None, None, None, None)
        items.append(
            ActivityItem(
                runId=run_id_val,
                spaceId=row_space_id,
                spaceName=row.get("domain", ""),
                status=row.get("status", ""),
                initiatedBy=row.get("triggered_by") or "system",
                baselineScore=scores.baseline,
                optimizedScore=scores.optimized,
                bestIteration=scores.best_iteration,
                timestamp=ensure_utc_iso(row.get("started_at")) or "",
            )
        )
        if len(items) >= limit:
            break
    return items


def _load_run_scores(
    spark, run_ids: list[str], catalog: str, schema: str,
) -> dict[str, RunScores]:
    """Bulk-fetch canonical baseline + optimized + best_iteration for runs.

    Single Delta query for all run_ids (no N+1). Routes the per-run grouping
    + max() through ``compute_run_scores_by_run_id`` so list endpoints get
    the same floor-at-baseline / rolled-back-excluded semantics as detail
    endpoints. PR 4 will replace this with a denormalized
    ``display_accuracy`` column and drop the per-iteration scan.
    """
    from genie_space_optimizer.common.config import TABLE_ITERATIONS
    from genie_space_optimizer.common.delta_helpers import _fqn, run_query

    if not run_ids:
        return {}
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)
    ids_csv = ", ".join(f"'{rid}'" for rid in run_ids)
    try:
        df = run_query(
            spark,
            f"SELECT run_id, iteration, eval_scope, overall_accuracy, "
            f"correct_count, evaluated_count, rolled_back FROM {fqn} "
            f"WHERE run_id IN ({ids_csv})",
        )
    except Exception:
        logger.debug("Bulk run-scores query failed", exc_info=True)
        return {}
    if df.empty:
        return {}
    rows = df.to_dict("records")
    return compute_run_scores_by_run_id(rows, logger=logger)
