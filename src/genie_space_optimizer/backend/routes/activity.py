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
from ..utils import ensure_utc_iso, safe_float
from .._spark import get_spark

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

    baseline_scores = _load_baseline_scores(
        spark, list(df["run_id"]), config.catalog, config.schema_name,
    )

    items: list[ActivityItem] = []
    for _, row in df.iterrows():
        row_space_id = row.get("space_id", "")
        if access_by_space.get(row_space_id) is None:
            continue
        run_id_val = row.get("run_id", "")
        items.append(
            ActivityItem(
                runId=run_id_val,
                spaceId=row_space_id,
                spaceName=row.get("domain", ""),
                status=row.get("status", ""),
                initiatedBy=row.get("triggered_by") or "system",
                baselineScore=safe_float(baseline_scores.get(run_id_val)),
                optimizedScore=safe_float(row.get("best_accuracy")),
                timestamp=ensure_utc_iso(row.get("started_at")) or "",
            )
        )
        if len(items) >= limit:
            break
    return items


def _load_baseline_scores(
    spark, run_ids: list[str], catalog: str, schema: str,
) -> dict[str, float | None]:
    """Bulk-fetch baseline accuracy (iteration 0, full eval) for a list of runs."""
    from genie_space_optimizer.common.config import TABLE_ITERATIONS
    from genie_space_optimizer.common.delta_helpers import _fqn, run_query

    if not run_ids:
        return {}
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)
    ids_csv = ", ".join(f"'{rid}'" for rid in run_ids)
    try:
        df = run_query(
            spark,
            f"SELECT run_id, overall_accuracy FROM {fqn} "
            f"WHERE run_id IN ({ids_csv}) AND iteration = 0 AND eval_scope = 'full'",
        )
    except Exception:
        return {}
    result: dict[str, float | None] = {}
    if not df.empty:
        for _, row in df.iterrows():
            result[row.get("run_id", "")] = safe_float(row.get("overall_accuracy"))
    return result
