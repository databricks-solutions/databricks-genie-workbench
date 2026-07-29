"""System-table queries for the GenieWatch surface.

Reads:
  - system.query.history          (cost / usage / queries per space)
  - system.billing.usage          (warehouse-level billing apportioned to spaces)
  - system.access.audit           (Genie feedback events)
  - system.access.table_lineage   (executed-resource attribution)

All queries run as the *service principal* — system tables are not OBO-readable.
SP must hold `USE CATALOG system`, `USE SCHEMA system.{query,billing,access}`,
and SELECT on each table above. The deploy script grants these automatically.

`scripts/grant_permissions.py` is the source of truth for the grant list.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

from backend.services.auth import get_service_principal_client

logger = logging.getLogger(__name__)


# ─── In-process TTL cache ─────────────────────────────────────────────────
_CACHE_TTL_SECONDS = 300
_LONG_CACHE_TTL_SECONDS = 1800
_CACHE_MAX = 256
_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}


# ─── System-table accessibility signal ────────────────────────────────────
# Observational: None until a core query runs, True once one succeeds, False if
# one fails with a permission error. Surfaced via /api/watch/settings/health so
# the UI can distinguish "missing SP grants" from "no activity" (which both
# otherwise return empty results). See `scripts/grant_permissions.py`.
_SYSTEM_TABLES_ACCESSIBLE: bool | None = None


def _looks_like_permission_error(msg: str) -> bool:
    m = (msg or "").lower()
    return any(
        s in m
        for s in (
            "permission denied",
            "does not have",
            "not authorized",
            "requires permission",
            "access denied",
            "insufficient privileges",
        )
    )


def system_tables_status() -> bool | None:
    """Last observed system-table accessibility (None=unknown, False=grants missing)."""
    return _SYSTEM_TABLES_ACCESSIBLE


def _cache_key(sql: str, parameters: list[StatementParameterListItem]) -> str:
    bag = sorted([(p.name, p.value, getattr(p.type, "value", str(p.type))) for p in parameters])
    return f"{hash(sql)}|{json.dumps(bag)}"


def _cache_get(key: str, ttl_seconds: int = _CACHE_TTL_SECONDS) -> list[dict[str, Any]] | None:
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if not entry:
            return None
        ts, rows = entry
        if time.monotonic() - ts > ttl_seconds:
            _CACHE.pop(key, None)
            return None
        return rows


def _cache_put(key: str, rows: list[dict[str, Any]]) -> None:
    with _CACHE_LOCK:
        if len(_CACHE) >= _CACHE_MAX:
            oldest = min(_CACHE, key=lambda k: _CACHE[k][0])
            _CACHE.pop(oldest, None)
        _CACHE[key] = (time.monotonic(), rows)


def warm_cost_overview_cache(days: int = 7) -> None:
    """Pre-run the Cost-tab overview queries so the cache is hot before users
    hit the page. Call from a background task."""
    for fn, kwargs in (
        (workspace_summary, {"days": days}),
        (daily_volume_all_spaces, {"days": days}),
        (top_spenders, {"days": days, "limit": 10}),
    ):
        try:
            fn(**kwargs)
        except Exception:
            logger.warning("warmup of %s failed", fn.__name__, exc_info=True)


def _warehouse_id() -> str:
    wh = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
    if not wh:
        raise RuntimeError("SQL_WAREHOUSE_ID env var is empty — system table queries cannot run.")
    return wh


def _client() -> WorkspaceClient:
    return get_service_principal_client()


def _run(
    sql: str,
    parameters: list[StatementParameterListItem],
    poll_total_seconds: int = 180,
    poll_interval_seconds: float = 2.0,
    track_health: bool = True,
    ttl_seconds: int = _CACHE_TTL_SECONDS,
) -> list[dict[str, Any]]:
    global _SYSTEM_TABLES_ACCESSIBLE
    key = _cache_key(sql, parameters)
    cached = _cache_get(key, ttl_seconds=ttl_seconds)
    if cached is not None:
        return cached

    client = _client()
    resp = client.statement_execution.execute_statement(
        warehouse_id=_warehouse_id(),
        statement=sql,
        parameters=parameters,
        wait_timeout="50s",
    )

    statement_id = resp.statement_id if resp else None
    deadline = time.monotonic() + poll_total_seconds
    while resp and resp.status and resp.status.state in (
        StatementState.PENDING, StatementState.RUNNING,
    ):
        if time.monotonic() > deadline or not statement_id:
            logger.warning(
                "system-table query %s still %s after %ss; returning [].",
                statement_id, resp.status.state, poll_total_seconds,
            )
            try:
                client.statement_execution.cancel_execution(statement_id=statement_id)
            except Exception:
                pass
            return []
        time.sleep(poll_interval_seconds)
        resp = client.statement_execution.get_statement(statement_id=statement_id)

    if resp is None or resp.status is None:
        return []
    state = resp.status.state
    if state != StatementState.SUCCEEDED:
        err = resp.status.error
        msg = err.message if err else state
        if track_health and _looks_like_permission_error(str(msg)):
            _SYSTEM_TABLES_ACCESSIBLE = False
        logger.warning("system-table query %s ended in %s: %s", statement_id, state, msg)
        return []

    if track_health:
        _SYSTEM_TABLES_ACCESSIBLE = True

    if not resp.result or not resp.result.data_array:
        return []
    schema = resp.manifest.schema if resp.manifest else None
    if schema is None or schema.columns is None:
        return []
    cols = [c.name for c in schema.columns]
    rows = [
        {cols[i]: row[i] for i in range(len(cols))}
        for row in resp.result.data_array
    ]
    _cache_put(key, rows)
    return rows


def _p(name: str, value: Any, value_type: str = "STRING") -> StatementParameterListItem:
    return StatementParameterListItem(name=name, value=str(value), type=value_type)


# ─── Workspace scoping ─────────────────────────────────────────────────────
# The geniewatch system-table queries were originally account-wide (one deploy
# monitoring every workspace on the metastore). Genie Workbench deploys per
# workspace, so the cross-workspace listing queries (top spenders, resource
# rollup/graph) must be constrained to this app's own workspace.
#
# Scoping is done with the `workspace_id` column present on every system table
# we read (query.history, billing.usage, access.table_lineage, access.audit) —
# a Genie Agent's queries always run on a warehouse in the agent's own
# workspace, so `workspace_id` cleanly identifies the owning workspace and also
# preserves history for spaces that have since been deleted. The Genie API
# remains the source for the *set* of spaces and their metadata (titles,
# configured data sources); it has no cost/usage/lineage data of its own.

_CURRENT_WS_ID: str | None = None
_CURRENT_WS_ID_RESOLVED = False


def _current_workspace_id() -> str | None:
    """This app's own workspace id, resolved once via the SDK (cached).

    Returns None if resolution fails; `_ws_clause` then fails *closed*
    (scopes the query to no rows) rather than leaking other workspaces' data.
    """
    global _CURRENT_WS_ID, _CURRENT_WS_ID_RESOLVED
    if not _CURRENT_WS_ID_RESOLVED:
        _CURRENT_WS_ID_RESOLVED = True
        try:
            _CURRENT_WS_ID = str(_client().get_workspace_id())
        except Exception as e:  # noqa: BLE001 - never break queries on this
            logger.warning("could not resolve current workspace id: %s", e)
            _CURRENT_WS_ID = None
    return _CURRENT_WS_ID


def _ws_clause(params: list[StatementParameterListItem], col: str = "workspace_id") -> str:
    """Return an `AND <col> = :ws_id` predicate (and append its bind param).

    Substituted into the `{ws}` slot of a scoped query. If the workspace id
    can't be resolved, returns a predicate that matches no rows (fail-closed)
    so a resolution failure never leaks other workspaces' data.
    """
    wid = _current_workspace_id()
    if not wid:
        logger.warning("workspace id unresolved — scoping query to no rows (fail-closed)")
        return "AND 1=0"
    params.append(_p("ws_id", wid))
    return f"AND {col} = :ws_id"


# ─── Cost ─────────────────────────────────────────────────────────────────

_COST_PER_SPACE_SQL = """
WITH q AS (
    SELECT date_trunc('day', start_time) AS d,
           compute.warehouse_id AS wh,
           date_trunc('hour', start_time) AS hr,
           SUM(total_task_duration_ms) AS task_ms,
           COUNT(*) AS n
    FROM system.query.history
    WHERE query_source.genie_space_id = :space_id
      AND start_time >= current_date() - :days
      AND total_task_duration_ms > 0
    GROUP BY 1, 2, 3
), hr_total AS (
    SELECT compute.warehouse_id AS wh,
           date_trunc('hour', start_time) AS hr,
           SUM(total_task_duration_ms) AS hr_task_ms
    FROM system.query.history
    WHERE start_time >= current_date() - :days
      AND total_task_duration_ms > 0
    GROUP BY 1, 2
), hr_cost AS (
    SELECT u.usage_metadata.warehouse_id AS wh,
           date_trunc('hour', u.usage_start_time) AS hr,
           SUM(u.usage_quantity) AS hr_dbus,
           SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) AS hr_usd
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices p
      ON u.sku_name = p.sku_name
     AND u.cloud = p.cloud
     AND u.usage_start_time >= p.price_start_time
     AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
    WHERE u.usage_metadata.warehouse_id IS NOT NULL
      AND u.usage_start_time >= current_date() - :days
    GROUP BY 1, 2
)
SELECT q.d AS day,
       q.wh AS warehouse_id,
       SUM(q.n) AS query_count,
       SUM((q.task_ms / NULLIF(t.hr_task_ms, 0)) * COALESCE(c.hr_dbus, 0)) AS approx_dbus,
       SUM((q.task_ms / NULLIF(t.hr_task_ms, 0)) * COALESCE(c.hr_usd, 0))  AS approx_usd
FROM q
JOIN hr_total t USING (wh, hr)
LEFT JOIN hr_cost c USING (wh, hr)
GROUP BY q.d, q.wh
ORDER BY q.d
"""


def cost_per_space(space_id: str, days: int = 7) -> list[dict[str, Any]]:
    return _run(_COST_PER_SPACE_SQL, [
        _p("space_id", space_id),
        _p("days", days, "INT"),
    ])


_TOP_SPENDERS_SQL = """
WITH q AS (
    SELECT query_source.genie_space_id AS space_id,
           workspace_id,
           compute.warehouse_id AS wh,
           date_trunc('hour', start_time) AS hr,
           SUM(total_task_duration_ms) AS task_ms,
           COUNT(*) AS n
    FROM system.query.history
    WHERE query_source.genie_space_id IS NOT NULL
      AND start_time >= current_date() - :days
      AND total_task_duration_ms > 0
      {ws}
    GROUP BY 1, 2, 3, 4
), hr_total AS (
    SELECT compute.warehouse_id AS wh,
           date_trunc('hour', start_time) AS hr,
           SUM(total_task_duration_ms) AS hr_task_ms
    FROM system.query.history
    WHERE start_time >= current_date() - :days
      AND total_task_duration_ms > 0
    GROUP BY 1, 2
), hr_cost AS (
    SELECT u.usage_metadata.warehouse_id AS wh,
           date_trunc('hour', u.usage_start_time) AS hr,
           SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) AS hr_usd
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices p
      ON u.sku_name = p.sku_name
     AND u.cloud = p.cloud
     AND u.usage_start_time >= p.price_start_time
     AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
    WHERE u.usage_metadata.warehouse_id IS NOT NULL
      AND u.usage_start_time >= current_date() - :days
    GROUP BY 1, 2
)
SELECT q.space_id,
       q.workspace_id,
       SUM(q.n) AS query_count,
       SUM((q.task_ms / NULLIF(t.hr_task_ms, 0)) * COALESCE(c.hr_usd, 0)) AS approx_usd
FROM q
JOIN hr_total t USING (wh, hr)
LEFT JOIN hr_cost c USING (wh, hr)
GROUP BY q.space_id, q.workspace_id
ORDER BY approx_usd DESC NULLS LAST
LIMIT :limit
"""


_WORKSPACE_NAMES_CACHE: dict[str, str] = {}
_WORKSPACE_NAMES_DISABLED: bool = False


def _workspace_names(workspace_ids: set[str]) -> dict[str, str]:
    global _WORKSPACE_NAMES_DISABLED
    if _WORKSPACE_NAMES_DISABLED or not workspace_ids:
        return {}
    missing = [wid for wid in workspace_ids if wid and wid not in _WORKSPACE_NAMES_CACHE]
    if not missing:
        return {wid: _WORKSPACE_NAMES_CACHE[wid] for wid in workspace_ids if wid in _WORKSPACE_NAMES_CACHE}

    placeholders = ", ".join(f":w{i}" for i in range(len(missing)))
    sql = f"""
SELECT workspace_id, workspace_name
FROM system.access.workspaces_latest
WHERE workspace_id IN ({placeholders})
"""
    params = [_p(f"w{i}", wid) for i, wid in enumerate(missing)]
    # workspaces_latest is optional/newer; its absence must not flip the global
    # system-tables-accessible signal (the core tables may still be readable).
    rows = _run(sql, params, track_health=False)
    if not rows:
        _WORKSPACE_NAMES_DISABLED = True
        return {}
    for r in rows:
        wid, name = r.get("workspace_id"), r.get("workspace_name")
        if wid and name:
            _WORKSPACE_NAMES_CACHE[wid] = name
    return {wid: _WORKSPACE_NAMES_CACHE[wid] for wid in workspace_ids if wid in _WORKSPACE_NAMES_CACHE}


def top_spenders(days: int = 7, limit: int = 10) -> list[dict[str, Any]]:
    params = [_p("days", days, "INT"), _p("limit", limit, "INT")]
    sql = _TOP_SPENDERS_SQL.format(ws=_ws_clause(params))
    rows = _run(sql, params, ttl_seconds=_LONG_CACHE_TTL_SECONDS)
    workspace_ids = {r.get("workspace_id") for r in rows if r.get("workspace_id")}
    names = _workspace_names(workspace_ids)
    for r in rows:
        wid = r.get("workspace_id")
        r["workspace_name"] = names.get(wid) if wid else None
    return rows


_COST_PER_CONVERSATION_SQL = """
WITH q AS (
    SELECT statement_id,
           query_source.genie_space_id AS space_id,
           compute.warehouse_id AS wh,
           date_trunc('hour', start_time) AS hr,
           start_time,
           total_task_duration_ms AS task_ms
    FROM system.query.history
    WHERE query_source.genie_space_id = :space_id
      AND start_time >= current_date() - :days
      AND total_task_duration_ms > 0
), hr_total AS (
    SELECT compute.warehouse_id AS wh,
           date_trunc('hour', start_time) AS hr,
           SUM(total_task_duration_ms) AS hr_task_ms
    FROM system.query.history
    WHERE start_time >= current_date() - :days
      AND total_task_duration_ms > 0
    GROUP BY 1, 2
), hr_cost AS (
    SELECT u.usage_metadata.warehouse_id AS wh,
           date_trunc('hour', u.usage_start_time) AS hr,
           SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) AS hr_usd
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices p
      ON u.sku_name = p.sku_name
     AND u.cloud = p.cloud
     AND u.usage_start_time >= p.price_start_time
     AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
    WHERE u.usage_metadata.warehouse_id IS NOT NULL
      AND u.usage_start_time >= current_date() - :days
    GROUP BY 1, 2
), attributed AS (
    SELECT q.statement_id,
           q.start_time,
           q.task_ms,
           (q.task_ms / NULLIF(t.hr_task_ms, 0)) * COALESCE(c.hr_usd, 0) AS query_usd
    FROM q
    JOIN hr_total t USING (wh, hr)
    LEFT JOIN hr_cost c USING (wh, hr)
), audit_events AS (
    SELECT request_params.conversation_id AS conversation_id,
           user_identity.email AS user_email,
           event_time
    FROM system.access.audit
    WHERE service_name = 'aibiGenie'
      AND request_params.space_id = :space_id
      AND request_params.conversation_id IS NOT NULL
      AND event_time >= current_date() - :days - 1
), correlations AS (
    SELECT a.statement_id,
           a.start_time,
           a.query_usd,
           ae.conversation_id,
           ae.user_email,
           ae.event_time,
           ROW_NUMBER() OVER (
             PARTITION BY a.statement_id
             ORDER BY a.start_time - ae.event_time ASC
           ) AS rn
    FROM attributed a
    JOIN audit_events ae
      ON a.start_time >= ae.event_time
     AND a.start_time <= ae.event_time + INTERVAL 10 MINUTE
)
SELECT conversation_id,
       ANY_VALUE(user_email) AS user_email,
       MIN(start_time) AS first_query_at,
       MAX(start_time) AS last_query_at,
       COUNT(*) AS query_count,
       ROUND(SUM(query_usd), 4) AS approx_usd
FROM correlations
WHERE rn = 1
GROUP BY conversation_id
ORDER BY approx_usd DESC NULLS LAST
LIMIT :limit
"""


def cost_per_conversation(
    space_id: str, days: int = 7, limit: int = 50,
) -> list[dict[str, Any]]:
    return _run(_COST_PER_CONVERSATION_SQL, [
        _p("space_id", space_id),
        _p("days", days, "INT"),
        _p("limit", limit, "INT"),
    ])


# ─── Usage ────────────────────────────────────────────────────────────────

_USAGE_PER_SPACE_SQL = """
SELECT date_trunc('day', start_time) AS day,
       COUNT(*) AS queries,
       APPROX_PERCENTILE(total_duration_ms, 0.5) AS p50_ms,
       APPROX_PERCENTILE(total_duration_ms, 0.95) AS p95_ms,
       SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS errors,
       COUNT(DISTINCT executed_by) AS distinct_users
FROM system.query.history
WHERE query_source.genie_space_id = :space_id
  AND start_time >= current_date() - :days
GROUP BY 1
ORDER BY 1
"""


def usage_per_space(space_id: str, days: int = 30) -> list[dict[str, Any]]:
    return _run(_USAGE_PER_SPACE_SQL, [
        _p("space_id", space_id),
        _p("days", days, "INT"),
    ])


_USAGE_SUMMARY_SQL = """
SELECT
    query_source.genie_space_id AS space_id,
    COUNT(*) AS queries,
    COUNT(DISTINCT executed_by) AS distinct_users,
    MAX(start_time) AS last_query_at
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_date() - :days
  {ws}
GROUP BY 1
"""


def usage_summary_all_spaces(days: int = 7) -> list[dict[str, Any]]:
    params = [_p("days", days, "INT")]
    sql = _USAGE_SUMMARY_SQL.format(ws=_ws_clause(params))
    return _run(sql, params)


# ─── Workspace overview (native cost-tab dashboard) ────────────────────────
# Powers the app's own Genie Agents Overview panel (KPI tiles + daily trend),
# replacing the embedded AI/BI dashboard. Cost is attributed the same way as
# top_spenders: a genie query gets a share of its warehouse-hour's USD by its
# share of that hour's task time. Workspace-scoped + fail-closed via {ws}.

_WORKSPACE_SUMMARY_SQL = """
WITH q AS (
    SELECT query_source.genie_space_id AS space_id,
           executed_by,
           compute.warehouse_id AS wh,
           date_trunc('hour', start_time) AS hr,
           total_task_duration_ms AS task_ms
    FROM system.query.history
    WHERE query_source.genie_space_id IS NOT NULL
      AND start_time >= current_date() - :days
      AND total_task_duration_ms > 0
      {ws}
), hr_total AS (
    SELECT compute.warehouse_id AS wh,
           date_trunc('hour', start_time) AS hr,
           SUM(total_task_duration_ms) AS hr_task_ms
    FROM system.query.history
    WHERE start_time >= current_date() - :days
      AND total_task_duration_ms > 0
    GROUP BY 1, 2
), hr_cost AS (
    SELECT u.usage_metadata.warehouse_id AS wh,
           date_trunc('hour', u.usage_start_time) AS hr,
           SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) AS hr_usd
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices p
      ON u.sku_name = p.sku_name
     AND u.cloud = p.cloud
     AND u.usage_start_time >= p.price_start_time
     AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
    WHERE u.usage_metadata.warehouse_id IS NOT NULL
      AND u.usage_start_time >= current_date() - :days
    GROUP BY 1, 2
), attributed AS (
    SELECT q.space_id,
           q.executed_by,
           (q.task_ms / NULLIF(t.hr_task_ms, 0)) * COALESCE(c.hr_usd, 0) AS query_usd
    FROM q
    JOIN hr_total t USING (wh, hr)
    LEFT JOIN hr_cost c USING (wh, hr)
), totals AS (
    SELECT COUNT(DISTINCT space_id) AS active_spaces,
           COUNT(*) AS total_queries,
           COUNT(DISTINCT executed_by) AS distinct_users,
           ROUND(SUM(query_usd), 2) AS approx_usd
    FROM attributed
), fb AS (
    SELECT SUM(CASE WHEN request_params.feedback_rating = 'THUMBS_UP'   THEN 1 ELSE 0 END) AS pos_feedback,
           SUM(CASE WHEN request_params.feedback_rating = 'THUMBS_DOWN' THEN 1 ELSE 0 END) AS neg_feedback
    FROM system.access.audit
    WHERE service_name = 'aibiGenie'
      AND action_name = 'updateConversationMessageFeedback'
      AND event_time >= current_date() - :days
      {ws}
)
SELECT t.active_spaces, t.total_queries, t.distinct_users, t.approx_usd,
       COALESCE(f.pos_feedback, 0) AS pos_feedback,
       COALESCE(f.neg_feedback, 0) AS neg_feedback
FROM totals t CROSS JOIN fb f
"""


def workspace_summary(days: int = 7) -> dict[str, Any]:
    # One {ws} bind reused across both occurrences (q + fb); resolved once so
    # only a single ws_id param is appended.
    params = [_p("days", days, "INT")]
    sql = _WORKSPACE_SUMMARY_SQL.format(ws=_ws_clause(params))
    rows = _run(sql, params, ttl_seconds=_LONG_CACHE_TTL_SECONDS)
    return rows[0] if rows else {}


# Zero-fill every day in the window (LEFT JOIN a generated date series) so the
# trend always spans the full last-N-days range, even when only a few days have
# activity — otherwise the chart (and its axis) collapses to the single day that
# had queries.
_DAILY_VOLUME_ALL_SQL = """
WITH days AS (
    SELECT explode(sequence(current_date() - :days, current_date(), interval 1 day)) AS day
), vol AS (
    SELECT to_date(start_time) AS day,
           COUNT(*) AS queries
    FROM system.query.history
    WHERE query_source.genie_space_id IS NOT NULL
      AND start_time >= current_date() - :days
      {ws}
    GROUP BY 1
)
SELECT d.day AS day,
       COALESCE(v.queries, 0) AS queries
FROM days d
LEFT JOIN vol v ON d.day = v.day
ORDER BY d.day
"""


def daily_volume_all_spaces(days: int = 30) -> list[dict[str, Any]]:
    params = [_p("days", days, "INT")]
    sql = _DAILY_VOLUME_ALL_SQL.format(ws=_ws_clause(params))
    return _run(sql, params, ttl_seconds=_LONG_CACHE_TTL_SECONDS)


_TOP_QUERIES_SQL = """
SELECT statement_id,
       executed_by,
       start_time,
       total_duration_ms,
       execution_status,
       statement_text
FROM system.query.history
WHERE query_source.genie_space_id = :space_id
  AND start_time >= current_date() - :days
ORDER BY total_duration_ms DESC NULLS LAST
LIMIT :limit
"""


def top_expensive_queries(space_id: str, days: int = 7, limit: int = 20) -> list[dict[str, Any]]:
    return _run(_TOP_QUERIES_SQL, [
        _p("space_id", space_id),
        _p("days", days, "INT"),
        _p("limit", limit, "INT"),
    ])


# ─── Feedback ─────────────────────────────────────────────────────────────
#
# The audit log stores `feedback_rating` as 'THUMBS_UP' / 'THUMBS_DOWN'.
# All feedback queries normalize these to 'POSITIVE' / 'NEGATIVE' so callers
# (routers, summary aggregations, frontend filters) can use a single
# canonical vocabulary.

_FEEDBACK_PER_SPACE_SQL = """
SELECT event_time,
       user_identity.email AS user_email,
       request_params.space_id AS space_id,
       CASE
         WHEN request_params.feedback_rating = 'THUMBS_UP'   THEN 'POSITIVE'
         WHEN request_params.feedback_rating = 'THUMBS_DOWN' THEN 'NEGATIVE'
         ELSE request_params.feedback_rating
       END AS rating,
       request_params.comment AS comment,
       request_params.message_id AS message_id,
       request_params.conversation_id AS conversation_id
FROM system.access.audit
WHERE service_name = 'aibiGenie'
  AND action_name = 'updateConversationMessageFeedback'
  AND request_params.space_id = :space_id
  AND event_time >= current_date() - :days
ORDER BY event_time DESC
LIMIT :limit
"""


def feedback_per_space(space_id: str, days: int = 30, limit: int = 200) -> list[dict[str, Any]]:
    return _run(_FEEDBACK_PER_SPACE_SQL, [
        _p("space_id", space_id),
        _p("days", days, "INT"),
        _p("limit", limit, "INT"),
    ])


_FEEDBACK_SUMMARY_SQL = """
SELECT request_params.space_id AS space_id,
       SUM(CASE WHEN request_params.feedback_rating = 'THUMBS_UP'   THEN 1 ELSE 0 END) AS pos,
       SUM(CASE WHEN request_params.feedback_rating = 'THUMBS_DOWN' THEN 1 ELSE 0 END) AS neg,
       COUNT(*) AS total
FROM system.access.audit
WHERE service_name = 'aibiGenie'
  AND action_name = 'updateConversationMessageFeedback'
  AND request_params.feedback_rating IS NOT NULL
  AND event_time >= current_date() - :days
  {ws}
GROUP BY 1
"""


def feedback_summary_all_spaces(days: int = 7) -> list[dict[str, Any]]:
    params = [_p("days", days, "INT")]
    sql = _FEEDBACK_SUMMARY_SQL.format(ws=_ws_clause(params))
    return _run(sql, params)


_FEEDBACK_EVENTS_ALL_SQL = """
SELECT event_time,
       user_identity.email AS user_email,
       request_params.space_id AS space_id,
       CASE
         WHEN request_params.feedback_rating = 'THUMBS_UP'   THEN 'POSITIVE'
         WHEN request_params.feedback_rating = 'THUMBS_DOWN' THEN 'NEGATIVE'
         ELSE request_params.feedback_rating
       END AS rating,
       request_params.comment AS comment,
       request_params.message_id AS message_id,
       request_params.conversation_id AS conversation_id
FROM system.access.audit
WHERE service_name = 'aibiGenie'
  AND action_name = 'updateConversationMessageFeedback'
  AND request_params.space_id IS NOT NULL
  AND request_params.feedback_rating IS NOT NULL
  AND event_time >= current_date() - :days
  {ws}
ORDER BY event_time DESC
LIMIT :limit
"""


def feedback_events_all_spaces(days: int = 7, limit: int = 500) -> list[dict[str, Any]]:
    params = [_p("days", days, "INT"), _p("limit", limit, "INT")]
    sql = _FEEDBACK_EVENTS_ALL_SQL.format(ws=_ws_clause(params))
    return _run(sql, params)



# ─── Lineage / executed resources ─────────────────────────────────────────

_EXECUTED_RESOURCES_SQL = """
SELECT entity_metadata.genie_space_id AS space_id,
       source_table_full_name AS full_name,
       COUNT(*) AS query_count,
       MAX(event_time) AS last_used
FROM system.access.table_lineage
WHERE entity_metadata.genie_space_id = :space_id
  AND source_table_full_name IS NOT NULL
  AND event_time >= current_date() - :days
GROUP BY 1, 2
ORDER BY query_count DESC
"""


def executed_resources(space_id: str, days: int = 30) -> list[dict[str, Any]]:
    return _run(_EXECUTED_RESOURCES_SQL, [
        _p("space_id", space_id),
        _p("days", days, "INT"),
    ])


_RESOURCE_ROLLUP_SQL = """
SELECT source_table_full_name AS full_name,
       COUNT(DISTINCT entity_metadata.genie_space_id) AS space_count,
       COUNT(*) AS query_count_total,
       MAX(event_time) AS last_used
FROM system.access.table_lineage
WHERE entity_metadata.genie_space_id IS NOT NULL
  AND source_table_full_name IS NOT NULL
  AND event_time >= current_date() - :days
  {ws}
GROUP BY 1
ORDER BY space_count DESC, query_count_total DESC
LIMIT :limit
"""


def resource_rollup(days: int = 30, limit: int = 50) -> list[dict[str, Any]]:
    params = [_p("days", days, "INT"), _p("limit", limit, "INT")]
    sql = _RESOURCE_ROLLUP_SQL.format(ws=_ws_clause(params))
    return _run(sql, params)


_RESOURCE_SPACES_SQL = """
SELECT DISTINCT entity_metadata.genie_space_id AS space_id
FROM system.access.table_lineage
WHERE source_table_full_name = :full_name
  AND entity_metadata.genie_space_id IS NOT NULL
  AND event_time >= current_date() - :days
  {ws}
"""


def spaces_using_resource(full_name: str, days: int = 30) -> list[str]:
    params = [_p("full_name", full_name), _p("days", days, "INT")]
    sql = _RESOURCE_SPACES_SQL.format(ws=_ws_clause(params))
    rows = _run(sql, params)
    return [r["space_id"] for r in rows if r.get("space_id")]


_RESOURCE_GRAPH_SQL = """
SELECT entity_metadata.genie_space_id AS space_id,
       workspace_id,
       source_table_full_name        AS full_name,
       COUNT(*)                      AS query_count,
       MAX(event_time)               AS last_used
FROM system.access.table_lineage
WHERE entity_metadata.genie_space_id IS NOT NULL
  AND source_table_full_name IS NOT NULL
  AND event_time >= current_date() - :days
  {ws}
GROUP BY 1, 2, 3
ORDER BY query_count DESC
LIMIT :limit
"""


def resource_graph_edges(days: int = 30, limit: int = 2000) -> list[dict[str, Any]]:
    params = [_p("days", days, "INT"), _p("limit", limit, "INT")]
    sql = _RESOURCE_GRAPH_SQL.format(ws=_ws_clause(params))
    return _run(sql, params)
