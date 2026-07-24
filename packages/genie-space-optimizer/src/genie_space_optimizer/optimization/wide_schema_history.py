"""Bounded, privacy-preserving query-history evidence for wide schemas."""

from __future__ import annotations

import hashlib
import copy
import json
import logging
import os
import re
import time
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Iterable

from genie_space_optimizer.optimization.wide_schema import (
    CONTRACT_VERSION,
    QUERY_ROLE_WEIGHTS,
    content_hash,
    render_identifier,
    sql_column_evidence,
)

logger = logging.getLogger(__name__)

HISTORY_LOOKBACK_DAYS = 30
MAX_SYSTEM_STATEMENTS = 10_000
MAX_REST_STATEMENTS = 5_000
MAX_STATEMENT_BYTES = 256 * 1024
MAX_RAW_SQL_BYTES = 50 * 1024 * 1024
MAX_HISTORY_PARSE_SECONDS = 120
AGGREGATE_CACHE_TTL_SECONDS = 24 * 60 * 60
_MAX_AGGREGATE_CACHE_ENTRIES = 128
_AGGREGATE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_AGGREGATE_CACHE_LOCK = threading.Lock()

_RECENCY = ((7, "0_7_days", 1.0), (14, "8_14_days", 0.5), (30, "15_30_days", 0.25))
_ERROR_CODE_RE = re.compile(r"\[([A-Z][A-Z0-9_.-]+)\]")


def _query_tags(run_id: str, purpose: str = "history_collection") -> list[Any]:
    from genie_space_optimizer.common.query_tags import gso_query_tags

    return gso_query_tags(purpose=purpose, run_id=run_id)


def _response_rows(response: Any) -> list[dict[str, Any]]:
    manifest_schema = response.manifest.schema if response.manifest else None
    schema_columns = manifest_schema.columns if manifest_schema else None
    names = [str(column.name or "") for column in (schema_columns or [])]
    data = response.result.data_array if response.result and response.result.data_array else []
    return [dict(zip(names, row)) for row in data]


def _safe_error_code(error: Any) -> str:
    """Return a bounded diagnostic code without persisting error text."""
    if error is None:
        return "UNKNOWN_ERROR"
    for attribute in ("error_code", "code"):
        value = getattr(error, attribute, None)
        if value:
            return str(value)[:128]
    match = _ERROR_CODE_RE.search(str(error))
    if match:
        return match.group(1)[:128]
    return type(error).__name__[:128]


def _system_history_rows(w: Any, warehouse_id: str, *, run_id: str) -> list[dict[str, Any]]:
    from databricks.sdk.service.sql import Disposition, Format, StatementState

    try:
        workspace_id = int(w.get_workspace_id())
    except Exception as exc:
        raise RuntimeError(
            "current workspace ID is required to scope system.query.history"
        ) from exc
    if workspace_id <= 0:
        raise RuntimeError(
            "current workspace ID is required to scope system.query.history"
        )

    statement = f"""
        SELECT
          statement_id,
          statement_text,
          start_time,
          executed_by,
          query_source,
          query_tags
        FROM system.query.history
        WHERE start_time >= current_timestamp() - INTERVAL {HISTORY_LOOKBACK_DAYS} DAYS
          AND execution_status = 'FINISHED'
          AND workspace_id = {workspace_id}
        ORDER BY start_time DESC
        LIMIT {MAX_SYSTEM_STATEMENTS}
    """
    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        query_tags=_query_tags(run_id),
    )
    if not response.status or response.status.state != StatementState.SUCCEEDED:
        code = _safe_error_code(
            response.status.error if response.status else None
        )
        raise RuntimeError(
            f"system.query.history bounded read failed [{code}]"
        )
    return _response_rows(response)


def _rest_history_rows(
    w: Any,
    workload_warehouse_ids: list[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    from databricks.sdk.service.sql import QueryFilter, QueryStatus, TimeRange

    now_ms = int(time.time() * 1000)
    start_ms = now_ms - HISTORY_LOOKBACK_DAYS * 24 * 60 * 60 * 1000
    rows: list[dict[str, Any]] = []
    inaccessible: list[str] = []
    warehouses = list(dict.fromkeys(
        str(value).strip()
        for value in workload_warehouse_ids
        if str(value).strip()
    ))
    active: deque[tuple[str, str | None]] = deque(
        (warehouse, None) for warehouse in warehouses
    )
    while active and len(rows) < MAX_REST_STATEMENTS:
        round_size = len(active)
        for round_index in range(round_size):
            warehouse, token = active.popleft()
            remaining = MAX_REST_STATEMENTS - len(rows)
            if remaining <= 0:
                break
            warehouses_left = round_size - round_index
            max_results = min(
                999,
                max(1, remaining // max(1, warehouses_left)),
            )
            try:
                response = w.query_history.list(
                    filter_by=QueryFilter(
                        query_start_time_range=TimeRange(start_time_ms=start_ms, end_time_ms=now_ms),
                        statuses=[QueryStatus.FINISHED],
                        warehouse_ids=[warehouse],
                    ),
                    include_metrics=False,
                    max_results=max_results,
                    page_token=token,
                )
                for item in response.res or []:
                    raw = item.as_dict() if hasattr(item, "as_dict") else dict(item)
                    raw["statement_text"] = raw.pop("query_text", None)
                    raw["statement_id"] = raw.get("query_id")
                    raw["start_time_ms"] = raw.get("query_start_time_ms")
                    raw["executed_by"] = raw.get("executed_as_user_name") or raw.get("user_name")
                    rows.append(raw)
                    if len(rows) >= MAX_REST_STATEMENTS:
                        break
                next_token = (
                    response.next_page_token
                    if response.has_next_page
                    else None
                )
                if next_token and len(rows) < MAX_REST_STATEMENTS:
                    active.append((warehouse, next_token))
            except Exception:
                inaccessible.append(warehouse)
                logger.info(
                    "Query History REST unavailable for warehouse %s",
                    warehouse,
                    exc_info=True,
                )
    rows.sort(key=lambda row: _start_epoch(row), reverse=True)
    return rows[:MAX_REST_STATEMENTS], inaccessible


def _start_epoch(row: dict[str, Any]) -> float:
    raw_ms = row.get("start_time_ms") or row.get("query_start_time_ms")
    if raw_ms is not None:
        try:
            return float(raw_ms) / 1000.0
        except (TypeError, ValueError):
            return 0.0
    raw = row.get("start_time")
    if isinstance(raw, datetime):
        return raw.replace(tzinfo=raw.tzinfo or timezone.utc).timestamp()
    if raw:
        try:
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp()
        except ValueError:
            return 0.0
    return 0.0


def _tags(row: dict[str, Any]) -> dict[str, str]:
    raw = row.get("query_tags") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return {}
    if isinstance(raw, list):
        return {
            str(item.get("key") or "").casefold(): str(item.get("value") or "")
            for item in raw if isinstance(item, dict)
        }
    if isinstance(raw, dict):
        return {str(key).casefold(): str(value) for key, value in raw.items()}
    return {}


def _query_source_job_id(row: dict[str, Any]) -> str:
    raw = row.get("query_source") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return ""
    if not isinstance(raw, dict):
        return ""
    job_info = raw.get("job_info") or {}
    return str(job_info.get("job_id") or "") if isinstance(job_info, dict) else ""


def _legacy_gso_shape(sql: str) -> bool:
    normalized = " ".join(str(sql or "").casefold().split())
    markers = (
        "tablesample (100 rows)",
        "_card_",
        "collect_set(",
        "select * from (explain",
        "genie_opt_",
    )
    return any(marker in normalized for marker in markers)


def _excluded(
    row: dict[str, Any],
    sql: str,
    *,
    gso_job_id: str,
    service_principal_identities: set[str],
) -> bool:
    if gso_job_id and _query_source_job_id(row) == gso_job_id:
        return True
    tags = _tags(row)
    if tags.get("application") == "genie_workbench" or tags.get("component") == "gso":
        return True
    identity = str(row.get("executed_by") or "").casefold()
    if identity and identity in service_principal_identities:
        return True
    return _legacy_gso_shape(sql)


def _normalized_shape_hash(sql: str) -> str | None:
    try:
        import sqlglot
        from sqlglot import expressions as exp

        tree = sqlglot.parse_one(sql, read="databricks")
        tree = tree.transform(
            lambda node: exp.Literal.string("?") if isinstance(node, exp.Literal) else node
        )
        normalized = tree.sql(dialect="databricks", comments=False, pretty=False)
    except Exception:
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _recency(start_epoch: float, now_epoch: float) -> tuple[str, float] | None:
    age_days = max(0.0, (now_epoch - start_epoch) / 86400.0)
    for max_days, bucket, multiplier in _RECENCY:
        if age_days <= max_days:
            return bucket, multiplier
    return None


def normalize_history_rows(
    rows: Iterable[dict[str, Any]],
    inventory: dict[str, Any],
    *,
    source_mode: str,
    source_scope: list[str],
    gso_job_id: str = "",
    service_principal_identities: Iterable[str] = (),
    max_statements: int,
    inaccessible_scope: list[str] | None = None,
) -> dict[str, Any]:
    """Parse raw SQL in memory and return only normalized aggregate evidence."""
    started = time.monotonic()
    now_epoch = time.time()
    identities = {str(value).casefold() for value in service_principal_identities if value}
    counters: defaultdict[str, int] = defaultdict(int)
    total_bytes = 0
    accepted = 0
    seen_shapes: set[str] = set()
    column_shapes: dict[tuple[str, ...], dict[str, dict[str, Any]]] = defaultdict(dict)

    for row in rows:
        if accepted >= max_statements:
            counters["count_limit_reached"] += 1
            break
        if time.monotonic() - started >= MAX_HISTORY_PARSE_SECONDS:
            counters["parse_deadline_reached"] += 1
            break
        sql = str(row.get("statement_text") or "")
        size = len(sql.encode("utf-8"))
        if size > MAX_STATEMENT_BYTES:
            counters["oversized"] += 1
            continue
        if total_bytes + size > MAX_RAW_SQL_BYTES:
            counters["byte_limit_reached"] += 1
            break
        total_bytes += size
        if _excluded(
            row,
            sql,
            gso_job_id=gso_job_id,
            service_principal_identities=identities,
        ):
            counters["gso_excluded"] += 1
            continue
        shape_hash = _normalized_shape_hash(sql)
        if not shape_hash:
            counters["unparsed"] += 1
            continue
        if shape_hash in seen_shapes:
            counters["duplicate_shape"] += 1
            continue
        recency = _recency(_start_epoch(row), now_epoch)
        if recency is None:
            counters["outside_lookback"] += 1
            continue
        evidence = sql_column_evidence(sql, inventory, diagnostics=counters)
        if not evidence:
            counters["no_column_attribution"] += 1
            continue
        seen_shapes.add(shape_hash)
        accepted += 1
        bucket, multiplier = recency
        for item in evidence:
            key = tuple(item["column_key"])
            role = str(item.get("sql_role") or "projection")
            column_shapes[key][shape_hash] = {
                "role": role,
                "bucket": bucket,
                "score": QUERY_ROLE_WEIGHTS.get(role, 1.0) * multiplier,
                "last_used_epoch": _start_epoch(row),
            }

    columns: list[dict[str, Any]] = []
    for key, shapes in sorted(column_shapes.items()):
        counts: defaultdict[str, defaultdict[str, int]] = defaultdict(lambda: defaultdict(int))
        score = 0.0
        last_used = 0.0
        for shape in shapes.values():
            counts[shape["role"]][shape["bucket"]] += 1
            score += float(shape["score"])
            last_used = max(last_used, float(shape["last_used_epoch"]))
        columns.append({
            "column_key": list(key),
            "column_id": render_identifier(key),
            "evidence_score": score,
            "distinct_query_counts": {
                role: dict(sorted(buckets.items())) for role, buckets in sorted(counts.items())
            },
            "last_used_timestamp": datetime.fromtimestamp(last_used, timezone.utc).isoformat() if last_used else None,
            "query_shape_hashes": sorted(shapes),
        })

    return {
        "contract_version": CONTRACT_VERSION,
        "inventory_hash": inventory["inventory_hash"],
        "source_mode": source_mode,
        "source_scope": source_scope,
        "coverage": {
            "accepted_statements": accepted,
            "distinct_query_shapes": len(seen_shapes),
            "raw_sql_bytes_processed": total_bytes,
            "inaccessible_scope": inaccessible_scope or [],
        },
        "degradation_counts": dict(sorted(counters.items())),
        "columns": columns,
    }


def collect_query_history_evidence(
    w: Any,
    inventory: dict[str, Any],
    *,
    profiling_warehouse_id: str,
    workload_warehouse_ids: list[str] | None,
    run_id: str,
    gso_job_id: str | None = None,
    service_principal_identities: Iterable[str] = (),
) -> dict[str, Any]:
    """Use exactly one source hierarchy and degrade to ``none`` on failures."""
    job_id = str(gso_job_id or os.getenv("GSO_JOB_ID", ""))
    identities = sorted(
        str(value).casefold()
        for value in service_principal_identities
        if value
    )
    warehouses = list(dict.fromkeys(workload_warehouse_ids or []))
    cache_key = content_hash({
        "inventory_hash": inventory.get("inventory_hash"),
        "profiling_warehouse_id": profiling_warehouse_id,
        "workload_warehouse_ids": warehouses,
        "gso_job_id": job_id,
        "service_principal_identities_hash": hashlib.sha256(
            json.dumps(identities, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    })
    now = time.monotonic()
    with _AGGREGATE_CACHE_LOCK:
        cached = _AGGREGATE_CACHE.get(cache_key)
        if cached and now - cached[0] <= AGGREGATE_CACHE_TTL_SECONDS:
            return copy.deepcopy(cached[1])

    def cache(evidence: dict[str, Any]) -> dict[str, Any]:
        with _AGGREGATE_CACHE_LOCK:
            _AGGREGATE_CACHE[cache_key] = (time.monotonic(), copy.deepcopy(evidence))
            if len(_AGGREGATE_CACHE) > _MAX_AGGREGATE_CACHE_ENTRIES:
                oldest = min(_AGGREGATE_CACHE, key=lambda key: _AGGREGATE_CACHE[key][0])
                _AGGREGATE_CACHE.pop(oldest, None)
        return evidence

    try:
        system_rows = _system_history_rows(w, profiling_warehouse_id, run_id=run_id)
        evidence = normalize_history_rows(
            system_rows,
            inventory,
            source_mode="system_table",
            source_scope=["system.query.history"],
            gso_job_id=job_id,
            service_principal_identities=identities,
            max_statements=MAX_SYSTEM_STATEMENTS,
        )
        evidence["source_attempts"] = {
            "system_table": "succeeded",
            "warehouse_api": "not_attempted",
        }
        evidence["source_errors"] = {}
        evidence["warnings"] = []
        return cache(evidence)
    except Exception as exc:
        system_error_code = _safe_error_code(exc)
        logger.info("system.query.history unavailable; probing warehouse history", exc_info=True)

    if warehouses:
        rows, inaccessible = _rest_history_rows(w, warehouses)
        accessible = [warehouse for warehouse in warehouses if warehouse not in inaccessible]
        if accessible:
            evidence = normalize_history_rows(
                rows,
                inventory,
                source_mode="warehouse_api",
                source_scope=accessible,
                gso_job_id=job_id,
                service_principal_identities=identities,
                max_statements=MAX_REST_STATEMENTS,
                inaccessible_scope=inaccessible,
            )
            evidence.setdefault("degradation_counts", {})[
                "system_history_unavailable"
            ] = 1
            evidence["source_attempts"] = {
                "system_table": "failed",
                "warehouse_api": "succeeded",
            }
            evidence["source_errors"] = {
                "system_table": system_error_code,
            }
            evidence["warnings"] = [
                "system.query.history was unavailable; used warehouse query history fallback",
            ]
            return cache(evidence)
    warehouse_status = "failed" if warehouses else "not_configured"
    return {
        "contract_version": CONTRACT_VERSION,
        "inventory_hash": inventory["inventory_hash"],
        "source_mode": "none",
        "source_scope": [],
        "coverage": {
            "accepted_statements": 0,
            "inaccessible_scope": warehouses,
        },
        "degradation_counts": {
            "history_unavailable": 1,
            "system_history_unavailable": 1,
            f"warehouse_history_{warehouse_status}": 1,
        },
        "source_attempts": {
            "system_table": "failed",
            "warehouse_api": warehouse_status,
        },
        "source_errors": {
            "system_table": system_error_code,
            **(
                {"warehouse_api": "INACCESSIBLE"}
                if warehouse_status == "failed"
                else {}
            ),
        },
        "warnings": [
            "Query-history evidence was unavailable; column ranking used local evidence only",
        ],
        "columns": [],
    }


def evidence_fingerprint(evidence: dict[str, Any]) -> str:
    """Stable fingerprint for optional 24-hour aggregate caches."""
    return content_hash(evidence)
