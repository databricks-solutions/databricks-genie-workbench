"""Bounded, privacy-preserving query-history evidence for wide schemas."""

from __future__ import annotations

import hashlib
import copy
import json
import logging
import math
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
EXTENDED_HISTORY_LOOKBACK_DAYS = 90
MIN_USEFUL_QUERY_SHAPES = 20
MAX_SYSTEM_STATEMENTS = 10_000
MAX_REST_STATEMENTS = 5_000
MAX_STATEMENT_BYTES = 256 * 1024
MAX_RAW_SQL_BYTES = 50 * 1024 * 1024
MAX_HISTORY_PARSE_SECONDS = 120
AGGREGATE_CACHE_TTL_SECONDS = 24 * 60 * 60
_MAX_AGGREGATE_CACHE_ENTRIES = 128
_AGGREGATE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_AGGREGATE_CACHE_LOCK = threading.Lock()

_RECENCY = (
    (7, "0_7_days", 1.0),
    (14, "8_14_days", 0.5),
    (30, "15_30_days", 0.25),
    (90, "31_90_days", 0.1),
)
_ERROR_CODE_RE = re.compile(r"\[([A-Z][A-Z0-9_.-]+)\]")
_MAX_FREQUENCY_MULTIPLIER = 3.0
_SPACE_QUERY_WEIGHT = 2.0


def _query_tags(run_id: str, purpose: str = "history_collection") -> list[Any]:
    from genie_space_optimizer.common.query_tags import gso_query_tags

    return gso_query_tags(purpose=purpose, run_id=run_id)


def _statement_result_rows(
    w: Any,
    response: Any,
    *,
    max_rows: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch every INLINE result chunk and return bounded row dictionaries."""
    manifest_schema = response.manifest.schema if response.manifest else None
    schema_columns = manifest_schema.columns if manifest_schema else None
    names = [str(column.name or "") for column in (schema_columns or [])]
    result = response.result
    rows: list[dict[str, Any]] = []
    chunks_fetched = 0
    seen_chunks: set[int] = set()

    def append_chunk(chunk: Any) -> None:
        nonlocal chunks_fetched
        if chunk is None:
            return
        chunks_fetched += 1
        for raw in chunk.data_array or []:
            if len(rows) >= max_rows:
                break
            rows.append(dict(zip(names, raw)))

    append_chunk(result)
    next_index = getattr(result, "next_chunk_index", None) if result else None
    total_chunks = int(
        getattr(response.manifest, "total_chunk_count", 0) or 0
    ) if response.manifest else 0
    statement_id = str(getattr(response, "statement_id", "") or "")

    while len(rows) < max_rows:
        if next_index is None:
            remaining = [
                index for index in range(1, total_chunks)
                if index not in seen_chunks
            ]
            if not remaining:
                break
            next_index = remaining[0]
        chunk_index = int(next_index)
        if chunk_index in seen_chunks:
            raise RuntimeError("statement result chunk chain contains a cycle")
        if not statement_id:
            raise RuntimeError("statement result is chunked but statement_id is missing")
        seen_chunks.add(chunk_index)
        chunk = w.statement_execution.get_statement_result_chunk_n(
            statement_id=statement_id,
            chunk_index=chunk_index,
        )
        append_chunk(chunk)
        next_index = getattr(chunk, "next_chunk_index", None)

    manifest = response.manifest
    return rows, {
        "chunks_fetched": chunks_fetched,
        "rows_returned": len(rows),
        "result_total_rows": int(getattr(manifest, "total_row_count", 0) or 0),
        "result_total_chunks": total_chunks,
        "result_truncated": bool(getattr(manifest, "truncated", False)),
    }


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


def _sql_literal(value: Any) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _asset_relevance_predicate(inventory: dict[str, Any]) -> str:
    normalized_sql = "lower(replace(coalesce(statement_text, ''), '`', ''))"
    exact_names: set[str] = set()
    leaf_names: set[str] = set()
    for asset in inventory.get("assets") or []:
        parts = [str(value or "").casefold() for value in asset.get("asset_key") or []]
        if len(parts) != 3:
            continue
        exact_names.add(".".join(parts))
        exact_names.add(".".join(parts[-2:]))
        leaf_names.add(parts[-1])
    predicates = [
        f"instr({normalized_sql}, {_sql_literal(name)}) > 0"
        for name in sorted(exact_names)
    ]
    for name in sorted(leaf_names):
        boundary_pattern = rf"(^|[^a-z0-9_]){re.escape(name)}([^a-z0-9_]|$)"
        predicates.append(
            f"regexp_like({normalized_sql}, {_sql_literal(boundary_pattern)})"
        )
    return " OR ".join(predicates) or "FALSE"


def _system_history_statement(
    workspace_id: int,
    inventory: dict[str, Any],
    *,
    target_space_id: str,
    gso_job_id: str,
    service_principal_identities: Iterable[str],
    lookback_days: int,
) -> str:
    normalized_sql = "lower(replace(coalesce(statement_text, ''), '`', ''))"
    tokenized_sql = (
        "lower(trim(regexp_replace("
        "replace(coalesce(statement_text, ''), '`', ''), "
        "'[^A-Za-z0-9_]+', ' ')))"
    )
    relevance = [_asset_relevance_predicate(inventory)]
    if target_space_id:
        relevance.insert(
            0,
            "query_source.genie_space_id = " + _sql_literal(target_space_id),
        )
    exclusions = [
        "coalesce(query_tags['application'], '') <> 'genie_workbench'",
        "coalesce(query_tags['component'], '') <> 'gso'",
    ]
    if gso_job_id:
        exclusions.append(
            "coalesce(query_source.job_info.job_id, '') <> "
            + _sql_literal(gso_job_id)
        )
    identities = sorted({
        str(value).casefold() for value in service_principal_identities if value
    })
    if identities:
        rendered_identities = ", ".join(
            _sql_literal(value) for value in identities
        )
        exclusions.extend((
            "lower(coalesce(executed_by, '')) NOT IN ("
            + rendered_identities
            + ")",
            "lower(coalesce(executed_as, '')) NOT IN ("
            + rendered_identities
            + ")",
        ))
    for marker in (
        "tablesample (100 rows)",
        "_card_",
        "collect_set(",
        "select * from (explain",
        "genie_opt_",
    ):
        exclusions.append(
            f"instr({normalized_sql}, {_sql_literal(marker)}) = 0"
        )
    exclusions.extend((
        "NOT ("
        f"instr({tokenized_sql}, 'with sampleddata') > 0 "
        f"AND instr({tokenized_sql}, 'sample_size') > 0 "
        f"AND instr({tokenized_sql}, '_null_count') > 0 "
        f"AND instr({tokenized_sql}, '_distinct_count') > 0"
        ")",
        "NOT ("
        f"instr({tokenized_sql}, 'approx_top_k ') > 0 "
        f"AND instr({tokenized_sql}, 'item item as value') > 0"
        ")",
    ))
    order_by = "start_time DESC"
    if target_space_id:
        order_by = (
            "CASE WHEN query_source.genie_space_id = "
            + _sql_literal(target_space_id)
            + " THEN 0 ELSE 1 END, start_time DESC"
        )

    return f"""
        SELECT
          statement_id,
          statement_text,
          statement_type,
          start_time,
          executed_by,
          executed_as,
          query_source,
          query_tags
        FROM system.query.history
        WHERE start_time >= current_timestamp() - INTERVAL {int(lookback_days)} DAYS
          AND execution_status = 'FINISHED'
          AND statement_type = 'SELECT'
          AND workspace_id = {int(workspace_id)}
          AND ({' OR '.join(relevance)})
          AND {' AND '.join(exclusions)}
        ORDER BY {order_by}
        LIMIT {MAX_SYSTEM_STATEMENTS}
    """


def _system_history_rows(
    w: Any,
    warehouse_id: str,
    *,
    run_id: str,
    inventory: dict[str, Any],
    target_space_id: str = "",
    gso_job_id: str = "",
    service_principal_identities: Iterable[str] = (),
    lookback_days: int = HISTORY_LOOKBACK_DAYS,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
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

    statement = _system_history_statement(
        workspace_id,
        inventory,
        target_space_id=target_space_id,
        gso_job_id=gso_job_id,
        service_principal_identities=service_principal_identities,
        lookback_days=lookback_days,
    )
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
    rows, read_telemetry = _statement_result_rows(
        w,
        response,
        max_rows=MAX_SYSTEM_STATEMENTS,
    )
    read_telemetry.update({
        "lookback_days": int(lookback_days),
        "workspace_scoped": True,
        "server_side_select_filter": True,
        "server_side_gso_filter": True,
        "server_side_generated_profile_filter": True,
        "server_side_relevance_filter": True,
        "target_space_scoped": bool(target_space_id),
        "target_space_prioritized": bool(target_space_id),
        "configured_asset_count": len(inventory.get("assets") or []),
    })
    return rows, read_telemetry


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


def _query_source_space_id(row: dict[str, Any]) -> str:
    raw = row.get("query_source") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return ""
    if not isinstance(raw, dict):
        return ""
    return str(raw.get("genie_space_id") or "")


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


def _databricks_generated_profile_shape(sql: str) -> str | None:
    """Identify only the two known Databricks-generated profiling shapes."""
    normalized = str(sql or "").replace("`", "").casefold()
    sampled_profile = (
        bool(re.search(r"\bwith\s+sampleddata\b", normalized))
        and "sample_size" in normalized
        and "_null_count" in normalized
        and "_distinct_count" in normalized
    )
    if sampled_profile:
        return "excluded_databricks_sample_profile"

    top_k_profile = (
        bool(re.search(r"\bapprox_top_k\s*\(", normalized))
        and bool(re.search(r"\bitem\s*\.\s*item\s+as\s+value\b", normalized))
    )
    if top_k_profile:
        return "excluded_databricks_top_k_profile"
    return None


def _exclusion_reason(
    row: dict[str, Any],
    sql: str,
    *,
    gso_job_id: str,
    service_principal_identities: set[str],
) -> str | None:
    if gso_job_id and _query_source_job_id(row) == gso_job_id:
        return "excluded_gso_job"
    tags = _tags(row)
    if tags.get("application") == "genie_workbench" or tags.get("component") == "gso":
        return "excluded_gso_tags"
    row_identities = {
        str(row.get(field) or "").casefold()
        for field in (
            "executed_by",
            "executed_as",
            "executed_as_user_name",
            "user_name",
        )
        if row.get(field)
    }
    if row_identities & service_principal_identities:
        return "excluded_service_principal"
    if _legacy_gso_shape(sql):
        return "excluded_legacy_gso_shape"
    generated_profile_reason = _databricks_generated_profile_shape(sql)
    if generated_profile_reason:
        return generated_profile_reason
    return None


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
    target_space_id: str = "",
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
    accepted_source_counts: defaultdict[str, int] = defaultdict(int)
    shape_columns: dict[str, list[tuple[tuple[str, ...], str]]] = {}
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
        statement_type = str(row.get("statement_type") or "").upper()
        if statement_type and statement_type != "SELECT":
            counters["non_select"] += 1
            continue
        exclusion_reason = _exclusion_reason(
            row,
            sql,
            gso_job_id=gso_job_id,
            service_principal_identities=identities,
        )
        if exclusion_reason:
            counters[exclusion_reason] += 1
            counters["gso_excluded"] += 1
            continue
        shape_hash = _normalized_shape_hash(sql)
        if not shape_hash:
            counters["unparsed"] += 1
            continue
        recency = _recency(_start_epoch(row), now_epoch)
        if recency is None:
            counters["outside_lookback"] += 1
            continue
        space_scoped = bool(
            target_space_id
            and _query_source_space_id(row) == target_space_id
        )
        source_kind = "target_space" if space_scoped else "configured_asset"
        bucket, multiplier = recency
        if shape_hash in shape_columns:
            counters["duplicate_shape"] += 1
            accepted += 1
            accepted_source_counts[source_kind] += 1
            for key, _role in shape_columns[shape_hash]:
                shape = column_shapes[key][shape_hash]
                shape["frequency"] += 1
                shape["space_scoped"] = bool(
                    shape["space_scoped"] or space_scoped
                )
                if multiplier > float(shape["recency_multiplier"]):
                    shape["bucket"] = bucket
                    shape["recency_multiplier"] = multiplier
                shape["last_used_epoch"] = max(
                    float(shape["last_used_epoch"]),
                    _start_epoch(row),
                )
            continue
        evidence = sql_column_evidence(sql, inventory, diagnostics=counters)
        if not evidence:
            counters["no_column_attribution"] += 1
            continue
        accepted += 1
        accepted_source_counts[source_kind] += 1
        shape_columns[shape_hash] = []
        for item in evidence:
            key = tuple(item["column_key"])
            role = str(item.get("sql_role") or "projection")
            shape_columns[shape_hash].append((key, role))
            column_shapes[key][shape_hash] = {
                "role": role,
                "bucket": bucket,
                "recency_multiplier": multiplier,
                "frequency": 1,
                "space_scoped": space_scoped,
                "last_used_epoch": _start_epoch(row),
            }

    columns: list[dict[str, Any]] = []
    for key, shapes in sorted(column_shapes.items()):
        counts: defaultdict[str, defaultdict[str, int]] = defaultdict(lambda: defaultdict(int))
        score = 0.0
        last_used = 0.0
        occurrence_count = 0
        target_space_shape_count = 0
        for shape in shapes.values():
            counts[shape["role"]][shape["bucket"]] += 1
            frequency = int(shape["frequency"])
            frequency_multiplier = min(
                _MAX_FREQUENCY_MULTIPLIER,
                1.0 + math.log1p(max(0, frequency - 1)),
            )
            space_multiplier = (
                _SPACE_QUERY_WEIGHT if shape["space_scoped"] else 1.0
            )
            score += (
                QUERY_ROLE_WEIGHTS.get(shape["role"], 1.0)
                * float(shape["recency_multiplier"])
                * frequency_multiplier
                * space_multiplier
            )
            occurrence_count += frequency
            target_space_shape_count += int(bool(shape["space_scoped"]))
            last_used = max(last_used, float(shape["last_used_epoch"]))
        columns.append({
            "column_key": list(key),
            "column_id": render_identifier(key),
            "evidence_score": score,
            "distinct_query_counts": {
                role: dict(sorted(buckets.items())) for role, buckets in sorted(counts.items())
            },
            "query_occurrence_count": occurrence_count,
            "target_space_query_shape_count": target_space_shape_count,
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
            "distinct_query_shapes": len(shape_columns),
            "target_space_statements": accepted_source_counts["target_space"],
            "configured_asset_statements": accepted_source_counts["configured_asset"],
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
    target_space_id: str = "",
    gso_job_id: str | None = None,
    service_principal_identities: Iterable[str] = (),
) -> dict[str, Any]:
    """Collect relevant history, expanding/falling back when evidence is empty."""
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
        "target_space_id": target_space_id,
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

    def read_system(lookback_days: int) -> dict[str, Any]:
        system_rows, read_telemetry = _system_history_rows(
            w,
            profiling_warehouse_id,
            run_id=run_id,
            inventory=inventory,
            target_space_id=target_space_id,
            gso_job_id=job_id,
            service_principal_identities=identities,
            lookback_days=lookback_days,
        )
        result = normalize_history_rows(
            system_rows,
            inventory,
            source_mode="system_table",
            source_scope=["system.query.history"],
            gso_job_id=job_id,
            service_principal_identities=identities,
            target_space_id=target_space_id,
            max_statements=MAX_SYSTEM_STATEMENTS,
        )
        result.setdefault("coverage", {})["read_telemetry"] = read_telemetry
        return result

    system_evidence: dict[str, Any] | None = None
    system_error_code = ""
    system_warnings: list[str] = []
    try:
        system_evidence = read_system(HISTORY_LOOKBACK_DAYS)
        distinct_shapes = int(
            system_evidence.get("coverage", {}).get("distinct_query_shapes") or 0
        )
        if distinct_shapes < MIN_USEFUL_QUERY_SHAPES:
            try:
                extended = read_system(EXTENDED_HISTORY_LOOKBACK_DAYS)
                extended_shapes = int(
                    extended.get("coverage", {}).get("distinct_query_shapes") or 0
                )
                if extended_shapes >= distinct_shapes:
                    system_evidence = extended
                    system_warnings.append(
                        "Expanded query-history lookback from "
                        f"{HISTORY_LOOKBACK_DAYS} to {EXTENDED_HISTORY_LOOKBACK_DAYS} days "
                        f"because only {distinct_shapes} distinct query shapes were found"
                    )
            except Exception:
                system_warnings.append(
                    "Extended query-history lookback failed; retained the 30-day evidence"
                )
                logger.info("Extended system.query.history read failed", exc_info=True)

        if int(system_evidence.get("coverage", {}).get("accepted_statements") or 0) > 0:
            system_evidence["source_attempts"] = {
                "system_table": "succeeded",
                "warehouse_api": "not_attempted",
            }
            system_evidence["source_errors"] = {}
            system_evidence["warnings"] = system_warnings
            return cache(system_evidence)
        system_warnings.append(
            "system.query.history returned no attributable user workload; probing warehouse history"
        )
    except Exception as exc:
        system_error_code = _safe_error_code(exc)
        logger.info("system.query.history unavailable; probing warehouse history", exc_info=True)

    warehouse_status = "not_configured"
    if warehouses:
        rows, inaccessible = _rest_history_rows(w, warehouses)
        accessible = [warehouse for warehouse in warehouses if warehouse not in inaccessible]
        warehouse_status = "failed"
        if accessible:
            warehouse_status = "succeeded"
            evidence = normalize_history_rows(
                rows,
                inventory,
                source_mode="warehouse_api",
                source_scope=accessible,
                gso_job_id=job_id,
                service_principal_identities=identities,
                target_space_id=target_space_id,
                max_statements=MAX_REST_STATEMENTS,
                inaccessible_scope=inaccessible,
            )
            if system_evidence is None:
                evidence.setdefault("degradation_counts", {})[
                    "system_history_unavailable"
                ] = 1
            else:
                evidence.setdefault("degradation_counts", {})[
                    "system_history_no_usable_statements"
                ] = 1
            evidence["source_attempts"] = {
                "system_table": (
                    "failed" if system_evidence is None else "succeeded_no_usable_rows"
                ),
                "warehouse_api": "succeeded",
            }
            evidence["source_errors"] = (
                {"system_table": system_error_code}
                if system_evidence is None else {}
            )
            evidence["warnings"] = system_warnings + [
                (
                    "system.query.history was unavailable; used warehouse query history fallback"
                    if system_evidence is None
                    else "system.query.history had no usable rows; used warehouse query history fallback"
                ),
            ]
            if int(evidence.get("coverage", {}).get("accepted_statements") or 0) > 0:
                return cache(evidence)
            warehouse_status = "succeeded_no_usable_rows"

    if system_evidence is not None:
        system_evidence.setdefault("degradation_counts", {})[
            "history_no_usable_statements"
        ] = 1
        system_evidence["source_mode"] = "none"
        system_evidence["source_scope"] = []
        system_evidence["source_attempts"] = {
            "system_table": "succeeded_no_usable_rows",
            "warehouse_api": warehouse_status,
        }
        system_evidence["source_errors"] = {}
        system_evidence["warnings"] = system_warnings + [
            "No attributable query-history columns were found; column ranking used local evidence only"
        ]
        return cache(system_evidence)

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
