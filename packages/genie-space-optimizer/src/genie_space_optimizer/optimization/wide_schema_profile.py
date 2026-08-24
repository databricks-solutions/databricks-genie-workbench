"""Bounded SQL-warehouse profiling for wide-schema selection plans."""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.optimization.wide_schema import (
    MAX_AGGREGATE_EXPRESSIONS_PER_STATEMENT,
    MAX_CONCURRENT_PROFILING_STATEMENTS,
    MAX_CUMULATIVE_PROFILED_COLUMNS_PER_ASSET,
    MAX_ELIGIBLE_ASSETS,
    MAX_PROFILING_STATEMENTS_PER_ASSET,
    MAX_PROFILING_STATEMENTS_PER_RUN,
    MAX_VALUE_LIST_COLUMNS_PER_ASSET,
    PROFILING_STAGE_DEADLINE_SECONDS,
    PROFILING_STATEMENT_DEADLINE_SECONDS,
    inventory_indexes,
    quote_component,
    render_identifier,
    validate_selection_plan,
)

logger = logging.getLogger(__name__)

_NUMERIC_TYPES = frozenset({
    "byte", "short", "smallint", "int", "integer", "long", "bigint",
    "float", "double", "decimal", "numeric", "number",
})
_DATE_TYPES = frozenset({"date", "timestamp", "timestamp_ntz"})
_COMPLEX_PREFIXES = ("array", "map", "struct", "binary", "variant")


@dataclass(frozen=True)
class MetricSpec:
    column_key: tuple[str, str, str, str]
    column_name: str
    metric: str
    alias: str


@dataclass
class ProfileWorkItem:
    asset_key: tuple[str, str, str]
    asset_type: str
    kind: str
    sql: str
    metrics: list[MetricSpec] = field(default_factory=list)
    column_keys: list[tuple[str, str, str, str]] = field(default_factory=list)
    split_depth: int = 0
    view_fallback_sql: str | None = None
    uses_limit_projection: bool = False


@dataclass
class WorkResult:
    item: ProfileWorkItem
    state: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None
    statement_id: str | None = None
    submitted: bool = False
    cancelled: bool = False


def _dtype_base(value: Any) -> str:
    return str(value or "").casefold().split("(", 1)[0].strip()


def _profilable(column: dict[str, Any]) -> bool:
    dtype = _dtype_base(column.get("data_type"))
    return bool(dtype) and not any(dtype.startswith(prefix) for prefix in _COMPLEX_PREFIXES)


def _metric_names(column: dict[str, Any]) -> list[str]:
    if not _profilable(column):
        return []
    metrics = ["cardinality"]
    if _dtype_base(column.get("data_type")) in _NUMERIC_TYPES | _DATE_TYPES:
        metrics.extend(["min", "max"])
    return metrics


def _whole_column_groups(columns: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    groups: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    expression_count = 0
    for column in columns:
        count = len(_metric_names(column))
        if not count:
            continue
        if current and expression_count + count > MAX_AGGREGATE_EXPRESSIONS_PER_STATEMENT:
            groups.append(current)
            current = []
            expression_count = 0
        current.append(column)
        expression_count += count
    if current:
        groups.append(current)
    return groups


def _aggregate_item(
    asset: dict[str, Any],
    columns: list[dict[str, Any]],
    *,
    sample_size: int,
) -> ProfileWorkItem:
    asset_key = tuple(asset["asset_key"])
    asset_id = render_identifier(asset_key)
    selected = ", ".join(quote_component(column["name"]) for column in columns)
    expressions: list[str] = []
    specs: list[MetricSpec] = []
    alias_index = 0
    for column in columns:
        column_key = tuple(column["column_key"])
        quoted = quote_component(column["name"])
        for metric in _metric_names(column):
            alias = f"m{alias_index}"
            alias_index += 1
            if metric == "cardinality":
                expression = f"approx_count_distinct({quoted})"
            else:
                expression = f"{metric}({quoted})"
            expressions.append(f"{expression} AS {quote_component(alias)}")
            specs.append(MetricSpec(column_key, column["name"], metric, alias))
    if asset["asset_type"] == "metric_view":
        inner = f"SELECT {selected} FROM {asset_id} GROUP BY {selected}"
        query = f"SELECT {', '.join(expressions)} FROM ({inner})"
        fallback = None
    else:
        sample = f"SELECT {selected} FROM {asset_id} TABLESAMPLE ({sample_size} ROWS)"
        query = f"SELECT {', '.join(expressions)} FROM ({sample})"
        fallback = None
        if asset["asset_type"] == "view":
            fallback_inner = f"SELECT {selected} FROM {asset_id} LIMIT {sample_size}"
            fallback = f"SELECT {', '.join(expressions)} FROM ({fallback_inner})"
    return ProfileWorkItem(
        asset_key=asset_key,
        asset_type=asset["asset_type"],
        kind="aggregate",
        sql=query,
        metrics=specs,
        column_keys=[tuple(column["column_key"]) for column in columns],
        view_fallback_sql=fallback,
    )


def _row_count_item(asset: dict[str, Any]) -> ProfileWorkItem:
    asset_key = tuple(asset["asset_key"])
    return ProfileWorkItem(
        asset_key=asset_key,
        asset_type=asset["asset_type"],
        kind="row_count",
        sql=f"SELECT COUNT(*) AS {quote_component('row_count')} FROM {render_identifier(asset_key)}",
    )


def _value_list_item(asset: dict[str, Any], column: dict[str, Any], *, sample_size: int) -> ProfileWorkItem:
    asset_key = tuple(asset["asset_key"])
    asset_id = render_identifier(asset_key)
    name = quote_component(column["name"])
    if asset["asset_type"] == "metric_view":
        inner = f"SELECT {name} FROM {asset_id} WHERE {name} IS NOT NULL GROUP BY {name}"
        sql = f"SELECT collect_set({name}) AS {quote_component('values')} FROM ({inner})"
        fallback = None
    else:
        inner = f"SELECT {name} FROM {asset_id} TABLESAMPLE ({sample_size} ROWS)"
        sql = f"SELECT collect_set({name}) AS {quote_component('values')} FROM ({inner}) WHERE {name} IS NOT NULL"
        fallback = None
        if asset["asset_type"] == "view":
            fallback_inner = f"SELECT {name} FROM {asset_id} LIMIT {sample_size}"
            fallback = f"SELECT collect_set({name}) AS {quote_component('values')} FROM ({fallback_inner}) WHERE {name} IS NOT NULL"
    return ProfileWorkItem(
        asset_key=asset_key,
        asset_type=asset["asset_type"],
        kind="value_list",
        sql=sql,
        column_keys=[tuple(column["column_key"])],
        view_fallback_sql=fallback,
    )


def build_initial_work(
    inventory: dict[str, Any],
    plan: dict[str, Any],
    *,
    sample_size: int = 100,
) -> tuple[dict[tuple[str, str, str], deque[ProfileWorkItem]], dict[tuple[str, str, str, str], dict[str, Any]]]:
    """Create round-robin queues without exceeding cumulative column budgets."""
    validate_selection_plan(plan, inventory_hash=inventory["inventory_hash"])
    assets, columns = inventory_indexes(inventory)
    plan_assets = {tuple(asset["asset_key"]): asset for asset in plan.get("assets") or []}
    queues: dict[tuple[str, str, str], deque[ProfileWorkItem]] = {}
    outcomes: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for asset_index, (asset_key, asset) in enumerate(sorted(assets.items())):
        plan_asset = plan_assets.get(asset_key)
        if not plan_asset:
            continue
        cumulative = sum(1 for row in plan_asset["columns"] if row.get("cumulatively_value_profiled"))
        capacity = max(0, MAX_CUMULATIVE_PROFILED_COLUMNS_PER_ASSET - cumulative)
        pending_rows = sorted(
            (
                row for row in plan_asset["columns"]
                if row.get("active") and row.get("profile_status") == "pending"
            ),
            key=lambda row: (int(row.get("stable_rank") or 999999), row["column_id"]),
        )
        if asset_index >= MAX_ELIGIBLE_ASSETS:
            for row in pending_rows:
                outcomes[tuple(row["column_key"])] = {
                    "profile_status": "metadata_only",
                    "submitted": False,
                    "available_metrics": [],
                }
            continue
        selected_rows = pending_rows[:capacity]
        selected_columns: list[dict[str, Any]] = []
        for row in selected_rows:
            key = tuple(row["column_key"])
            column = columns[key]
            if asset["asset_type"] == "metric_view" and column.get("metric_role") == "measure":
                outcomes[key] = {"profile_status": "metadata_only", "submitted": False, "available_metrics": []}
            elif not _profilable(column):
                outcomes[key] = {"profile_status": "metadata_only", "submitted": False, "available_metrics": []}
            else:
                selected_columns.append(column)
        for row in pending_rows[capacity:]:
            outcomes[tuple(row["column_key"])] = {
                "profile_status": "metadata_only",
                "submitted": False,
                "available_metrics": [],
            }
        if not selected_columns:
            continue
        work = deque([_row_count_item(asset)])
        for group in _whole_column_groups(selected_columns):
            work.append(_aggregate_item(asset, group, sample_size=sample_size))
        queues[asset_key] = work
    return queues, outcomes


def _state_name(response: Any) -> str:
    state = response.status.state if response.status else None
    return str(getattr(state, "value", state) or "UNKNOWN").upper()


def _response_rows(response: Any) -> list[dict[str, Any]]:
    manifest_schema = response.manifest.schema if response.manifest else None
    schema_columns = manifest_schema.columns if manifest_schema else None
    names = [str(column.name or "") for column in (schema_columns or [])]
    data = response.result.data_array if response.result and response.result.data_array else []
    return [dict(zip(names, row)) for row in data]


def _query_tags(run_id: str) -> list[Any]:
    from genie_space_optimizer.common.query_tags import gso_query_tags

    return gso_query_tags(purpose="profiling", run_id=run_id)


def _execute(w: Any, warehouse_id: str, item: ProfileWorkItem, *, run_id: str) -> WorkResult:
    from databricks.sdk.service.sql import (
        Disposition,
        ExecuteStatementRequestOnWaitTimeout,
        Format,
    )

    response = None
    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=item.sql,
            wait_timeout=f"{PROFILING_STATEMENT_DEADLINE_SECONDS}s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
            query_tags=_query_tags(run_id),
        )
        state = _state_name(response)
        statement_id = str(getattr(response, "statement_id", "") or "")
        if state == "SUCCEEDED":
            return WorkResult(item=item, state="succeeded", rows=_response_rows(response), statement_id=statement_id, submitted=True)
        if state in {"PENDING", "RUNNING"}:
            cancelled = False
            if statement_id:
                try:
                    w.statement_execution.cancel_execution(statement_id=statement_id)
                    cancelled = True
                except Exception:
                    logger.warning("Could not cancel timed-out profiling statement %s", statement_id, exc_info=True)
            return WorkResult(item=item, state="timed_out", error=f"state={state}", statement_id=statement_id, submitted=True, cancelled=cancelled)
        error = response.status.error.message if response.status and response.status.error else f"state={state}"
        return WorkResult(item=item, state="failed", error=str(error)[:1000], statement_id=statement_id, submitted=True)
    except Exception as exc:
        return WorkResult(item=item, state="failed", error=f"{type(exc).__name__}: {exc}"[:1000], submitted=response is not None)


def _sample_unsupported(error: str | None) -> bool:
    lowered = str(error or "").casefold()
    return "tablesample" in lowered and any(token in lowered for token in ("unsupported", "not support", "parse", "syntax"))


def _split_item(item: ProfileWorkItem) -> list[ProfileWorkItem]:
    if item.kind != "aggregate" or item.split_depth >= 1 or len(item.column_keys) <= 1:
        return []
    midpoint = len(item.column_keys) // 2
    specs_by_key: dict[tuple[str, str, str, str], list[MetricSpec]] = defaultdict(list)
    for metric in item.metrics:
        specs_by_key[metric.column_key].append(metric)
    split: list[ProfileWorkItem] = []
    for keys in (item.column_keys[:midpoint], item.column_keys[midpoint:]):
        expressions: list[str] = []
        selected: list[str] = []
        new_specs: list[MetricSpec] = []
        alias_index = 0
        for key in keys:
            selected.append(quote_component(key[3]))
            for old in specs_by_key[key]:
                alias = f"m{alias_index}"
                alias_index += 1
                if old.metric == "cardinality":
                    expr = f"approx_count_distinct({quote_component(key[3])})"
                else:
                    expr = f"{old.metric}({quote_component(key[3])})"
                expressions.append(f"{expr} AS {quote_component(alias)}")
                new_specs.append(MetricSpec(key, key[3], old.metric, alias))
        asset_id = render_identifier(item.asset_key)
        projection = ", ".join(selected)
        if item.asset_type == "metric_view":
            inner = f"SELECT {projection} FROM {asset_id} GROUP BY {projection}"
            sql = f"SELECT {', '.join(expressions)} FROM ({inner})"
            fallback = None
        else:
            inner = f"SELECT {projection} FROM {asset_id} TABLESAMPLE (100 ROWS)"
            sql = f"SELECT {', '.join(expressions)} FROM ({inner})"
            fallback = None
            if item.asset_type == "view":
                fallback_inner = f"SELECT {projection} FROM {asset_id} LIMIT 100"
                fallback = f"SELECT {', '.join(expressions)} FROM ({fallback_inner})"
            if item.uses_limit_projection:
                inner = f"SELECT {projection} FROM {asset_id} LIMIT 100"
                sql = f"SELECT {', '.join(expressions)} FROM ({inner})"
                fallback = None
        split.append(ProfileWorkItem(
            asset_key=item.asset_key,
            asset_type=item.asset_type,
            kind="aggregate",
            sql=sql,
            metrics=new_specs,
            column_keys=list(keys),
            split_depth=1,
            view_fallback_sql=fallback,
            uses_limit_projection=item.uses_limit_projection,
        ))
    return split


def _parse_values(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            raw = [raw]
    if not isinstance(raw, list):
        raw = list(raw) if isinstance(raw, (set, tuple)) else [raw]
    return sorted(str(value) for value in raw)[:100]


def _apply_success(
    result: WorkResult,
    profile: dict[str, dict[str, Any]],
    outcomes: dict[tuple[str, str, str, str], dict[str, Any]],
) -> None:
    asset_id = render_identifier(result.item.asset_key)
    table = profile.setdefault(asset_id, {"row_count": -1, "columns": {}, "kind": result.item.asset_type})
    row = result.rows[0] if result.rows else {}
    if result.item.kind == "row_count":
        raw = row.get("row_count")
        table["row_count"] = int(raw) if raw is not None else -1
        return
    if result.item.kind == "value_list":
        key = result.item.column_keys[0]
        values = _parse_values(row.get("values"))
        if values:
            table["columns"].setdefault(key[3], {})["distinct_values"] = values
            outcomes.setdefault(key, {"profile_status": "profiled", "submitted": True, "available_metrics": []})
            outcomes[key].setdefault("available_metrics", []).append("distinct_values")
        return
    for key in result.item.column_keys:
        outcomes.setdefault(
            key,
            {
                "profile_status": "partial",
                "submitted": True,
                "available_metrics": [],
            },
        )["submitted"] = True
    for metric in result.item.metrics:
        value = row.get(metric.alias)
        if value is None:
            continue
        column = table["columns"].setdefault(metric.column_name, {})
        if metric.metric == "cardinality":
            try:
                column[metric.metric] = int(value)
            except (TypeError, ValueError):
                continue
        else:
            column[metric.metric] = str(value)
        outcome = outcomes.setdefault(metric.column_key, {"profile_status": "profiled", "submitted": True, "available_metrics": []})
        outcome["submitted"] = True
        outcome["profile_status"] = "profiled"
        outcome.setdefault("available_metrics", []).append(metric.metric)


def _mark_failure(result: WorkResult, outcomes: dict[tuple[str, str, str, str], dict[str, Any]]) -> None:
    status = "timed_out" if result.state == "timed_out" else "partial"
    for key in result.item.column_keys:
        outcome = outcomes.get(key)
        was_submitted = bool(outcome and outcome.get("submitted"))
        if not result.submitted and not was_submitted:
            outcomes[key] = {
                "profile_status": "metadata_only",
                "submitted": False,
                "available_metrics": [],
            }
            continue
        outcome = outcomes.setdefault(key, {"profile_status": status, "submitted": True, "available_metrics": []})
        outcome["submitted"] = True
        if outcome.get("available_metrics"):
            outcome["profile_status"] = "partial"
        else:
            outcome["profile_status"] = status


def _note_submitted(
    result: WorkResult,
    outcomes: dict[tuple[str, str, str, str], dict[str, Any]],
) -> None:
    """Record cumulative-profile consumption before any bounded retry."""
    if not result.submitted:
        return
    status = "timed_out" if result.state == "timed_out" else "partial"
    for key in result.item.column_keys:
        outcome = outcomes.setdefault(
            key,
            {"profile_status": status, "submitted": True, "available_metrics": []},
        )
        outcome["submitted"] = True
        if not outcome.get("available_metrics"):
            outcome["profile_status"] = status


def _non_retryable_failure(error: str | None) -> bool:
    lowered = str(error or "").casefold()
    markers = (
        "permission",
        "permission_denied",
        "insufficient_privilege",
        "not authorized",
        "unauthorized",
        "forbidden",
        "access denied",
        "warehouse unavailable",
        "warehouse not found",
        "does not have access",
    )
    return any(marker in lowered for marker in markers)


def _should_split(result: WorkResult) -> bool:
    if result.state == "timed_out":
        return True
    return result.submitted and not _non_retryable_failure(result.error)


def _round_robin_batch(
    queues: dict[tuple[str, str, str], deque[ProfileWorkItem]],
    asset_order: deque[tuple[str, str, str]],
    *,
    size: int,
) -> list[ProfileWorkItem]:
    batch: list[ProfileWorkItem] = []
    empty_passes = 0
    while len(batch) < size and asset_order and empty_passes < len(asset_order):
        asset = asset_order[0]
        asset_order.rotate(-1)
        queue = queues.get(asset)
        if queue:
            batch.append(queue.popleft())
            empty_passes = 0
        else:
            empty_passes += 1
    return batch


def build_profiling_budget(
    telemetry_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Aggregate persisted telemetry into the run-wide profiling budget."""
    def nonnegative_int(value: Any) -> int:
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    budget: dict[str, Any] = {
        "submitted_statements": 0,
        "elapsed_ms": 0,
        "asset_statement_counts": {},
    }
    for row in telemetry_rows or []:
        if not isinstance(row, dict):
            continue
        budget["submitted_statements"] += nonnegative_int(
            row.get("submitted_statements"),
        )
        budget["elapsed_ms"] += nonnegative_int(row.get("elapsed_ms"))
        for asset_id, count in (row.get("asset_statement_counts") or {}).items():
            asset_id = str(asset_id)
            budget["asset_statement_counts"][asset_id] = (
                int(budget["asset_statement_counts"].get(asset_id, 0))
                + nonnegative_int(count)
            )
    return budget


def merge_profiling_budgets(*budgets: dict[str, Any]) -> dict[str, Any]:
    """Take component-wise maxima across equivalent durable budget sources."""
    merged = build_profiling_budget()
    for budget in budgets:
        normalized = build_profiling_budget([budget])
        merged["submitted_statements"] = max(
            merged["submitted_statements"],
            normalized["submitted_statements"],
        )
        merged["elapsed_ms"] = max(
            merged["elapsed_ms"],
            normalized["elapsed_ms"],
        )
        for asset_id, count in normalized["asset_statement_counts"].items():
            merged["asset_statement_counts"][asset_id] = max(
                merged["asset_statement_counts"].get(asset_id, 0),
                count,
            )
    return merged


def run_bounded_profile(
    w: Any,
    warehouse_id: str,
    inventory: dict[str, Any],
    plan: dict[str, Any],
    *,
    run_id: str,
    sample_size: int = 100,
    low_cardinality_threshold: int = 20,
    budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute the bounded profiling stage and return profile/outcome telemetry."""
    started = time.monotonic()
    budget = budget if isinstance(budget, dict) else build_profiling_budget()
    prior_submitted = max(0, int(budget.get("submitted_statements") or 0))
    prior_elapsed_ms = max(0, int(budget.get("elapsed_ms") or 0))
    prior_asset_counts = {
        str(asset_id): max(0, int(count or 0))
        for asset_id, count in (budget.get("asset_statement_counts") or {}).items()
    }
    remaining_deadline_seconds = max(
        0.0,
        PROFILING_STAGE_DEADLINE_SECONDS - prior_elapsed_ms / 1000.0,
    )
    deadline = started + remaining_deadline_seconds
    queues, outcomes = build_initial_work(inventory, plan, sample_size=sample_size)
    assets, columns = inventory_indexes(inventory)
    asset_order = deque(sorted(queues))
    asset_counts: defaultdict[tuple[str, str, str], int] = defaultdict(int)
    total_submitted = 0
    telemetry: defaultdict[str, int] = defaultdict(int)
    profile: dict[str, dict[str, Any]] = {}

    def can_submit(item: ProfileWorkItem) -> bool:
        asset_id = render_identifier(item.asset_key)
        return (
            time.monotonic() < deadline
            and prior_submitted + total_submitted < MAX_PROFILING_STATEMENTS_PER_RUN
            and prior_asset_counts.get(asset_id, 0) + asset_counts[item.asset_key]
            < MAX_PROFILING_STATEMENTS_PER_ASSET
        )

    while any(queues.values()):
        if (
            time.monotonic() >= deadline
            or prior_submitted + total_submitted
            >= MAX_PROFILING_STATEMENTS_PER_RUN
        ):
            break
        batch = _round_robin_batch(queues, asset_order, size=MAX_CONCURRENT_PROFILING_STATEMENTS)
        submit_batch: list[ProfileWorkItem] = []
        for item in batch:
            if can_submit(item):
                # Reserve run and per-asset capacity before checking the next
                # concurrent item so a batch cannot cross either hard cap.
                total_submitted += 1
                asset_counts[item.asset_key] += 1
                submit_batch.append(item)
            else:
                for key in item.column_keys:
                    outcomes.setdefault(key, {
                        "profile_status": "metadata_only",
                        "submitted": False,
                        "available_metrics": [],
                    })
        if not submit_batch:
            break
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_PROFILING_STATEMENTS) as pool:
            futures = {}
            for item in submit_batch:
                futures[pool.submit(_execute, w, warehouse_id, item, run_id=run_id)] = item
            for future in as_completed(futures):
                result = future.result()
                if result.submitted:
                    telemetry["accepted_statements"] += 1
                if result.cancelled:
                    telemetry["cancellations"] += 1
                if result.state == "succeeded":
                    telemetry["succeeded_statements"] += 1
                    _apply_success(result, profile, outcomes)
                    continue

                telemetry[f"{result.state}_statements"] += 1
                _note_submitted(result, outcomes)
                if (
                    result.state != "timed_out"
                    and result.submitted
                    and result.item.view_fallback_sql
                    and _sample_unsupported(result.error)
                    and not _non_retryable_failure(result.error)
                    and can_submit(result.item)
                ):
                    fallback = ProfileWorkItem(
                        asset_key=result.item.asset_key,
                        asset_type=result.item.asset_type,
                        kind=result.item.kind,
                        sql=result.item.view_fallback_sql,
                        metrics=result.item.metrics,
                        column_keys=result.item.column_keys,
                        split_depth=result.item.split_depth,
                        uses_limit_projection=True,
                    )
                    queues.setdefault(result.item.asset_key, deque()).appendleft(fallback)
                    telemetry["view_shape_fallbacks"] += 1
                    continue
                split = _split_item(result.item) if _should_split(result) else []
                if split and all(can_submit(item) for item in split):
                    for item in reversed(split):
                        queues.setdefault(item.asset_key, deque()).appendleft(item)
                    telemetry["split_retries"] += len(split)
                else:
                    _mark_failure(result, outcomes)

    # Stop conditions convert all unscheduled selected columns to metadata-only.
    for queue in queues.values():
        for item in queue:
            for key in item.column_keys:
                outcomes.setdefault(key, {
                    "profile_status": "metadata_only",
                    "submitted": False,
                    "available_metrics": [],
                })
    if time.monotonic() >= deadline:
        telemetry["stage_deadline_reached"] += 1
    if prior_submitted + total_submitted >= MAX_PROFILING_STATEMENTS_PER_RUN:
        telemetry["run_statement_limit_reached"] += 1

    # Follow-up values are capped independently and run through the same budgets.
    value_queues: dict[tuple[str, str, str], deque[ProfileWorkItem]] = {}
    plan_assets = {tuple(asset["asset_key"]): asset for asset in plan.get("assets") or []}
    for asset_key, asset in sorted(assets.items()):
        table = profile.get(render_identifier(asset_key), {})
        plan_asset = plan_assets.get(asset_key, {})
        rank_by_key = {tuple(row["column_key"]): int(row.get("stable_rank") or 999999) for row in plan_asset.get("columns") or []}
        eligible: list[tuple[int, dict[str, Any]]] = []
        for key, column in columns.items():
            if key[:3] != asset_key or key not in outcomes:
                continue
            stats = (table.get("columns") or {}).get(key[3], {})
            cardinality = stats.get("cardinality")
            dtype = _dtype_base(column.get("data_type"))
            if (
                isinstance(cardinality, int)
                and 0 < cardinality <= low_cardinality_threshold
                and dtype not in _NUMERIC_TYPES | _DATE_TYPES
                and _profilable(column)
            ):
                eligible.append((rank_by_key.get(key, 999999), column))
        if eligible:
            eligible.sort(key=lambda item: (item[0], item[1]["column_id"]))
            value_queues[asset_key] = deque(
                _value_list_item(asset, column, sample_size=sample_size)
                for _rank, column in eligible[:MAX_VALUE_LIST_COLUMNS_PER_ASSET]
            )

    value_order = deque(sorted(value_queues))
    while (
        any(value_queues.values())
        and time.monotonic() < deadline
        and prior_submitted + total_submitted < MAX_PROFILING_STATEMENTS_PER_RUN
    ):
        batch = _round_robin_batch(value_queues, value_order, size=MAX_CONCURRENT_PROFILING_STATEMENTS)
        submit_batch = []
        for item in batch:
            if can_submit(item):
                total_submitted += 1
                asset_counts[item.asset_key] += 1
                submit_batch.append(item)
            else:
                for key in item.column_keys:
                    outcomes.setdefault(key, {
                        "profile_status": "metadata_only",
                        "submitted": False,
                        "available_metrics": [],
                    })
        if not submit_batch:
            break
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_PROFILING_STATEMENTS) as pool:
            futures = {}
            for item in submit_batch:
                futures[pool.submit(_execute, w, warehouse_id, item, run_id=run_id)] = item
            for future in as_completed(futures):
                result = future.result()
                if result.submitted:
                    telemetry["accepted_statements"] += 1
                if result.cancelled:
                    telemetry["cancellations"] += 1
                if result.state == "succeeded":
                    telemetry["succeeded_statements"] += 1
                    _apply_success(result, profile, outcomes)
                else:
                    telemetry[f"{result.state}_statements"] += 1
                    _note_submitted(result, outcomes)
                    if (
                        result.state != "timed_out"
                        and result.submitted
                        and result.item.view_fallback_sql
                        and _sample_unsupported(result.error)
                        and not _non_retryable_failure(result.error)
                        and can_submit(result.item)
                    ):
                        value_queues.setdefault(
                            result.item.asset_key,
                            deque(),
                        ).appendleft(ProfileWorkItem(
                            asset_key=result.item.asset_key,
                            asset_type=result.item.asset_type,
                            kind=result.item.kind,
                            sql=result.item.view_fallback_sql,
                            metrics=result.item.metrics,
                            column_keys=result.item.column_keys,
                            split_depth=result.item.split_depth,
                            uses_limit_projection=True,
                        ))
                        telemetry["view_shape_fallbacks"] += 1
                    else:
                        _mark_failure(result, outcomes)

    for key, outcome in outcomes.items():
        outcome["available_metrics"] = sorted(set(outcome.get("available_metrics") or []))
        if outcome.get("submitted") and not outcome["available_metrics"] and outcome["profile_status"] == "profiled":
            outcome["profile_status"] = "partial"
    telemetry["submitted_statements"] = total_submitted
    telemetry["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    telemetry["assets_with_profile"] = len(profile)
    telemetry["columns_with_profile"] = sum(1 for outcome in outcomes.values() if outcome.get("available_metrics"))
    incremental_asset_counts = {
        render_identifier(key): count for key, count in sorted(asset_counts.items())
    }
    budget["submitted_statements"] = prior_submitted + total_submitted
    budget["elapsed_ms"] = prior_elapsed_ms + telemetry["elapsed_ms"]
    budget["asset_statement_counts"] = dict(prior_asset_counts)
    for asset_id, count in incremental_asset_counts.items():
        budget["asset_statement_counts"][asset_id] = (
            budget["asset_statement_counts"].get(asset_id, 0) + count
        )
    return {
        "data_profile": profile,
        "outcomes": outcomes,
        "telemetry": dict(telemetry),
        "asset_statement_counts": incremental_asset_counts,
        "cumulative_budget": {
            "submitted_statements": budget["submitted_statements"],
            "elapsed_ms": budget["elapsed_ms"],
            "asset_statement_counts": dict(budget["asset_statement_counts"]),
        },
    }
