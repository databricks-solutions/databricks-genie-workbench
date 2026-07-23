"""Shared complete-request packer for every GSO LLM call."""

from __future__ import annotations

import copy
import json
import os
import threading
from datetime import datetime, timezone
from collections import defaultdict, deque
from typing import Any, Iterable

from genie_space_optimizer.optimization.wide_schema import (
    MAX_LLM_REQUEST_CHARS,
    inventory_indexes,
    validate_selection_plan,
)

_HIGH_PRIORITY_KEYS = (
    "repair_targets",
    "optimization_targets",
    "benchmarks_to_fix",
    "previous_eval",
    "failures",
    "current_space_config",
    "response_schema",
    "patch_rules",
)

_PROMPT_TELEMETRY_LOCK = threading.Lock()
_PROMPT_TELEMETRY: list[dict[str, Any]] = []
_MAX_PROMPT_TELEMETRY_ROWS = 1_000


def _hash_metadata(messages: list[dict[str, str]]) -> dict[str, Any]:
    found: dict[str, Any] = {"plan_hash": None, "inventory_hash": None}

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key in found and found[key] is None and isinstance(child, str):
                    found[key] = child
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    for message in messages:
        try:
            visit(json.loads(str(message.get("content") or "")))
        except (TypeError, ValueError):
            continue
    if found["plan_hash"] is None:
        found["plan_hash"] = os.getenv("GSO_WIDE_SCHEMA_PLAN_HASH") or None
    if found["inventory_hash"] is None:
        found["inventory_hash"] = os.getenv("GSO_WIDE_SCHEMA_INVENTORY_HASH") or None
    return found


def _record_prompt_telemetry(stats: dict[str, Any]) -> None:
    row = copy.deepcopy(stats)
    row["recorded_at"] = datetime.now(timezone.utc).isoformat()
    with _PROMPT_TELEMETRY_LOCK:
        _PROMPT_TELEMETRY.append(row)
        del _PROMPT_TELEMETRY[:-_MAX_PROMPT_TELEMETRY_ROWS]


def drain_prompt_telemetry() -> list[dict[str, Any]]:
    """Return and clear bounded per-process prompt-packing telemetry."""
    with _PROMPT_TELEMETRY_LOCK:
        rows = copy.deepcopy(_PROMPT_TELEMETRY)
        _PROMPT_TELEMETRY.clear()
    return rows


def serialized_messages(messages: list[dict[str, str]]) -> str:
    return json.dumps(messages, ensure_ascii=False, separators=(",", ":"))


def messages_size(messages: list[dict[str, str]]) -> int:
    return len(serialized_messages(messages))


def validate_messages(messages: list[dict[str, str]], *, max_chars: int = MAX_LLM_REQUEST_CHARS) -> None:
    size = messages_size(messages)
    if size > max_chars:
        raise ValueError(f"complete LLM messages payload is {size} characters; maximum is {max_chars}")


def _truncate_descriptions(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        is_column = bool(
            value.get("column_id")
            or value.get("column_name")
            or (value.get("name") and value.get("data_type"))
        )
        for key, child in value.items():
            if key == "description" and isinstance(child, str):
                out[key] = child[:300 if is_column else 1000]
            elif key in {"column_description", "comment"} and isinstance(child, str):
                out[key] = child[:300]
            else:
                out[key] = _truncate_descriptions(child)
        return out
    if isinstance(value, list):
        return [_truncate_descriptions(child) for child in value]
    return value


def _list_round_robin_prefix(values: list[Any], count: int) -> list[Any]:
    if count >= len(values):
        return values
    return values[: max(0, count)]


def _compact_json_object(value: Any, *, target_chars: int) -> tuple[Any, dict[str, int]]:
    compact = _truncate_descriptions(copy.deepcopy(value))
    omitted: defaultdict[str, int] = defaultdict(int)
    if len(json.dumps(compact, ensure_ascii=False)) <= target_chars:
        return compact, {}

    # Remove known low-value name lists before dropping schema rows.
    def prune_names(node: Any) -> None:
        if isinstance(node, dict):
            for key in list(node):
                child = node[key]
                if key in {"omitted_identifiers", "omitted_sections", "query_shape_hashes"} and isinstance(child, list):
                    omitted[key] += len(child)
                    node[key] = []
                else:
                    prune_names(child)
        elif isinstance(node, list):
            for child in node:
                prune_names(child)

    prune_names(compact)
    if len(json.dumps(compact, ensure_ascii=False)) <= target_chars:
        return compact, dict(omitted)

    # Lists are reduced as complete JSON values. High-priority fields are kept
    # intact until every ordinary collection has been reduced.
    def collect(
        node: Any,
        candidates: list[tuple[int, dict[str, Any], str, list[Any]]],
        path: tuple[str, ...] = (),
    ) -> None:
        if isinstance(node, dict):
            for key, child in node.items():
                if isinstance(child, list) and len(child) > 1:
                    priority = 1 if key in _HIGH_PRIORITY_KEYS or any(part in _HIGH_PRIORITY_KEYS for part in path) else 0
                    candidates.append((priority, node, key, child))
                collect(child, candidates, (*path, key))
        elif isinstance(node, list):
            for child in node:
                collect(child, candidates, path)

    # Fully exhaust ordinary collections before touching repair targets,
    # failures, response schemas, or other required operation context.
    for priority in (0, 1):
        while len(json.dumps(compact, ensure_ascii=False)) > target_chars:
            candidates: list[tuple[int, dict[str, Any], str, list[Any]]] = []
            collect(compact, candidates)
            candidates = [
                item for item in candidates if item[0] == priority
            ]
            candidates.sort(key=lambda item: (-len(item[3]), item[2]))
            changed = False
            for _priority, parent, key, _original in candidates:
                current = parent.get(key)
                if not isinstance(current, list) or len(current) <= 1:
                    continue
                keep = max(1, len(current) // 2)
                omitted[key] += len(current) - keep
                parent[key] = _list_round_robin_prefix(current, keep)
                changed = True
                if len(json.dumps(compact, ensure_ascii=False)) <= target_chars:
                    break
            if not changed:
                break
    return compact, dict(omitted)


def _pack_plain_text(content: str, target_chars: int) -> tuple[str, int]:
    if len(content) <= target_chars:
        return content, 0
    blocks = [block for block in content.split("\n\n") if block]
    if len(blocks) <= 1:
        lines = content.splitlines()
        blocks = lines if len(lines) > 1 else [content]
    if len(blocks) <= 1:
        raise ValueError("oversized indivisible prompt block cannot be safely packed")
    kept_front: list[str] = []
    kept_back: deque[str] = deque()
    used = 0
    front = 0
    back = len(blocks) - 1
    marker_reserve = 100
    take_front = True
    while front <= back:
        index = front if take_front else back
        block = blocks[index]
        cost = len(block) + 2
        if used + cost + marker_reserve > target_chars:
            break
        if take_front:
            kept_front.append(block)
            front += 1
        else:
            kept_back.appendleft(block)
            back -= 1
        used += cost
        take_front = not take_front
    omitted = max(0, back - front + 1)
    marker = f"[OMITTED_CONTEXT: {omitted} complete blocks removed to enforce request budget]"
    packed = "\n\n".join([*kept_front, marker, *kept_back])
    return packed, omitted


def fit_messages(
    messages: list[dict[str, str]],
    *,
    max_chars: int = MAX_LLM_REQUEST_CHARS,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    """Pack complete messages without slicing serialized JSON or SQL."""
    packed = copy.deepcopy(messages)
    original_size = messages_size(packed)
    stats: dict[str, Any] = {
        "original_request_chars": original_size,
        "final_request_chars": original_size,
        "included_counts": {"messages": len(packed)},
        "omitted_counts": {},
        **_hash_metadata(packed),
    }
    if original_size <= max_chars:
        _record_prompt_telemetry(stats)
        return packed, stats

    fixed_overhead = len(serialized_messages([{**message, "content": ""} for message in packed]))
    available = max_chars - fixed_overhead
    if available <= 0:
        raise ValueError("LLM message envelope exceeds request budget")
    content_lengths = [max(1, len(str(message.get("content") or ""))) for message in packed]
    total_content = sum(content_lengths)
    omitted: defaultdict[str, int] = defaultdict(int)

    for index, message in enumerate(packed):
        content = str(message.get("content") or "")
        target = max(256, int(available * content_lengths[index] / total_content))
        try:
            parsed = json.loads(content)
        except (TypeError, ValueError):
            parsed = None
        if isinstance(parsed, (dict, list)):
            compact, counts = _compact_json_object(parsed, target_chars=target)
            message["content"] = json.dumps(compact, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            for key, count in counts.items():
                omitted[key] += count
        else:
            message["content"], count = _pack_plain_text(content, target)
            omitted["text_blocks"] += count

    # Proportional estimates can leave JSON overhead above the exact cap. Make
    # complete-block passes over the largest non-system message until it fits.
    while messages_size(packed) > max_chars:
        candidates = sorted(
            range(len(packed)),
            key=lambda idx: (packed[idx].get("role") == "system", -len(packed[idx].get("content") or "")),
        )
        changed = False
        excess = messages_size(packed) - max_chars
        for index in candidates:
            content = packed[index].get("content") or ""
            target = max(256, len(content) - excess - 128)
            try:
                parsed = json.loads(content)
            except (TypeError, ValueError):
                parsed = None
            if isinstance(parsed, (dict, list)):
                compact, counts = _compact_json_object(parsed, target_chars=target)
                new_content = json.dumps(compact, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                for key, count in counts.items():
                    omitted[key] += count
            else:
                try:
                    new_content, count = _pack_plain_text(content, target)
                except ValueError:
                    continue
                omitted["text_blocks"] += count
            if len(new_content) < len(content):
                packed[index]["content"] = new_content
                changed = True
                break
        if not changed:
            raise ValueError("required LLM context cannot fit within the complete request budget")
    validate_messages(packed, max_chars=max_chars)
    stats["final_request_chars"] = messages_size(packed)
    stats["omitted_counts"] = dict(sorted(omitted.items()))
    _record_prompt_telemetry(stats)
    return packed, stats


def pack_active_schema(
    inventory: dict[str, Any],
    plan: dict[str, Any],
    *,
    data_profile: dict[str, Any] | None = None,
    target_columns: Iterable[tuple[str, str, str, str]] = (),
    max_chars: int = 40_000,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Pack ranked active columns round-robin across relevant assets."""
    validate_selection_plan(plan, inventory_hash=inventory["inventory_hash"])
    assets, columns = inventory_indexes(inventory)
    targets = set(target_columns)
    plan_assets = {tuple(asset["asset_key"]): asset for asset in plan.get("assets") or []}
    ordered_assets = sorted(
        assets,
        key=lambda key: (not any(target[:3] == key for target in targets), render_identifier_tuple(key)),
    )
    projection: dict[str, Any] = {
        "assets": [
            {
                "asset_id": assets[key]["asset_id"],
                "asset_type": assets[key]["asset_type"],
                "columns": [],
            }
            for key in ordered_assets
        ],
        "omitted_context_summary": {},
        "inventory_hash": inventory["inventory_hash"],
        "plan_hash": plan["plan_hash"],
    }
    output_by_key = {key: projection["assets"][index] for index, key in enumerate(ordered_assets)}
    queues: dict[tuple[str, str, str], deque[dict[str, Any]]] = {}
    for asset_key in ordered_assets:
        plan_asset = plan_assets.get(asset_key, {})
        rows = [row for row in plan_asset.get("columns") or [] if row.get("active")]
        rows.sort(key=lambda row: (tuple(row["column_key"]) not in targets, int(row.get("stable_rank") or 999999), row["column_id"]))
        queues[asset_key] = deque(rows)
    included = 0
    omitted = 0
    while any(queues.values()):
        progressed = False
        for asset_key in ordered_assets:
            if not queues[asset_key]:
                continue
            row = queues[asset_key].popleft()
            key = tuple(row["column_key"])
            column = columns[key]
            asset_profile = (data_profile or {}).get(assets[asset_key]["asset_id"], {})
            profile = (asset_profile.get("columns") or {}).get(key[3])
            projected = {
                "column_id": column["column_id"],
                "name": column["name"],
                "data_type": column.get("data_type", ""),
                "description": str(column.get("description") or "")[:300],
                "metric_role": column.get("metric_role"),
                "measure_expression": column.get("measure_expression"),
                "constraint_roles": column.get("constraint_roles") or [],
                "reason_codes": row.get("reason_codes") or [],
                "profile_status": row.get("profile_status"),
            }
            if isinstance(profile, dict):
                projected["profile"] = copy.deepcopy(profile)
            output_by_key[asset_key]["columns"].append(projected)
            if len(json.dumps(projection, ensure_ascii=False)) > max_chars and key not in targets:
                output_by_key[asset_key]["columns"].pop()
                omitted += 1
            else:
                included += 1
            progressed = True
        if not progressed:
            break
    omitted += sum(len(queue) for queue in queues.values())
    projection["omitted_context_summary"] = {
        "active_columns_included": included,
        "active_columns_omitted": omitted,
        "full_inventory_columns": len(columns),
    }
    stats = {
        "included_counts": {"assets": len(ordered_assets), "columns": included},
        "omitted_counts": {"columns": omitted},
        "plan_hash": plan["plan_hash"],
        "inventory_hash": inventory["inventory_hash"],
        "projection_chars": len(json.dumps(projection, ensure_ascii=False)),
    }
    return projection, stats


def render_identifier_tuple(parts: tuple[str, ...]) -> str:
    return ".".join(parts)
