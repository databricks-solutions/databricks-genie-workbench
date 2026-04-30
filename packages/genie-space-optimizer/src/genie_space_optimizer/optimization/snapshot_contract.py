"""Pre-action-group Genie Space snapshot contract.

The live optimizer uses this module to make rollback verification depend on
the in-process pre-AG snapshot instead of a Delta row that may be stale or
missing after a concurrent-write conflict.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.genie_client import (
    fetch_space_config,
    sort_genie_config,
    strip_non_exportable_fields,
)


_HIGH_SIGNAL_PATHS: tuple[tuple[str, ...], ...] = (
    ("data_sources", "tables"),
    ("data_sources", "metric_views"),
    ("instructions", "text_instructions"),
    ("instructions", "sql_snippets"),
    ("instructions", "example_question_sqls"),
)


def _parsed_space(config: dict[str, Any] | None) -> dict[str, Any]:
    """Return the parsed Genie ``serialized_space`` shape from a fetch response."""
    if not isinstance(config, dict):
        return {}
    parsed = config.get("_parsed_space")
    if isinstance(parsed, dict):
        return parsed
    serialized = config.get("serialized_space")
    if isinstance(serialized, dict):
        return serialized
    return config


def _normalize_recursive(value: Any) -> Any:
    """Strip private (``_``-prefixed) keys and sort dicts recursively."""
    if isinstance(value, dict):
        return {
            str(k): _normalize_recursive(v)
            for k, v in sorted(value.items(), key=lambda item: str(item[0]))
            if not str(k).startswith("_")
        }
    if isinstance(value, list):
        return [_normalize_recursive(item) for item in value]
    return value


def canonical_snapshot(value: Any) -> Any:
    """Return a stable compare shape for Genie Space snapshots.

    The rollback contract is the parsed Genie config, not the full API response.
    Normalize through the same exportable/sorted helpers used before PATCH so
    fields like ``_uc_columns``, ``_data_profile``, and ``uc_comment`` do not
    create false rollback mismatches.

    ``strip_non_exportable_fields`` and ``sort_genie_config`` are applied once
    at the top level (they know the top-level Genie config schema). The result
    is then walked recursively to sort nested dict keys and drop any remaining
    ``_``-prefixed runtime state.
    """
    if isinstance(value, dict):
        try:
            value = sort_genie_config(strip_non_exportable_fields(value))
        except Exception:
            value = dict(value)
    return _normalize_recursive(value)


def snapshot_digest(snapshot: dict[str, Any] | None) -> str:
    payload = json.dumps(
        canonical_snapshot(snapshot or {}),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _get_path(value: Any, path: tuple[str, ...]) -> Any:
    current = value
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def high_signal_projection(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    normalized = canonical_snapshot(snapshot or {})
    return {
        ".".join(path): _get_path(normalized, path)
        for path in _HIGH_SIGNAL_PATHS
    }


def _first_diff(expected: Any, live: Any, path: str = "") -> dict[str, Any] | None:
    if expected == live:
        return None
    if isinstance(expected, dict) and isinstance(live, dict):
        keys = sorted(set(expected) | set(live), key=str)
        for key in keys:
            child = _first_diff(
                expected.get(key),
                live.get(key),
                f"{path}.{key}" if path else str(key),
            )
            if child is not None:
                return child
    if isinstance(expected, list) and isinstance(live, list):
        for idx in range(max(len(expected), len(live))):
            expected_item = expected[idx] if idx < len(expected) else None
            live_item = live[idx] if idx < len(live) else None
            child = _first_diff(expected_item, live_item, f"{path}[{idx}]")
            if child is not None:
                return child
    return {
        "first_diff_path": path or "$",
        "first_diff_expected": expected,
        "first_diff_live": live,
    }


def capture_pre_ag_snapshot(
    *,
    w: WorkspaceClient | Any,
    space_id: str,
    ag_id: str,
) -> dict[str, Any]:
    try:
        snapshot = fetch_space_config(w, space_id)
    except Exception as exc:
        return {
            "captured": False,
            "ag_id": ag_id,
            "reason": "fetch_failed",
            "error": str(exc)[:500],
            "snapshot": {},
            "digest": "",
        }
    parsed = _parsed_space(snapshot)
    return {
        "captured": True,
        "ag_id": ag_id,
        "reason": "captured",
        "snapshot": parsed,
        "digest": snapshot_digest(parsed),
    }


def compare_live_to_expected_snapshot(
    *,
    w: WorkspaceClient | Any | None,
    space_id: str,
    expected_snapshot: dict[str, Any],
) -> dict[str, Any]:
    if w is None:
        return {"verified": True, "reason": "no_workspace_client"}
    try:
        live = fetch_space_config(w, space_id)
    except Exception as exc:
        return {
            "verified": False,
            "reason": "fetch_failed",
            "error": str(exc)[:500],
        }

    expected_norm = canonical_snapshot(expected_snapshot or {})
    live_norm = canonical_snapshot(_parsed_space(live))
    expected_digest = snapshot_digest(expected_norm)
    live_digest = snapshot_digest(live_norm)
    if expected_digest == live_digest:
        return {
            "verified": True,
            "reason": "matched_pre_snapshot",
            "expected_digest": expected_digest,
            "live_digest": live_digest,
        }

    expected_signal = high_signal_projection(expected_norm)
    live_signal = high_signal_projection(live_norm)
    diff = _first_diff(expected_norm, live_norm) or {}
    if expected_signal == live_signal:
        return {
            "verified": True,
            "reason": "matched_high_signal_config",
            "expected_digest": expected_digest,
            "live_digest": live_digest,
            **diff,
        }
    return {
        "verified": False,
        "reason": "live_config_differs_from_pre_snapshot",
        "expected_digest": expected_digest,
        "live_digest": live_digest,
        **diff,
    }
