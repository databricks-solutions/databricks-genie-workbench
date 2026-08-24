"""Shared numeric and JSON coercion helpers used by Workbench."""

from __future__ import annotations

import json
import math
from typing import Any


def safe_float(val: Any) -> float | None:
    """Convert to float, returning None for None / NaN / Inf / non-numeric."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def safe_int(val: Any) -> int | None:
    """Convert to int via float, returning None for None / NaN / Inf / non-numeric."""
    if val is None:
        return None
    try:
        f = float(val)
        if not math.isfinite(f):
            return None
        return int(f)
    except (TypeError, ValueError):
        return None


def safe_finite(val: Any, default: float = 0.0) -> float:
    """Convert to float, returning *default* for None / NaN / Inf / non-numeric."""
    if val is None:
        return default
    try:
        f = float(val)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def safe_json_parse(val: Any) -> Any:
    """Parse a JSON string if needed; return the original value on failure.

    Already-parsed dicts/lists pass through unchanged. Returns None for None.
    Handles double/triple-encoded JSON strings by unwrapping iteratively.
    """
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    if not isinstance(val, str):
        return val
    try:
        result = json.loads(val)
        for _ in range(3):
            if not isinstance(result, str):
                break
            result = json.loads(result)
        return result
    except (json.JSONDecodeError, TypeError):
        return val
