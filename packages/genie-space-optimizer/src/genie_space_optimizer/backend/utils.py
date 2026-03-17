"""Shared utility helpers for the Genie Space Optimizer backend."""

from __future__ import annotations

import json
import math
import os
from typing import Any

from databricks.sdk import WorkspaceClient
from fastapi import HTTPException


def scrub_nan_inf(obj: Any) -> Any:
    """Recursively replace NaN / Inf floats with None and coerce numpy scalars."""
    if isinstance(obj, float):
        return None if not math.isfinite(obj) else obj
    if isinstance(obj, dict):
        return {k: scrub_nan_inf(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [scrub_nan_inf(v) for v in obj]
    if hasattr(obj, "item"):
        try:
            return scrub_nan_inf(obj.item())
        except Exception:
            pass
    return obj


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
    """
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    if not isinstance(val, str):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return val


def ensure_utc_iso(val: Any) -> str | None:
    """Normalize a timestamp value to an ISO 8601 string with ``Z`` suffix.

    Handles Pandas Timestamps, Python datetimes, and bare strings that come
    back from Delta ``toPandas()`` without timezone info.  Returns ``None``
    for empty / unparseable values so callers can fall through gracefully.
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s or s in ("NaT", "None", "nan"):
        return None
    if s.endswith("Z") or "+" in s[10:]:
        return s
    try:
        import pandas as pd
        ts = pd.Timestamp(s)
        if ts is pd.NaT:
            return None
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.isoformat().replace("+00:00", "Z")
    except Exception:
        pass
    if "T" not in s and len(s) >= 10:
        s = s[:10] + "T" + s[11:]
    return s + "Z"


def get_sp_principal(ws: WorkspaceClient) -> str:
    """Return the app's service-principal client ID, or raise HTTP 500."""
    cid = ws.config.client_id or os.getenv("DATABRICKS_CLIENT_ID", "")
    if not cid:
        raise HTTPException(
            status_code=500,
            detail="Cannot determine app service principal ID.",
        )
    return cid
