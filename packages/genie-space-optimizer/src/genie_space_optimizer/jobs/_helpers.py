"""Shared logging helpers for DAG notebook tasks.

Each notebook imports these and binds its own TASK_LABEL for consistent,
structured output across all 6 tasks in the optimization DAG.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone


def _ts() -> str:
    """Return current UTC timestamp string for log prefixes."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _banner(task_label: str, title: str) -> None:
    """Print a 120-char separator with ``[task_label] title`` for visual section breaks."""
    print("\n" + "=" * 120)
    print(f"[{_ts()}] [{task_label}] {title}")
    print("=" * 120)


def _log(task_label: str, event: str, **payload: object) -> None:
    """Log *event* with optional JSON payload, prefixed by *task_label*."""
    print(f"[{_ts()}] [{task_label}] {event}")
    if payload:
        print(json.dumps(payload, indent=2, default=str))
