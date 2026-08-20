"""Shared logging helpers for DAG notebook tasks.

Each notebook imports these and binds its own TASK_LABEL for consistent,
structured output across the 4-task optimization DAG.
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


def _diagnostic(task_label: str, event: str, **payload: object) -> None:
    """Print a bounded, human-readable operator diagnostic.

    GSO's durable Delta rows remain the source of truth.  This display is the
    first-response layer for an operator looking at Lakeflow Job output: it
    highlights the decision and gives enough identifiers to continue in the
    run tables without dumping large configs, benchmark SQL, or result rows.
    """
    print(f"\n[GSO DIAGNOSTIC] [{task_label}] {event}")
    for key, value in payload.items():
        label = key.replace("_", " ").strip().title()
        if isinstance(value, set):
            rendered = json.dumps(sorted(value, key=str), default=str)
        elif isinstance(value, (dict, list, tuple)):
            try:
                rendered = json.dumps(value, default=str, sort_keys=True)
            except TypeError:
                rendered = str(value)
        elif value is None:
            rendered = "(none)"
        else:
            rendered = str(value)
        rendered = rendered.replace("\r", "\\r").replace("\n", "\\n")
        if len(rendered) > 500:
            rendered = rendered[:497] + "..."
        print(f"  {label}: {rendered}")
