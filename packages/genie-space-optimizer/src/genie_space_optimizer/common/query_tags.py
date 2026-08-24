"""Query tags shared by all GSO-controlled Statement Execution requests."""

from __future__ import annotations

import os


def gso_query_tags(*, purpose: str, run_id: str | None = None) -> list[object]:
    """Return the required traffic-attribution tags for a SQL statement."""
    from databricks.sdk.service.sql import QueryTag

    resolved_run_id = str(run_id or os.getenv("GSO_RUN_ID") or "unknown")
    return [
        QueryTag(key="application", value="genie_workbench"),
        QueryTag(key="component", value="gso"),
        QueryTag(key="purpose", value=purpose),
        QueryTag(key="run_id", value=resolved_run_id),
    ]
