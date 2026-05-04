"""Stable anchor resolution for decision-trail MLflow artifacts.

Phase E.0 Task 4. The harness rotates the active MLflow run via
end_run/start_run between stages, so mlflow.active_run() at
persistence time is whichever stage was last started — typically the
strategy or full_eval child, not the parent lever_loop run. This
module resolves a stable run for every decision-trail artifact upload
so all three artifact prefixes land on a single, operator-discoverable
run regardless of stage.

Resolution rules (in priority order):
1. Sibling run whose tag ``genie.run_role=lever_loop`` or
   ``genie.run_type=lever_loop``. Both vocabularies are accepted because
   the canonical parent-run vocabulary in
   :mod:`genie_space_optimizer.common.mlflow_names` stamps
   ``genie.run_role`` while older code paths stamped ``genie.run_type``.
2. Earliest `start_time` sibling — typically the parent lever_loop.
3. Empty string when no sibling matches the optimization_run_id tag.

The caller decides what to do with an empty anchor — `harness.py` in
Task 5 falls back to the existing `mlflow.active_run()` behavior with
a stdout-marker note so the audit trail is preserved.
"""

from __future__ import annotations

from typing import Any, Sequence


_LEVER_LOOP_RUN_TYPE = "lever_loop"


def resolve_anchor_run_id(
    *,
    client: Any,
    opt_run_id: str,
    experiment_ids: Sequence[str],
) -> str:
    """Return the run_id of the stable anchor for decision-trail artifacts."""
    if not opt_run_id or not experiment_ids:
        return ""
    filter_string = f"tags.`genie.optimization_run_id` = '{opt_run_id}'"
    runs = client.search_runs(
        experiment_ids=list(experiment_ids),
        filter_string=filter_string,
        max_results=200,
    )
    if not runs:
        return ""
    for run in runs:
        tags = run.data.tags or {}
        run_role = tags.get("genie.run_role", "")
        run_type = tags.get("genie.run_type", "")
        if run_role == _LEVER_LOOP_RUN_TYPE or run_type == _LEVER_LOOP_RUN_TYPE:
            return run.info.run_id
    earliest = min(runs, key=lambda r: r.info.start_time or 0)
    return earliest.info.run_id
