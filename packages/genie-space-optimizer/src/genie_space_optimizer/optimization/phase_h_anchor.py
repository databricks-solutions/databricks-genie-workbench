"""Stable Phase H parent-run resolver/creator.

Phase H uploads ``gso_postmortem_bundle/`` artifacts (manifest, artifact
index, run summary, operator transcript) to a single, operator-discoverable
MLflow run per optimization. The lever-loop notebook does not wrap the
harness in an explicit ``mlflow.start_run(...)`` block, so relying on
``mlflow.active_run()`` at harness startup produces ``None`` in practice
— that is the root cause of pretty-print reliability issues where the
notebook logs ``phase_h_assembly_skipped_or_failed``.

This helper decouples anchor resolution from whatever the ambient
MLflow state happens to be at harness startup. It:

1. Searches the configured experiment for an existing parent lever-loop
   run tagged ``genie.run_role=lever_loop`` (and/or
   ``genie.run_type=lever_loop``) with a matching
   ``genie.optimization_run_id``.
2. Creates a new run using the canonical parent-run name and tags from
   :mod:`genie_space_optimizer.common.mlflow_names` when none exists.
3. Returns ``None`` on any MLflow/client failure so observability never
   breaks the optimizer hot path.

The returned run id is stable across harness invocations for the same
``optimization_run_id``, which is exactly what Phase H artifact uploads
need.
"""

from __future__ import annotations

import logging
from typing import Any

from genie_space_optimizer.common.mlflow_names import (
    lever_loop_parent_run_name,
    lever_loop_parent_run_tags,
)


logger = logging.getLogger(__name__)


_LEVER_LOOP_TAG_VALUE = "lever_loop"


def _find_existing_parent(
    client: Any,
    experiment_id: str,
    optimization_run_id: str,
) -> str | None:
    """Return an existing parent run id for this optimization, if any."""
    filter_string = (
        f"tags.`genie.optimization_run_id` = '{optimization_run_id}'"
    )
    try:
        runs = client.search_runs(
            experiment_ids=[experiment_id],
            filter_string=filter_string,
            max_results=200,
        )
    except Exception:
        logger.debug(
            "Phase H anchor: search_runs failed (non-fatal)",
            exc_info=True,
        )
        return None
    for run in runs or []:
        tags = run.data.tags or {}
        if (
            tags.get("genie.run_role", "") == _LEVER_LOOP_TAG_VALUE
            or tags.get("genie.run_type", "") == _LEVER_LOOP_TAG_VALUE
        ):
            return run.info.run_id
    if runs:
        earliest = min(runs, key=lambda r: r.info.start_time or 0)
        return earliest.info.run_id
    return None


def _resolve_experiment_id(client: Any, experiment_name: str) -> str | None:
    try:
        experiment = client.get_experiment_by_name(experiment_name)
    except Exception:
        logger.debug(
            "Phase H anchor: get_experiment_by_name failed (non-fatal)",
            exc_info=True,
        )
        return None
    if experiment is None:
        return None
    return experiment.experiment_id


def resolve_or_create_phase_h_anchor(
    *,
    experiment_name: str,
    optimization_run_id: str,
    databricks_job_id: str = "",
    databricks_parent_run_id: str = "",
    lever_loop_task_run_id: str = "",
    client: Any | None = None,
) -> str | None:
    """Return a stable parent run id for Phase H artifacts.

    Prefer an existing run tagged as the lever-loop parent for
    ``optimization_run_id``. Create one using the canonical parent-run
    name and tags when none exists. Return ``None`` on any MLflow/client
    failure so observability never breaks the optimizer.

    ``client`` is injectable for tests; when omitted, an
    :class:`mlflow.tracking.MlflowClient` is created lazily so callers
    that do not have MLflow installed never hit an ``ImportError``.

    The created/located run is tagged with BOTH ``genie.run_role`` and
    ``genie.run_type`` set to ``lever_loop`` so it is discoverable by
    both the canonical parent-tag vocabulary (see
    :mod:`genie_space_optimizer.common.mlflow_names`) and the legacy
    artifact-anchor resolution path in
    :mod:`genie_space_optimizer.tools.mlflow_artifact_anchor`.
    """
    if not experiment_name or not optimization_run_id:
        return None

    if client is None:
        try:
            from mlflow.tracking import MlflowClient  # type: ignore[import-not-found]
        except Exception:
            logger.debug(
                "Phase H anchor: mlflow not importable (non-fatal)",
                exc_info=True,
            )
            return None
        try:
            client = MlflowClient()
        except Exception:
            logger.debug(
                "Phase H anchor: MlflowClient() construction failed (non-fatal)",
                exc_info=True,
            )
            return None

    experiment_id = _resolve_experiment_id(client, experiment_name)
    if experiment_id is None:
        return None

    existing = _find_existing_parent(
        client, experiment_id, optimization_run_id,
    )
    if existing:
        return existing

    tags = dict(lever_loop_parent_run_tags(
        optimization_run_id=optimization_run_id,
        databricks_job_id=databricks_job_id,
        databricks_parent_run_id=databricks_parent_run_id,
        lever_loop_task_run_id=lever_loop_task_run_id,
    ))
    tags["genie.run_type"] = _LEVER_LOOP_TAG_VALUE

    try:
        run = client.create_run(
            experiment_id=experiment_id,
            tags=tags,
            run_name=lever_loop_parent_run_name(optimization_run_id),
        )
    except Exception:
        logger.debug(
            "Phase H anchor: create_run failed (non-fatal)",
            exc_info=True,
        )
        return None
    try:
        return run.info.run_id
    except Exception:
        return None
