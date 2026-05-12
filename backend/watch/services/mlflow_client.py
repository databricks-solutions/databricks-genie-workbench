"""MLflow tracking helpers for the GenieWatch evals tab."""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _client():
    import mlflow
    from mlflow.tracking import MlflowClient

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "databricks")
    mlflow.set_tracking_uri(tracking_uri)
    return MlflowClient(tracking_uri=tracking_uri)


def get_experiment(experiment_id: str) -> Optional[dict[str, Any]]:
    if not experiment_id:
        return None
    try:
        exp = _client().get_experiment(experiment_id)
    except Exception as e:
        logger.info("get_experiment(%s) failed: %s", experiment_id, e)
        return None
    if not exp:
        return None
    return {
        "experiment_id": exp.experiment_id,
        "name": exp.name,
        "lifecycle_stage": exp.lifecycle_stage,
        "artifact_location": exp.artifact_location,
        "creation_time": exp.creation_time,
        "last_update_time": exp.last_update_time,
        "tags": dict(exp.tags or {}),
    }


def search_runs(experiment_id: str, max_results: int = 50) -> list[dict[str, Any]]:
    if not experiment_id:
        return []
    try:
        runs = _client().search_runs(
            experiment_ids=[experiment_id],
            max_results=max_results,
            order_by=["start_time DESC"],
        )
    except Exception as e:
        logger.info("search_runs(%s) failed: %s", experiment_id, e)
        return []
    out = []
    for r in runs:
        info = r.info
        data = r.data
        out.append({
            "run_id": info.run_id,
            "run_name": info.run_name,
            "status": info.status,
            "start_time": info.start_time,
            "end_time": info.end_time,
            "user_id": info.user_id,
            "metrics": dict(data.metrics or {}),
            "params": dict(data.params or {}),
            "tags": {k: v for k, v in (data.tags or {}).items() if not k.startswith("mlflow.")},
        })
    return out


def get_run(run_id: str) -> Optional[dict[str, Any]]:
    if not run_id:
        return None
    try:
        r = _client().get_run(run_id)
    except Exception as e:
        logger.info("get_run(%s) failed: %s", run_id, e)
        return None
    return {
        "run_id": r.info.run_id,
        "run_name": r.info.run_name,
        "status": r.info.status,
        "start_time": r.info.start_time,
        "end_time": r.info.end_time,
        "experiment_id": r.info.experiment_id,
        "metrics": dict(r.data.metrics or {}),
        "params": dict(r.data.params or {}),
        "tags": dict(r.data.tags or {}),
    }
