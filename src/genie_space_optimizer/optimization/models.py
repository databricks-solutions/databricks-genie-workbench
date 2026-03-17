"""
LoggedModel management — create, promote, rollback, and link scores.

Each evaluation iteration snapshots the Genie Space config + UC metadata
as an MLflow LoggedModel. This provides full lineage and one-click rollback.
"""

from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import mlflow

from genie_space_optimizer.common.config import MODEL_NAME_TEMPLATE, format_mlflow_template
from genie_space_optimizer.optimization.state import (
    load_iterations,
    load_run,
    update_run_status,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def create_genie_model_version(
    w: WorkspaceClient,
    space_id: str,
    config: dict,
    iteration: int,
    domain: str,
    experiment_name: str | None = None,
    *,
    uc_schema: str,
    uc_columns: list[dict] | None = None,
    uc_tags: list[dict] | None = None,
    uc_routines: list[dict] | None = None,
    patch_set: list[dict] | None = None,
    parent_model_id: str | None = None,
    optimization_run_id: str = "",
) -> str:
    """Create an MLflow LoggedModel snapshot for a Genie Space iteration.

    Captures the full Genie config and UC metadata state as model params
    and artifacts.  Must be called inside an active ``mlflow.start_run()``
    context so that artifacts are logged to the caller's run.

    Returns the ``model_id`` string.
    """
    model_name = format_mlflow_template(MODEL_NAME_TEMPLATE, space_id=space_id)

    try:
        if experiment_name:
            mlflow.set_experiment(experiment_name)

        resolved_columns, resolved_tags, resolved_routines = _resolve_uc_metadata(
            config=config,
            uc_columns=uc_columns,
            uc_tags=uc_tags,
            uc_routines=uc_routines,
        )
        metadata_snapshot: dict[str, Any] = {
            "space_config": config,
            "iteration": iteration,
            "uc_schema": uc_schema,
            "uc_columns": resolved_columns,
            "uc_tags": resolved_tags,
            "uc_routines": resolved_routines,
            "patch_set": patch_set or [],
            "parent_model_id": parent_model_id,
        }
        artifact_prefix = f"model_snapshots/iter_{iteration}"

        active_run = mlflow.active_run()
        if not active_run:
            raise RuntimeError(
                "create_genie_model_version requires an active MLflow run "
                "for artifact logging.  Call from inside run_evaluation() "
                "or a dedicated mlflow.start_run() block."
            )

        run_id = active_run.info.run_id
        _log_dict_artifact(config, f"{artifact_prefix}/space_config.json")
        _log_dict_artifact(metadata_snapshot, f"{artifact_prefix}/metadata_snapshot.json")

        model = _initialize_logged_model(
            name=model_name,
            source_run_id=run_id,
            params={
                "space_id": space_id,
                "domain": domain,
                "iteration": str(iteration),
                "uc_schema": uc_schema,
                "uc_columns_count": str(len(resolved_columns)),
                "uc_tags_count": str(len(resolved_tags)),
                "uc_routines_count": str(len(resolved_routines)),
                "patch_count": str(len(patch_set or [])),
                "parent_model_id": str(parent_model_id or ""),
                "snapshot_run_id": run_id,
                "space_config_artifact": f"{artifact_prefix}/space_config.json",
                "metadata_artifact": f"{artifact_prefix}/metadata_snapshot.json",
                "model_space_config": _safe_serialize(config),
            },
            tags={
                "domain": domain,
                "space_id": space_id,
                "iteration": str(iteration),
                "uc_schema": uc_schema,
                "traceability": "genie_space_optimizer",
                **({"genie.optimization_run_id": optimization_run_id} if optimization_run_id else {}),
            },
        )
        model_id = model.model_id
        _finalize_logged_model(model_id)

        logger.info(
            "Created LoggedModel %s (iter=%d, model_id=%s)",
            model_name, iteration, model_id,
        )
        return model_id

    except Exception:
        logger.exception("Failed to create LoggedModel for iter %d", iteration)
        return ""


def promote_best_model(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> str | None:
    """Promote the best iteration's model to the 'champion' alias.

    Reads all iterations from Delta to find the one with highest
    ``overall_accuracy``, then sets the MLflow alias.

    Returns the promoted model_id, or None on failure.
    """
    run_row = load_run(spark, run_id, catalog, schema)
    if not run_row:
        logger.error("Cannot promote: run %s not found", run_id)
        return None

    iterations_df = load_iterations(spark, run_id, catalog, schema)
    if iterations_df.empty:
        logger.warning("No iterations found for run %s", run_id)
        return None

    full_evals = iterations_df[iterations_df["eval_scope"] == "full"]
    if full_evals.empty:
        full_evals = iterations_df

    best_idx = full_evals["overall_accuracy"].idxmax()
    best_row = full_evals.loc[best_idx]
    best_model_id = best_row.get("model_id")
    best_iteration = int(best_row.get("iteration", 0))
    best_accuracy = float(best_row.get("overall_accuracy", 0.0))

    if not best_model_id:
        logger.warning("Best iteration %d has no model_id", best_iteration)
        return None

    space_id = run_row.get("space_id", "")
    model_name = format_mlflow_template(MODEL_NAME_TEMPLATE, space_id=space_id)

    try:
        alias_fn = getattr(mlflow, "set_logged_model_alias", None)
        if callable(alias_fn):
            alias_fn(
                model_id=best_model_id,
                alias="champion",
            )
        else:
            logger.warning("mlflow.set_logged_model_alias is unavailable in this environment")
        logger.info(
            "Promoted model %s (iter=%d, accuracy=%.1f%%) as champion",
            best_model_id, best_iteration, best_accuracy,
        )
    except Exception:
        logger.exception("Failed to set champion alias on %s", best_model_id)

    update_run_status(
        spark, run_id, catalog, schema,
        best_iteration=best_iteration,
        best_accuracy=best_accuracy,
        best_model_id=best_model_id,
    )

    return best_model_id


_EVAL_JUDGES = [
    "eval_result_correctness", "eval_syntax_validity",
    "eval_schema_accuracy", "eval_logical_accuracy",
    "eval_semantic_equivalence", "eval_completeness",
    "eval_response_quality", "eval_asset_routing",
]


import pandas as pd  # runtime import — MLflow introspects predict() type hints


class _GenieConfigSnapshot(mlflow.pyfunc.PythonModel):
    """Minimal pyfunc wrapper so the config snapshot can be registered to UC.

    UC model registration requires a valid MLflow model directory with an
    ``MLmodel`` file.  This wrapper satisfies that requirement while the
    real value lives in the embedded ``space_config.json`` artifact.
    """

    def predict(
        self,
        context: mlflow.pyfunc.PythonModelContext,
        model_input: pd.DataFrame,
        params: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        result: dict = {"status": "config_snapshot_only"}
        if context and context.artifacts:
            cfg_path = context.artifacts.get("space_config", "")
            if cfg_path:
                result = json.loads(Path(cfg_path).read_text(encoding="utf-8"))
        return pd.DataFrame([result])


def _extract_space_dimensions(space_config: dict) -> dict[str, Any]:
    """Extract configuration dimensions from a parsed Genie Space config."""
    parsed = space_config.get("_parsed_space", space_config)
    ds = parsed.get("data_sources", {}) if isinstance(parsed, dict) else {}
    instr = parsed.get("instructions", {}) if isinstance(parsed, dict) else {}
    benchmarks_block = parsed.get("benchmarks", {}) if isinstance(parsed, dict) else {}
    cfg_block = parsed.get("config", {}) if isinstance(parsed, dict) else {}

    tables = [t.get("identifier", "") for t in ds.get("tables", []) if isinstance(t, dict)]
    metric_views = [m.get("identifier", "") for m in ds.get("metric_views", []) if isinstance(m, dict)]
    functions = [f.get("identifier", "") for f in (
        instr.get("sql_functions", []) or ds.get("functions", [])
    ) if isinstance(f, dict)]

    return {
        "tables": tables,
        "metric_views": metric_views,
        "functions": functions,
        "text_instructions": instr.get("text_instructions", []) or [],
        "join_specs": instr.get("join_specs", []) or [],
        "example_sqls": instr.get("example_question_sqls", []) or [],
        "sql_snippets": instr.get("sql_snippets", {}),
        "benchmarks": benchmarks_block.get("questions", []) or [],
        "sample_questions": cfg_block.get("sample_questions", []) or [],
        "ds_raw": ds,
        "instr_raw": instr,
    }


def _register_uc_version(
    *,
    client: Any,
    uc_model_name: str,
    source_run_id: str,
    best_iteration: int,
    space_config: dict,
    space_id: str,
    domain: str,
    space_name: str = "",
    dimensions: dict[str, Any] | None = None,
    best_accuracy: float = 0.0,
    baseline_accuracy: float | None = None,
    best_iter_row: dict[str, Any] | None = None,
    params: dict[str, str] | None = None,
) -> Any:
    """Create a pyfunc model via log_model and register it to UC.

    Uses ``mlflow.pyfunc.log_model(params=...)`` so that parameters
    propagate to the UC model version (visible in Catalog Explorer).
    """
    from mlflow.models.signature import ModelSignature
    from mlflow.types.schema import ColSpec, Schema

    dims = dimensions or _extract_space_dimensions(space_config)

    # ── Build configuration-aware signature ──
    input_cols: list[ColSpec] = [
        ColSpec("string", "space_id"),
        ColSpec("string", "space_name"),
        ColSpec("string", "domain"),
    ]
    for t in dims["tables"]:
        input_cols.append(ColSpec("string", f"table:{t}"))
    for mv in dims["metric_views"]:
        input_cols.append(ColSpec("string", f"metric_view:{mv}"))
    for fn in dims["functions"]:
        input_cols.append(ColSpec("string", f"function:{fn}"))
    input_cols += [
        ColSpec("long", "instruction_count"),
        ColSpec("long", "join_spec_count"),
        ColSpec("long", "example_sql_count"),
        ColSpec("long", "benchmark_count"),
    ]

    output_cols = [ColSpec("double", j) for j in _EVAL_JUDGES]
    output_cols += [
        ColSpec("double", "overall_accuracy"),
        ColSpec("double", "baseline_accuracy"),
    ]

    signature = ModelSignature(
        inputs=Schema(input_cols),  # type: ignore[arg-type]  # ColSpec is a valid subtype
        outputs=Schema(output_cols),
    )

    # ── Build an input example matching the full signature ──
    _example_row: dict[str, object] = {
        "space_id": space_id,
        "space_name": space_name,
        "domain": domain,
    }
    for t in dims["tables"]:
        _example_row[f"table:{t}"] = t
    for mv in dims["metric_views"]:
        _example_row[f"metric_view:{mv}"] = mv
    for fn in dims["functions"]:
        _example_row[f"function:{fn}"] = fn
    _example_row.update({
        "instruction_count": len(dims["text_instructions"]),
        "join_spec_count": len(dims["join_specs"]),
        "example_sql_count": len(dims["example_sqls"]),
        "benchmark_count": len(dims["benchmarks"]),
    })
    input_example = pd.DataFrame([_example_row])

    # ── Write artifacts and log pyfunc model ──
    with tempfile.TemporaryDirectory(prefix="genie-uc-") as tmp:
        config_path = Path(tmp) / "space_config.json"
        config_path.write_text(
            json.dumps(space_config, default=str, indent=2), encoding="utf-8",
        )

        uc_metadata = {
            "tables": dims["ds_raw"].get("tables", []),
            "metric_views": dims["ds_raw"].get("metric_views", []),
            "functions": dims["instr_raw"].get("sql_functions", []),
            "join_specs": dims["instr_raw"].get("join_specs", []),
            "text_instructions": dims["instr_raw"].get("text_instructions", []),
            "example_sqls": dims["instr_raw"].get("example_question_sqls", []),
        }
        uc_metadata_path = Path(tmp) / "uc_metadata.json"
        uc_metadata_path.write_text(
            json.dumps(uc_metadata, default=str, indent=2), encoding="utf-8",
        )

        benchmark_summary: dict[str, Any] = {
            "iteration": best_iteration,
            "overall_accuracy": best_accuracy,
            "baseline_accuracy": baseline_accuracy,
        }
        if best_iter_row:
            rows_json = best_iter_row.get("rows_json")
            if rows_json:
                benchmark_summary["rows_json"] = rows_json
        benchmark_path = Path(tmp) / "benchmark_summary.json"
        benchmark_path.write_text(
            json.dumps(benchmark_summary, default=str, indent=2), encoding="utf-8",
        )

        data_profile = space_config.get("_data_profile", {})
        data_profile_path = Path(tmp) / "data_profile.json"
        data_profile_path.write_text(
            json.dumps(data_profile, default=str, indent=2), encoding="utf-8",
        )

        artifacts = {
            "space_config": str(config_path),
            "uc_metadata": str(uc_metadata_path),
            "benchmark_summary": str(benchmark_path),
            "data_profile": str(data_profile_path),
        }

        with mlflow.start_run(run_id=source_run_id):
            model_info = mlflow.pyfunc.log_model(
                name="uc_model",
                python_model=_GenieConfigSnapshot(),
                artifacts=artifacts,
                signature=signature,
                input_example=input_example,
                params=params or {},
                pip_requirements=["mlflow[databricks]>=3.10.1", "pandas"],
            )

        logger.info(
            "Logged pyfunc model with params + 4 artifacts to run %s",
            source_run_id,
        )

    mv = mlflow.register_model(model_info.model_uri, uc_model_name)
    logger.info(
        "Registered UC version %s via pyfunc wrapper (run=%s)",
        mv.version, source_run_id,
    )
    return mv


def register_uc_model(
    spark: "SparkSession",
    run_id: str,
    catalog: str,
    schema: str,
    ws: "WorkspaceClient | None" = None,
    deploy_target: str | None = None,
) -> dict | None:
    """Register champion LoggedModel as a UC Registered Model version.

    Creates/updates the UC registered model with Genie Space metadata,
    registers a new version from the best iteration's eval run, and
    promotes to ``@champion`` only if metrics exceed the existing champion.

    Returns a dict with ``uc_model_name``, ``version``,
    ``promoted_to_champion``, and ``comparison`` (or ``None`` on failure).
    """
    from genie_space_optimizer.common.config import (
        ENABLE_UC_MODEL_REGISTRATION,
        UC_REGISTERED_MODEL_TEMPLATE,
        format_mlflow_template,
    )

    if not ENABLE_UC_MODEL_REGISTRATION:
        logger.info("UC model registration disabled, skipping")
        return None

    run_row = load_run(spark, run_id, catalog, schema)
    if not run_row:
        logger.error("Cannot register UC model: run %s not found", run_id)
        return None

    best_model_id = run_row.get("best_model_id")
    space_id = run_row.get("space_id", "")
    domain = run_row.get("domain", "")
    best_accuracy = float(run_row.get("best_accuracy", 0.0))
    best_iteration = int(run_row.get("best_iteration", 0))
    convergence_reason = run_row.get("convergence_reason", "")

    if not best_model_id:
        logger.warning("No best_model_id for run %s, skipping UC registration", run_id)
        return None

    iterations_df = load_iterations(spark, run_id, catalog, schema)
    best_iter_rows = iterations_df[
        (iterations_df["iteration"] == best_iteration)
        & (iterations_df["eval_scope"] == "full")
    ]
    source_run_id = ""
    if not best_iter_rows.empty:
        source_run_id = str(best_iter_rows.iloc[0].get("mlflow_run_id", ""))
    if not source_run_id:
        logger.warning("No mlflow_run_id for best iteration %d, skipping UC registration", best_iteration)
        return None

    baseline_rows = iterations_df[
        (iterations_df["iteration"] == 0) & (iterations_df["eval_scope"] == "full")
    ]
    baseline_accuracy = (
        float(baseline_rows.iloc[0].get("overall_accuracy", 0.0))
        if not baseline_rows.empty else None
    )

    space_name = ""
    space_description = ""
    _space_config: dict = {}
    if ws:
        try:
            from genie_space_optimizer.common.genie_client import fetch_space_config
            _cfg = fetch_space_config(ws, space_id)
            _space_config = _cfg
            space_name = _cfg.get("title", _cfg.get("name", ""))
            space_description = _cfg.get("description", "")
        except Exception:
            logger.debug("Could not fetch space config for UC description", exc_info=True)

    uc_model_name = format_mlflow_template(
        UC_REGISTERED_MODEL_TEMPLATE,
        catalog=catalog, schema=schema, space_id=space_id,
    )

    try:
        from mlflow.tracking import MlflowClient
        from mlflow import set_registry_uri as _set_registry_uri
        _set_registry_uri("databricks-uc")
        client = MlflowClient(registry_uri="databricks-uc")

        # ── 1. Ensure UC registered model with description + model-level tags ──
        model_description = (
            f"Optimization snapshots for Genie Space: {space_name}\n\n"
            f"{space_description}"
        ).strip() if (space_name or space_description) else "Genie Space optimization model"

        try:
            client.get_registered_model(uc_model_name)
            client.update_registered_model(uc_model_name, description=model_description)
        except Exception:
            client.create_registered_model(uc_model_name, description=model_description)

        _model_tags = {
            "genie.space_id": space_id,
            "genie.space_name": space_name,
            "genie.domain": domain,
            "genie.managed_by": "genie_space_optimizer",
        }
        for k, v in _model_tags.items():
            if v:
                try:
                    client.set_registered_model_tag(uc_model_name, k, v)
                except Exception:
                    logger.debug("Failed to set model tag %s", k, exc_info=True)

        # ── 2. Extract space config dimensions ──
        dims = _extract_space_dimensions(_space_config)

        best_row_dict: dict[str, Any] = {}
        if not best_iter_rows.empty:
            best_row_dict = best_iter_rows.iloc[0].to_dict()

        # ── 3. Build params and register a new version ──
        _params: dict[str, str] = {
            "space_id": space_id,
            "space_name": space_name,
            "domain": domain,
            "best_accuracy": f"{best_accuracy:.1f}",
            "baseline_accuracy": f"{baseline_accuracy:.1f}" if baseline_accuracy is not None else "n/a",
            "accuracy_delta": f"{best_accuracy - (baseline_accuracy or 0):.1f}",
            "best_iteration": str(best_iteration),
            "convergence_reason": convergence_reason,
            "tables_count": str(len(dims["tables"])),
            "metric_views_count": str(len(dims["metric_views"])),
            "tvf_count": str(len(dims["functions"])),
            "instruction_count": str(len(dims["text_instructions"])),
            "join_spec_count": str(len(dims["join_specs"])),
            "example_sql_count": str(len(dims["example_sqls"])),
            "benchmark_count": str(len(dims["benchmarks"])),
        }

        mv = _register_uc_version(
            client=client,
            uc_model_name=uc_model_name,
            source_run_id=source_run_id,
            best_iteration=best_iteration,
            space_config=_space_config,
            space_id=space_id,
            domain=domain,
            space_name=space_name,
            dimensions=dims,
            best_accuracy=best_accuracy,
            baseline_accuracy=baseline_accuracy,
            best_iter_row=best_row_dict,
            params=_params,
        )

        version = str(mv.version)

        # ── 4. Fetch per-judge eval scores from source run ──
        tracking_client = mlflow.tracking.MlflowClient()
        new_scores: dict[str, float] = {}
        held_out_acc: float | None = None
        held_out_count: float | None = None
        try:
            new_run_data = tracking_client.get_run(source_run_id).data
            new_scores = {j: new_run_data.metrics.get(j, 0.0) for j in _EVAL_JUDGES}
            held_out_acc = new_run_data.metrics.get("held_out_accuracy")
            held_out_count = new_run_data.metrics.get("held_out_count")
        except Exception:
            logger.debug("Could not fetch eval metrics from source run", exc_info=True)

        overall_judge_avg = (
            sum(new_scores.values()) / max(len(new_scores), 1)
            if new_scores else 0.0
        )

        # ── 5. Version-level tags (run metadata + per-judge scores) ──
        _version_tags: dict[str, str] = {
            "genie.optimization_run_id": run_id,
            "genie.iteration": str(best_iteration),
            "genie.accuracy": f"{best_accuracy:.1f}",
            "genie.convergence_reason": convergence_reason,
            "genie.source_run_id": source_run_id,
            "genie.overall_accuracy": f"{best_accuracy:.1f}",
            "genie.overall_score": f"{overall_judge_avg:.2f}",
        }
        if baseline_accuracy is not None:
            _version_tags["genie.baseline_accuracy"] = f"{baseline_accuracy:.1f}"
        if held_out_acc is not None:
            _version_tags["genie.held_out_accuracy"] = f"{held_out_acc:.1f}"
        if held_out_count is not None:
            _version_tags["genie.held_out_count"] = f"{int(held_out_count)}"
        if deploy_target:
            _version_tags["genie.deploy_target_url"] = deploy_target
        for judge, score in new_scores.items():
            _version_tags[judge] = f"{score:.2f}"
        for k, v in _version_tags.items():
            if v:
                try:
                    client.set_model_version_tag(uc_model_name, version, k, v)
                except Exception:
                    logger.debug("Failed to set version tag %s", k, exc_info=True)

        # ── 6. Metric-based gating for @champion alias ──
        should_promote = True
        existing_scores: dict[str, float] = {}
        prev_champion_version: str | None = None
        comparison: dict | None = None

        try:
            existing_mv = client.get_model_version_by_alias(uc_model_name, "champion")
            prev_champion_version = str(existing_mv.version)
            if existing_mv.run_id:
                existing_run_data = tracking_client.get_run(existing_mv.run_id).data
                existing_scores = {j: existing_run_data.metrics.get(j, 0.0) for j in _EVAL_JUDGES}

                comparison = {
                    j: {"new": new_scores.get(j, 0.0), "existing": existing_scores[j]}
                    for j in _EVAL_JUDGES
                }

                new_rc = new_scores.get("eval_result_correctness", 0.0)
                existing_rc = existing_scores.get("eval_result_correctness", 0.0)
                if new_rc < existing_rc:
                    should_promote = False
                    logger.info(
                        "Gating: result_correctness %.1f < existing %.1f",
                        new_rc, existing_rc,
                    )

                existing_avg = sum(existing_scores.values()) / max(len(existing_scores), 1)
                if overall_judge_avg < existing_avg:
                    should_promote = False
                    logger.info(
                        "Gating: avg judge score %.1f < existing %.1f",
                        overall_judge_avg, existing_avg,
                    )
        except Exception:
            logger.info("No existing @champion or error fetching metrics — promoting by default")

        if should_promote:
            client.set_registered_model_alias(uc_model_name, "champion", version)
            logger.info("Promoted UC model %s version %s as @champion", uc_model_name, version)
        else:
            logger.info(
                "UC model %s version %s registered but NOT promoted "
                "(existing champion v%s is better)",
                uc_model_name, version, prev_champion_version,
            )

        # ── 7. Link deployment job (optional) ──
        if ws is not None:
            try:
                from genie_space_optimizer.backend.job_launcher import ensure_deployment_job
                deploy_job_id = ensure_deployment_job(
                    ws, space_id=space_id, catalog=catalog, schema=schema,
                )
                client.update_registered_model(
                    uc_model_name, deployment_job_id=str(deploy_job_id),
                )
                logger.info("Linked deployment job %s to UC model %s", deploy_job_id, uc_model_name)
            except Exception:
                logger.exception("Failed to link deployment job (non-fatal)")

        return {
            "uc_model_name": uc_model_name,
            "version": version,
            "promoted_to_champion": should_promote,
            "previous_champion_version": prev_champion_version,
            "comparison": comparison,
            "deploy_target": deploy_target,
        }
    except Exception:
        logger.exception("Failed to register UC model %s", uc_model_name)
        return None


def rollback_to_model(
    w: WorkspaceClient,
    model_id: str,
) -> dict | None:
    """Restore Genie config from a LoggedModel snapshot.

    Reads the config artifact from the model and re-applies it via
    the Genie PATCH API. Returns the restored config or None on failure.
    """
    try:
        model = mlflow.get_logged_model(model_id=model_id)
        params = model.params or {}
        space_id = params.get("space_id")
        if not space_id:
            logger.error("Model %s has no space_id param", model_id)
            return None

        config_json = params.get("model_space_config")
        if not config_json:
            logger.warning(
                "Model %s has no config snapshot; rollback requires manual intervention",
                model_id,
            )
            return None

        config = json.loads(config_json)
        from genie_space_optimizer.common.genie_client import patch_space_config
        patch_space_config(w, space_id, config)
        logger.info("Rolled back space %s to model %s", space_id, model_id)
        return config

    except Exception:
        logger.exception("Rollback to model %s failed", model_id)
        return None


def link_eval_scores_to_model(
    model_id: str,
    scores: dict[str, float],
    eval_run_id: str = "",
) -> None:
    """Link evaluation metrics to a LoggedModel.

    Logs metrics to the eval run via ``MlflowClient`` (avoiding implicit
    run creation) and directly to the LoggedModel so they appear on the
    Model card in the UI.
    """
    try:
        from mlflow.tracking import MlflowClient

        if eval_run_id:
            client = MlflowClient()
            for judge, score in scores.items():
                client.log_metric(eval_run_id, f"eval_{judge}", score)
        else:
            for judge, score in scores.items():
                mlflow.log_metric(f"eval_{judge}", score)

        if model_id and model_id.startswith("m-"):
            try:
                mlflow.set_active_model(model_id=model_id)
                metrics_dict = {f"eval_{judge}": score for judge, score in scores.items()}
                mlflow.log_metrics(metrics_dict, model_id=model_id)
            except (TypeError, AttributeError):
                pass
            except Exception:
                logger.debug("Model-level metric logging not supported, run-level only", exc_info=True)

        logger.info("Linked %d scores to model %s", len(scores), model_id)
    except Exception:
        logger.exception("Failed to link scores to model %s", model_id)


def _safe_serialize(obj: Any) -> str:
    """JSON-serialize, truncating to 250 chars for MLflow params."""
    try:
        s = json.dumps(obj, default=str)
        return s[:250] if len(s) > 250 else s
    except Exception:
        return str(obj)[:250]


def _resolve_uc_metadata(
    *,
    config: dict,
    uc_columns: list[dict] | None,
    uc_tags: list[dict] | None,
    uc_routines: list[dict] | None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Resolve UC metadata from explicit args first, then prefetched snapshot."""
    prefetched = config.get("_prefetched_uc_metadata", {})
    prefetched_columns = prefetched.get("uc_columns", []) if isinstance(prefetched, dict) else []
    prefetched_tags = prefetched.get("uc_tags", []) if isinstance(prefetched, dict) else []
    prefetched_routines = prefetched.get("uc_routines", []) if isinstance(prefetched, dict) else []

    resolved_columns = uc_columns if isinstance(uc_columns, list) else (
        prefetched_columns if isinstance(prefetched_columns, list) else []
    )
    resolved_tags = uc_tags if isinstance(uc_tags, list) else (
        prefetched_tags if isinstance(prefetched_tags, list) else []
    )
    resolved_routines = uc_routines if isinstance(uc_routines, list) else (
        prefetched_routines if isinstance(prefetched_routines, list) else []
    )
    return resolved_columns, resolved_tags, resolved_routines


def _initialize_logged_model(
    *,
    name: str,
    source_run_id: str | None,
    params: dict[str, str],
    tags: dict[str, str],
) -> Any:
    """Create a logged model across MLflow API variants."""
    init_fn = getattr(mlflow, "initialize_logged_model", None)
    if callable(init_fn):
        return init_fn(
            name=name,
            source_run_id=source_run_id,
            params=params,
            tags=tags,
            model_type="agent",
        )

    create_fn = getattr(mlflow, "create_logged_model", None)
    if callable(create_fn):
        return create_fn(name=name, params=params, tags=tags)

    raise RuntimeError("No supported MLflow LoggedModel creation API found")


def _finalize_logged_model(model_id: str) -> None:
    """Finalize logged model if the MLflow API supports it."""
    finalize_fn = getattr(mlflow, "finalize_logged_model", None)
    if not callable(finalize_fn):
        return
    try:
        finalize_fn(model_id=model_id, status="READY")
    except Exception:
        logger.debug("Ignoring finalize_logged_model failure for %s", model_id, exc_info=True)


def _log_dict_artifact(payload: dict[str, Any], artifact_file: str) -> None:
    """Log dict artifact across MLflow API variants."""
    log_dict_fn = getattr(mlflow, "log_dict", None)
    if callable(log_dict_fn):
        log_dict_fn(payload, artifact_file)
        return

    tmp_dir = Path(tempfile.mkdtemp(prefix="genie-opt-"))
    tmp_file = tmp_dir / Path(artifact_file).name
    tmp_file.write_text(json.dumps(payload, default=str, indent=2), encoding="utf-8")
    mlflow.log_artifact(str(tmp_file), artifact_path=str(Path(artifact_file).parent))
