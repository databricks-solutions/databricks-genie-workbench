"""
Comprehensive report generation for Genie Space optimization runs.

Reads the optimization Delta tables and produces a structured Markdown report
with executive summary, per-iteration details, patch inventory, repeatability,
and MLflow links.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from genie_space_optimizer.common.config import LEVER_NAMES
from genie_space_optimizer.optimization.state import (
    load_iterations,
    load_patches,
    load_run,
    load_stages,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def generate_report(
    spark: SparkSession,
    run_id: str,
    domain: str,
    catalog: str,
    schema: str,
    output_dir: str | None = None,
) -> str | None:
    """Generate a comprehensive Markdown report for an optimization run.

    Reads from Delta: runs, stages, iterations, patches, ASI.
    Writes a .md file and returns its path, or None on failure.
    """
    run_row = load_run(spark, run_id, catalog, schema)
    if not run_row:
        logger.error("Run %s not found", run_id)
        return None

    stages_df = load_stages(spark, run_id, catalog, schema)
    iterations_df = load_iterations(spark, run_id, catalog, schema)
    patches_df = load_patches(spark, run_id, catalog, schema)

    sections = [
        _build_header(run_row),
        _build_executive_summary(run_row, iterations_df),
        _build_iteration_table(iterations_df),
        _build_lever_detail(iterations_df, patches_df),
        _build_patch_inventory(patches_df),
        _build_repeatability_report(iterations_df),
        _build_generalization_section(iterations_df),
        _build_mlflow_links(run_row, iterations_df),
    ]

    report = "\n\n---\n\n".join(s for s in sections if s)

    if output_dir is None:
        output_dir = f"/tmp/genie_opt_reports/{domain}"
    os.makedirs(output_dir, exist_ok=True)

    filename = f"optimization_report_{run_id[:8]}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.md"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w") as f:
        f.write(report)

    logger.info("Report written to %s", filepath)
    return filepath


# ── Section Builders ──────────────────────────────────────────────────


def _build_header(run_row: dict) -> str:
    space_id = run_row.get("space_id", "")
    domain = run_row.get("domain", "")
    status = run_row.get("status", "UNKNOWN")
    convergence = run_row.get("convergence_reason", "")
    started = run_row.get("started_at", "")
    completed = run_row.get("completed_at", "")
    triggered_by = run_row.get("triggered_by", "")
    run_id = run_row.get("run_id", "")

    return (
        f"# Genie Space Optimization Report\n\n"
        f"| Field | Value |\n"
        f"|-------|-------|\n"
        f"| **Run ID** | `{run_id}` |\n"
        f"| **Space ID** | `{space_id}` |\n"
        f"| **Domain** | {domain} |\n"
        f"| **Triggered By** | {triggered_by} |\n"
        f"| **Started** | {started} |\n"
        f"| **Completed** | {completed} |\n"
        f"| **Final Status** | **{status}** |\n"
        f"| **Convergence Reason** | {convergence} |\n"
    )


def _build_executive_summary(run_row: dict, iterations_df: pd.DataFrame) -> str:
    best_accuracy = run_row.get("best_accuracy", 0.0)
    best_iteration = run_row.get("best_iteration", 0)

    baseline_accuracy = 0.0
    if not iterations_df.empty:
        # Pin baseline to ``eval_scope == "full"`` so the
        # post-enrichment iter 0 row (``eval_scope == "enrichment"``)
        # cannot accidentally hijack the markdown summary's baseline
        # number.
        baseline_rows = iterations_df[
            (iterations_df["iteration"] == 0)
            & (iterations_df["eval_scope"] == "full")
        ]
        if not baseline_rows.empty:
            baseline_accuracy = float(baseline_rows.iloc[0].get("overall_accuracy", 0.0))

    improvement = best_accuracy - baseline_accuracy
    total_iterations = int(iterations_df["iteration"].max()) if not iterations_df.empty else 0

    full_evals = iterations_df[iterations_df["eval_scope"] == "full"] if not iterations_df.empty else iterations_df
    lever_iterations = full_evals[full_evals["iteration"] > 0] if not full_evals.empty else pd.DataFrame()

    accepted = 0
    rolled_back = 0
    if not lever_iterations.empty:
        for _, row in lever_iterations.iterrows():
            tm = row.get("thresholds_met")
            if tm:
                accepted += 1
            else:
                rolled_back += 1

    return (
        f"## Executive Summary\n\n"
        f"- **Baseline Score:** {baseline_accuracy:.1f}%\n"
        f"- **Best Score:** {best_accuracy:.1f}% (iteration {best_iteration})\n"
        f"- **Improvement:** {improvement:+.1f} percentage points\n"
        f"- **Total Iterations:** {total_iterations}\n"
        f"- **Levers Accepted:** {accepted}\n"
        f"- **Levers Rolled Back:** {rolled_back}\n"
    )


def _build_iteration_table(iterations_df: pd.DataFrame) -> str:
    if iterations_df.empty:
        return "## Per-Iteration Detail\n\n_No iterations recorded._"

    header = "## Per-Iteration Detail\n\n"
    table = "| Iter | Lever | Scope | Accuracy | Correct/Total | Thresholds | Model ID | MLflow Run |\n"
    table += "|------|-------|-------|----------|---------------|------------|----------|------------|\n"

    for _, row in iterations_df.iterrows():
        iteration = int(row.get("iteration", 0))
        lever = row.get("lever", "—")
        if pd.isna(lever):
            lever = "—"
        scope = row.get("eval_scope", "full")
        accuracy = float(row.get("overall_accuracy", 0.0))
        correct = int(row.get("correct_count", 0))
        total = int(row.get("total_questions", 0))
        thresholds = "PASS" if row.get("thresholds_met") else "FAIL"
        model_id = row.get("model_id", "")
        if model_id and len(str(model_id)) > 12:
            model_id = str(model_id)[:12] + "…"
        mlflow_run = row.get("mlflow_run_id", "")
        if mlflow_run and len(str(mlflow_run)) > 12:
            mlflow_run = str(mlflow_run)[:12] + "…"

        scores_json = row.get("scores_json", "{}")
        if isinstance(scores_json, str):
            try:
                scores = json.loads(scores_json)
            except (json.JSONDecodeError, TypeError):
                scores = {}
        else:
            scores = scores_json if isinstance(scores_json, dict) else {}

        table += (
            f"| {iteration} | {lever} | {scope} | {accuracy:.1f}% "
            f"| {correct}/{total} | {thresholds} "
            f"| {model_id} | {mlflow_run} |\n"
        )

    return header + table


def _build_lever_detail(
    iterations_df: pd.DataFrame,
    patches_df: pd.DataFrame,
) -> str:
    if iterations_df.empty:
        return ""

    lever_iters = iterations_df[
        iterations_df["lever"].notna() & (iterations_df["iteration"] > 0)
    ]
    if lever_iters.empty:
        return ""

    sections: list[str] = ["## Per-Lever Detail\n"]

    for _, row in lever_iters.iterrows():
        lever = int(row.get("lever", 0))
        iteration = int(row.get("iteration", 0))
        lever_name = LEVER_NAMES.get(lever, f"Lever {lever}")
        accuracy = float(row.get("overall_accuracy", 0.0))

        lever_patches = patches_df[
            (patches_df["iteration"] == iteration) & (patches_df["lever"] == lever)
        ] if not patches_df.empty else pd.DataFrame()

        section = f"### Lever {lever}: {lever_name} (Iteration {iteration})\n\n"
        section += f"- **Accuracy:** {accuracy:.1f}%\n"
        section += f"- **Patches Applied:** {len(lever_patches)}\n"

        if not lever_patches.empty:
            section += "\n| # | Type | Target | Risk | Status |\n"
            section += "|---|------|--------|------|--------|\n"
            for _, p in lever_patches.iterrows():
                rb = "Rolled Back" if p.get("rolled_back") else "Applied"
                section += (
                    f"| {int(p.get('patch_index', 0))} "
                    f"| {p.get('patch_type', '')} "
                    f"| {p.get('target_object', '')} "
                    f"| {p.get('risk_level', '')} "
                    f"| {rb} |\n"
                )

        sections.append(section)

    return "\n".join(sections)


def _build_patch_inventory(patches_df: pd.DataFrame) -> str:
    if patches_df.empty:
        return "## Patch Inventory\n\n_No patches applied._"

    header = "## Patch Inventory\n\n"
    table = "| Iter | Lever | Type | Target | Risk | Scope | Status | Rollback Reason |\n"
    table += "|------|-------|------|--------|------|-------|--------|-----------------|\n"

    for _, p in patches_df.iterrows():
        rolled_back = p.get("rolled_back", False)
        status = "Rolled Back" if rolled_back else "Applied"
        rollback_reason = p.get("rollback_reason", "") or ""

        table += (
            f"| {int(p.get('iteration', 0))} "
            f"| {int(p.get('lever', 0))} "
            f"| {p.get('patch_type', '')} "
            f"| {p.get('target_object', '')} "
            f"| {p.get('risk_level', '')} "
            f"| {p.get('scope', '')} "
            f"| {status} "
            f"| {rollback_reason} |\n"
        )

    return header + table


def _build_repeatability_report(iterations_df: pd.DataFrame) -> str:
    if iterations_df.empty:
        return ""

    repeat_rows = iterations_df[iterations_df["repeatability_pct"].notna()]
    if repeat_rows.empty:
        return ""

    header = "## Repeatability Report\n\n"
    section = ""

    for _, row in repeat_rows.iterrows():
        iteration = int(row.get("iteration", 0))
        pct = float(row.get("repeatability_pct", 0.0))
        section += f"- **Iteration {iteration}:** {pct:.1f}% repeatability\n"

        details_json = row.get("repeatability_json")
        if details_json:
            details = details_json
            if isinstance(details_json, str):
                try:
                    details = json.loads(details_json)
                except (json.JSONDecodeError, TypeError):
                    details = None

            if isinstance(details, list):
                classifications = {}
                for d in details:
                    cls = d.get("classification", "UNKNOWN")
                    classifications[cls] = classifications.get(cls, 0) + 1
                for cls, count in sorted(classifications.items()):
                    section += f"  - {cls}: {count} questions\n"

    return header + section


def _build_generalization_section(iterations_df: pd.DataFrame) -> str:
    """V2 has no train/held-out split; keep legacy reports quiet."""
    _ = iterations_df
    return ""


def _build_mlflow_links(run_row: dict, iterations_df: pd.DataFrame) -> str:
    experiment_name = run_row.get("experiment_name", "")
    experiment_id = run_row.get("experiment_id", "")

    header = "## MLflow Links\n\n"
    section = ""

    if experiment_name:
        section += f"- **Experiment:** `{experiment_name}`"
        if experiment_id:
            section += f" (id: `{experiment_id}`)"
        section += "\n"

    if not iterations_df.empty:
        if "mlflow_run_id" in iterations_df.columns:
            run_ids = iterations_df["mlflow_run_id"].dropna().unique()
            if len(run_ids) > 0:
                section += "- **Evaluation Run IDs:**\n"
                for rid in run_ids:
                    section += f"  - `{rid}`\n"

        if "model_id" in iterations_df.columns:
            model_ids = iterations_df["model_id"].dropna().unique()
            if len(model_ids) > 0:
                section += "- **Model IDs:**\n"
                for mid in model_ids:
                    section += f"  - `{mid}`\n"

    best_model = run_row.get("best_model_id", "")
    if best_model:
        section += f"- **Best (Champion) Model:** `{best_model}`\n"

    return header + section

