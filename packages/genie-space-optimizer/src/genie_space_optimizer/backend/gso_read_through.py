"""Read-through layer: Lakebase (fast) → Delta (authoritative fallback).

Current/active runs always read from Delta (the optimization job writes there).
Past/completed runs try Lakebase first (assuming Delta→Lakebase sync), then
fall back to Delta if Lakebase is unavailable or empty.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd
from sqlmodel import Session, select

from .models_db import GSOIterationRecord, GSOPatchRecord, GSORunRecord, GSOStageRecord

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

ACTIVE_RUN_STATUSES = frozenset({"QUEUED", "IN_PROGRESS"})


def _run_record_to_dict(record: GSORunRecord) -> dict:
    """Convert a SQLModel run record to a dict matching Delta load_run output."""
    d = record.model_dump()
    # Parse JSON columns to match Delta reader behavior
    for col in ("levers", "config_snapshot"):
        if d.get(col) and isinstance(d[col], str):
            try:
                d[col] = json.loads(d[col])
            except (json.JSONDecodeError, TypeError):
                pass
    return d


def _records_to_dataframe(records: list) -> pd.DataFrame:
    """Convert a list of SQLModel records to a DataFrame."""
    if not records:
        return pd.DataFrame()
    return pd.DataFrame([r.model_dump() for r in records])


# ── Runs ─────────────────────────────────────────────────────────────────


def load_run_with_fallback(
    session: Session | None,
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> dict | None:
    """Load a single run — always from Delta (authoritative for single-run lookups)."""
    from genie_space_optimizer.optimization.state import load_run

    return load_run(spark, run_id, catalog, schema)


def load_runs_for_space_with_fallback(
    session: Session | None,
    spark: SparkSession,
    space_id: str,
    catalog: str,
    schema: str,
) -> pd.DataFrame:
    """Load all runs for a space: Lakebase for past runs, Delta for active + fallback."""
    from genie_space_optimizer.optimization.state import load_runs_for_space

    if session is not None:
        try:
            rows = session.exec(
                select(GSORunRecord)
                .where(GSORunRecord.space_id == space_id)
                .order_by(GSORunRecord.started_at.desc())  # type: ignore[arg-type]
            ).all()
            if rows:
                lb_df = pd.DataFrame([_run_record_to_dict(r) for r in rows])
                # Active runs must be refreshed from Delta (authoritative source)
                active_mask = lb_df["status"].isin(list(ACTIVE_RUN_STATUSES))
                if active_mask.any():
                    active_ids = set(lb_df.loc[active_mask, "run_id"].tolist())
                    delta_df = load_runs_for_space(spark, space_id, catalog, schema)
                    active_delta = delta_df[delta_df["run_id"].isin(active_ids)]
                    past_lb = lb_df[~active_mask]
                    merged = pd.concat([active_delta, past_lb], ignore_index=True)
                    return merged.sort_values("started_at", ascending=False).reset_index(drop=True)
                return lb_df
        except Exception:
            logger.warning(
                "Lakebase read failed for space %s, falling back to Delta",
                space_id,
                exc_info=True,
            )

    return load_runs_for_space(spark, space_id, catalog, schema)


def load_recent_activity_with_fallback(
    session: Session | None,
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    space_id: str | None = None,
    limit: int = 20,
) -> pd.DataFrame:
    """Dashboard activity: try Lakebase, fall back to Delta."""
    from genie_space_optimizer.optimization.state import load_recent_activity

    if session is not None:
        try:
            query = (
                select(GSORunRecord)
                .order_by(GSORunRecord.started_at.desc())  # type: ignore[arg-type]
                .limit(limit)
            )
            if space_id:
                query = query.where(GSORunRecord.space_id == space_id)
            rows = session.exec(query).all()
            if rows:
                return pd.DataFrame([_run_record_to_dict(r) for r in rows])
        except Exception:
            logger.warning(
                "Lakebase read failed for recent activity, falling back to Delta",
                exc_info=True,
            )

    return load_recent_activity(spark, catalog, schema, space_id=space_id, limit=limit)


# ── Stages ───────────────────────────────────────────────────────────────


def load_stages_with_fallback(
    session: Session | None,
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    *,
    is_active: bool = False,
) -> pd.DataFrame:
    """Load stages for a run: Lakebase for completed runs, Delta for active."""
    from genie_space_optimizer.optimization.state import load_stages

    if is_active or session is None:
        return load_stages(spark, run_id, catalog, schema)

    try:
        rows = session.exec(
            select(GSOStageRecord)
            .where(GSOStageRecord.run_id == run_id)
            .order_by(GSOStageRecord.started_at)  # type: ignore[arg-type]
        ).all()
        if rows:
            return _records_to_dataframe(rows)
    except Exception:
        logger.warning(
            "Lakebase read failed for stages (run %s), falling back to Delta",
            run_id,
            exc_info=True,
        )

    return load_stages(spark, run_id, catalog, schema)


# ── Iterations ───────────────────────────────────────────────────────────


def load_iterations_with_fallback(
    session: Session | None,
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    *,
    is_active: bool = False,
) -> pd.DataFrame:
    """Load iterations for a run: Lakebase for completed runs, Delta for active."""
    from genie_space_optimizer.optimization.state import load_iterations

    if is_active or session is None:
        return load_iterations(spark, run_id, catalog, schema)

    try:
        rows = session.exec(
            select(GSOIterationRecord)
            .where(GSOIterationRecord.run_id == run_id)
            .order_by(GSOIterationRecord.iteration)  # type: ignore[arg-type]
        ).all()
        if rows:
            return _records_to_dataframe(rows)
    except Exception:
        logger.warning(
            "Lakebase read failed for iterations (run %s), falling back to Delta",
            run_id,
            exc_info=True,
        )

    return load_iterations(spark, run_id, catalog, schema)


# ── Patches ──────────────────────────────────────────────────────────────


def load_patches_with_fallback(
    session: Session | None,
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    *,
    is_active: bool = False,
) -> pd.DataFrame:
    """Load patches for a run: Lakebase for completed runs, Delta for active."""
    from genie_space_optimizer.optimization.state import load_patches

    if is_active or session is None:
        return load_patches(spark, run_id, catalog, schema)

    try:
        rows = session.exec(
            select(GSOPatchRecord)
            .where(GSOPatchRecord.run_id == run_id)
            .order_by(GSOPatchRecord.applied_at)  # type: ignore[arg-type]
        ).all()
        if rows:
            return _records_to_dataframe(rows)
    except Exception:
        logger.warning(
            "Lakebase read failed for patches (run %s), falling back to Delta",
            run_id,
            exc_info=True,
        )

    return load_patches(spark, run_id, catalog, schema)
