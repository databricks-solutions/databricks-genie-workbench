"""SQLModel table definitions for Lakebase-backed GSO state.

These mirror the Delta tables in ``optimization/state.py`` and are used as
the fast-read layer for completed (past) optimization runs.  The Delta tables
remain the authoritative write store; data is synced to Lakebase separately.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel


class GSORunRecord(SQLModel, table=True):
    """Mirrors ``genie_opt_runs`` Delta table — one row per optimization run."""

    __tablename__ = "genie_opt_runs"

    run_id: str = Field(primary_key=True, max_length=64)
    space_id: str = Field(index=True, max_length=64)
    domain: str = Field(max_length=256)
    status: str = Field(max_length=32, index=True)
    started_at: datetime
    completed_at: Optional[datetime] = None
    job_run_id: Optional[str] = Field(default=None, max_length=64)
    job_id: Optional[str] = Field(default=None, max_length=64)
    apply_mode: str = Field(default="genie_config", max_length=32)
    levers: Optional[str] = None  # JSON array
    best_iteration: Optional[int] = None
    best_accuracy: Optional[float] = None
    best_repeatability: Optional[float] = None
    convergence_reason: Optional[str] = None
    experiment_name: Optional[str] = None
    experiment_id: Optional[str] = None
    config_snapshot: Optional[str] = None  # JSON
    triggered_by: Optional[str] = None
    labeling_session_name: Optional[str] = None
    labeling_session_run_id: Optional[str] = None
    labeling_session_url: Optional[str] = None
    updated_at: datetime = Field(default_factory=lambda: datetime.utcnow())


class GSOStageRecord(SQLModel, table=True):
    """Mirrors ``genie_opt_stages`` Delta table — stage transition timeline."""

    __tablename__ = "genie_opt_stages"

    # Composite key: run_id + started_at (no single PK in Delta, use id for PG)
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(index=True, max_length=64)
    task_key: Optional[str] = Field(default=None, max_length=64)
    stage: str = Field(max_length=128)
    status: str = Field(max_length=32)
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    lever: Optional[int] = None
    iteration: Optional[int] = None
    detail_json: Optional[str] = None  # JSON
    error_message: Optional[str] = None


class GSOIterationRecord(SQLModel, table=True):
    """Mirrors ``genie_opt_iterations`` Delta table — evaluation results per iteration."""

    __tablename__ = "genie_opt_iterations"

    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(index=True, max_length=64)
    iteration: int
    lever: Optional[int] = None
    eval_scope: str = Field(max_length=32)
    timestamp: datetime
    mlflow_run_id: Optional[str] = Field(default=None, max_length=128)
    model_id: Optional[str] = Field(default=None, max_length=128)
    overall_accuracy: float
    total_questions: int
    correct_count: int
    scores_json: str  # JSON
    failures_json: Optional[str] = None  # JSON
    remaining_failures: Optional[str] = None  # JSON
    arbiter_actions_json: Optional[str] = None  # JSON
    repeatability_pct: Optional[float] = None
    repeatability_json: Optional[str] = None  # JSON
    thresholds_met: bool = False
    rows_json: Optional[str] = None  # JSON
    reflection_json: Optional[str] = None  # JSON


class GSOPatchRecord(SQLModel, table=True):
    """Mirrors ``genie_opt_patches`` Delta table — patch audit trail."""

    __tablename__ = "genie_opt_patches"

    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(index=True, max_length=64)
    iteration: int
    lever: int
    patch_index: int
    patch_type: str = Field(max_length=64)
    scope: str = Field(max_length=32)
    risk_level: str = Field(max_length=16)
    target_object: Optional[str] = None
    patch_json: str  # JSON
    command_json: Optional[str] = None  # JSON
    rollback_json: Optional[str] = None  # JSON
    applied_at: datetime
    rolled_back: Optional[bool] = None
    rolled_back_at: Optional[datetime] = None
    rollback_reason: Optional[str] = None
    proposal_id: Optional[str] = Field(default=None, max_length=64)
    cluster_id: Optional[str] = Field(default=None, max_length=64)
    provenance_json: Optional[str] = None  # JSON
