from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


# ===== GenieIQ Models =====

class MaturityLevel(str, Enum):
    """Maturity level for a Genie Agent (3-tier)."""
    NOT_READY = "Not Ready"
    READY_TO_OPTIMIZE = "Ready to Optimize"
    TRUSTED = "Trusted"


class CheckDetail(BaseModel):
    """A single scoring check result."""
    label: str
    passed: bool
    detail: str | None = None       # Human-readable context (e.g., "3/8 tables (38%)")
    severity: Literal["pass", "warning", "fail"] | None = None


class ScanResult(BaseModel):
    """IQ scan result for a Genie Agent."""
    space_id: str
    score: int = Field(..., ge=0, le=12)
    total: int = 12
    maturity: MaturityLevel
    optimization_accuracy: float | None = None  # 0.0-1.0, None if never optimized
    checks: list[CheckDetail] = Field(default_factory=list)
    findings: list[str] = Field(default_factory=list)
    next_steps: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)               # Advisory findings from warning-severity checks
    warning_next_steps: list[str] = Field(default_factory=list)     # Paired with warnings
    scanned_at: str  # ISO datetime string


class SpaceListItem(BaseModel):
    """Summary item for the space list."""
    space_id: str
    display_name: str
    score: int | None = None
    maturity: str | None = None
    optimization_accuracy: float | None = None  # 0.0-1.0, None if never optimized
    is_starred: bool = False
    last_scanned: str | None = None  # ISO datetime
    space_url: str | None = None


class SpaceScanRequest(BaseModel):
    """Request to trigger an IQ scan."""
    space_id: str = Field(..., min_length=1, max_length=64)


class StarToggleRequest(BaseModel):
    """Request to toggle star on a space."""
    starred: bool


class AdminDashboardStats(BaseModel):
    """Org-wide statistics for the admin dashboard."""
    total_spaces: int
    scanned_spaces: int
    avg_score: float
    critical_count: int  # score <= 20
    maturity_distribution: dict[str, int]


class LeaderboardEntry(BaseModel):
    """Entry in the leaderboard."""
    space_id: str
    display_name: str
    score: int
    maturity: str
    last_scanned: str | None = None


class AlertItem(BaseModel):
    """Alert for a space with critical issues."""
    space_id: str
    display_name: str
    score: int
    top_finding: str | None = None


# ===== Create Wizard Models =====

class CreateSpaceRequest(BaseModel):
    """Request body for the Create Space Wizard endpoint."""
    display_name: str = Field(..., min_length=1, max_length=255)
    serialized_space: dict
    parent_path: str | None = Field(None, max_length=1000)


class CreateSpaceResponse(BaseModel):
    """Response from the Create Space Wizard endpoint."""
    space_id: str
    display_name: str
    space_url: str


class LLMModelInfo(BaseModel):
    """Selectable chat model served by Databricks Model Serving."""
    name: str
    displayName: str
    isDefault: bool = False
    optimizerPromptBudgetChars: int | None = None
    contextTier: Literal["standard", "long"] | None = None


# ── Auto-Optimize preflight permissions ──────────────────────────────────
# Mirrored on the frontend as `GSOPermissionCheck` in `frontend/src/types/index.ts`.
# Both halves must stay in sync — update together (see AGENTS.md §Models).


class SchemaAccessStatus(BaseModel):
    """Per-schema access summary returned by the Auto-Optimize preflight.

    Emitted once per UC schema the job SP would read from. ``grant_sql`` is
    populated when ``read_granted`` is ``False`` so the UI can show a
    one-click remediation hint."""

    catalog: str
    schema_name: str
    read_granted: bool
    grant_sql: str | None = None


class QueryHistoryWarehouseStatus(BaseModel):
    warehouse_id: str
    name: str
    accessible: bool = False


class QueryUsageSignal(BaseModel):
    status: Literal[
        "system_table_available",
        "warehouse_api_available",
        "partially_available",
        "unavailable",
    ] = "unavailable"
    system_table_available: bool = False
    warehouse_api_available: bool = False
    warehouses: list[QueryHistoryWarehouseStatus] = Field(default_factory=list)
    inaccessible_warehouses: list[str] = Field(default_factory=list)
    system_grant_sql: str | None = None


class PermissionCheckResponse(BaseModel):
    """Payload for ``GET /auto-optimize/permissions``.

    Shape contract for the Auto-Optimize permissions preflight. The UI's
    PermissionAlert consumes the SP and schema access fields to show
    grant-based remediation."""

    sp_display_name: str
    sp_application_id: str = ""
    sp_has_manage: bool
    schemas: list[SchemaAccessStatus]
    can_start: bool
    errors: list[str] = []
    query_usage_signal: QueryUsageSignal | None = None


# ── Auto-Optimize current version ────────────────────────────────────────
# Mirrored on the frontend as `CurrentVersionResponse` in
# `frontend/src/types/index.ts`. Both halves must stay in sync — update
# together (see AGENTS.md §Models).


class VersionMatch(BaseModel):
    """One known optimization version whose fingerprint matches the live config."""

    run_id: str
    target: Literal["baseline", "champion"]
    started_at: str | None = None
    best_accuracy: float | None = None


class CurrentVersionResponse(BaseModel):
    """Payload for ``GET /auto-optimize/spaces/{space_id}/current-version``.

    Answers "which known optimization version is the live agent on?" by
    fingerprint-matching the live ``serialized_space`` against every captured
    run baseline / champion config:

    * ``matched`` — live config equals ≥1 known version (``current`` is the
      most recent; ``also_matches`` lists semantically equivalent versions);
    * ``drifted`` — known versions exist but none match → the config was
      changed outside Auto-Optimize;
    * ``history_incomplete`` — at least one expected baseline/champion lacks
      an authoritative API-observed capture, so a non-match is inconclusive;
    * ``no_known_versions`` — no runs with captured configs (nothing to
      compare); ``unavailable`` — the check itself failed (fail-open, the UI
      renders nothing); ``optimization_in_progress`` — an active run is
      mutating the live config, so matching would be noise.
    """

    status: Literal[
        "matched",
        "drifted",
        "history_incomplete",
        "no_known_versions",
        "unavailable",
        "optimization_in_progress",
    ]
    current: VersionMatch | None = None
    also_matches: list[VersionMatch] = []
    live_update_time: str | None = None
