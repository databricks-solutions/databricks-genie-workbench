from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


# ===== GenieIQ Models =====

class MaturityLevel(str, Enum):
    """Maturity level for a Genie Space (3-tier)."""
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
    """IQ scan result for a Genie Space."""
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


class FixRequest(BaseModel):
    """Request to run the AI fix agent on a space."""
    space_id: str = Field(..., min_length=1, max_length=64)
    findings: list[str] = Field(default_factory=list)
    space_config: dict = Field(default_factory=dict)


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


class PermissionCheckResponse(BaseModel):
    """Payload for ``GET /auto-optimize/permissions``.

    Shape contract for the Auto-Optimize permissions preflight. The UI's
    PermissionAlert consumes the ``prompt_registry_*`` fields to decide
    whether to show the "Prompt Registry disabled" banner vs. a grant-based
    remediation; the /trigger endpoint re-checks the same shape."""

    sp_display_name: str
    sp_application_id: str = ""
    sp_has_manage: bool
    schemas: list[SchemaAccessStatus]
    # Fail-closed default: availability must be proven by the probe, not assumed.
    prompt_registry_available: bool = False
    prompt_registry_error: str | None = None
    # Stable reason code for UI/alerting; paired with prompt_registry_error.
    # One of: ok | feature_not_enabled | missing_uc_permissions |
    # registry_path_not_found | missing_sp_scope | vendor_bug |
    # unknown (legacy) | probe_error.
    prompt_registry_reason_code: str | None = None
    # Raw vendor error code (e.g. ENDPOINT_NOT_FOUND). Surfaced verbatim in
    # the UI mono block so the next unmapped code is visible without a log
    # dive. May be None when the probe succeeded or raised a non-SDK error.
    prompt_registry_error_code: str | None = None
    # Two-axis actionability: "customer" (admin flips toggle / grants perms)
    # vs. "platform" (our bug or Databricks' bug). Drives UI chip color and
    # alert routing. None = unknown (treated as platform by the UI).
    prompt_registry_actionable_by: str | None = None
    can_start: bool
    errors: list[str] = []
