"""Pydantic v2 models for the GenieWatch (observability) surface.

Keep these in sync with `frontend/src/watch/types/api.ts` whenever they change.
Models are prefixed/named distinctly from `backend.models` so imports don't
shadow workbench types (e.g. `WatchSpaceSummary` vs workbench's own).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


# ─── Spaces ───────────────────────────────────────────────────────────────


class WatchSpacePermission(BaseModel):
    principal: Optional[str] = None
    permission_level: Optional[str] = None
    principal_type: Optional[str] = None
    inherited: Optional[bool] = None


class WatchSpaceSummary(BaseModel):
    space_id: str
    title: Optional[str] = None
    owner_email: Optional[str] = None
    description: Optional[str] = None
    permissions: list[WatchSpacePermission] = Field(default_factory=list)
    last_seen_at: Optional[datetime] = None


class WatchSpaceListItem(WatchSpaceSummary):
    queries_7d: int = 0
    cost_7d_usd: float = 0.0
    feedback_pos_7d: int = 0
    feedback_neg_7d: int = 0
    last_query_at: Optional[datetime] = None


# ─── Cost ─────────────────────────────────────────────────────────────────


class CostPoint(BaseModel):
    day: datetime
    warehouse_id: Optional[str] = None
    query_count: int = 0
    approx_dbus: Optional[float] = None
    approx_usd: Optional[float] = None


class CostRollup(BaseModel):
    space_id: str
    days: int
    total_query_count: int
    total_approx_usd: Optional[float] = None
    total_approx_dbus: Optional[float] = None
    by_warehouse: list[CostPoint]
    time_series: list[CostPoint]
    apportionment: str = "warehouse_share"


class CostTopSpender(BaseModel):
    space_id: str
    title: Optional[str] = None
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None
    query_count: int
    approx_usd: Optional[float] = None


class CostPerConversation(BaseModel):
    conversation_id: str
    user_email: Optional[str] = None
    first_query_at: Optional[datetime] = None
    last_query_at: Optional[datetime] = None
    query_count: int = 0
    approx_usd: Optional[float] = None


# ─── Usage ────────────────────────────────────────────────────────────────


class UsagePoint(BaseModel):
    day: datetime
    queries: int = 0
    p50_ms: Optional[float] = None
    p95_ms: Optional[float] = None
    errors: int = 0
    distinct_users: int = 0


# ─── Workspace overview (native cost-tab dashboard) ────────────────────────


class DailyVolumePoint(BaseModel):
    day: datetime
    queries: int = 0


class WorkspaceOverview(BaseModel):
    days: int
    active_spaces: int = 0
    total_queries: int = 0
    distinct_users: int = 0
    approx_usd: Optional[float] = None
    feedback_pos: int = 0
    feedback_neg: int = 0
    daily: list[DailyVolumePoint] = Field(default_factory=list)


class FeedbackEvent(BaseModel):
    event_time: datetime
    user_email: Optional[str] = None
    rating: Optional[str] = None
    comment: Optional[str] = None
    message_id: Optional[str] = None
    conversation_id: Optional[str] = None
    # Populated by the all-spaces feedback endpoint. Single-space callers
    # (e.g. /spaces/{id}/feedback) leave these as None.
    space_id: Optional[str] = None
    space_title: Optional[str] = None


class FeedbackSummary(BaseModel):
    positive: int = 0
    negative: int = 0
    total: int = 0
    sample: list[FeedbackEvent] = Field(default_factory=list)


class Conversation(BaseModel):
    conversation_id: str
    user_email: Optional[str] = None
    created_at: Optional[datetime] = None
    message_count: int = 0
    last_message_at: Optional[datetime] = None


class UsageRollup(BaseModel):
    space_id: str
    days: int
    total_queries: int
    total_errors: int
    distinct_users: int
    time_series: list[UsagePoint]
    feedback: FeedbackSummary
    conversations: list[Conversation] = Field(default_factory=list)


class TopQuery(BaseModel):
    statement_id: str
    executed_by: Optional[str] = None
    start_time: datetime
    total_duration_ms: Optional[int] = None
    execution_status: Optional[str] = None
    statement_text: Optional[str] = None


# ─── Feedback tab (workspace-wide aggregation) ────────────────────────────


class FeedbackTabSummary(BaseModel):
    positive: int = 0
    negative: int = 0
    total: int = 0
    neg_rate_pct: float = 0.0


class FeedbackTrendPoint(BaseModel):
    day: date
    positive: int = 0
    negative: int = 0


class FeedbackSpaceRow(BaseModel):
    space_id: str
    title: Optional[str] = None
    owner_email: Optional[str] = None
    positive: int = 0
    negative: int = 0
    total: int = 0
    neg_rate_pct: float = 0.0
    last_feedback_at: Optional[datetime] = None


class FeedbackTabResponse(BaseModel):
    days: int
    summary: FeedbackTabSummary
    trend: list[FeedbackTrendPoint] = Field(default_factory=list)
    per_space: list[FeedbackSpaceRow] = Field(default_factory=list)
    events: list[FeedbackEvent] = Field(default_factory=list)


class FeedbackMessageComment(BaseModel):
    """A user-typed comment attached to a Genie message. Sourced lazily from
    the Genie Conversation API when a user expands a feedback event."""
    message_comment_id: str
    content: str
    created_at: datetime
    user_id: Optional[int] = None


# ─── Resources ────────────────────────────────────────────────────────────


class ResourceUsage(BaseModel):
    full_name: str
    kind: str = "table"
    source: str = "configured"
    query_count: int = 0
    last_used: Optional[datetime] = None
    owner: Optional[str] = None
    comment: Optional[str] = None


class ResourceRollupItem(BaseModel):
    full_name: str
    space_count: int
    query_count_total: int
    last_used: Optional[datetime] = None


class ResourceGraphEdge(BaseModel):
    space_id: str
    full_name: str
    query_count: int = 0
    last_used: Optional[datetime] = None


class ResourceGraphSpaceNode(BaseModel):
    space_id: str
    title: Optional[str] = None
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None


class ResourceGraph(BaseModel):
    edges: list[ResourceGraphEdge]
    spaces: list[ResourceGraphSpaceNode]
    days: int
    truncated: bool = False


# ─── Settings ─────────────────────────────────────────────────────────────


class HealthStatus(BaseModel):
    lakebase_available: bool
    obo_active: bool
    warehouse_id: Optional[str] = None
    workspace_host: Optional[str] = None
    # Last observed system-table accessibility for the app SP:
    #   None  → unknown (no system-table query has run yet)
    #   True  → a query succeeded (SP has the required SELECT grants)
    #   False → a query failed with a permission error (grants missing)
    system_tables_accessible: Optional[bool] = None
