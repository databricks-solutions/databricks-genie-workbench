import re
from enum import Enum

from pydantic import BaseModel, Field, field_validator


class AssessmentCategory(str, Enum):
    """Qualitative assessment category replacing numeric scores."""

    GOOD_TO_GO = "good_to_go"
    QUICK_WINS = "quick_wins"
    FOUNDATION_NEEDED = "foundation_needed"


class CompensatingStrength(BaseModel):
    """Represents how one section compensates for another's weakness."""

    covering_section: str  # Section providing the strength
    covered_section: str  # Section being compensated for
    explanation: str  # How the strength compensates


class SynthesisResult(BaseModel):
    """Cross-sectional synthesis result from analyzing all sections together."""

    assessment: AssessmentCategory
    assessment_rationale: str
    compensating_strengths: list[CompensatingStrength]
    celebration_points: list[str]  # What's working well
    top_quick_wins: list[str]  # Actionable improvements

# Genie Space ID format: alphanumeric, hyphens, underscores (max 64 chars)
_GENIE_SPACE_ID_PATTERN = re.compile(r"^[a-zA-Z0-9\-_]{1,64}$")

# Maximum length for user text fields
MAX_TEXT_LENGTH = 10000
MAX_FEEDBACK_LENGTH = 2000


class AgentInput(BaseModel):
    """Input for the Genie Space Analyzer agent."""

    genie_space_id: str = Field(..., min_length=1, max_length=64)

    @field_validator("genie_space_id")
    @classmethod
    def validate_genie_space_id(cls, v: str) -> str:
        if not _GENIE_SPACE_ID_PATTERN.match(v):
            raise ValueError(
                "genie_space_id must contain only alphanumeric characters, "
                "hyphens, and underscores (max 64 characters)"
            )
        return v


class ChecklistItem(BaseModel):
    """A single checklist item from the analysis."""

    id: str  # e.g., "at-least-1-table-is-configured"
    description: str  # Human-readable description
    passed: bool  # Whether the check passed
    details: str | None = None  # Additional context (e.g., "Found 3 tables")


class Finding(BaseModel):
    """A single finding from the analysis."""

    category: str  # "best_practice", "warning", "suggestion"
    severity: str  # "high", "medium", "low"
    description: str
    recommendation: str
    reference: str  # Relevant best practice section


class SectionAnalysis(BaseModel):
    """Analysis results for a single section."""

    section_name: str  # e.g., "config.sample_questions", "data_sources.tables"
    checklist: list[ChecklistItem]  # Structured checklist results
    findings: list[Finding]  # Detailed findings for failed items
    score: int  # 0-10 compliance score (passed_items / total_items * 10)
    summary: str


class AgentOutput(BaseModel):
    """Output from the Genie Space Analyzer agent."""

    genie_space_id: str
    analyses: list[SectionAnalysis]
    synthesis: SynthesisResult | None = None  # Cross-sectional synthesis (full analysis only)
    overall_score: int  # Kept for backward compatibility
    trace_id: str


# Optimization models


class OptimizationSuggestion(BaseModel):
    """A single field-level optimization suggestion."""

    field_path: str  # e.g., "instructions.text_instructions[0].content"
    current_value: str | dict | list | bool | int | float | None  # Current value
    suggested_value: str | dict | list | bool | int | float | None  # Suggested new value
    rationale: str  # Why this change helps
    checklist_reference: str | None = None  # Related checklist item ID
    priority: str  # "high", "medium", "low"
    category: str  # instruction, sql_example, filter, expression, measure, etc.


class ComparisonDiscrepancy(BaseModel):
    """A single discrepancy found when comparing SQL results."""

    type: str  # "column_mismatch", "extra_rows", "missing_rows", "value_diff", "error"
    detail: str


class ComparisonResult(BaseModel):
    """Result of comparing Genie vs expected SQL results."""

    match_type: str  # "exact", "value_match", "partial", "row_count_only", "mismatch"
    confidence: float  # 0.0 - 1.0
    auto_label: bool  # suggested label
    discrepancies: list[ComparisonDiscrepancy]
    summary: str  # human-readable explanation


class LabelingFeedbackItem(BaseModel):
    """A single labeling feedback item from the benchmark session."""

    question_text: str = Field(..., min_length=1, max_length=MAX_TEXT_LENGTH)
    is_correct: bool | None
    feedback_text: str | None = Field(None, max_length=MAX_FEEDBACK_LENGTH)
    auto_label: bool | None = None  # What the auto-comparator suggested
    user_overrode_auto_label: bool = False  # Did user disagree with auto-label?
    auto_comparison_summary: str | None = None  # Human-readable comparison summary


class FailureDiagnosis(BaseModel):
    """Diagnosis of why a benchmark question failed."""

    question: str
    failure_types: list[str]
    explanation: str


class OptimizationRequest(BaseModel):
    """Request to generate optimization suggestions."""

    genie_space_id: str = Field(..., min_length=1, max_length=64)
    space_data: dict
    labeling_feedback: list[LabelingFeedbackItem] = Field(..., max_length=100)

    @field_validator("genie_space_id")
    @classmethod
    def validate_genie_space_id(cls, v: str) -> str:
        if not _GENIE_SPACE_ID_PATTERN.match(v):
            raise ValueError(
                "genie_space_id must contain only alphanumeric characters, "
                "hyphens, and underscores (max 64 characters)"
            )
        return v


class OptimizationResponse(BaseModel):
    """Response containing optimization suggestions."""

    suggestions: list[OptimizationSuggestion]
    summary: str
    trace_id: str
    diagnosis: list[FailureDiagnosis] = []  # Failure diagnosis for incorrect questions


class ConfigMergeRequest(BaseModel):
    """Request to merge optimization suggestions into a config."""

    space_data: dict
    suggestions: list[OptimizationSuggestion]


class ConfigMergeResponse(BaseModel):
    """Response containing merged configuration."""

    merged_config: dict
    summary: str
    trace_id: str


# Genie Space creation models


class GenieCreateRequest(BaseModel):
    """Request to create a new Genie Space."""

    display_name: str = Field(..., min_length=1, max_length=255)
    merged_config: dict
    parent_path: str | None = Field(None, max_length=1000)


class GenieCreateResponse(BaseModel):
    """Response from creating a new Genie Space."""

    genie_space_id: str
    display_name: str
    space_url: str


# ===== GenieIQ Models =====

class MaturityLevel(str, Enum):
    """Maturity level for a Genie Space."""
    OPTIMIZED = "Optimized"
    PROFICIENT = "Proficient"
    DEVELOPING = "Developing"
    BASIC = "Basic"
    NASCENT = "Nascent"


class ScoreBreakdown(BaseModel):
    """Score breakdown by dimension."""
    foundation: int = Field(0, ge=0, le=30, description="Foundation score (0-30)")
    data_setup: int = Field(0, ge=0, le=25, description="Data Setup score (0-25)")
    sql_assets: int = Field(0, ge=0, le=25, description="SQL Assets score (0-25)")
    optimization: int = Field(0, ge=0, le=20, description="Optimization score (0-20)")


class ScanResult(BaseModel):
    """IQ scan result for a Genie Space."""
    space_id: str
    score: int = Field(..., ge=0, le=100)
    maturity: MaturityLevel
    breakdown: ScoreBreakdown
    findings: list[str] = Field(default_factory=list)
    next_steps: list[str] = Field(default_factory=list)
    scanned_at: str  # ISO datetime string


class SpaceListItem(BaseModel):
    """Summary item for the space list."""
    space_id: str
    display_name: str
    score: int | None = None
    maturity: str | None = None
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
    critical_count: int  # score < 40
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
