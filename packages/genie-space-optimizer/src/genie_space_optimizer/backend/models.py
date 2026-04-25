"""Pydantic response models for the Genie Space Optimizer API."""

from __future__ import annotations

import math
from typing import Annotated, Any

from pydantic import AfterValidator, BaseModel, Field, model_serializer

from .. import __version__
from .utils import scrub_nan_inf


def _validate_pct_0_100(value: float | None) -> float | None:
    """Ensure score fields are on the 0–100 percentage scale.

    The wire contract: every accuracy / dimension-score field is a float in
    [0, 100]. Before this validator existed, the workbench frontend had to
    carry a defensive ``n > 1 ? n : n * 100`` rescaler because some
    backends send 0.83 and others send 83. That silently masked
    misconfigured endpoints. We now reject anything outside [0, 100] loudly
    so misroutes get caught in tests, not on a customer's screen.
    """
    if value is None:
        return None
    if not isinstance(value, (int, float)) or math.isnan(value) or math.isinf(value):
        return None
    if value < 0.0 or value > 100.0:
        raise ValueError(
            f"score field must be a percentage in [0, 100]; got {value}. "
            "Wire contract: backends MUST send 0–100 floats. If you have "
            "a 0–1 fraction, multiply by 100 at the source."
        )
    return float(value)


# Annotated alias so every score field gets the same validator without
# repeating it on every model.
ScorePct = Annotated[float | None, AfterValidator(_validate_pct_0_100)]


def _safe_model_dict(model: BaseModel) -> dict:
    """Recursively convert a model to a plain dict, bypassing Pydantic's
    type-strict serializer.  Every leaf value is passed through
    ``scrub_nan_inf`` to coerce numpy scalars and replace NaN/Inf."""
    out: dict[str, Any] = {}
    for key in model.model_fields:
        val = getattr(model, key, None)
        if isinstance(val, BaseModel):
            out[key] = _safe_model_dict(val)
        elif isinstance(val, list):
            out[key] = [
                _safe_model_dict(item) if isinstance(item, BaseModel)
                else scrub_nan_inf(item)
                for item in val
            ]
        elif isinstance(val, dict):
            out[key] = {
                dk: _safe_model_dict(dv) if isinstance(dv, BaseModel)
                else scrub_nan_inf(dv)
                for dk, dv in val.items()
            }
        else:
            out[key] = scrub_nan_inf(val)
    return out


class SafeModel(BaseModel):
    """BaseModel that converts NaN/Inf to None during serialization.

    Uses ``mode="plain"`` so pydantic_core's C serializer does NOT
    re-validate the returned dict — avoiding TypeError when numpy
    float values sit in int-typed fields.
    """

    @model_serializer(mode="plain")
    def _nan_safe_serialize(self) -> dict:
        return scrub_nan_inf(_safe_model_dict(self))


class VersionOut(BaseModel):
    version: str

    @classmethod
    def from_metadata(cls):
        return cls(version=__version__)


# ── Space Models ────────────────────────────────────────────────────────


class SpaceSummary(SafeModel):
    id: str
    name: str
    description: str = ""
    tableCount: int = 0
    lastModified: str = ""
    qualityScore: float | None = None
    accessLevel: str | None = None


class SpaceListResponse(SafeModel):
    spaces: list[SpaceSummary]
    totalCount: int
    scopedToUser: bool = False


class CheckAccessRequest(BaseModel):
    spaceIds: list[str]


class AccessLevelEntry(BaseModel):
    spaceId: str
    accessLevel: str | None = None


class TableInfo(BaseModel):
    name: str
    catalog: str
    schema_name: str
    description: str
    columnCount: int
    rowCount: int | None = None


class FunctionInfo(BaseModel):
    name: str
    catalog: str
    schema_name: str


class JoinInfo(BaseModel):
    leftTable: str
    rightTable: str
    relationshipType: str | None = None
    joinColumns: list[str] = []


class RunSummary(SafeModel):
    runId: str
    status: str
    baselineScore: ScorePct = None
    optimizedScore: ScorePct = None
    bestIteration: int | None = None
    timestamp: str


class SpaceDetail(BaseModel):
    id: str
    name: str
    description: str
    instructions: str
    sampleQuestions: list[str]
    benchmarkQuestions: list[str] = []
    tables: list[TableInfo]
    joins: list[JoinInfo] = []
    functions: list[FunctionInfo] = []
    optimizationHistory: list[RunSummary]
    hasActiveRun: bool = False


class OptimizeResponse(BaseModel):
    runId: str
    jobRunId: str
    jobUrl: str | None = None


# ── Trigger API Models ──────────────────────────────────────────────────


class TriggerRequest(BaseModel):
    space_id: str
    apply_mode: str = "genie_config"
    levers: list[int] | None = None
    deploy_target: str | None = None
    target_benchmark_count: int | None = None


class TriggerResponse(BaseModel):
    runId: str
    jobRunId: str
    jobUrl: str | None = None
    status: str


class LeverInfo(BaseModel):
    id: int
    name: str
    description: str


class RunStatusResponse(SafeModel):
    runId: str
    status: str
    spaceId: str
    startedAt: str | None = None
    completedAt: str | None = None
    baselineScore: ScorePct = None
    optimizedScore: ScorePct = None
    bestIteration: int | None = None
    convergenceReason: str | None = None


# ── Pipeline Models ─────────────────────────────────────────────────────


class PipelineStep(SafeModel):
    stepNumber: int
    name: str
    status: str
    durationSeconds: float | None = None
    summary: str | None = None
    inputs: dict | None = None
    outputs: dict | None = None


class LeverStatus(SafeModel):
    lever: int
    name: str
    status: str
    patchCount: int = 0
    scoreBefore: ScorePct = None
    scoreAfter: ScorePct = None
    scoreDelta: float | None = None
    rollbackReason: str | None = None
    patches: list[dict] = []
    iterations: list[dict] = []


class PipelineLink(BaseModel):
    label: str
    url: str
    category: str


class PipelineRun(SafeModel):
    runId: str
    spaceId: str
    spaceName: str
    status: str
    startedAt: str
    completedAt: str | None = None
    initiatedBy: str = "system"
    baselineScore: ScorePct = None
    optimizedScore: ScorePct = None
    bestIteration: int | None = None
    steps: list[PipelineStep]
    levers: list[LeverStatus] = []
    convergenceReason: str | None = None
    links: list[PipelineLink] = []
    deploymentJobStatus: str | None = None
    deploymentJobUrl: str | None = None


# ── Comparison Models ───────────────────────────────────────────────────


class DimensionScore(SafeModel):
    dimension: str
    baseline: ScorePct
    optimized: ScorePct
    delta: float


class TableDescription(BaseModel):
    tableName: str
    description: str


class SpaceConfiguration(BaseModel):
    instructions: str
    sampleQuestions: list[str]
    tableDescriptions: list[TableDescription]


class ComparisonData(SafeModel):
    runId: str
    spaceId: str
    spaceName: str
    baselineScore: ScorePct
    optimizedScore: ScorePct
    improvementPct: float
    perDimensionScores: list[DimensionScore]
    original: SpaceConfiguration
    optimized: SpaceConfiguration
    bestIteration: int | None = None


# ── Action Models ───────────────────────────────────────────────────────


class ActionResponse(BaseModel):
    status: str
    runId: str
    message: str


# ── Activity Models ─────────────────────────────────────────────────────


class ActivityItem(SafeModel):
    runId: str
    spaceId: str
    spaceName: str
    status: str
    initiatedBy: str = "system"
    baselineScore: ScorePct = None
    optimizedScore: ScorePct = None
    bestIteration: int | None = None
    timestamp: str


# ── Permission Dashboard Models (advisor-only) ───────────────────────


class SchemaPermission(BaseModel):
    catalog: str
    schema_name: str
    readGranted: bool
    writeGranted: bool
    readGrantCommand: str | None = None
    writeGrantCommand: str | None = None


class SpacePermissions(BaseModel):
    spaceId: str
    title: str
    spHasManage: bool
    schemas: list[SchemaPermission]
    status: str
    spGrantInstructions: str | None = None
    spDisplayName: str | None = None


class PermissionDashboard(BaseModel):
    spaces: list[SpacePermissions]
    spPrincipalId: str
    spPrincipalDisplayName: str | None = None
    frameworkCatalog: str
    frameworkSchema: str
    experimentBasePath: str
    jobName: str
    workspaceHost: str | None = None
    jobUrl: str | None = None


# ── Health Check Model ────────────────────────────────────────────────


class HealthStatus(BaseModel):
    healthy: bool
    catalogExists: bool = True
    schemaExists: bool
    tablesReady: bool
    tablesAccessible: bool
    volumeReady: bool = True
    jobHealthy: bool = True
    catalog: str
    schema_: str = Field(alias="schema")
    message: str | None = None
    createSchemaCommand: str | None = None
    grantCommand: str | None = None
    jobMessage: str | None = None
    spClientId: str | None = None

    model_config = {"populate_by_name": True}


# ── ASI (Judge Feedback) Models ───────────────────────────────────────


class AsiResult(SafeModel):
    questionId: str
    judge: str
    value: str
    failureType: str | None = None
    severity: str | None = None
    confidence: float | None = None
    blameSet: list[str] = []
    counterfactualFix: str | None = None
    wrongClause: str | None = None
    expectedValue: str | None = None
    actualValue: str | None = None


class AsiSummary(SafeModel):
    runId: str
    iteration: int
    totalResults: int
    passCount: int
    failCount: int
    failureTypeDistribution: dict[str, int] = {}
    blameDistribution: dict[str, int] = {}
    judgePassRates: dict[str, float] = {}
    results: list[AsiResult] = []


# ── Provenance Models ─────────────────────────────────────────────────


class ProvenanceRecord(SafeModel):
    questionId: str
    signalType: str
    judge: str
    judgeVerdict: str
    resolvedRootCause: str
    resolutionMethod: str
    blameSet: list[str] = []
    counterfactualFix: str | None = None
    clusterId: str
    proposalId: str | None = None
    patchType: str | None = None
    gateType: str | None = None
    gateResult: str | None = None


class ProvenanceSummary(SafeModel):
    runId: str
    iteration: int
    lever: int
    totalRecords: int
    clusterCount: int
    proposalCount: int
    rootCauseDistribution: dict[str, int] = {}
    gateResults: dict[str, int] = {}
    records: list[ProvenanceRecord] = []


# ── Iteration Models ──────────────────────────────────────────────────


class IterationSummary(SafeModel):
    iteration: int
    lever: int | None = None
    evalScope: str
    overallAccuracy: ScorePct = None  # type: ignore[assignment]
    # Bug #2 denominator contract — see _resolve_eval_counts in routes/runs.py.
    # totalQuestions is retained for back-compat; UI should prefer
    # evaluatedCount as the denominator of overallAccuracy.
    totalQuestions: int
    evaluatedCount: int = 0
    correctCount: int
    excludedCount: int = 0
    quarantinedCount: int = 0
    repeatabilityPct: float | None = None
    thresholdsMet: bool
    judgeScores: dict[str, float | None] = {}
    # Bug #4 — benchmark leakage observability. See optimization/leakage.py.
    leakageCountByType: dict[str, int] = {}
    firewallRejectionCountByType: dict[str, int] = {}
    secondaryMiningBlocked: int = 0
    # Bug #4 Phase 3 — structural synthesis observability.
    synthesisSlotsPersisted: int = 0
    arbiterRejectionCount: int = 0
    clusterFallbackToInstructionCount: int = 0
    synthesisArchetypeDistribution: dict[str, int] = {}


# ── Pending Reviews Models ───────────────────────────────────────────


class PendingReviewItem(BaseModel):
    questionId: str
    questionText: str = ""
    reason: str = ""
    confidenceTier: str = ""
    itemType: str = "flagged_question"


class PendingReviewsOut(BaseModel):
    flaggedQuestions: int = 0
    queuedPatches: int = 0
    totalPending: int = 0
    labelingSessionUrl: str | None = None
    items: list[PendingReviewItem] = []


# ── Suggestion Models ────────────────────────────────────────────────


class SuggestionOut(SafeModel):
    suggestionId: str
    runId: str
    spaceId: str
    iteration: int | None = None
    suggestionType: str
    title: str
    rationale: str | None = None
    definition: str | None = None
    affectedQuestions: list[str] = []
    estimatedImpact: str | None = None
    status: str
    reviewedBy: str | None = None
    reviewedAt: str | None = None


class SuggestionReviewRequest(BaseModel):
    status: str
    comment: str | None = None


# ── Iteration Detail (Transparency) Models ───────────────────────────


class QuestionResult(SafeModel):
    questionId: str
    question: str = ""
    resultCorrectness: str | None = None
    judgeVerdicts: dict[str, str] = {}
    failureTypes: list[str] = []
    matchType: str | None = None
    expectedSql: str | None = None
    generatedSql: str | None = None
    # Bug #3 — when a row was excluded from the arbiter-adjusted denominator,
    # these fields explain why. The UI renders these in the iteration
    # drill-down so items no longer silently disappear. reasonCode is the
    # stable enum (maps to copy/icons); reasonDetail is a human sentence.
    excluded: bool = False
    exclusionReasonCode: str | None = None
    exclusionReasonDetail: str | None = None


class GateResult(SafeModel):
    gateName: str
    accuracy: ScorePct = None
    totalQuestions: int | None = None
    passed: bool | None = None
    mlflowRunId: str | None = None


class ReflectionEntry(SafeModel):
    iteration: int
    agId: str
    accepted: bool
    action: str = ""
    levers: list[int] = []
    targetObjects: list[str] = []
    scoreDeltas: dict[str, float] = {}
    accuracyDelta: float = 0
    newFailures: str | None = None
    rollbackReason: str | None = None
    doNotRetry: list[str] = []
    affectedQuestionIds: list[str] = []
    fixedQuestions: list[str] = []
    stillFailing: list[str] = []
    newRegressions: list[str] = []
    reflectionText: str = ""
    refinementMode: str = ""


class QuarantinedBenchmark(SafeModel):
    """A benchmark removed by pre-eval quarantine (invalid/permission/etc)."""

    questionId: str
    question: str = ""
    reasonCode: str = "quarantined"
    reasonDetail: str = ""


class IterationDetail(SafeModel):
    iteration: int
    agId: str | None = None
    status: str
    overallAccuracy: ScorePct = None  # type: ignore[assignment]
    judgeScores: dict[str, float | None] = {}
    # Bug #2 denominator contract — prefer evaluatedCount for UI math.
    totalQuestions: int = 0
    evaluatedCount: int = 0
    correctCount: int = 0
    excludedCount: int = 0
    quarantinedCount: int = 0
    mlflowRunId: str | None = None
    modelId: str | None = None
    gates: list[GateResult] = []
    patches: list[dict] = []
    reflection: ReflectionEntry | None = None
    questions: list[QuestionResult] = []
    # Bug #3 — benchmarks removed BEFORE mlflow.genai.evaluate() (quarantine).
    # Rendered in drill-down alongside per-row runtime exclusions.
    quarantinedBenchmarks: list[QuarantinedBenchmark] = []
    clusterInfo: dict | None = None
    timestamp: str | None = None
    # Bug #4 — benchmark leakage observability. See optimization/leakage.py.
    leakageCountByType: dict[str, int] = {}
    firewallRejectionCountByType: dict[str, int] = {}
    secondaryMiningBlocked: int = 0
    # Bug #4 Phase 3 — structural synthesis observability.
    synthesisSlotsPersisted: int = 0
    arbiterRejectionCount: int = 0
    clusterFallbackToInstructionCount: int = 0
    synthesisArchetypeDistribution: dict[str, int] = {}


class IterationDetailResponse(SafeModel):
    runId: str
    spaceId: str
    baselineScore: ScorePct = None
    optimizedScore: ScorePct = None
    bestIteration: int | None = None
    totalIterations: int
    iterations: list[IterationDetail]
    flaggedQuestions: list[dict] = []
    labelingSessionUrl: str | None = None
    proactiveChanges: dict | None = None
